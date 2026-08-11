"""sql.py — build the DuckDB SQL that extracts each table (including the
sequence-layout detectors), run it, and enforce the no-fabricated-values
guard. Imports core and classification."""

from __future__ import annotations

import csv
import glob
import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime
import duckdb

from core import *
from classification import *

__all__ = ['_SENSE_OLIGO_RE', '_ANTISENSE_OLIGO_RE', '_DUPLEX_COL_RE', '_STRAND_VALUES', '_OLIGOISH_RE', '_MEASUREMENT_COL_RE', '_detect_strand_table', '_detect_oligo_strand_table', '_has_sequence_column', '_build_seq_colmap_prompt', '_llm_identify_sequence_columns', '_verify_sequence_mapping', '_build_sequence_sql_from_mapping', '_detect_implicit_strand_table', '_detect_oligo_map', '_detect_wide_two_seq', '_detect_seq_sidecar', '_build_primary_prompt', '_build_ic50_prompt', '_build_viability_prompt', '_extract_sql', 'generate_sql_query', '_run_sql_on_csv', '_COORDINATE_VALUE_FIELDS', '_SEQUENCE_VALUE_FIELDS', '_SQL_NONCOL_TOKENS', '_enforced_value_fields', '_split_top_level', '_FROM_RE', '_COMMA_RE', '_AS_RE', '_ALIAS_RE', '_aliased_select_items', '_expr_has_column_ref', '_value_is_fabricated', '_measurement_value_violations', '_execute_sql']



_SENSE_OLIGO_RE     = re.compile(r'\bsense[_\s]?oligo[_\s]?(id|num|number)?\b', re.IGNORECASE)
_ANTISENSE_OLIGO_RE = re.compile(r'\bantisense[_\s]?oligo[_\s]?(id|num|number)?\b', re.IGNORECASE)
_DUPLEX_COL_RE      = re.compile(r'\bduplex[_\s]?(id|num|number)?\b', re.IGNORECASE)


# Strand-per-row sequence tables list the sense strand on one row and the
# antisense strand on another, both keyed by duplex_id. They must be PIVOTED
# (one row per duplex with both strands) — a transformation the LLM does
# unreliably, so we detect and build it deterministically.
_STRAND_VALUES = {"s", "a", "as", "sense", "antisense",
                  "sensestrand", "antisensestrand", "sensestrand"}

# Oligo-ID values look like 'A-32335' / 'AD-18534.1': a short letter code, an
# optional hyphen, then digits. Used to spot the oligo-ID column in a
# strand-per-row table so its per-strand oligo IDs are kept, not discarded.
_OLIGOISH_RE = re.compile(r'^[A-Za-z]{1,3}-?\d{2,}')


# Columns whose presence means a table carries MEASUREMENTS, not just sequences.
# Used to keep the pure-sequence detector below from firing on a combined
# sequence+activity table (which must go through the normal extraction so its
# assay values are not dropped).
_MEASUREMENT_COL_RE = re.compile(
    r'(avg|mean|conc|dose|\bnm\b|\bpm\b|\bum\b|inhib|remain|knockdown|'
    r'viab|ic\s*_?50|ec\s*_?50|percent|\bpct\b|fold|expression|activity)',
    re.IGNORECASE,
)

def _detect_strand_table(headers: list[str], rows: list[list[str]]
                         ) -> tuple[str, str, str, str | None] | None:
    """Detect a strand-per-row sequence table. Returns
    (duplex_col, strand_col, sequence_col, oligo_col) or None, where oligo_col
    is the single column carrying each row's oligo ID (e.g. 'A-32335') or None
    when absent. The sequence column chosen is the most chemically-modified one
    available (falls back to a plain one)."""
    dup_idx = next((i for i, h in enumerate(headers) if _DUPLEX_COL_RE.search(h)), None)
    if dup_idx is None:
        return None

    # strand column: most preview values are S / A / sense / antisense
    strand_idx = None
    for i in range(len(headers)):
        vals = [row[i].strip().lower().replace(" ", "")
                for row in rows if i < len(row) and row[i].strip()]
        if vals and sum(1 for v in vals if v in _STRAND_VALUES) / len(vals) >= 0.6:
            strand_idx = i
            break
    if strand_idx is None:
        return None

    # sequence columns: most preview values look like nucleotide strings
    seq_cols = []
    for i in range(len(headers)):
        if i in (dup_idx, strand_idx):
            continue
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            seq_cols.append(i)
    if not seq_cols:
        return None

    # prefer the column whose values carry chemical modifications
    def _mod_score(i: int) -> int:
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        return sum(1 for v in vals if _is_modified_sequence(str(v).strip()))
    seq_idx = max(seq_cols, key=_mod_score)

    # oligo-ID column: a non-duplex/strand/sequence column whose values look like
    # oligo IDs ('A-32335'). The duplex_id column itself is excluded so we do not
    # mistake 'AD-...' duplex values for an oligo column.
    oligo_idx = None
    for i in range(len(headers)):
        if i in (dup_idx, strand_idx, seq_idx):
            continue
        vals = [str(row[i]).strip() for row in rows
                if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _OLIGOISH_RE.match(v)) / len(vals) >= 0.6:
            oligo_idx = i
            break

    oligo_col = headers[oligo_idx] if oligo_idx is not None else None
    return headers[dup_idx], headers[strand_idx], headers[seq_idx], oligo_col


def _detect_oligo_strand_table(headers: list[str], rows: list[list[str]]
                               ) -> tuple[str, str, str] | None:
    """Detect a strand-per-row SEQUENCE table keyed by OLIGO ID with NO duplex
    column. Returns (strand_col, oligo_col, sequence_col) or None.

    Layout (the EPO 'Modified Strand Sequences' listing, e.g. headed
    Strand | Oligo # | Position | Sequence | SEQ ID NO): one single strand per
    row, identified by an oligo ID, with a strand marker (s/as) and a sequence,
    and the duplex membership stated only in a separate table. The duplex-based
    strand detectors above all require a duplex_id column to pair sense with
    antisense, so this table slips past them and would otherwise reach the LLM,
    which does not reliably pivot it. Detecting it here lets the oligo IDs that
    activity rows pick up from an oligo-map resolve to their sequences during the
    merge (via the oligo-ID sequence repos).

    Deliberately fires only when there is NO duplex column — a table that has one
    is handled by _detect_strand_table, which can also pair the strands.
    """
    if any(_DUPLEX_COL_RE.search(h) for h in headers):
        return None

    strand_idx = None
    for i in range(len(headers)):
        vals = [row[i].strip().lower().replace(" ", "")
                for row in rows if i < len(row) and row[i].strip()]
        if vals and sum(1 for v in vals if v in _STRAND_VALUES) / len(vals) >= 0.6:
            strand_idx = i
            break
    if strand_idx is None:
        return None

    seq_cols = []
    for i in range(len(headers)):
        if i == strand_idx:
            continue
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            seq_cols.append(i)
    if not seq_cols:
        return None

    def _mod_score(i: int) -> int:
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        return sum(1 for v in vals if _is_modified_sequence(str(v).strip()))
    seq_idx = max(seq_cols, key=_mod_score)

    oligo_idx = None
    for i in range(len(headers)):
        if i in (strand_idx, seq_idx):
            continue
        vals = [str(row[i]).strip() for row in rows
                if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _OLIGOISH_RE.match(v)) / len(vals) >= 0.6:
            oligo_idx = i
            break
    if oligo_idx is None:
        return None

    return headers[strand_idx], headers[oligo_idx], headers[seq_idx]


# ---------------------------------------------------------------------------
# LLM-as-column-identifier fallback for sequence tables
#
# When a table clearly holds sequences but none of the deterministic detectors
# above could map it (unusual strand vocabulary like guide/passenger, or odd
# headers), we ask the LLM ONLY to label the columns — never to transcribe a
# sequence. A verifier checks the labels against the real values, and then the
# same kind of deterministic SQL as the detectors copies the exact bytes. This
# keeps the LLM's flexibility on the brittle part (which column is which) while
# the nucleotide strings are still copied verbatim, never written by the model.
# ---------------------------------------------------------------------------

def _has_sequence_column(headers: list[str], rows: list[list[str]]) -> bool:
    """True if any column's values are predominantly nucleotide sequences."""
    for i in range(len(headers)):
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            return True
    return False


def _build_seq_colmap_prompt(headers: list[str], rows: list[list[str]]) -> str:
    """Prompt asking the LLM to LABEL the columns of a strand/sequence table.
    It must return only column names and short strand-marker values — never a
    nucleotide sequence."""
    sample = "\n".join(" | ".join(str(c) for c in r) for r in rows[:8])
    cols   = ", ".join(f'"{h}"' for h in headers)
    return f"""You are labelling the COLUMNS of one table taken from a patent. The table
lists siRNA / oligonucleotide STRANDS and their sequences. Your job is only to
say which column is which — you must NOT copy or rewrite any sequence.

RULES:
- Output ONLY one JSON object. No prose, no markdown fences.
- Use column names EXACTLY as given below. Use null when a column is absent.
- NEVER output a nucleotide sequence. You output only column names and the
  short strand-marker values (e.g. "s", "as", "guide", "passenger").
- If the table contains assay MEASUREMENTS (inhibition %, IC50, viability, dose
  response) rather than being purely a strand/sequence listing, set
  "is_sequence_table" to false and leave the other fields null.

Columns: {cols}

First rows (cells separated by " | "):
{sample}

Return exactly this JSON shape:
{{"is_sequence_table": true_or_false,
 "duplex_col": column_name_or_null,
 "strand_col": column_name_or_null,
 "sense_values": [strand_values_meaning_the_SENSE_strand],
 "antisense_values": [strand_values_meaning_the_ANTISENSE_strand],
 "oligo_col": column_name_or_null,
 "sequence_col": column_name_or_null}}

Biology notes for the strand mapping:
- The GUIDE strand IS the antisense strand; the PASSENGER strand IS the sense
  strand. Map guide -> antisense_values, passenger -> sense_values.
- Typical markers: s / sense / passenger -> sense_values;
  as / a / antisense / guide -> antisense_values."""


def _llm_identify_sequence_columns(headers: list[str], rows: list[list[str]],
                                   trace_file: str) -> dict | None:
    """Ask the LLM to label the columns of a strand/sequence table. Returns the
    parsed mapping dict, or None."""
    raw = _llm_chat(_build_seq_colmap_prompt(headers, rows))
    log_trace(trace_file, "LLM-COLMAP RAW", raw)
    return _parse_json_object(raw)


def _verify_sequence_mapping(m: dict | None, headers: list[str],
                             rows: list[list[str]]) -> bool:
    """Sanity-check an LLM column mapping against the real values before trusting
    it. Guards against the LLM mislabelling a column or grabbing an assay table.
    Requires a strand column (this fallback handles strand-per-row tables) and at
    least one ID column (oligo or duplex) to join on."""
    if not m or not m.get("is_sequence_table"):
        return False
    hset = set(headers)

    def col_vals(name):
        i = headers.index(name)
        return [row[i] for row in rows if i < len(row) and str(row[i]).strip()]

    seq_col = m.get("sequence_col")
    if seq_col not in hset:
        return False
    sv = col_vals(seq_col)
    if not sv or sum(1 for v in sv if _looks_like_sequence(v)) / len(sv) < 0.6:
        return False                                  # named column isn't sequences

    strand_col = m.get("strand_col")
    if strand_col not in hset:
        return False
    declared = {str(x).strip().lower()
                for x in (m.get("sense_values") or []) + (m.get("antisense_values") or [])}
    stv = [str(v).strip().lower() for v in col_vals(strand_col)]
    if not declared or not stv or sum(1 for v in stv if v in declared) / len(stv) < 0.6:
        return False                                  # strand values don't match the data

    for key in ("oligo_col", "duplex_col"):
        if m.get(key) is not None and m[key] not in hset:
            return False
    if m.get("oligo_col") is None and m.get("duplex_col") is None:
        return False                                  # nothing to join the sequence onto
    return True


def _build_sequence_sql_from_mapping(m: dict) -> str:
    """Build the deterministic strand-per-row pivot SQL from a verified mapping.
    Routes each row's oligo ID and sequence to the correct strand field by the
    LLM-identified strand values; the sequence is copied verbatim from the cell.
    """
    seq_c, strand_c = m["sequence_col"], m["strand_col"]
    oligo_c, dup_c  = m.get("oligo_col"), m.get("duplex_col")

    def _in_list(vals):
        cleaned = sorted({str(v).strip().lower() for v in vals if str(v).strip()})
        if not cleaned:
            return "('\\x00')"                        # matches nothing
        return "(" + ", ".join("'" + v.replace("'", "''") + "'" for v in cleaned) + ")"

    sense_in = _in_list(m.get("sense_values") or [])
    anti_in  = _in_list(m.get("antisense_values") or [])
    norm     = f'lower(trim(CAST("{strand_c}" AS VARCHAR)))'
    dup_sql  = f'"{dup_c}"' if dup_c else "NULL"
    s_oligo  = f'CASE WHEN {norm} IN {sense_in} THEN "{oligo_c}" END' if oligo_c else "NULL"
    a_oligo  = f'CASE WHEN {norm} IN {anti_in}  THEN "{oligo_c}" END' if oligo_c else "NULL"
    return f"""
SELECT
    {dup_sql} AS duplex_id,
    CASE WHEN {norm} IN {sense_in} THEN "{seq_c}" END AS sense_sequence,
    CASE WHEN {norm} IN {anti_in}  THEN "{seq_c}" END AS antisense_sequence,
    {s_oligo} AS sense_oligo_id,
    {a_oligo} AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS replicate,
    NULL AS transfection_method,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM secondary_table
WHERE "{seq_c}" IS NOT NULL
""".strip()


def _detect_implicit_strand_table(headers: list[str], rows: list[list[str]]
                                  ) -> tuple[str, str, str | None, str | None] | None:
    """Detect a strand-per-row sequence table that has NO explicit strand column.

    This is the common 'Modified Single Strands and Duplex Sequences' layout:
    each duplex spans two consecutive rows (sense then antisense), the duplex_id
    is printed only on the first (sense) row, and the second (antisense) row
    leaves duplex_id blank. There is one sequence column and usually one oligo-ID
    column; the strand is implied by row order, not labelled.

    Returns (duplex_col, sequence_col, oligo_col|None, order_col|None) or None.
    order_col is a strictly-increasing numeric column (e.g. seq_id_no) used to
    recover the original row order inside the pivot; None falls back to scan order.

    Guards against false positives: bails on tables carrying measurement columns
    (those are activity tables) and only fires when the duplex_id column is
    SPARSE (blank on roughly half the rows) while the sequence column is dense —
    the signature of lead-row-only duplex labelling.
    """
    # Activity tables are never sequence tables — never pivot them.
    if any(_MEASUREMENT_COL_RE.search(h) for h in headers):
        return None

    dup_idx = next((i for i, h in enumerate(headers) if _DUPLEX_COL_RE.search(h)), None)
    if dup_idx is None:
        return None

    n = len(rows)
    if n < 2:
        return None

    # sequence column: most preview values look like nucleotide strings
    seq_idx = None
    for i in range(len(headers)):
        if i == dup_idx:
            continue
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            seq_idx = i
            break
    if seq_idx is None:
        return None

    # duplex column must be SPARSE (lead-row labelling) while the sequence column
    # is DENSE — this is what separates this layout from a one-row-per-duplex
    # table (handled by the wide detector) or an explicit strand table.
    dup_filled = sum(1 for row in rows if dup_idx < len(row) and str(row[dup_idx]).strip())
    seq_filled = sum(1 for row in rows if seq_idx < len(row) and str(row[seq_idx]).strip())
    if not (seq_filled >= 0.8 * n and 1 <= dup_filled <= 0.6 * n and dup_filled < seq_filled):
        return None

    # oligo-ID column: oligo-ish values, not the duplex or sequence column
    oligo_idx = None
    for i in range(len(headers)):
        if i in (dup_idx, seq_idx):
            continue
        vals = [str(row[i]).strip() for row in rows
                if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _OLIGOISH_RE.match(v)) / len(vals) >= 0.6:
            oligo_idx = i
            break

    # order column: a strictly-increasing integer column (e.g. seq_id_no) that
    # lets us reconstruct row order deterministically inside the pivot.
    order_idx = None
    for i in range(len(headers)):
        if i in (dup_idx, seq_idx):
            continue
        nums = []
        for row in rows:
            if i < len(row) and str(row[i]).strip():
                try:
                    nums.append(int(float(str(row[i]).strip())))
                except ValueError:
                    nums = []
                    break
        if len(nums) >= max(2, int(0.8 * n)) and all(
                nums[k] < nums[k + 1] for k in range(len(nums) - 1)):
            order_idx = i
            break

    oligo_col = headers[oligo_idx] if oligo_idx is not None else None
    order_col = headers[order_idx] if order_idx is not None else None
    return headers[dup_idx], headers[seq_idx], oligo_col, order_col


def _detect_oligo_map(headers: list[str]) -> tuple[str | None, str | None, str | None]:
    duplex_col    = next((h for h in headers if _DUPLEX_COL_RE.search(h)), None)
    sense_col     = next((h for h in headers if _SENSE_OLIGO_RE.search(h)), None)
    antisense_col = next((h for h in headers if _ANTISENSE_OLIGO_RE.search(h)), None)
    if duplex_col and sense_col and antisense_col:
        return duplex_col, sense_col, antisense_col
    return None, None, None

def _detect_wide_two_seq(headers: list[str], rows: list[list[str]]):
    """Detect a one-row-per-duplex sequence table whose sense/antisense sequence
    columns may be GENERICALLY named (e.g. `sequence` / `sequence_5` /
    `sequence_5_to_3`) rather than `sense_sequence` / `antisense_sequence`.

    Returns (duplex_col, sense_seq_col, antisense_seq_col,
             sense_oligo_col|None, antisense_oligo_col|None) or None.

    Handling this deterministically (rather than via the LLM) makes the bulk of a
    patent's sequences extract reproducibly and WITHOUT an API call — which both
    removes a large source of run-to-run variance and cuts the request volume
    that drives rate-limiting at scale. sense vs antisense is decided
    structurally: a column header containing 'antisense' marks the antisense
    side; the sequence-like column on the sense side becomes sense_sequence and
    the one on the antisense side becomes antisense_sequence. With no such marker
    we fall back to source order (first sequence column = sense)."""
    dup_idx = next((i for i, h in enumerate(headers) if _DUPLEX_COL_RE.search(h)), None)
    if dup_idx is None:
        return None

    # Bail out if the table also carries measurement columns — it is a combined
    # table and must keep its assay values (handled by the normal path).
    if any(_MEASUREMENT_COL_RE.search(h) for h in headers):
        return None

    seq_idxs = []
    for i, h in enumerate(headers):
        if i == dup_idx:
            continue
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            seq_idxs.append(i)
    if len(seq_idxs) != 2:
        return None

    def _is_anti(h: str) -> bool:
        return "antisense" in h.lower()

    def _is_sense(h: str) -> bool:
        return re.search(r'(?<!anti)sense', h.lower()) is not None

    s_idx = a_idx = None
    # 1) decide from markers on the sequence columns themselves
    for i in seq_idxs:
        if _is_anti(headers[i]):
            a_idx = i
        elif _is_sense(headers[i]):
            s_idx = i
    # 2) otherwise split on the first 'antisense' column anywhere in the header
    if s_idx is None or a_idx is None:
        anti_positions = [i for i, h in enumerate(headers) if _is_anti(h)]
        if anti_positions:
            boundary = min(anti_positions)
            before = [i for i in seq_idxs if i < boundary]
            after  = [i for i in seq_idxs if i >= boundary]
            if len(before) == 1 and len(after) == 1:
                s_idx, a_idx = before[0], after[0]
    # 3) last resort: source order
    if s_idx is None or a_idx is None:
        s_idx, a_idx = sorted(seq_idxs)

    _OLIGOISH_RE = re.compile(r'^[A-Za-z]{1,3}-?\d{2,}')
    def _col_is_oligoish(i: int) -> bool:
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if not vals:
            return False
        return sum(1 for v in vals if _OLIGOISH_RE.match(str(v).strip())) / len(vals) >= 0.6

    def _find_oligo(want_anti: bool):
        # Prefer an explicit *oligo* column; otherwise accept a *_name column on
        # the correct strand side whose values look like oligo IDs (e.g. the
        # `sense_name`/`antisense_name` columns that carry A-115161.1). Requiring
        # ID-like values avoids grabbing a gene/target name column.
        cands: list[tuple[int, int]] = []
        for i, h in enumerate(headers):
            hl = h.lower()
            side_ok = _is_anti(hl) if want_anti else _is_sense(hl)
            if not side_ok:
                continue
            if "oligo" in hl and ("name" in hl or "id" in hl):
                cands.append((0, i))
            elif ("name" in hl or "oligo" in hl) and _col_is_oligoish(i):
                cands.append((1, i))
        if cands:
            cands.sort()
            return headers[cands[0][1]]
        return None

    return (headers[dup_idx], headers[s_idx], headers[a_idx],
            _find_oligo(False), _find_oligo(True))


def _detect_seq_sidecar(headers: list[str], rows: list[list[str]]):
    """For a COMBINED table (sequences AND activity in the same table, e.g. a
    table with `duplex_id, sense_strand, antisense_strand, pct_mrna_remaining_*`),
    detect the sense/antisense sequence columns (and oligo-id columns) so the
    sequences can be emitted as seq-only rows and merged onto the activity rows
    by duplex_id.

    Why this is needed: when the LLM unpivots a combined table's dose columns
    into activity rows it routinely writes `NULL AS sense_sequence` in every
    branch, silently dropping sequences that are sitting right there in the
    table. _detect_wide_two_seq deliberately bails on combined tables (it must
    not clobber the assay values), so nothing rescues those sequences. This
    detector runs as a SIDECAR alongside the normal activity extraction: it does
    not touch the activity rows, it only emits the duplex→sequence mapping.

    Returns (duplex_col, sense_seq_col, antisense_seq_col,
             sense_oligo_col|None, antisense_oligo_col|None) or None. Fires only
    when exactly two sequence-valued columns are present, so pure-activity tables
    (no sequence columns) are unaffected."""
    dup_idx = next((i for i, h in enumerate(headers) if _DUPLEX_COL_RE.search(h)), None)
    if dup_idx is None:
        return None

    seq_idxs = []
    for i, h in enumerate(headers):
        if i == dup_idx:
            continue
        vals = [row[i] for row in rows if i < len(row) and str(row[i]).strip()]
        if vals and sum(1 for v in vals if _looks_like_sequence(v)) / len(vals) >= 0.6:
            seq_idxs.append(i)
    if len(seq_idxs) != 2:
        return None

    def _is_anti(h: str) -> bool:
        hl = h.lower()
        return ("antisense" in hl) or (re.search(r'(^|_)as(_|$)', hl) is not None)

    def _is_sense(h: str) -> bool:
        hl = h.lower()
        return (re.search(r'(?<!anti)sense', hl) is not None) and not _is_anti(h)

    s_idx = a_idx = None
    for i in seq_idxs:
        if _is_anti(headers[i]):
            a_idx = i
        elif _is_sense(headers[i]):
            s_idx = i
    if s_idx is None or a_idx is None:
        anti_pos = [i for i, h in enumerate(headers) if _is_anti(h)]
        if anti_pos:
            b = min(anti_pos)
            before = [i for i in seq_idxs if i < b]
            after  = [i for i in seq_idxs if i >= b]
            if len(before) == 1 and len(after) == 1:
                s_idx, a_idx = before[0], after[0]
    if s_idx is None or a_idx is None:
        s_idx, a_idx = sorted(seq_idxs)

    def _find_oligo(want_anti: bool):
        # An oligo-id column on the correct strand side: has 'id'/'oligo'/'name'
        # but is NOT a SEQ-ID column (seq_id / seq_id_no) and is not a sequence
        # column itself. Catches sense_id / antisense_id / as_id.
        for i, h in enumerate(headers):
            if i in (dup_idx, s_idx, a_idx):
                continue
            hl = h.lower()
            if "seq" in hl:
                continue
            if not ("id" in hl or "oligo" in hl or "name" in hl):
                continue
            if want_anti and _is_anti(h):
                return headers[i]
            if (not want_anti) and _is_sense(h):
                return headers[i]
        return None

    return (headers[dup_idx], headers[s_idx], headers[a_idx],
            _find_oligo(False), _find_oligo(True))


# ---------------------------------------------------------------------------
# LLM prompts — one per table type
# ---------------------------------------------------------------------------

def _build_primary_prompt(csv_headers: list[str], context_text: str,
                           csv_preview: str, scale_hint: str | None = None) -> str:
    fields_str  = ",\n  ".join([f"NULL AS {f}" for f in PRIMARY_FIELDS])
    headers_str = ", ".join(csv_headers)
    n_fields    = len(PRIMARY_FIELDS)

    # A verified, data-derived directive about the value scale, so the model does
    # not have to guess whether "remaining" columns are fractions or percentages.
    if scale_hint == "fraction":
        scale_directive = (
            "\n*** VERIFIED SCALE: the measurement values in THIS table are "
            "FRACTIONS on a 0-1 scale (e.g. 0.03 = 3% remaining). For any "
            "'remaining' column you MUST use (1 - value) * 100 — do NOT use "
            "100 - value. ***\n"
        )
    elif scale_hint == "percent":
        scale_directive = (
            "\n*** VERIFIED SCALE: the measurement values in THIS table are "
            "PERCENTAGES on a 0-100 scale. For any 'remaining' column use "
            "100 - value. ***\n"
        )
    elif scale_hint == "large":
        scale_directive = (
            "\n*** NOTE: the measurement values in THIS table exceed 100 by a "
            "wide margin (relative-expression ratios). Do NOT assume a "
            "'percent remaining' meaning or apply 100 - value blindly — follow "
            "the context's definition of the metric; if it is not a knockdown "
            "percentage, leave inhibition_percent NULL. ***\n"
        )
    else:
        scale_directive = ""

    return f"""\
You are an expert data engineer working with siRNA patent data.
Write a single DuckDB SQL SELECT that extracts knockdown-activity data \
from `secondary_table` into the fixed schema below.
{scale_directive}
=======================================================================
AVAILABLE COLUMNS IN `secondary_table`:
-----------------------------------------------------------------------
{headers_str}

=======================================================================
CONTEXT:
-----------------------------------------------------------------------
{context_text[:4000]}

=======================================================================
DATA PREVIEW (first 15 rows):
-----------------------------------------------------------------------
{csv_preview}

=======================================================================
REQUIRED OUTPUT COLUMNS (ALL {n_fields} — NULL for anything unknown):
-----------------------------------------------------------------------
{fields_str}

=======================================================================
EXTRACTION RULES:
1. Always query from `secondary_table`.
2. Output EXACTLY these {n_fields} columns in this order.
3. duplex_id: map the Duplex/compound ID column. Output EXACTLY as it appears.
   If no duplex_id-like column exists output __SKIP__.
4. inhibition_percent (a PERCENT on a 0-100 scale):
   - *** NEVER derive inhibition_percent from a CONCENTRATION/POTENCY column —
     IC50, IC 50, EC50, CC50, GI50, KD, Ki, or any column carrying a dose in
     nM/µM/pM. A concentration is NOT a percentage; there is NO formula that
     converts an IC50 into a knockdown %. Do NOT write things like
     (100 - ic50) * 100 or 100 - ic50. ***
   - *** If the table's ONLY measurement is an IC50/EC50/concentration (i.e.
     there is NO inhibition %, knockdown %, "% remaining", or relative-to-control
     column), then inhibition_percent MUST be NULL for every row. The IC50 is
     captured by a separate IC50 extraction — never fabricate a knockdown % from
     it. ***
   - Direct knockdown/inhibition % column → use directly.
   - "remaining" column (message/mRNA/transcript remaining, or relative to a
     control) → CONVERT to inhibition. CHOOSE THE SCALE by looking at the DATA
     PREVIEW values and the context wording:
       * PERCENT remaining — values run roughly 0-100 (e.g. 82.0, 11.2):
             inhibition_percent = 100 - value
       * FRACTION remaining — values run roughly 0-1.5 (e.g. 0.03, 0.47), or the
         context says "fraction of message remaining":
             inhibition_percent = (1 - value) * 100
     Rule of thumb: if the remaining values in the preview are all <= ~1.5 they
     are FRACTIONS; if some exceed ~1.5 they are PERCENTAGES. Wrap the source
     column in TRY_CAST(col AS DOUBLE) before the arithmetic.
   - Multiple DOSE columns (wide format, different concentrations in the column
     names e.g. 10nM/1nM/500pM, or conc_0_1_nM meaning 0.1 nM) → UNION ALL
     unpivot; extract the numeric dose from the column name into dose_nM; pair
     each value with its SD column; apply the same scale conversion to each.
   - Pure sequence table → NULL.
5. value_sd: map from any stdev/SD column. NULL if absent (e.g. for single
   replicate measurements, which have no SD).
6. replicate — single repeated measurements at ONE dose:
   - If the table gives several SEPARATE measurement columns taken at the SAME
     dose (i.e. individual replicates, NOT an averaged value), e.g. columns
     labelled rep1/rep2/rep3, exp1/exp2, run_1/run_2, #1/#2/#3, I/II/III, or
     two/three otherwise-identical unnamed % columns — UNION ALL unpivot so that
     EACH replicate becomes its OWN row. Put the replicate label ('1','2','3' or
     'I','II', etc.) in the replicate column. dose_nM is the SAME for all of them.
     Do NOT average them and do NOT keep only one.
   - If there is a single value per dose (averaged or otherwise), replicate = NULL.
   - CAUTION: replicate columns repeat the SAME dose; dose columns carry DIFFERENT
     concentrations in their names. Do not treat one as the other.
7. dose_nM: dose in nM. Hardcode from column name or context. µM×1000, pM/1000.
8. cell_line: hardcode from context if not a column. NULL if unknown.
9. sense_sequence / antisense_sequence:
   - Prefer chemically modified over plain nucleotide sequences.
   - Pivot strand-per-row tables via GROUP BY + MAX(CASE WHEN ...).
   - The sequence columns may be GENERICALLY named (e.g. `sequence`, `sequence_5`,
     `sequence_5_to_3`) rather than `sense_sequence`/`antisense_sequence`. When two
     sequence-like columns exist, the one on the SENSE side (the column adjacent to
     a `sense*`/`sense_oligo_name` column, or the FIRST of the two) is the sense
     strand; the one on the ANTISENSE side (adjacent to `antisense*`, or the SECOND)
     is the antisense strand. Use the neighbouring `sense`/`antisense` column names
     to decide which is which — do NOT leave them NULL just because the headers are
     generic.
   - When the strand is implicit (a strand-per-row table with NO explicit
     sense/antisense label column, e.g. sense on the odd row and antisense on the
     even row of each duplex, distinguishable by a position/`_as` suffix), pivot by
     duplex_id and assign by that row-order/position pattern.
   - COMBINED TABLES: if the table contains sequence columns (e.g. `sense_strand`,
     `antisense_strand`, `sense_sequence`) AND activity/dose columns, you MUST
     carry the sequence and oligo-id columns into EVERY unpivoted row — populate
     sense_sequence/antisense_sequence/sense_oligo_id/antisense_oligo_id from those
     columns rather than writing NULL. Map `sense_strand`→sense_sequence,
     `antisense_strand`→antisense_sequence, `sense_id`→sense_oligo_id,
     `antisense_id`/`as_id`→antisense_oligo_id.
   - Single-letter values are strand labels, NOT sequences → NULL.
   - NULL if not present.
10. sense_oligo_id / antisense_oligo_id:
   - Only complex alphanumeric IDs (e.g. A-32745). NOT plain integers, NOT duplex IDs.
   - NULL if absent.
11. target_gene_name: infer from context. NULL if unknown.
12. patent_id: always NULL.
13. source_file: always NULL.
14. transfection_method: always NULL (filled later from the context text).
15. Return ONLY valid SQL (SELECT or CTE). No markdown, no explanation.
16. ARITHMETIC TYPE SAFETY: always wrap arithmetic columns in TRY_CAST(col AS DOUBLE).
    Correct: 100.0 - TRY_CAST(pct_remaining AS DOUBLE)
    Wrong:   100 - pct_remaining
17. STRING/NUMERIC TYPE SAFETY:
    - Do NOT hand-parse scientific notation. TRY_CAST(col AS DOUBLE) already parses
      values like '1.5e-3' or '2E-9' directly. Never split on 'e' with
      SUBSTR/POSITION to rebuild a number.
    - Numeric-looking columns are auto-typed as DOUBLE, so a string function
      (POSITION, SUBSTR, LIKE, REGEXP_*, SPLIT_PART, …) applied to them FAILS.
      If you must use a string function on a column, wrap it as
      CAST(col AS VARCHAR) first. Correct: CAST(ic50 AS VARCHAR) LIKE '%nM'.

EXAMPLE — three single inhibition measurements at one dose (10 nM), HeLa:
  Columns: duplex_id | inhib_rep1 | inhib_rep2 | inhib_rep3
SELECT duplex_id AS duplex_id, NULL AS sense_sequence, NULL AS antisense_sequence,
  NULL AS sense_oligo_id, NULL AS antisense_oligo_id, 'HeLa' AS cell_line,
  10.0 AS dose_nM, TRY_CAST(inhib_rep1 AS DOUBLE) AS inhibition_percent,
  NULL AS value_sd, '1' AS replicate, NULL AS transfection_method,
  'TTR' AS target_gene_name, NULL AS patent_id, NULL AS source_file
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, NULL, NULL, NULL, NULL, 'HeLa', 10.0,
  TRY_CAST(inhib_rep2 AS DOUBLE), NULL, '2', NULL, 'TTR', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, NULL, NULL, NULL, NULL, 'HeLa', 10.0,
  TRY_CAST(inhib_rep3 AS DOUBLE), NULL, '3', NULL, 'TTR', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL;
"""


def _build_ic50_prompt(csv_headers: list[str], context_text: str,
                        csv_preview: str) -> str:
    fields_str  = ",\n  ".join([f"NULL AS {f}" for f in IC50_FIELDS])
    headers_str = ", ".join(csv_headers)
    n_fields    = len(IC50_FIELDS)

    return f"""\
You are an expert data engineer working with siRNA patent data.
Write a single DuckDB SQL SELECT that extracts IC50 data \
from `secondary_table` into the fixed schema below.
Produce ONE ROW PER (duplex_id, cell_line, timepoint_hrs, replicate).

=======================================================================
AVAILABLE COLUMNS IN `secondary_table`:
-----------------------------------------------------------------------
{headers_str}

=======================================================================
CONTEXT:
-----------------------------------------------------------------------
{context_text[:4000]}

=======================================================================
DATA PREVIEW (first 15 rows):
-----------------------------------------------------------------------
{csv_preview}

=======================================================================
REQUIRED OUTPUT COLUMNS (ALL {n_fields} — NULL for anything unknown):
-----------------------------------------------------------------------
{fields_str}

=======================================================================
EXTRACTION RULES:
1. Always query from `secondary_table`.
2. Output EXACTLY these {n_fields} columns in this order.
3. duplex_id: the Duplex/compound ID column. If absent output __SKIP__.
4. cell_line:
   - If there is a cell-line column, use it.
   - If the context states a single cell line (e.g. 'Hep3B'), hardcode it.
   - If the IC50 values are spread across MULTIPLE COLUMNS that are each a
     different CELL LINE (e.g. columns named 'mel202', 'omm1_3', 'a549', or
     'Hep3B'/'PCH'), UNPIVOT via UNION ALL so each column becomes its own row
     carrying that cell_line and that column's IC50 value.
   - NULL if unknown.
5. timepoint_hrs: assay timepoint in hours as a number. EVERY IC50 row MUST
   carry the timepoint of the column it came from.
   - Read it from the table's own structure FIRST: many IC50 tables group the
     IC50 columns under a spanning timepoint header (e.g. a '24 hrs' header over
     IC50 I / II / weighted, and a '120 hrs' header over the next three). When
     the table is flattened to CSV these become repeated/suffixed columns (e.g.
     ic50_i_nM, ic50_ii_nM, ic50_weighted_nM, then ic50_i_nM_4, ic50_ii_nM_5,
     ic50_weighted_nM_6). Look at the data preview to map each IC50 column to its
     timepoint group, and assign that timepoint to its unpivoted rows.
   - Otherwise derive it from context NOTEs (e.g. '24 hrs' → 24.0, '120 hrs' → 120.0).
   - If a single timepoint applies to the whole table, hardcode it on every row.
   - NULL only if no timepoint is stated anywhere.
6. replicate: one of 'I', 'II', 'weighted', or NULL.
   - If the table has multiple IC50 columns representing replicates (e.g.
     ic50_i_nM, ic50_ii_nM, ic50_weighted_nM), UNPIVOT using UNION ALL so
     each replicate becomes its own row with the correct replicate label.
   - If the table has columns for multiple timepoints (e.g. 24h and 120h),
     each timepoint gets its own set of rows in the UNION ALL, with the
     correct timepoint_hrs value.
   - If only one IC50 column exists, set replicate = NULL.
7. ic50_nM: the IC50 value CONVERTED TO nM, as DOUBLE. NULL if not present.
   - Detect the source unit from the column header (e.g. 'IC50 (µM)') or context,
     then convert to nM with these factors:
       M  → ×1e9      mM → ×1e6      µM/uM → ×1e3
       nM → ×1        pM → ÷1e3      fM   → ÷1e6
   - ALWAYS wrap the source column in TRY_CAST(col AS DOUBLE) before multiplying.
   - Strip qualifier characters BEFORE casting: values like '>50', '<0.1', '~5'
     or '50 nM' must become numbers, e.g.
       TRY_CAST(REGEXP_REPLACE(CAST(col AS VARCHAR), '[^0-9.eE+-]', '', 'g') AS DOUBLE).
   - MIXED UNITS IN ONE COLUMN: if the unit is embedded PER CELL (e.g. some rows
     read '50 nM' and others '1.2 µM' in the SAME column), you MUST convert each
     value by ITS OWN embedded unit, not record the unit while leaving the value
     unscaled. Example:
       CASE WHEN CAST(col AS VARCHAR) LIKE '%µM' OR CAST(col AS VARCHAR) LIKE '%uM'
              THEN TRY_CAST(REGEXP_REPLACE(CAST(col AS VARCHAR),'[^0-9.eE+-]','','g') AS DOUBLE) * 1e3
            WHEN CAST(col AS VARCHAR) LIKE '%pM'
              THEN TRY_CAST(REGEXP_REPLACE(CAST(col AS VARCHAR),'[^0-9.eE+-]','','g') AS DOUBLE) / 1e3
            ELSE TRY_CAST(REGEXP_REPLACE(CAST(col AS VARCHAR),'[^0-9.eE+-]','','g') AS DOUBLE)
       END AS ic50_nM
8. ic50_unit_original: the unit EXACTLY as stated in the source table/context,
   as a string ('nM', 'µM', 'pM', 'M', etc.). This is provenance for auditing the
   conversion above — record it even when it is already 'nM'. NULL only if no unit
   is stated anywhere.
9. target_gene_name: infer from context. NULL if unknown.
10. patent_id: always NULL.
11. source_file: always NULL.
12. transfection_method: always NULL (filled later from the context text).
13. Return ONLY valid SQL. No markdown, no explanation.
14. ARITHMETIC TYPE SAFETY: always TRY_CAST arithmetic columns to DOUBLE.
15. STRING/NUMERIC TYPE SAFETY: do NOT hand-parse scientific notation —
    TRY_CAST(col AS DOUBLE) parses '1.5e-3' / '2E-9' directly (never split on 'e'
    with SUBSTR/POSITION). A numeric-looking column is auto-typed as DOUBLE, so
    applying a string function (POSITION/SUBSTR/LIKE/REGEXP_*) to it FAILS — wrap
    it as CAST(col AS VARCHAR) first if you truly need one.

EXAMPLE — table with 6 IC50 columns for two timepoints × three replicates,
values already in nM (column order must match the schema exactly):
  Columns: duplex_id | ic50_i_nM | ic50_ii_nM | ic50_weighted_nM |
           ic50_i_nM_4 | ic50_ii_nM_5 | ic50_weighted_nM_6
  Context notes: Hep3B IC50 — 24 hrs / 120 hrs

SELECT duplex_id AS duplex_id, 'Hep3B' AS cell_line,
  24.0 AS timepoint_hrs, 'I' AS replicate,
  TRY_CAST(ic50_i_nM AS DOUBLE) AS ic50_nM, 'nM' AS ic50_unit_original,
  NULL AS transfection_method, 'ANGPTL3' AS target_gene_name,
  NULL AS patent_id, NULL AS source_file
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'Hep3B', 24.0, 'II',
  TRY_CAST(ic50_ii_nM AS DOUBLE), 'nM', NULL, 'ANGPTL3', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'Hep3B', 24.0, 'weighted',
  TRY_CAST(ic50_weighted_nM AS DOUBLE), 'nM', NULL, 'ANGPTL3', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'Hep3B', 120.0, 'I',
  TRY_CAST(ic50_i_nM_4 AS DOUBLE), 'nM', NULL, 'ANGPTL3', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'Hep3B', 120.0, 'II',
  TRY_CAST(ic50_ii_nM_5 AS DOUBLE), 'nM', NULL, 'ANGPTL3', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'Hep3B', 120.0, 'weighted',
  TRY_CAST(ic50_weighted_nM_6 AS DOUBLE), 'nM', NULL, 'ANGPTL3', NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL;

EXAMPLE — single IC50 column reported in µM (must convert to nM):
  Column: ic50_uM   →   TRY_CAST(ic50_uM AS DOUBLE) * 1e3 AS ic50_nM,
                        'µM' AS ic50_unit_original

EXAMPLE — IC50 columns that are each a CELL LINE, values in pM:
  Columns: duplex_id | mel202 | omm1_3 | a549
  Context: "Table 18b: IC 50 (pM) in 3 cell lines"  (so unit = pM → ÷1000 → nM)
SELECT duplex_id AS duplex_id, 'mel202' AS cell_line, NULL AS timepoint_hrs,
  NULL AS replicate, TRY_CAST(mel202 AS DOUBLE) / 1e3 AS ic50_nM,
  'pM' AS ic50_unit_original, NULL AS transfection_method,
  'GNAQ' AS target_gene_name, NULL AS patent_id, NULL AS source_file
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'omm1_3', NULL, NULL, TRY_CAST(omm1_3 AS DOUBLE)/1e3, 'pM',
  NULL, 'GNAQ', NULL, NULL FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'a549', NULL, NULL, TRY_CAST(a549 AS DOUBLE)/1e3, 'pM',
  NULL, 'GNAQ', NULL, NULL FROM secondary_table WHERE duplex_id IS NOT NULL;
"""


def _build_viability_prompt(csv_headers: list[str], context_text: str,
                              csv_preview: str) -> str:
    # Columns the SQL must emit. viability_basis / viability_relative_to /
    # transfection_method are assay properties stamped on the rows afterwards
    # (the LLM never produces them), so they are excluded from the prompt.
    _sql_fields = [f for f in VIABILITY_FIELDS
                   if f not in ("viability_basis", "viability_relative_to",
                                "transfection_method")]
    fields_str  = ",\n  ".join([f"NULL AS {f}" for f in _sql_fields])
    headers_str = ", ".join(csv_headers)
    n_fields    = len(_sql_fields)

    return f"""\
You are an expert data engineer working with siRNA patent data.
Write a single DuckDB SQL SELECT that extracts cell-viability data \
from `secondary_table` into the fixed schema below.
Produce ONE ROW PER (duplex_id, cell_line, day, dose_nM).

=======================================================================
AVAILABLE COLUMNS IN `secondary_table`:
-----------------------------------------------------------------------
{headers_str}

=======================================================================
CONTEXT:
-----------------------------------------------------------------------
{context_text[:4000]}

=======================================================================
DATA PREVIEW (first 15 rows):
-----------------------------------------------------------------------
{csv_preview}

=======================================================================
REQUIRED OUTPUT COLUMNS (ALL {n_fields} — NULL for anything unknown):
-----------------------------------------------------------------------
{fields_str}

=======================================================================
EXTRACTION RULES:
1. Always query from `secondary_table`.
2. Output EXACTLY these {n_fields} columns in this order.
3. duplex_id: the Duplex/compound ID column. If absent output __SKIP__.
4. cell_line:
   - If a COLUMN holds the cell line (repeated cell-line names such as
     'OMM-1.3', 'MEL202', 'MEL-285', 'HeLa', 'A549'), SELECT that column AS
     cell_line. It varies per row, so DO NOT hardcode it and DO NOT filter rows
     by it — every cell line is then captured automatically.
   - Otherwise, if the context states a single cell line, hardcode it.
   - NULL if unknown.
5. day: assay day as integer (e.g. 3, 5, 7).
   - If the value-column NAME encodes the day (e.g. day_3_1nm → 3,
     day_7_0_001nm → 7), parse it from the name.
   - Otherwise derive from the context NOTEs. NULL if not stated.
6. dose_nM: siRNA dose in nM, parsed from the value-column NAME:
   avg_10_nM → 10.0, avg_500_pM → 0.5, avg_100_pM → 0.1, avg_50_pM → 0.05,
   day_3_1nm → 1.0, day_5_0_01nm → 0.01, day_7_0_001nm → 0.001.
   UNPIVOT wide tables with UNION ALL — ONE SELECT per value column.
   Pair each value column with its matching stdev/sd column WHEN one exists
   (a viability matrix may have no SD columns — then viability_sd is NULL).
7. viability_value: the viability value, stored EXACTLY as reported (a percent
   or a normalised ratio — do NOT rescale it). ALWAYS TRY_CAST(col AS DOUBLE).
8. viability_sd: matching SD column. TRY_CAST(col AS DOUBLE). NULL if absent.
9. target_gene_name: infer from context or a 'target' column. NULL if unknown.
10. patent_id: always NULL.
11. source_file: always NULL.
12. transfection_method: do NOT output it (filled later from the context text).
13. Return ONLY valid SQL. No markdown, no explanation.
14. ARITHMETIC TYPE SAFETY: always TRY_CAST arithmetic columns to DOUBLE.
15. Wide layouts come in two shapes — use whichever matches the columns:
    (a) cell_line is a COLUMN and day & dose are encoded in the value-column
        NAMES (e.g. day_3_1nm … day_7_0_001nm). Emit one SELECT per value
        column, carrying duplex_id and the cell_line column, and parse day and
        dose from each column name. (EXAMPLE A.)
    (b) the table stacks groups of rows for different cell_line/day, labelled
        in order by the context NOTEs, with dose columns shared across groups.
        Emit one SELECT per (cell_line × day × dose), hardcoding cell_line and
        day from the NOTE labels. (EXAMPLE B.)

EXAMPLE A — cell_line is a column; day & dose live in the value-column names:
  Columns: cell_line | duplex_id | day_3_1nm | day_3_0_1nm | ... | day_7_0_001nm
SELECT duplex_id AS duplex_id, cell_line AS cell_line, 3 AS day, 1.0 AS dose_nM,
  TRY_CAST(day_3_1nm AS DOUBLE) AS viability_value,
  NULL AS viability_sd,
  NULL AS target_gene_name, NULL AS patent_id, NULL AS source_file
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, cell_line, 3, 0.1,
  TRY_CAST(day_3_0_1nm AS DOUBLE), NULL, NULL, NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
-- ... continue for EVERY day_x_ynm value column (all 12 here) ...
;

EXAMPLE B — stacked cell_line/day groups labelled by NOTEs; shared dose columns:
  Columns: target | duplex_id | avg_10_nM | avg_1_nM | avg_500_pM |
           avg_100_pM | avg_50_pM | stdev_10_nM | stdev_1_nM |
           stdev_500_pM | stdev_100_pM | stdev_50_pM
  Context NOTEs (in order): HeLa day 3 / HeLa day 6 / Hep3B day 3 / Hep3B day 6
SELECT duplex_id AS duplex_id, 'HeLa' AS cell_line, 3 AS day, 10.0 AS dose_nM,
  TRY_CAST(avg_10_nM AS DOUBLE) AS viability_value,
  TRY_CAST(stdev_10_nM AS DOUBLE) AS viability_sd,
  target AS target_gene_name, NULL AS patent_id, NULL AS source_file
FROM secondary_table WHERE duplex_id IS NOT NULL
UNION ALL
SELECT duplex_id, 'HeLa', 3, 0.5,
  TRY_CAST(avg_500_pM AS DOUBLE), TRY_CAST(stdev_500_pM AS DOUBLE),
  target, NULL, NULL
FROM secondary_table WHERE duplex_id IS NOT NULL
-- ... continue for all dose columns and all cell_line/day groups ...
;
"""


# ---------------------------------------------------------------------------
# SQL extraction helper
# ---------------------------------------------------------------------------

def _extract_sql(raw: str) -> str:
    m = re.search(r"```sql\s*\n(.*?)```", raw, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return raw.strip().strip("`")


# ---------------------------------------------------------------------------
# SQL generation (LLM dispatch)
# ---------------------------------------------------------------------------

def generate_sql_query(csv_path: str, context_text: str, trace_file: str,
                        table_type: str = "primary",
                        knockdown_expected: bool = True) -> str:
    """
    Return a DuckDB SQL SELECT for this CSV.
    table_type: 'primary' | 'ic50' | 'viability'
    knockdown_expected: whether the classifier found a 'knockdown' measurement.
        When False on a 'primary' table, the knockdown extractor is NOT run —
        the table may still yield sequence rows via the detectors, but it is
        never forced through the % inhibition formula (which would fabricate
        knockdown from non-knockdown columns, e.g. an immunostimulation table's
        % IFN-alpha / % TNF-alpha).
    Returns '__SKIP__' when the table should be skipped entirely.
    """
    if not _CLIENTS:
        raise RuntimeError("Client pool not initialised.")

    try:
        with open(csv_path, encoding="utf-8", errors="replace") as f:
            csv_lines = f.readlines()
        if not csv_lines:
            return ""
        try:
            dialect = csv.Sniffer().sniff(csv_lines[0], delimiters=",;\t|")
            delim   = dialect.delimiter
        except csv.Error:
            delim   = ";"
        csv_headers = [h.strip() for h in csv_lines[0].strip().split(delim)]
        csv_preview = "".join(csv_lines[:15])
    except Exception as exc:
        log_trace(trace_file, "CSV READ ERROR", str(exc))
        return ""

    # Skip tables with no duplex_id-like column
    headers_lower = {h.lower() for h in csv_headers}
    id_like = {"duplex_id", "duplex id", "compound_id", "sample_name",
               "sirna_id", "duplex_num", "duplex_number"}
    has_id_col = (
        bool(id_like.intersection(headers_lower))
        or any(_DUPLEX_COL_RE.search(h) for h in csv_headers)
        or any(sub in h for h in headers_lower for sub in _ID_SUBSTRINGS)
    )
    if not has_id_col:
        log_trace(trace_file, "TABLE SKIPPED",
                  f"No duplex_id-like or seq-ID-like column in: {csv_headers}")
        return "__SKIP__"

    # For primary tables: detect duplex-pair tables (Table 15 pattern)
    # Two columns both matching duplex_id_NNN → pure ID cross-ref, skip.
    _DUPLEX_PAIR_RE = re.compile(r'^duplex[_\s]?id[_\s]?\d*$', re.IGNORECASE)
    if table_type == "primary" and all(_DUPLEX_PAIR_RE.match(h) for h in csv_headers):
        log_trace(trace_file, "TABLE SKIPPED",
                  "Duplex-pair ID cross-reference table — no activity data.")
        return "__SKIP__"

    # Oligo-map tables (duplex_id + sense/antisense oligo IDs, no sequences) are
    # intentionally NOT fast-tracked. A deterministic bypass here would emit SQL
    # hardcoding target_gene_name = NULL, discarding any gene the patent states
    # only in this table's surrounding context — which is then lost for good,
    # because oligo-map rows receive no other gene enrichment in the merge. They
    # therefore fall through to the LLM path below (_build_primary_prompt): it
    # reads the context and can populate target_gene_name, while assay columns it
    # cannot find stay NULL exactly as the prompt instructs. The row is still an
    # oligo-map (oligo IDs, no sequences, no assay values) for the merge, but it
    # now carries the gene.

    # For primary tables: detect strand-per-row sequence tables and PIVOT them
    # deterministically (sense on one row + antisense on another → one row with
    # both strands), instead of relying on the LLM's GROUP BY/CASE WHEN pivot
    # (which it does unreliably). This also avoids an API call.
    if table_type == "primary":
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        strand_hit = _detect_strand_table(csv_headers, preview_rows)
        if strand_hit:
            dup_c, strand_c, seq_c, oligo_c = strand_hit
            log_trace(trace_file, "STRAND-PIVOT",
                      f"duplex={dup_c}, strand={strand_c}, sequence={seq_c}, "
                      f"oligo={oligo_c}")
            norm = f"lower(trim(CAST(\"{strand_c}\" AS VARCHAR)))"
            sense_when = "('s','sense','sensestrand','sense strand')"
            anti_when  = "('a','as','antisense','antisensestrand','antisense strand')"
            # Keep the per-strand oligo IDs when the table carries them, so the
            # output oligo columns are populated AND the duplex<->oligo bridge is
            # preserved for oligo-keyed sequence tables to join through later.
            sense_oligo_sql = (f'MAX(CASE WHEN {norm} IN {sense_when} THEN "{oligo_c}" END)'
                               if oligo_c else "NULL")
            anti_oligo_sql  = (f'MAX(CASE WHEN {norm} IN {anti_when} THEN "{oligo_c}" END)'
                               if oligo_c else "NULL")
            sql = f"""
SELECT
    "{dup_c}" AS duplex_id,
    MAX(CASE WHEN {norm} IN {sense_when}
             THEN "{seq_c}" END) AS sense_sequence,
    MAX(CASE WHEN {norm} IN {anti_when}
             THEN "{seq_c}" END) AS antisense_sequence,
    {sense_oligo_sql} AS sense_oligo_id,
    {anti_oligo_sql} AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS replicate,
    NULL AS transfection_method,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM secondary_table
WHERE "{dup_c}" IS NOT NULL
GROUP BY "{dup_c}"
""".strip()
            log_trace(trace_file, "STRAND-PIVOT SQL", sql)
            return sql

    # For primary tables: detect a strand-per-row sequence table with NO explicit
    # strand column — duplex_id printed only on the lead (sense) row, antisense on
    # the next row with a blank duplex_id (the 'Modified Single Strands and Duplex
    # Sequences' layout). Forward-fill the duplex_id over the blank rows, then pair
    # the two rows of each duplex by order (1st = sense, 2nd = antisense). Done
    # deterministically because the LLM routinely emits all-NULL SQL on this shape.
    if table_type == "primary":
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        imp_hit = _detect_implicit_strand_table(csv_headers, preview_rows)
        if imp_hit:
            dup_c, seq_c, oligo_c, order_c = imp_hit
            ord_expr = (f'TRY_CAST("{order_c}" AS DOUBLE)' if order_c
                        else "row_number() OVER ()")
            sense_oligo_sql = (f'MAX(CASE WHEN _srn = 1 THEN "{oligo_c}" END)'
                               if oligo_c else "NULL")
            anti_oligo_sql  = (f'MAX(CASE WHEN _srn = 2 THEN "{oligo_c}" END)'
                               if oligo_c else "NULL")
            log_trace(trace_file, "IMPLICIT-STRAND-PIVOT",
                      f"duplex={dup_c}, sequence={seq_c}, oligo={oligo_c}, "
                      f"order={order_c}")
            sql = f"""
WITH base AS (
    SELECT *,
        NULLIF(trim(CAST("{dup_c}" AS VARCHAR)), '') AS _dupval,
        {ord_expr} AS _ord
    FROM secondary_table
),
filled AS (
    SELECT *,
        last_value(_dupval IGNORE NULLS) OVER (
            ORDER BY _ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _dup
    FROM base
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY _dup ORDER BY _ord) AS _srn
    FROM filled
)
SELECT
    _dup AS duplex_id,
    MAX(CASE WHEN _srn = 1 THEN "{seq_c}" END) AS sense_sequence,
    MAX(CASE WHEN _srn = 2 THEN "{seq_c}" END) AS antisense_sequence,
    {sense_oligo_sql} AS sense_oligo_id,
    {anti_oligo_sql} AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS replicate,
    NULL AS transfection_method,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM ranked
WHERE _dup IS NOT NULL
GROUP BY _dup
""".strip()
            log_trace(trace_file, "IMPLICIT-STRAND-PIVOT SQL", sql)
            return sql

    # For primary tables: detect a one-row-per-duplex sequence table whose
    # sense/antisense sequence columns are generically named (e.g. `sequence`
    # and `sequence_5`). Emit the mapping deterministically — no API call, no
    # LLM variance — instead of relying on the model to guess which generic
    # column is which strand (a frequent silent failure).
    if table_type == "primary":
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        wide_hit = _detect_wide_two_seq(csv_headers, preview_rows)
        if wide_hit:
            dup_c, s_seq, a_seq, s_ol, a_ol = wide_hit
            sense_oligo_sql = f'"{s_ol}"' if s_ol else "NULL"
            anti_oligo_sql  = f'"{a_ol}"' if a_ol else "NULL"
            log_trace(trace_file, "WIDE-2SEQ",
                      f"duplex={dup_c}, sense_seq={s_seq}, antisense_seq={a_seq}, "
                      f"sense_oligo={s_ol}, antisense_oligo={a_ol}")
            sql = f"""
SELECT
    "{dup_c}" AS duplex_id,
    "{s_seq}" AS sense_sequence,
    "{a_seq}" AS antisense_sequence,
    {sense_oligo_sql} AS sense_oligo_id,
    {anti_oligo_sql} AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS replicate,
    NULL AS transfection_method,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM secondary_table
WHERE "{dup_c}" IS NOT NULL
""".strip()
            log_trace(trace_file, "WIDE-2SEQ SQL", sql)
            return sql

    # For primary tables: a strand-per-row SEQUENCE table keyed by OLIGO ID with
    # NO duplex column (the 'Modified Strand Sequences' listing). None of the
    # duplex-based detectors above can pair its strands, and the LLM does not
    # pivot it reliably, so emit one seq-only row per strand — routing the oligo
    # ID and its sequence to the correct strand field by the strand marker. The
    # merge then resolves these by oligo ID onto activity rows that carry the
    # oligo IDs (e.g. from an oligo-map), filling sense_/antisense_sequence.
    if table_type == "primary":
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        ostr_hit = _detect_oligo_strand_table(csv_headers, preview_rows)
        if ostr_hit:
            strand_c, oligo_c, seq_c = ostr_hit
            norm       = f"lower(trim(CAST(\"{strand_c}\" AS VARCHAR)))"
            sense_when = "('s','sense','sensestrand','sense strand')"
            anti_when  = "('a','as','antisense','antisensestrand','antisense strand')"
            log_trace(trace_file, "OLIGO-STRAND-SEQ",
                      f"strand={strand_c}, oligo={oligo_c}, sequence={seq_c}")
            sql = f"""
SELECT
    NULL AS duplex_id,
    CASE WHEN {norm} IN {sense_when} THEN "{seq_c}"   END AS sense_sequence,
    CASE WHEN {norm} IN {anti_when}  THEN "{seq_c}"   END AS antisense_sequence,
    CASE WHEN {norm} IN {sense_when} THEN "{oligo_c}" END AS sense_oligo_id,
    CASE WHEN {norm} IN {anti_when}  THEN "{oligo_c}" END AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS replicate,
    NULL AS transfection_method,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM secondary_table
WHERE "{oligo_c}" IS NOT NULL AND "{seq_c}" IS NOT NULL
""".strip()
            log_trace(trace_file, "OLIGO-STRAND-SEQ SQL", sql)
            return sql

    # For primary tables: a sequence-looking column is present but none of the
    # deterministic detectors above could map the table (unusual strand vocabulary
    # such as guide/passenger, or odd headers). Ask the LLM ONLY to label the
    # columns — never to transcribe a sequence — verify the labels against the
    # real values, then copy the exact bytes with deterministic SQL. The mapped
    # SQL is cached so a re-run does not re-hit the API.
    if table_type == "primary":
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        if _has_sequence_column(csv_headers, preview_rows):
            colmap_cache = ""
            if _CACHE_DIR[0]:
                os.makedirs(_CACHE_DIR[0], exist_ok=True)
                colmap_cache = os.path.join(
                    _CACHE_DIR[0], f"{_sql_cache_key(csv_path, 'colmap', '')}.sql")
                if os.path.exists(colmap_cache) and _cache_is_preexisting(colmap_cache):
                    try:
                        cached = open(colmap_cache, encoding="utf-8").read().strip()
                    except OSError:
                        cached = ""
                    if cached:
                        log_trace(trace_file, "LLM-COLMAP CACHE HIT (prior run)", colmap_cache)
                        print("  (cached column-map SQL reused from a prior run — no API call)")
                        return cached
            mapping = _llm_identify_sequence_columns(csv_headers, preview_rows, trace_file)
            if _verify_sequence_mapping(mapping, csv_headers, preview_rows):
                sql = _build_sequence_sql_from_mapping(mapping)
                log_trace(trace_file, "LLM-COLMAP SEQ SQL", sql)
                if colmap_cache:
                    try:
                        with open(colmap_cache, "w", encoding="utf-8") as f:
                            f.write(sql)
                    except OSError:
                        pass
                return sql
            log_trace(trace_file, "LLM-COLMAP",
                      "no usable sequence mapping — falling back to the standard prompt")

    # An empty-measurement table (e.g. immunostimulatory / cytokine activity,
    # off-target panels) can reach the primary branch with a duplex_id column and
    # dose-like values. It is allowed to yield SEQUENCE rows via the detectors
    # above, but if none fired it must NOT be run through the knockdown extractor:
    # forcing its non-knockdown columns (e.g. % IFN-alpha, % TNF-alpha) through the
    # "(1 - value) * 100" formula fabricates inhibition (0 % induction -> 100 %
    # "knockdown"). Skip it instead.
    if table_type == "primary" and not knockdown_expected:
        log_trace(trace_file, "TABLE SKIPPED",
                  "no knockdown measurement and not a sequence table — table not "
                  "run through the knockdown extractor")
        return "__SKIP__"

    # Build the right prompt
    if table_type == "ic50":
        prompt = _build_ic50_prompt(csv_headers, context_text, csv_preview)
    elif table_type == "viability":
        prompt = _build_viability_prompt(csv_headers, context_text, csv_preview)
    else:
        # Deterministically detect the value scale (fraction vs percent vs large)
        # from the actual numbers, and hand the model a verified directive so it
        # doesn't misread fraction-remaining columns (which would invert the
        # math, e.g. 0.03 -> 99.97% instead of 96.7%).
        preview_rows = [ln.split(delim) for ln in csv_lines[1:16] if ln.strip()]
        scale_hint   = _detect_value_scale(csv_headers, preview_rows)
        log_trace(trace_file, "VALUE SCALE DETECTED", str(scale_hint))
        prompt = _build_primary_prompt(csv_headers, context_text, csv_preview,
                                       scale_hint=scale_hint)

    max_attempts = max(8, len(_CLIENTS) * 3)

    # Reuse previously-generated SQL when available so a re-run does not re-hit
    # the API (and re-trigger the rate-limiting) for tables that already
    # succeeded. Only tables with no cache entry — i.e. ones that failed last
    # time, or are new — consume API calls.
    cache_path = ""
    if _CACHE_DIR[0]:
        os.makedirs(_CACHE_DIR[0], exist_ok=True)
        cache_path = os.path.join(_CACHE_DIR[0],
                                  f"{_sql_cache_key(csv_path, table_type, prompt)}.sql")
        if os.path.exists(cache_path) and _cache_is_preexisting(cache_path):
            try:
                with open(cache_path, encoding="utf-8") as f:
                    cached = f.read().strip()
            except OSError:
                cached = ""
            if cached:
                log_trace(trace_file, "SQL CACHE HIT (prior run)", cache_path)
                print("  (cached SQL reused from a prior run — no API call)")
                return cached

    for attempt in range(1, max_attempts + 1):
        idx = _next_available_idx()
        if idx is None:
            wait = _shortest_wait()
            print(f"  [Groq] All keys rate-limited — waiting {wait:.0f}s...")
            time.sleep(wait)
            idx = _next_available_idx() or 0

        _ACTIVE_IDX[0] = idx
        try:
            response  = _active_client().chat.completions.create(
                model       = _GROQ_MODEL,
                messages    = [{"role": "user", "content": prompt}],
                temperature = 0.0,
                max_tokens  = 2048,
            )
            raw       = response.choices[0].message.content.strip()
            sql_query = _extract_sql(raw)
            log_trace(trace_file, f"ATTEMPT {attempt} — SQL", sql_query)
            if cache_path and sql_query and sql_query != "__SKIP__":
                try:
                    with open(cache_path, "w", encoding="utf-8") as f:
                        f.write(sql_query)
                except OSError as exc:
                    log_trace(trace_file, "SQL CACHE WRITE ERROR", str(exc))
            return sql_query

        except Exception as e:
            err = str(e)
            if "429" in err or "rate_limit" in err.lower():
                wait_time = _parse_retry_after(err)
                _record_rate_limit(_active_key(), wait_time)
                print(f"    [Groq] token/rate limit on key #{idx + 1} — "
                      f"cooling down ~{wait_time:.0f}s "
                      f"(attempt {attempt}/{max_attempts}); trying another key...")
            else:
                print(f"    [Groq] API error on attempt {attempt}/{max_attempts}: "
                      f"{err[:80]}")
            log_trace(trace_file, f"ATTEMPT {attempt} — API ERROR",
                      f"{e}\n{traceback.format_exc()}")

    print(f"    [Groq] gave up after {max_attempts} attempts — no SQL produced.")
    return ""


# ---------------------------------------------------------------------------
# DuckDB execution
# ---------------------------------------------------------------------------

def _run_sql_on_csv(sql_query: str, csv_path: str, all_varchar: bool):
    """Create the secondary_table view over the CSV and run sql_query, returning a
    DataFrame. With all_varchar=True every column is read as text — string
    functions (POSITION/SUBSTR/LIKE/REGEXP…) then bind even on numeric-looking
    columns, while TRY_CAST(col AS DOUBLE) still parses values (incl. scientific
    notation like 1.5e-3) correctly."""
    opt = ", all_varchar=true" if all_varchar else ""
    with duckdb.connect(database=":memory:") as con:
        con.execute(
            f"CREATE VIEW secondary_table AS "
            f"SELECT * FROM read_csv('{csv_path}', header=True, auto_detect=True{opt})"
        )
        return con.execute(sql_query).df()


# ---------------------------------------------------------------------------
# Guard: the LLM must never put a fabricated VALUE in a measurement column
# ---------------------------------------------------------------------------
#
# The pipeline's core invariant is that the measured quantities — inhibition %,
# IC50, viability, and the sequences — are always READ FROM A CSV COLUMN by the
# generated SQL, never typed as a literal by the model. Execution is
# deterministic (DuckDB on the CSV), so a column reference is faithful; a numeric
# literal in a value position would be a value the model invented. The assay
# COORDINATES (day, dose) are exempt — those are legitimately literals parsed
# from a column name or the context. This guard parses the generated SQL and
# refuses to emit rows when a measurement column is assigned a non-NULL constant,
# turning "the LLM doesn't touch the data" from a convention into an invariant.

_COORDINATE_VALUE_FIELDS = {"day", "dose_nM", "timepoint_hrs"}
_SEQUENCE_VALUE_FIELDS   = {"sense_sequence", "antisense_sequence",
                            "sense_sequence_unmodified",
                            "antisense_sequence_unmodified"}
# Tokens inside a value expression that are NOT column references (keywords,
# casts, types, the handful of functions the generated SQL may use). Any other
# bareword identifier is taken to be a column name.
_SQL_NONCOL_TOKENS = {
    "try_cast", "cast", "convert", "as", "null", "coalesce", "nullif", "ifnull",
    "round", "abs", "floor", "ceil", "case", "when", "then", "else", "end",
    "double", "float", "real", "decimal", "numeric", "integer", "int", "bigint",
    "smallint", "varchar", "char", "text", "string", "boolean", "bool",
    "and", "or", "not", "is", "distinct", "true", "false",
}


def _enforced_value_fields(fields: list[str], numeric_fields: set[str]) -> set[str]:
    """Output columns whose value MUST come from a CSV column rather than a
    literal: the measured numerics (numeric fields minus the assay coordinates)
    plus any sequence columns present in this table's schema."""
    measured_numeric = set(numeric_fields) - _COORDINATE_VALUE_FIELDS
    return measured_numeric | (_SEQUENCE_VALUE_FIELDS & set(fields))


def _split_top_level(s: str, sep_re: re.Pattern) -> list[str]:
    """Split *s* on matches of *sep_re* that occur at parenthesis depth 0 and
    outside single/double quotes."""
    out, depth, quote, last, i = [], 0, None, 0, 0
    while i < len(s):
        c = s[i]
        if quote:
            if c == quote:
                quote = None
            i += 1
            continue
        if c in "'\"":
            quote = c
            i += 1
            continue
        if c == "(":
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0:
            m = sep_re.match(s, i)
            if m and m.end() > i:
                out.append(s[last:i])
                i = m.end()
                last = i
                continue
        i += 1
    out.append(s[last:])
    return out


_FROM_RE   = re.compile(r"from\b", re.IGNORECASE)
_COMMA_RE  = re.compile(r",")
_AS_RE     = re.compile(r"\s+as\s+", re.IGNORECASE)
_ALIAS_RE  = re.compile(r'\s*"?([A-Za-z_][A-Za-z0-9_]*)"?')


def _aliased_select_items(sql: str) -> list[tuple[str, str]]:
    """Return (alias, value_expression) for every top-level select item across
    all SELECTs in *sql*. Paren/quote-aware, so only the OUTER ``AS alias`` is
    treated as the alias (the inner ``AS DOUBLE`` of a TRY_CAST is ignored)."""
    s = re.sub(r"--[^\n]*", "", sql)                       # drop line comments
    items: list[tuple[str, str]] = []
    for m in re.finditer(r"\bselect\b", s, re.IGNORECASE):
        select_list = _split_top_level(s[m.end():], _FROM_RE)[0]
        for raw in _split_top_level(select_list, _COMMA_RE):
            seg = raw.strip()
            if not seg:
                continue
            as_parts = _split_top_level(seg, _AS_RE)
            if len(as_parts) < 2:                          # no explicit alias
                continue
            am = _ALIAS_RE.match(as_parts[-1])
            if not am:
                continue
            alias = am.group(1)
            expr  = " AS ".join(as_parts[:-1]).strip()
            items.append((alias, expr))
    return items


def _expr_has_column_ref(expr: str) -> bool:
    """True if a value expression references at least one CSV column. Double-
    quoted identifiers count as columns; single-quoted string literals are
    removed first; any remaining bareword that is not a SQL keyword/type/function
    is a column name."""
    if re.search(r'"[^"]+"', expr):                        # "Quoted Column"
        return True
    e = re.sub(r"'(?:[^']|'')*'", " ", expr)               # strip 'literals'
    return any(tok.lower() not in _SQL_NONCOL_TOKENS
               for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", e))


def _value_is_fabricated(expr: str) -> bool:
    """True if a measurement value expression is a NON-NULL constant the model
    typed (a literal number or string) rather than a column reference or NULL."""
    if _expr_has_column_ref(expr):
        return False                                       # reads a column → fine
    core = re.sub(r"\b(?:try_cast|cast)\b", "", expr, flags=re.IGNORECASE)
    core = core.replace("(", " ").replace(")", " ")
    core = re.sub(r"\bas\s+\w+", " ", core, flags=re.IGNORECASE)   # drop "AS DOUBLE"
    core = core.strip().strip(",").strip().upper()
    return core not in ("", "NULL")                        # non-null constant


def _measurement_value_violations(sql: str, fields: list[str],
                                  numeric_fields: set[str]) -> list[tuple[str, str]]:
    """List (column, expression) where the SQL assigns a fabricated constant to a
    measurement/sequence column. Empty list == the invariant holds."""
    enforced = _enforced_value_fields(fields, numeric_fields)
    if not enforced:
        return []
    seen, out = set(), []
    for alias, expr in _aliased_select_items(sql):
        if alias in enforced and _value_is_fabricated(expr):
            key = (alias, expr)
            if key not in seen:
                seen.add(key)
                out.append(key)
    return out


def _execute_sql(sql_query: str, csv_path: str, source_file: str,
                 trace_file: str, fields: list[str],
                 numeric_fields: set[str]) -> list[dict]:
    """Run the LLM-generated SQL against the CSV and return a list of row dicts."""
    # INVARIANT GUARD: refuse to emit rows if the SQL assigns a fabricated
    # constant to a measurement/sequence column. Measured values must be read
    # from a CSV column (deterministic), never typed by the model.
    violations = _measurement_value_violations(sql_query, fields, numeric_fields)
    if violations:
        detail = "; ".join(f"{col} <- {expr}" for col, expr in violations)
        log_trace(trace_file, "MEASUREMENT LITERAL BLOCKED",
                  f"SQL assigns a constant to a measurement column; refusing to "
                  f"emit fabricated values: {detail}")
        return []

    try:
        try:
            result = _run_sql_on_csv(sql_query, csv_path, all_varchar=False)
        except duckdb.Error as exc:
            # The most common DuckDB failure is a type/binder mismatch: the model
            # applied a string function (e.g. POSITION/LIKE/SUBSTR) to a column
            # DuckDB auto-typed as DOUBLE, or hand-parsed scientific notation. Re-
            # reading every column as VARCHAR makes those bind while keeping the
            # numeric TRY_CASTs valid — so we salvage the table instead of losing
            # it (no extra API call needed).
            log_trace(trace_file, "DUCKDB RETRY (all_varchar)",
                      f"typed view failed ({str(exc).splitlines()[0][:160]}); "
                      f"retrying with all columns as VARCHAR")
            result = _run_sql_on_csv(sql_query, csv_path, all_varchar=True)

        raw_rows   = result.to_dict("records")
        valid_rows = []

        for row in raw_rows:
            clean: dict = {f: None for f in fields}

            for k, v in row.items():
                if k not in fields:
                    continue
                if v is None or (isinstance(v, float) and v != v):
                    continue
                if str(v).strip().lower() in ("none", "nan", "null", "n/a", ""):
                    continue
                if isinstance(v, float) and k in numeric_fields:
                    v = round(v, 4)
                # Guard: sequence fields must be >1 character
                if k in {"sense_sequence", "antisense_sequence"} and len(str(v).strip()) <= 1:
                    continue
                # Guard: oligo_id fields must not be plain integers
                if k in {"sense_oligo_id", "antisense_oligo_id"}:
                    sv = str(v).strip()
                    if sv.isdigit():
                        continue
                clean[k] = v

            # Drop completely empty rows
            if all(clean.get(f) is None for f in fields
                   if f not in ("patent_id", "source_file")):
                continue

            clean["patent_id"]   = _derive_patent_id(source_file)
            clean["source_file"] = source_file

            valid_rows.append(clean)

        return valid_rows

    except Exception as exc:
        log_trace(trace_file, "DUCKDB EXEC ERROR",
                  f"{exc}\n{traceback.format_exc()}")
        return []
