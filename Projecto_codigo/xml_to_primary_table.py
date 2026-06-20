"""
sirna_primary_table.py — siRNA primary-table builder
===============================================================
Reads *_tables.csv + *_context.txt pairs from an input directory.
Uses Groq (llama-3.3-70b-versatile) to generate a DuckDB SQL SELECT
that maps each file's columns into one of three schemas.
Executes the SQL locally via DuckDB, then writes three output files.

THREE OUTPUT TABLES
-------------------

1. primary_table.csv  (knockdown activity rows — no IC50, no viability)
   duplex_id            – canonical duplex/compound identifier
   sense_sequence       – sense strand nucleotide sequence
   antisense_sequence   – antisense strand nucleotide sequence
   sense_oligo_id       – sense strand oligo identifier (e.g. A-32745)
   antisense_oligo_id   – antisense strand oligo identifier (e.g. A-32746)
   cell_line            – cell line used in the assay
   dose_nM              – siRNA dose in nM
   inhibition_percent   – percent knockdown (0-100)
   value_sd             – standard deviation / error of inhibition_percent
   target_gene_name     – HUGO gene name of the silenced target
   patent_id            – EP number derived from filename
   source_file          – basename of the originating CSV

2. primary_ic50_table.csv  (IC50 measurements, all replicates/timepoints)
   duplex_id            – canonical duplex/compound identifier
   cell_line            – cell line used in the IC50 assay
   timepoint_hrs        – assay timepoint in hours (NULL when not stated)
   replicate            – replicate label: 'I', 'II', 'weighted', or NULL
   ic50_nM              – IC50 value in nM
   target_gene_name     – HUGO gene name
   patent_id            – EP number derived from filename
   source_file          – basename of the originating CSV

3. primary_cell_viability_table.csv  (cell viability screens)
   duplex_id            – canonical duplex/compound identifier
   cell_line            – cell line used in the viability assay
   day                  – assay day as integer (NULL when not stated)
   dose_nM              – siRNA dose in nM
   viability_percent    – % viable cells relative to mock
   viability_sd         – standard deviation of viability_percent
   target_gene_name     – HUGO gene name
   patent_id            – EP number derived from filename
   source_file          – basename of the originating CSV

Table routing
-------------
- IC50 tables   : context title/NOTE contains 'IC50' or 'IC 50'
- Viability tables: context title/NOTE contains viability keywords
- All others    : primary knockdown table

Merge strategy (primary_table only)
------------------------------------
  Key: (patent_id, duplex_id, cell_line, dose_nM)
  - Scalar fields (sequences, oligo IDs, target_gene): first non-null wins
  - inhibition_percent / value_sd: first non-null retained
  - source_file: all contributing files joined with "; "
  - Sequence-only / oligo-map rows enriched onto activity rows; orphaned
    oligo-map rows are pruned.

IC50 and viability tables are NOT merged — every row is kept as-is so
that multiple replicates, timepoints, cell lines and doses are all
preserved with full provenance.

ML-output guarantees
---------------------
  - Empty cells written as "" (not "None" / "nan")
  - Numeric columns rounded to 4 dp
  - No leading/trailing whitespace in any cell
  - Consistent column order on every run

Usage:
  python xml_to_primary_table.py \\
      --input_dir ./data \\
      --output    ./primary_table.csv \\
      --api_key   gsk_...              # or set GROQ_API_KEY env var

  The IC50 and viability outputs are written alongside primary_table.csv
  as primary_ic50_table.csv and primary_cell_viability_table.csv.
"""

from __future__ import annotations

import csv
import glob
import hashlib
import os
import re
import time
import traceback
from datetime import datetime

import duckdb
from groq import Groq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_GROQ_MODEL = "llama-3.3-70b-versatile"

# ── Schema: knockdown activity table ────────────────────────────────────────
PRIMARY_FIELDS = [
    "duplex_id",
    "sense_sequence",
    "antisense_sequence",
    "sense_sequence_unmodified",
    "antisense_sequence_unmodified",
    "sense_oligo_id",
    "antisense_oligo_id",
    "cell_line",
    "dose_nM",
    "inhibition_percent",
    "value_sd",
    "replicate",
    "transfection_method",
    "target_gene_name",
    "patent_id",
    "source_file",
]

# ── Schema: IC50 table ───────────────────────────────────────────────────────
IC50_FIELDS = [
    "duplex_id",
    "cell_line",
    "timepoint_hrs",
    "replicate",
    "ic50_nM",
    "ic50_unit_original",
    "transfection_method",
    "target_gene_name",
    "patent_id",
    "source_file",
]

# ── Schema: cell viability table ─────────────────────────────────────────────
VIABILITY_FIELDS = [
    "duplex_id",
    "cell_line",
    "day",
    "dose_nM",
    "viability_percent",
    "viability_sd",
    "transfection_method",
    "target_gene_name",
    "patent_id",
    "source_file",
]

# Fields that carry sequence / annotation data (no measurement).
_SEQ_FIELDS = {"sense_sequence", "antisense_sequence",
               "sense_sequence_unmodified", "antisense_sequence_unmodified",
               "sense_oligo_id", "antisense_oligo_id", "target_gene_name"}

# Assay measurement fields for primary knockdown table.
_ASSAY_FIELDS = {"inhibition_percent"}

# Oligo-ID fields.
_OLIGO_ID_FIELDS = {"sense_oligo_id", "antisense_oligo_id"}

# Substrings that indicate a column is an ID/identifier worth processing.
_ID_SUBSTRINGS = frozenset({"oligo", "seq_id", "sense", "antisense", "duplex"})

# Numeric fields rounded to 4 dp in output.
_NUMERIC_FIELDS_PRIMARY    = {"dose_nM", "inhibition_percent", "value_sd"}
_NUMERIC_FIELDS_IC50       = {"timepoint_hrs", "ic50_nM"}
_NUMERIC_FIELDS_VIABILITY  = {"day", "dose_nM", "viability_percent", "viability_sd"}

# ---------------------------------------------------------------------------
# Groq client pool
# ---------------------------------------------------------------------------

_CLIENTS:     list[dict]       = []
_RATE_LIMITS: dict[str, float] = {}
_ACTIVE_IDX:  list[int]        = [0]
_SESSION_LOG: list[str]        = [""]

# Directory holding cached LLM-generated SQL (one .sql file per table, keyed by a
# hash of the table contents + prompt). Lets a re-run REUSE SQL it already
# produced and re-hit the API ONLY for tables that previously failed — so a run
# interrupted by rate-limiting resumes cheaply instead of regenerating (and
# re-rate-limiting) everything. Empty string disables caching.
_CACHE_DIR:   list[str]        = [""]

# Per-patent-group accumulator of tables that produced no usable output, written
# to a failed-tables manifest at the end of each group. Each entry is a dict with
# patent_id / source_file / table_type / stage / reason. This turns silent gaps
# (a table that gave up under rate-limiting) into an explicit, re-runnable list.
_FAILED:      list[list]       = [[]]

# Per-patent-group accumulator of VALUE-VALIDATION failures: output cells whose
# value does not match what the column is supposed to hold (e.g. a number in a
# sequence column, a sequence in a dose column). The offending cell is blanked
# so it cannot pollute the output, and the violation is written to a
# validation_failures manifest for review. This polices BOTH the deterministic
# detectors AND the LLM — it does not care which produced the bad value. Pure
# Python: no API calls, so it runs fine on the free tier and is unchanged if you
# later switch model providers (e.g. to the Claude API).
_VALIDATION_FAILURES: list[list] = [[]]

# Plausible upper bound for a concentration in nM (10 mM). Above this a "dose" or
# IC50 is almost certainly a mis-mapped value, not a real concentration.
_MAX_DOSE_NM = 1e7


def _init_clients(api_keys: list[str] | str | None) -> None:
    global _CLIENTS, _RATE_LIMITS, _ACTIVE_IDX
    if api_keys is None:
        env_key = os.environ.get("GROQ_API_KEY", "")
        keys = [env_key] if env_key else []
    elif isinstance(api_keys, str):
        keys = [k.strip() for k in api_keys.split(",") if k.strip()]
    else:
        keys = [k.strip() for k in api_keys if k.strip()]

    if not keys:
        raise ValueError("No Groq API key found. Set GROQ_API_KEY env var or pass --api_key.")

    _CLIENTS     = [{"key": k, "client": Groq(api_key=k)} for k in keys]
    _RATE_LIMITS = {}
    _ACTIVE_IDX  = [0]
    print(f"  [Groq] {len(_CLIENTS)} API key(s) loaded.")


def _active_client() -> Groq:
    return _CLIENTS[_ACTIVE_IDX[0]]["client"]

def _active_key() -> str:
    return _CLIENTS[_ACTIVE_IDX[0]]["key"]

def _is_available(key: str) -> bool:
    return _RATE_LIMITS.get(key, 0.0) <= time.time()

def _record_rate_limit(key: str, seconds: float) -> None:
    _RATE_LIMITS[key] = time.time() + max(seconds, 1.0)

def _next_available_idx() -> int | None:
    n = len(_CLIENTS)
    for offset in range(1, n + 1):
        idx = (_ACTIVE_IDX[0] + offset) % n
        if _is_available(_CLIENTS[idx]["key"]):
            return idx
    if _is_available(_active_key()):
        return _ACTIVE_IDX[0]
    return None

def _shortest_wait() -> float:
    now = time.time()
    pending = [exp for exp in _RATE_LIMITS.values() if exp > now]
    return (min(pending) - now + 0.5) if pending else 0.0

def _parse_retry_after(error_str: str) -> float:
    patterns = [
        (r"in\s+(\d+)h(\d+)m(\d+(?:\.\d+)?)s", "hr_min_sec"),
        (r"in\s+(\d+)m(\d+(?:\.\d+)?)s",        "min_sec"),
        (r"in\s+(\d+(?:\.\d+)?)\s*s",            "sec"),
        (r"retry.after[\":\s]+(\d+)",             "sec"),
    ]
    for pattern, fmt in patterns:
        m = re.search(pattern, error_str, re.IGNORECASE)
        if m:
            if fmt == "hr_min_sec":
                return float(m.group(1)) * 3600 + float(m.group(2)) * 60 + float(m.group(3))
            if fmt == "min_sec":
                return float(m.group(1)) * 60 + float(m.group(2))
            return float(m.group(1))
    return 60.0


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log_trace(trace_file: str, label: str, content: str) -> None:
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] [{label}]\n{content}\n{'-' * 60}\n"
    for path in {trace_file, _SESSION_LOG[0]} - {""}:
        with open(path, "a", encoding="utf-8") as f:
            f.write(entry)


def _sql_cache_key(csv_path: str, table_type: str, prompt: str) -> str:
    """Stable key for a generated-SQL cache entry: table content + type + prompt.
    If any of those change, the key changes and the SQL is regenerated."""
    h = hashlib.sha1()
    try:
        with open(csv_path, "rb") as f:
            h.update(f.read())
    except OSError:
        h.update(csv_path.encode("utf-8"))
    h.update(b"\x00")
    h.update(table_type.encode("utf-8"))
    h.update(b"\x00")
    h.update(prompt.encode("utf-8"))
    return h.hexdigest()[:16]


def _record_failed(patent_id: str | None, source_file: str, table_type: str,
                   stage: str, reason: str) -> None:
    """Append a failed-table record for the current patent group."""
    _FAILED[0].append({
        "patent_id":   patent_id or "",
        "source_file": source_file,
        "table_type":  table_type,
        "stage":       stage,
        "reason":      reason,
    })


def _validate_rows(rows: list[dict], fields: list[str], numeric_fields: set,
                   source_file: str, trace_file: str) -> list[dict]:
    """Block output values that do not fit their column, and log each one.

    This is the column-guardrail layer. It catches a value landing in the wrong
    column regardless of whether a deterministic detector or the LLM produced it
    — the exact "values in the wrong column" failure mode. Checks are chosen to
    be high-confidence / low-false-positive:

      - sequence columns (sense_sequence/antisense_sequence) must look like
        nucleotide sequences;
      - oligo-id columns must NOT be a sequence or a bare integer;
      - numeric columns must parse as numbers; a dose/IC50 must be in a plausible
        range (0..10 mM) and a standard deviation must be non-negative.

    A failing cell is set to None (blocked) so it cannot pollute the merge or the
    output, and a (patent_id, source_file, column, value, reason) record is added
    to the validation manifest. Ranges for inhibition/viability percentages are
    deliberately NOT enforced, so genuine upregulation outliers (large negative
    inhibition) are preserved rather than discarded."""
    pid        = _derive_patent_id(source_file)
    seq_cols   = {f for f in fields if "sequence" in f}
    oligo_cols = _OLIGO_ID_FIELDS & set(fields)

    def _flag(col: str, value, reason: str) -> None:
        _VALIDATION_FAILURES[0].append({
            "patent_id":   pid,
            "source_file": source_file,
            "column":      col,
            "value":       str(value)[:60],
            "reason":      reason,
        })

    for row in rows:
        for col in seq_cols:
            v = row.get(col)
            if v is not None and not _looks_like_sequence(str(v)):
                _flag(col, v, "value is not a nucleotide sequence")
                row[col] = None
        for col in oligo_cols:
            v = row.get(col)
            if v is None:
                continue
            sv = str(v).strip()
            if _looks_like_sequence(sv) or sv.isdigit():
                _flag(col, sv, "sequence or bare integer in an oligo-id column")
                row[col] = None
        for col in numeric_fields:
            v = row.get(col)
            if v is None:
                continue
            try:
                x = float(v)
            except (TypeError, ValueError):
                _flag(col, v, "non-numeric value in a numeric column")
                row[col] = None
                continue
            if col in ("dose_nM", "ic50_nM") and not (0 <= x <= _MAX_DOSE_NM):
                _flag(col, v, "dose/IC50 out of plausible range (<0 or >10 mM)")
                row[col] = None
            elif col in ("value_sd", "viability_sd") and x < 0:
                _flag(col, v, "negative standard deviation")
                row[col] = None
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PLAIN_NUCL_RE = re.compile(r'^[AUTCGautcg]+$')

def _is_modified_sequence(seq: str | None) -> bool:
    if not seq:
        return False
    s = str(seq).strip()
    return bool(s) and not bool(_PLAIN_NUCL_RE.match(s))

def _best_sequence_value(current: str | None, candidate: str | None) -> str | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    if _is_modified_sequence(candidate) and not _is_modified_sequence(current):
        return candidate
    return current

_CONTROL_ID_RE = re.compile(
    r'\b(AD[-\s]?1955|PBS|mock|scramble|scr\b|neg[\s_\-]?ctrl|'
    r'negative[\s_\-]?control|vehicle|luciferase|non[\s_\-]?targeting|'
    r'NT[\s_\-]?siRNA|siNeg|siCtrl|siScr|si[\s_\-]?control|BlockIT)\b',
    re.IGNORECASE,
)

def _detect_transfection_method(context_text: str) -> str | None:
    """
    Analyse the context text and return the transfection method / reagent used
    in the assay, or None when none is named.

    Recognises the common siRNA delivery reagents and methods seen in patents.
    Patterns are checked most-specific-first so e.g. 'Lipofectamine RNAiMAX'
    is not shadowed by the generic 'Lipofectamine'.
    """
    if not context_text:
        return None

    # (compiled pattern, canonical label) — order matters: specific → generic.
    patterns: list[tuple[str, str]] = [
        (r"lipofectamine\s*rnaimax",            "Lipofectamine RNAiMAX"),
        (r"\brnaimax\b",                        "Lipofectamine RNAiMAX"),
        (r"lipofectamine\s*2000",               "Lipofectamine 2000"),
        (r"lipofectamine\s*3000",               "Lipofectamine 3000"),
        (r"\boligofectamine\b",                 "Oligofectamine"),
        (r"\blipofectamine\b",                  "Lipofectamine"),
        (r"\bdharmafect\b",                     "DharmaFECT"),
        (r"\bdotap\b",                          "DOTAP"),
        (r"\binterferin\b",                     "INTERFERin"),
        (r"\bhiperfect\b",                      "HiPerFect"),
        (r"\btransit[\s-]*tko\b",               "TransIT-TKO"),
        (r"nucleofect(?:ion|or)",               "nucleofection"),
        (r"\belectroporat(?:e|ed|ion)\b",       "electroporation"),
        (r"reverse\s+transfect(?:ion|ed)",      "reverse transfection"),
        (r"(?:free|passive|gymnotic)[\s-]*uptake", "free uptake"),
    ]
    text = context_text.lower()
    for pat, label in patterns:
        if re.search(pat, text):
            return label
    return None


_SENSE_OLIGO_RE     = re.compile(r'\bsense[_\s]?oligo[_\s]?(id|num|number)?\b', re.IGNORECASE)
_ANTISENSE_OLIGO_RE = re.compile(r'\bantisense[_\s]?oligo[_\s]?(id|num|number)?\b', re.IGNORECASE)
_DUPLEX_COL_RE      = re.compile(r'\bduplex[_\s]?(id|num|number)?\b', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Table-type detection
# ---------------------------------------------------------------------------

_VIABILITY_TITLE_RE = re.compile(
    r'\b(viab(le|ility)|viable|CellTiter|cytotox|% viable|percent viable'
    r'|cell\s+survival|cell\s+death)\b',
    re.IGNORECASE,
)

_IC50_TITLE_RE = re.compile(
    r'\b(IC\s*50|half.maximal|EC\s*50|dose.response)\b',
    re.IGNORECASE,
)

# Canonical table-title line, e.g. "Table 8. KHK Single Dose Screen" or
# "Table 9: Dose Response". The period/colon after the number is what
# distinguishes a real title from a prose sentence such as
# "Table 9 shows the dose response ..." (which must NOT be read as a title).
# The number may carry a letter suffix ("Table 18b", "Table 3A") and the
# separator may be '.', ':', or a dash ("Table 12 -IC 50 data ...").
_TABLE_TITLE_RE = re.compile(r'^\s*table\s+\d+[a-z]?\s*[.:\-\u2013\u2014]', re.IGNORECASE)
# Any line that opens with "Table <number>" (used as a looser fallback).
_TABLE_LINE_RE  = re.compile(r'^\s*table\s+\d+[a-z]?\b', re.IGNORECASE)


def _classify_table(context_text: str) -> str:
    """
    Classify a table as 'viability', 'ic50' or 'primary' from its title.

    Only the table's OWN title line is used. Context blocks often contain prose
    that merely *references* other tables — e.g. "Table 9 shows the dose response
    ... IC 50 values ..." — which start with the word "Table" but are NOT this
    table's title. Reading those as titles misroutes the table (e.g. an IC50
    table read as a knockdown screen, or vice-versa, mangling the data).

    Strategy: the real title is appended at the END of the context block, so we
    prefer the LAST line that looks like a proper title — "Table N." / "Table N:"
    / "Table N -..." / "Table 18b:" (number + optional letter + a separator).
    Prose like "Table 9 shows ..." has a word (not a separator) after the number
    and is excluded. Fall back to the last bare "Table N" line, then NOTE lines.
    """
    lines = [ln.strip() for ln in context_text.splitlines() if ln.strip()]

    titles = [ln for ln in lines if _TABLE_TITLE_RE.match(ln)]
    if titles:
        signal = titles[-1]                       # the table's own appended title
    else:
        bare = [ln for ln in lines if _TABLE_LINE_RE.match(ln)]
        if bare:
            signal = bare[-1]
        else:
            notes  = [ln for ln in lines
                      if ln.lower().startswith(("note:", "note "))]
            signal = notes[-1] if notes else ""

    if _VIABILITY_TITLE_RE.search(signal):
        return "viability"
    if _IC50_TITLE_RE.search(signal):
        return "ic50"
    return "primary"


def _derive_patent_id(source_file: str | None) -> str | None:
    if not source_file:
        return None
    m = re.match(r'(EP\d+)', os.path.basename(str(source_file)), re.IGNORECASE)
    return m.group(1).upper() if m else None

def _strip_suffix(duplex_id: str) -> str:
    return re.sub(r'\.\d+$', '', duplex_id.strip())


# Canonical duplex-ID matching key.
#
# Some sequence tables prefix the duplex_id with a modification marker: the
# UNMODIFIED-sequence tables list e.g. "UM AD-56041.1" while the modified tables
# AND every activity/IC50 table use the plain "AD-56041.1" / "AD-56041". Once
# whitespace is stripped this prefix becomes "UMAD-56041.1", which can never
# string-match "AD-56041", so the sequence is silently lost during enrichment.
# We therefore MATCH on the canonical "AD-<digits>" core token (case-folded,
# any modifier prefix and ".N" version suffix removed) and only fall back to the
# plain suffix-strip when no such token exists (e.g. non-Alnylam compound IDs),
# which preserves the previous behaviour for those.
_DUPLEX_CORE_RE = re.compile(r'AD-\d+[A-Z]?', re.IGNORECASE)

def _canonical_duplex_id(duplex_id: str | None) -> str:
    if not duplex_id:
        return ""
    s = re.sub(r"\s+", "", str(duplex_id))
    m = _DUPLEX_CORE_RE.search(s)
    return m.group(0).upper() if m else _strip_suffix(s)


def _canonical_oligo_id(oligo_id: str | None) -> str:
    """Matching key for oligo IDs: drop whitespace and any ".N" version suffix so
    that a .1-vs-.2 difference between an activity row's oligo ID and a sequence
    row's oligo ID (e.g. A-26242.1 vs A-26242.2) still joins. Mirrors the
    duplex-ID suffix handling — without this the oligo->sequence fallback silently
    drops sequences whenever the two sides disagree on the version suffix."""
    if not oligo_id:
        return ""
    return _strip_suffix(re.sub(r"\s+", "", str(oligo_id))).upper()


# Columns that must contain NO whitespace at all — a stray space inside an ID
# (e.g. 'AD- 18324') or a sequence ('...cCfu UfcUfuUf') silently breaks the
# exact-string joins used in merge/enrichment.
_NOSPACE_COLS = {"duplex_id", "sense_oligo_id", "antisense_oligo_id",
                 "sense_sequence", "antisense_sequence"}

def _normalize_row(row: dict) -> dict:
    """Normalise whitespace in string cells BEFORE merge/enrichment.

    - ID and sequence columns: remove ALL whitespace (these never legitimately
      contain spaces, and a stray one prevents otherwise-identical values from
      matching during the join).
    - Other text columns: trim the ends and collapse internal whitespace runs to
      a single space.
    - Numeric/None cells are left untouched; a cell that becomes empty -> None.
    """
    for k, v in row.items():
        if not isinstance(v, str):
            continue
        v = re.sub(r"\s+", "", v) if k in _NOSPACE_COLS else re.sub(r"\s+", " ", v).strip()
        row[k] = v if v != "" else None
    return row


_IC50_COL_RE = re.compile(r'(?<![A-Za-z])ic\s*_?50', re.IGNORECASE)

def _read_csv_headers(csv_path: str) -> list[str]:
    """Return the header cells of a CSV, sniffing the delimiter."""
    try:
        with open(csv_path, encoding="utf-8", errors="replace") as f:
            line0 = f.readline()
        try:
            delim = csv.Sniffer().sniff(line0, delimiters=",;\t|").delimiter
        except csv.Error:
            delim = ";"
        return [h.strip() for h in line0.strip().split(delim)]
    except Exception:
        return []

def _has_ic50_column(headers: list[str]) -> bool:
    """True if the table carries a dedicated IC50 value column (e.g. 'ic50_nM')."""
    return any(_IC50_COL_RE.search(h) for h in headers)


# Markers used to recognise a table that was SKIPPED for lack of a recognised
# duplex/oligo ID column but nonetheless carries in-vitro knockdown/IC50/viability
# data — i.e. an activity table whose ID column had an unexpected header and so
# may have been dropped. Clinical/PK tables (mg doses, dosing regimens, patient
# demographics, placebo arms) legitimately have no duplex ID and must NOT be
# flagged. Used only to surface such tables in the failed-tables manifest for
# human review; it does not change extraction behaviour.
_INVITRO_MARKER_RE = re.compile(
    r'(pct[_\s]?inhib|inhibition|remaining|knockdown|\bkd[_\s]?pct\b|'
    r'ic\s*_?50|ec\s*_?50|viab|conc[_\s]?\d.*_[np]m\b|_[np]m\b|avg[_\s]?\d)',
    re.IGNORECASE,
)
_CLINICAL_MARKER_RE = re.compile(
    r'(_mg\b|\bmg_|age[_\s]?year|\bplacebo\b|gender|\bqw\b|\bq2w\b|\bqm\b|'
    r'residual|per[_\s]?vial|mg[_\s]?ml|infusion|patient|subject)',
    re.IGNORECASE,
)

def _skipped_table_has_activity(headers: list[str]) -> bool:
    if not headers or headers == [""]:
        return False
    has_invitro  = any(_INVITRO_MARKER_RE.search(h) for h in headers)
    is_clinical  = any(_CLINICAL_MARKER_RE.search(h) for h in headers)
    return has_invitro and not is_clinical


# Columns that are NOT assay-measurement values (used to decide the value scale).
_NONVALUE_COL_RE = re.compile(
    r'(duplex|oligo|seq[_\s]?id|^id$|_id$|position|\bpos\b|\bstart\b|location|'
    r'\bloc\b|coord|relative_to|\bnote\b|target|gene|'
    r'name|species|strand|ref[_\s]?seq|site|\bsd\b|stdev|std[_\s]|error|'
    r'deviation|ic\s*_?50|ec\s*_?50|timepoint|\bday\b|\bdose\b|num$|'
    r'modification|abbrev|nucleotide|sequence)',
    re.IGNORECASE,
)

def _detect_value_scale(headers: list[str], data_rows: list[list[str]]) -> str | None:
    """Inspect the numeric measurement values and report their scale:
       'fraction'  – values lie on a 0-1 scale  (e.g. 0.03 = 3% remaining)
       'percent'   – values lie on a 0-100 scale (e.g. 82.0)
       'large'     – values exceed ~120 (relative-expression ratios; do NOT
                     blindly apply 100 - value)
       None        – not enough numeric data to decide
    This is used to give the LLM an explicit, verified conversion scale instead
    of leaving it to guess from column names like 'pct_..._remaining'."""
    val_cols = [i for i, h in enumerate(headers) if not _NONVALUE_COL_RE.search(h)]
    vals: list[float] = []
    for row in data_rows:
        for i in val_cols:
            if i < len(row):
                cell = (row[i] or "").strip()
                try:
                    vals.append(float(cell))
                except (ValueError, TypeError):
                    pass
    if len(vals) < 3:
        return None
    vals_sorted = sorted(abs(v) for v in vals)
    p90 = vals_sorted[min(len(vals_sorted) - 1, int(0.9 * len(vals_sorted)))]
    # Use the 90th percentile so a few noisy/upregulated outliers don't flip the
    # verdict (e.g. a percent table with an occasional 130% remaining).
    if p90 <= 1.5:
        return "fraction"
    if p90 <= 150:
        return "percent"
    return "large"


# Strand-per-row sequence tables list the sense strand on one row and the
# antisense strand on another, both keyed by duplex_id. They must be PIVOTED
# (one row per duplex with both strands) — a transformation the LLM does
# unreliably, so we detect and build it deterministically.
_STRAND_VALUES = {"s", "a", "as", "sense", "antisense",
                  "sensestrand", "antisensestrand", "sensestrand"}

def _looks_like_sequence(v: str) -> bool:
    s = str(v).strip()
    if len(s) < 12:
        return False
    bases = sum(1 for c in s if c in "ACGUTacgut")
    return bases >= 8 and bases / len(s) > 0.4

# Oligo-ID values look like 'A-32335' / 'AD-18534.1': a short letter code, an
# optional hyphen, then digits. Used to spot the oligo-ID column in a
# strand-per-row table so its per-strand oligo IDs are kept, not discarded.
_OLIGOISH_RE = re.compile(r'^[A-Za-z]{1,3}-?\d{2,}')

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


# Columns whose presence means a table carries MEASUREMENTS, not just sequences.
# Used to keep the pure-sequence detector below from firing on a combined
# sequence+activity table (which must go through the normal extraction so its
# assay values are not dropped).
_MEASUREMENT_COL_RE = re.compile(
    r'(avg|mean|conc|dose|\bnm\b|\bpm\b|\bum\b|inhib|remain|knockdown|'
    r'viab|ic\s*_?50|ec\s*_?50|percent|\bpct\b|fold|expression|activity)',
    re.IGNORECASE,
)

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


def _read_csv_sample(csv_path: str, n: int = 15) -> tuple[list[str], list[list[str]]]:
    """Return (headers, up to n data rows) of a CSV, sniffing the delimiter.
    Used by the sequence-sidecar detector, which needs a few value rows to tell
    sequence columns from id columns."""
    try:
        with open(csv_path, encoding="utf-8", errors="replace") as f:
            lines = [f.readline() for _ in range(n + 1)]
        lines = [ln for ln in lines if ln]
        if not lines:
            return [], []
        try:
            delim = csv.Sniffer().sniff(lines[0], delimiters=",;\t|").delimiter
        except csv.Error:
            delim = ";"
        headers = [h.strip() for h in lines[0].strip().split(delim)]
        rows = [ln.rstrip("\n").split(delim) for ln in lines[1:] if ln.strip()]
        return headers, rows
    except Exception:
        return [], []


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
    fields_str  = ",\n  ".join([f"NULL AS {f}" for f in VIABILITY_FIELDS])
    headers_str = ", ".join(csv_headers)
    n_fields    = len(VIABILITY_FIELDS)

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
4. cell_line: hardcode from context NOTEs (e.g. 'HeLa', 'Hep3B'). NULL if unknown.
5. day: assay day as integer (e.g. 3, 6). Derive from context NOTEs. NULL if not stated.
6. dose_nM: siRNA dose in nM. Extract from column name (e.g. avg_10_nM → 10.0,
   avg_500_pM → 0.5, avg_100_pM → 0.1, avg_50_pM → 0.05).
   UNPIVOT wide-format tables using UNION ALL — one SELECT per dose column.
   Pair each avg column with its matching stdev column on the same row.
7. viability_percent: the % viable value. ALWAYS TRY_CAST(col AS DOUBLE).
8. viability_sd: matching SD column. TRY_CAST(col AS DOUBLE). NULL if absent.
9. target_gene_name: infer from context or a 'target' column. NULL if unknown.
10. patent_id: always NULL.
11. source_file: always NULL.
12. transfection_method: always NULL (filled later from the context text).
13. Return ONLY valid SQL. No markdown, no explanation.
14. ARITHMETIC TYPE SAFETY: always TRY_CAST arithmetic columns to DOUBLE.
14. The table may contain MULTIPLE GROUPS of rows for different cell lines / days,
    separated by blank rows. Context NOTEs label each group in order
    (e.g. "HeLa day 3", "HeLa day 6", "Hep3B day 3", "Hep3B day 6").
    The rows are stored sequentially in one CSV; you cannot filter by row number.
    Instead produce a UNION ALL that emits one SELECT per (cell_line × day × dose)
    combination using the full table. The merge step will deduplicate correctly.
    Hardcode each cell_line and day from the ordered NOTE labels in the context.

EXAMPLE — wide viability table, 4 cell_line/day groups, 5 dose columns:
  Columns: target | duplex_id | avg_10_nM | avg_1_nM | avg_500_pM |
           avg_100_pM | avg_50_pM | stdev_10_nM | stdev_1_nM |
           stdev_500_pM | stdev_100_pM | stdev_50_pM
  Context NOTEs (in order): HeLa day 3 / HeLa day 6 / Hep3B day 3 / Hep3B day 6

-- (repeat the pattern below for every cell_line × day × dose combination)
SELECT duplex_id AS duplex_id, 'HeLa' AS cell_line, 3 AS day,
  10.0 AS dose_nM,
  TRY_CAST(avg_10_nM AS DOUBLE) AS viability_percent,
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
                        table_type: str = "primary") -> str:
    """
    Return a DuckDB SQL SELECT for this CSV.
    table_type: 'primary' | 'ic50' | 'viability'
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

    # For primary tables: detect and handle oligo-map tables without LLM
    if table_type == "primary":
        duplex_col, sense_col, antisense_col = _detect_oligo_map(csv_headers)
        if duplex_col and sense_col and antisense_col:
            log_trace(trace_file, "TABLE TYPE",
                      f"Oligo-map detected — skipping LLM: "
                      f"duplex={duplex_col}, sense={sense_col}, antisense={antisense_col}")
            sql = f"""
SELECT
    "{duplex_col}"    AS duplex_id,
    NULL              AS sense_sequence,
    NULL              AS antisense_sequence,
    "{sense_col}"     AS sense_oligo_id,
    "{antisense_col}" AS antisense_oligo_id,
    NULL AS cell_line,
    NULL AS dose_nM,
    NULL AS inhibition_percent,
    NULL AS value_sd,
    NULL AS target_gene_name,
    NULL AS patent_id,
    NULL AS source_file
FROM secondary_table
WHERE "{duplex_col}" IS NOT NULL
""".strip()
            log_trace(trace_file, "OLIGO-MAP SQL", sql)
            return sql

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
        if os.path.exists(cache_path):
            try:
                with open(cache_path, encoding="utf-8") as f:
                    cached = f.read().strip()
            except OSError:
                cached = ""
            if cached:
                log_trace(trace_file, "SQL CACHE HIT", cache_path)
                print("  (cached SQL reused — no API call)")
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


def _execute_sql(sql_query: str, csv_path: str, source_file: str,
                 trace_file: str, fields: list[str],
                 numeric_fields: set[str]) -> list[dict]:
    """Run the LLM-generated SQL against the CSV and return a list of row dicts."""
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


# ---------------------------------------------------------------------------
# Merge (primary knockdown table only)
# ---------------------------------------------------------------------------

def _is_seq_only(row: dict) -> bool:
    has_assay = any(row.get(f) is not None for f in _ASSAY_FIELDS)
    has_seq   = any(row.get(f) is not None for f in _SEQ_FIELDS)
    return has_seq and not has_assay

def _is_oligo_map_only(row: dict) -> bool:
    has_oligo    = any(row.get(f) is not None for f in _OLIGO_ID_FIELDS)
    has_real_seq = any(row.get(f) is not None for f in _SEQ_FIELDS - _OLIGO_ID_FIELDS)
    has_assay    = any(row.get(f) is not None for f in _ASSAY_FIELDS)
    return has_oligo and not has_real_seq and not has_assay


def _merge_primary_rows(all_rows: list[dict]) -> list[dict]:
    """Merge knockdown activity rows on (patent_id, duplex_id, cell_line, dose_nM)."""
    oligo_map_rows = [r for r in all_rows if _is_oligo_map_only(r)]
    seq_rows       = [r for r in all_rows if _is_seq_only(r) and not _is_oligo_map_only(r)]
    activity_rows  = [r for r in all_rows if not _is_seq_only(r) and not _is_oligo_map_only(r)]

    print(f"  Total rows: {len(all_rows)} "
          f"({len(activity_rows)} activity + {len(seq_rows)} seq-only "
          f"+ {len(oligo_map_rows)} oligo-map)")

    seq_by_exact: dict[str, list[dict]] = {}
    seq_by_base:  dict[str, list[dict]] = {}
    for row in seq_rows:
        did = str(row.get("duplex_id") or "").strip()
        if not did:
            continue
        seq_by_exact.setdefault(did, []).append(row)
        seq_by_base.setdefault(_canonical_duplex_id(did), []).append(row)

    oligo_by_exact: dict[str, list[dict]] = {}
    oligo_by_base:  dict[str, list[dict]] = {}
    for row in oligo_map_rows:
        did = str(row.get("duplex_id") or "").strip()
        if not did:
            continue
        oligo_by_exact.setdefault(did, []).append(row)
        oligo_by_base.setdefault(_canonical_duplex_id(did), []).append(row)

    sense_oligo_seq:       dict[tuple[str, str], str] = {}
    antisense_oligo_seq:   dict[tuple[str, str], str] = {}
    # Parallel repos holding the PLAIN (unmodified) sequence per oligo ID. The
    # repos above keep the modified-preferred form (what feeds sense_sequence /
    # antisense_sequence); these keep the unmodified form so the *_unmodified
    # columns can be filled when sequences are joined by oligo ID.
    sense_oligo_unmod:     dict[tuple[str, str], str] = {}
    antisense_oligo_unmod: dict[tuple[str, str], str] = {}

    def _plain_from(row, unmod_col, primary_col):
        """First plain (non-modified) sequence on a row for one strand: an
        explicit *_unmodified value if present, otherwise a plain primary value."""
        for v in (row.get(unmod_col), row.get(primary_col)):
            if v is not None and not _is_modified_sequence(v):
                return v
        return None

    for row in seq_rows:
        pid     = str(row.get("patent_id") or "").strip()
        s_oligo = row.get("sense_oligo_id")
        a_oligo = row.get("antisense_oligo_id")
        if s_oligo is not None:
            key = (pid, _canonical_oligo_id(s_oligo))
            if row.get("sense_sequence") is not None:
                sense_oligo_seq[key] = _best_sequence_value(
                    sense_oligo_seq.get(key), row["sense_sequence"])
            plain = _plain_from(row, "sense_sequence_unmodified", "sense_sequence")
            if plain is not None:
                sense_oligo_unmod.setdefault(key, plain)
        if a_oligo is not None:
            key = (pid, _canonical_oligo_id(a_oligo))
            if row.get("antisense_sequence") is not None:
                antisense_oligo_seq[key] = _best_sequence_value(
                    antisense_oligo_seq.get(key), row["antisense_sequence"])
            plain = _plain_from(row, "antisense_sequence_unmodified", "antisense_sequence")
            if plain is not None:
                antisense_oligo_unmod.setdefault(key, plain)

    if sense_oligo_seq or antisense_oligo_seq:
        print(f"  oligo_id seq repos: {len(sense_oligo_seq)} sense, "
              f"{len(antisense_oligo_seq)} antisense entries "
              f"({len(sense_oligo_unmod)}/{len(antisense_oligo_unmod)} unmodified).")

    # Canonicalise the duplex_id by stripping any ".N" version suffix so that
    # AD-65696, AD-65696.1 and AD-65696.2 are treated as the SAME duplex and
    # merge into a single record (keyed also by cell_line + dose + replicate, so
    # distinct measurements of one duplex still stay on separate rows — including
    # individual replicate measurements taken at the same dose).
    key_to_rows: dict[tuple, list[dict]] = {}
    for row in activity_rows:
        k = (
            str(row.get("patent_id")  or "").strip(),
            _canonical_duplex_id(str(row.get("duplex_id") or "")),
            str(row.get("cell_line")  or "").strip(),
            str(row.get("dose_nM")    or "").strip(),
            str(row.get("replicate")  or "").strip(),
        )
        key_to_rows.setdefault(k, []).append(row)

    merged: list[dict] = []
    scalar_fields = {"sense_sequence", "antisense_sequence",
                     "sense_oligo_id", "antisense_oligo_id", "target_gene_name"}
    _SEQ_SCALAR   = {"sense_sequence", "antisense_sequence"}

    for key, rows in key_to_rows.items():
        out: dict = {f: None for f in PRIMARY_FIELDS}
        out["patent_id"], out["duplex_id"], out["cell_line"], dose_str, rep_str = key
        out["replicate"] = rep_str or None

        try:
            out["dose_nM"] = None if dose_str in ("", "None") else float(dose_str)
        except (ValueError, TypeError):
            out["dose_nM"] = None

        for f in (scalar_fields - _SEQ_SCALAR) | {"inhibition_percent", "value_sd",
                                                   "transfection_method"}:
            for r in rows:
                if r.get(f) is not None:
                    out[f] = r[f]
                    break

        for f in _SEQ_SCALAR:
            for r in rows:
                out[f] = _best_sequence_value(out.get(f), r.get(f))

        sources = sorted({
            os.path.basename(str(r.get("source_file") or ""))
            for r in rows if r.get("source_file")
        })
        out["source_file"] = "; ".join(sources) if sources else None

        act_id   = out["duplex_id"]
        act_base = _canonical_duplex_id(act_id)

        seq_candidates = seq_by_exact.get(act_id, []) or seq_by_base.get(act_base, [])
        for f in scalar_fields:
            if f in _SEQ_SCALAR:
                for cand in seq_candidates:
                    out[f] = _best_sequence_value(out.get(f), cand.get(f))
            else:
                if out.get(f) is not None:
                    continue
                for cand in seq_candidates:
                    if cand.get(f) is not None:
                        out[f] = cand[f]
                        break

        # Preserve the UNMODIFIED sequence form alongside the (modified-preferred)
        # primary sequence above. A duplex often appears in both an unmodified and
        # a modified sequence table; the modified one wins sense_sequence /
        # antisense_sequence, so without this the unmodified form — already
        # extracted and present here as a candidate — would simply be discarded.
        # Fill each *_unmodified column from the first plain (non-modified)
        # candidate value for that strand.
        for primary_col, unmod_col in (
                ("sense_sequence",     "sense_sequence_unmodified"),
                ("antisense_sequence", "antisense_sequence_unmodified")):
            if out.get(unmod_col) is None:
                for cand in seq_candidates:
                    v = _plain_from(cand, unmod_col, primary_col)
                    if v is not None:
                        out[unmod_col] = v
                        break

        oligo_candidates = (oligo_by_exact.get(act_id, [])
                            or oligo_by_base.get(act_base, []))
        for f in _OLIGO_ID_FIELDS:
            if out.get(f) is not None:
                continue
            for cand in oligo_candidates:
                if cand.get(f) is not None:
                    out[f] = cand[f]
                    break

        pid = str(out.get("patent_id") or "").strip()
        if out.get("sense_sequence") is None and out.get("sense_oligo_id") is not None:
            key_r = (pid, _canonical_oligo_id(out["sense_oligo_id"]))
            seq   = sense_oligo_seq.get(key_r)
            if seq is not None:
                out["sense_sequence"] = seq
        if out.get("antisense_sequence") is None and out.get("antisense_oligo_id") is not None:
            key_r = (pid, _canonical_oligo_id(out["antisense_oligo_id"]))
            seq   = antisense_oligo_seq.get(key_r)
            if seq is not None:
                out["antisense_sequence"] = seq

        # Unmodified forms via the oligo-ID repo (mirrors the sequence fallback
        # above). EPO sequence tables are frequently keyed by oligo ID with a
        # NULL duplex_id, so the duplex-keyed unmodified fill never reaches them.
        if (out.get("sense_sequence_unmodified") is None
                and out.get("sense_oligo_id") is not None):
            out["sense_sequence_unmodified"] = sense_oligo_unmod.get(
                (pid, _canonical_oligo_id(out["sense_oligo_id"])))
        if (out.get("antisense_sequence_unmodified") is None
                and out.get("antisense_oligo_id") is not None):
            out["antisense_sequence_unmodified"] = antisense_oligo_unmod.get(
                (pid, _canonical_oligo_id(out["antisense_oligo_id"])))

        merged.append(out)

    # Sequence-only and oligo-map rows are used ONLY to enrich activity rows
    # (above). They are never emitted on their own: the primary table must
    # contain knockdown-activity rows exclusively. Any row without an
    # inhibition_percent measurement is therefore dropped.
    n_before = len(merged)
    merged = [r for r in merged if r.get("inhibition_percent") is not None]
    n_dropped = n_before - len(merged)

    n_seq_orphans  = sum(
        1 for r in seq_rows
        if _canonical_duplex_id(str(r.get("duplex_id") or "")) not in
        {_canonical_duplex_id(str(m.get("duplex_id") or "")) for m in merged}
    )
    if n_dropped:
        print(f"  Dropped {n_dropped} merged row(s) with no inhibition_percent.")
    if n_seq_orphans:
        print(f"  Discarded {n_seq_orphans} sequence-only row(s) "
              f"(no matching activity).")

    print(f"  → {len(merged)} activity rows kept")
    return merged


# ---------------------------------------------------------------------------
# ML-clean CSV writer helpers
# ---------------------------------------------------------------------------

def _ml_clean(row: dict, fields: list[str], numeric_fields: set[str]) -> dict:
    out = {}
    for f in fields:
        v = row.get(f)
        if v is None or (isinstance(v, float) and v != v):
            out[f] = ""
        elif f in numeric_fields:
            try:
                out[f] = round(float(v), 4)
            except (ValueError, TypeError):
                out[f] = ""
        else:
            out[f] = str(v).strip()
    return out


def _write_csv(rows: list[dict], path: str, fields: list[str],
               numeric_fields: set[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows([_ml_clean(r, fields, numeric_fields) for r in rows])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _filter_files_by_prefix(
    csv_files:    list[str],
    file_prefixes: list[str] | str,
) -> list[str]:
    """Keep only the table files whose basename starts with one of the given prefixes.

    Parameters
    ----------
    csv_files     : the full list of discovered "*_tables.csv" paths.
    file_prefixes : one prefix (string) or a list of prefixes. A file is kept
                    when its basename starts with any of these prefixes, e.g. the
                    prefix "EP2373382NWB1" matches "EP2373382NWB1_tables.csv".
                    Matching is case-insensitive and ignores surrounding spaces.

    Returns
    -------
    The filtered list, preserving the original order. Any prefix that matched no
    file is reported on screen so typos are easy to spot.
    """
    # Accept a single string as a convenience, then tidy the list: drop empty
    # entries and surrounding whitespace, and compare in lower case so the
    # caller does not have to match the exact letter casing of the filenames.
    if isinstance(file_prefixes, str):
        file_prefixes = [file_prefixes]
    cleaned_prefixes = [p.strip() for p in file_prefixes if p and p.strip()]
    lowered_prefixes = [p.lower() for p in cleaned_prefixes]

    selected:      list[str] = []
    matched_flags: list[bool] = [False] * len(lowered_prefixes)

    for path in csv_files:
        name = os.path.basename(path).lower()
        hits = [name.startswith(prefix) for prefix in lowered_prefixes]
        if any(hits):
            selected.append(path)  # added once, even if several prefixes match
            for i, hit in enumerate(hits):
                if hit:
                    matched_flags[i] = True

    # Surface any prefix that did not match a single file. This mirrors the rest
    # of the pipeline's "no silent gaps" approach and helps catch mistyped IDs.
    unmatched = [cleaned_prefixes[i] for i, hit in enumerate(matched_flags) if not hit]
    if unmatched:
        print(f"Warning: no table files matched these prefix(es): {', '.join(unmatched)}")

    return selected


def build_primary_table(
    input_dir:     str,
    output_path:   str  = "primary_table.csv",
    api_keys:      list[str] | str | None = None,
    per_file_dir:  str | None = None,
    file_prefixes: list[str] | str | None = None,
) -> None:
    """
    Build the three siRNA output tables from all *_tables.csv files in `input_dir`.

    Parameters
    ----------
    input_dir    : directory containing *_tables.csv (+ optional *_context.txt) files
    output_path  : path for the primary knockdown table.
                   The IC50 and viability tables are written alongside it, inheriting
                   any suffix present in the filename stem.

                   Examples
                   --------
                   output_path="primary_table_EP4744669A2.csv"
                     → primary_table_EP4744669A2.csv
                     → primary_ic50_table_EP4744669A2.csv
                     → primary_cell_viability_table_EP4744669A2.csv

                   output_path="primary_table.csv"  (no suffix)
                     → primary_table.csv
                     → primary_ic50_table.csv
                     → primary_cell_viability_table.csv
    api_keys     : Groq API key(s); comma-separated string, list, or None (reads GROQ_API_KEY)
    per_file_dir : optional directory to write one intermediate CSV per input file
    file_prefixes: optional filter restricting which table files are processed.
                   When None (the default) every "*_tables.csv" file in input_dir
                   is treated independently, exactly as before. When given a list
                   (or a single string) of filename prefixes, only the table files
                   whose basename starts with one of those prefixes are processed.

                   Examples
                   --------
                   file_prefixes=None
                     → process every *_tables.csv file in input_dir

                   file_prefixes=["EP2373382NWB1", "EP2723758NWB1", "EP4365291NWA2"]
                     → process only EP2373382NWB1_tables.csv,
                       EP2723758NWB1_tables.csv and EP4365291NWA2_tables.csv
                       (matching is case-insensitive)
    """
    _init_clients(api_keys)

    output_dir = os.path.dirname(os.path.abspath(output_path)) or "."
    os.makedirs(output_dir, exist_ok=True)
    debug_dir = os.path.join(output_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)
    if per_file_dir:
        os.makedirs(per_file_dir, exist_ok=True)

    session_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    _SESSION_LOG[0] = os.path.join(debug_dir, f"session_{session_ts}.log")

    # Enable the on-disk SQL cache so re-runs reuse already-generated SQL and
    # only the previously-failed tables consume API calls. Safe to delete to
    # force a clean regeneration.
    _CACHE_DIR[0] = os.path.join(debug_dir, "sql_cache")
    os.makedirs(_CACHE_DIR[0], exist_ok=True)

    csv_files = sorted(glob.glob(os.path.join(input_dir, "*_tables.csv")))
    if not csv_files:
        print(f"No *_tables.csv files found in '{input_dir}'.")
        return

    # When the caller supplies a list of filename prefixes, narrow the set down
    # to just those files. With no list, every file is kept and treated
    # independently, which is the original behaviour.
    if file_prefixes is not None:
        csv_files = _filter_files_by_prefix(csv_files, file_prefixes)
        if not csv_files:
            print(f"No *_tables.csv files in '{input_dir}' matched the given prefixes.")
            return

    # Decide how output files are named.
    #   - Default behaviour: group by patent id (EP number, e.g. 'EP4141116').
    #     Multiple table files for one patent share one output set; only when
    #     more than one patent is present do we emit a separate set per patent.
    #   - When a prefix filter is active AND output_path is left at its default,
    #     label each output set by the matched file's stem (e.g. 'EP4744669NWA2')
    #     and ALWAYS write that suffix — so the bare prefixed call yields
    #     primary_cell_viability_table_EP4744669NWA2.csv (and companions).
    #     An explicit output_path always wins and is left untouched.
    default_output = (output_path == "primary_table.csv")
    label_by_file  = file_prefixes is not None and default_output

    # When labelling by prefix, every file that matched a given prefix is merged
    # into ONE group named after that prefix (e.g. all EP4744669NWA2_*_tables.csv
    # -> a single 'EP4744669NWA2' set). Falls back to the patent id otherwise.
    lowered_prefixes: list[str] = []
    if label_by_file:
        _pfx = [file_prefixes] if isinstance(file_prefixes, str) else file_prefixes
        lowered_prefixes = [p.strip().lower() for p in _pfx if p and p.strip()]

    def _group_key(path: str) -> str:
        if label_by_file:
            name = os.path.basename(path).lower()
            for p in lowered_prefixes:
                if name.startswith(p):
                    return p.upper()
        return _derive_patent_id(os.path.basename(path)) or "UNKNOWN"

    groups: dict[str, list[str]] = {}
    for f in csv_files:
        groups.setdefault(_group_key(f), []).append(f)

    print(f"Found {len(csv_files)} table file(s) across {len(groups)} group(s) "
          f"in '{input_dir}'.")

    # Write a separate, labelled output set per group when there is more than one
    # group, or whenever we are labelling by file (prefix filter + default path).
    if len(groups) > 1 or label_by_file:
        if len(groups) > 1:
            print(f"Detected {len(groups)} group(s): {', '.join(sorted(groups))}. "
                  f"Writing one CSV set per group.")
        total = len(groups)
        crashed: list[str] = []
        for n, label in enumerate(sorted(groups), start=1):
            grp_out = os.path.join(output_dir, f"primary_table_{label}.csv")
            print(f"\n{'='*64}\n[{n}/{total}] {label} — {len(groups[label])} tables\n{'='*64}")
            t0 = time.time()
            # Per-group isolation: a crash on ONE group must not abort the run
            # for the remaining groups. Record it and carry on.
            try:
                _process_table_group(groups[label], grp_out, per_file_dir,
                                     group_label=label, group_idx=n, group_total=total)
                print(f"[{n}/{total}] ✓ {label} done in {time.time()-t0:.1f}s "
                      f"({total - n} group(s) remaining)")
            except Exception as exc:
                crashed.append(label)
                log_trace(_SESSION_LOG[0], "GROUP CRASHED",
                          f"{label}\n{exc}\n{traceback.format_exc()}")
                print(f"[{n}/{total}] ✗ {label} FAILED after "
                      f"{time.time()-t0:.1f}s: {str(exc)[:120]} — continuing.")
                # Leave a one-line manifest so the crash is not silent.
                crash_manifest = os.path.join(output_dir,
                                              f"failed_tables_{label}.csv")
                try:
                    with open(crash_manifest, "w", newline="", encoding="utf-8") as cf:
                        w = csv.DictWriter(cf, fieldnames=["patent_id", "source_file",
                                           "table_type", "stage", "reason"])
                        w.writeheader()
                        w.writerow({"patent_id": label, "source_file": "",
                                    "table_type": "", "stage": "group",
                                    "reason": f"crashed: {str(exc)[:200]}"})
                except OSError:
                    pass
        print(f"\nAll {total} group(s) complete."
              + (f" {len(crashed)} crashed: {', '.join(crashed)}." if crashed else ""))
    else:
        _process_table_group(csv_files, output_path, per_file_dir)


def _process_table_group(
    csv_files:    list[str],
    output_path:  str,
    per_file_dir: str | None,
    group_label:  str | None = None,
    group_idx:    int | None = None,
    group_total:  int | None = None,
) -> None:
    """Extract the three siRNA tables from one set of input files and write them
    to output_path (+ the matching ic50 / viability companions)."""
    output_dir = os.path.dirname(os.path.abspath(output_path)) or "."
    primary_stem = os.path.splitext(os.path.basename(output_path))[0]
    suffix = re.sub(r"^primary_table", "", primary_stem)
    ic50_path      = os.path.join(output_dir, f"primary_ic50_table{suffix}.csv")
    viability_path = os.path.join(output_dir, f"primary_cell_viability_table{suffix}.csv")
    debug_dir = os.path.join(output_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)

    tag = f"{group_label} " if group_label else ""
    n_tables = len(csv_files)

    # Fresh failed-tables manifest for this patent group.
    _FAILED[0] = []
    # Fresh value-validation manifest for this patent group.
    _VALIDATION_FAILURES[0] = []

    primary_rows:    list[dict] = []
    ic50_rows:       list[dict] = []
    viability_rows:  list[dict] = []

    for t_i, csv_file in enumerate(csv_files, start=1):
        base     = os.path.splitext(os.path.basename(csv_file))[0]
        file_log = os.path.join(debug_dir, f"{base}.log")
        print(f"\n[{tag}table {t_i}/{n_tables}] Processing: {os.path.basename(csv_file)}")

        ctx_file     = csv_file.replace("_tables.csv", "_context.txt")
        context_text = ""
        if os.path.exists(ctx_file):
            with open(ctx_file, encoding="utf-8") as _f:
                context_text = _f.read()

        # Classify table type from context metadata lines
        table_type = _classify_table(context_text)
        print(f"  Table type: {table_type}")
        log_trace(file_log, "TABLE TYPE DETECTED", table_type)

        txn_method = _detect_transfection_method(context_text)

        print("  Generating SQL...")
        sql_query = generate_sql_query(csv_file, context_text, file_log, table_type)

        if not sql_query or sql_query == "__SKIP__":
            reason = "skipped" if sql_query == "__SKIP__" else "no SQL generated"
            print(f"  SKIP — {reason}.")
            # A genuine give-up (empty SQL, e.g. exhausted retries under
            # rate-limiting) is recorded so it can be re-run; an intentional
            # __SKIP__ (e.g. a pure cross-reference table) is not a failure.
            if not sql_query:
                _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                               table_type, "sql_generation",
                               "no SQL produced (likely rate-limit give-up)")
            # A table skipped for "no recognised ID column" that nonetheless looks
            # like an in-vitro activity table (% inhibition / nM doses / IC50 /
            # viability, and NOT a clinical/PK table) is surfaced for review — its
            # duplex-ID column may have had an unexpected header and been missed.
            elif _skipped_table_has_activity(_read_csv_headers(csv_file)):
                _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                               table_type, "skipped_review",
                               "skipped (no recognised duplex/oligo ID column) but has "
                               "in-vitro activity columns — verify the source ID column")
            continue

        # Choose schema for execution
        if table_type == "ic50":
            fields         = IC50_FIELDS
            numeric_fields = _NUMERIC_FIELDS_IC50
        elif table_type == "viability":
            fields         = VIABILITY_FIELDS
            numeric_fields = _NUMERIC_FIELDS_VIABILITY
        else:
            fields         = PRIMARY_FIELDS
            numeric_fields = _NUMERIC_FIELDS_PRIMARY

        print("  Executing SQL via DuckDB...")
        rows = _execute_sql(sql_query, csv_file, os.path.basename(csv_file),
                            file_log, fields, numeric_fields)
        rows = [_normalize_row(r) for r in rows]   # tidy whitespace before merge
        # Column guardrails: block any value that does not fit its column
        # (e.g. a number in a sequence column) and log it for review. Catches
        # mistakes from the LLM and the deterministic detectors alike.
        rows = _validate_rows(rows, fields, numeric_fields,
                              os.path.basename(csv_file), file_log)
        print(f"  → {len(rows)} row(s)")

        # A table that generated SQL but yielded NO rows is suspect (bad SQL, a
        # DuckDB exec error, or an empty/garbled source) — record it so it is not
        # a silent gap. Pure cross-reference tables are caught earlier as __SKIP__.
        if not rows:
            _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                           table_type, "execution", "SQL produced 0 rows")

        # Transfection method is a property of the assay, taken from the
        # context text rather than the table columns — stamp it on every row.
        log_trace(file_log, "TRANSFECTION METHOD", str(txn_method))
        for r in rows:
            r["transfection_method"] = txn_method

        if per_file_dir and rows:
            os.makedirs(per_file_dir, exist_ok=True)
            per_path = os.path.join(per_file_dir, f"{base}_{table_type}.csv")
            _write_csv(rows, per_path, fields, numeric_fields)
            print(f"  → per-file CSV: {per_path}")

        if table_type == "ic50":
            ic50_rows.extend(rows)
        elif table_type == "viability":
            viability_rows.extend(rows)
        else:
            primary_rows.extend(rows)

        # SEQUENCE SIDECAR: a combined table holds sequences AND activity in the
        # same rows (e.g. duplex_id, sense_strand, antisense_strand, pct_mrna_*).
        # When the LLM unpivots the dose columns into activity rows it routinely
        # writes NULL for the sequence columns, dropping sequences that are right
        # there. _detect_wide_two_seq bails on such tables (it must not clobber
        # the assay values), so we rescue the sequences deterministically here:
        # emit duplex→sequence seq-only rows that the merge attaches to the
        # activity rows by duplex_id. Runs only on combined primary tables that
        # actually contain two sequence columns (pure-activity tables are
        # untouched; pure sequence tables were already handled by the wide-seq
        # detector during SQL generation).
        if table_type == "primary":
            side_headers, side_rows = _read_csv_sample(csv_file)
            combined = any(_MEASUREMENT_COL_RE.search(h) for h in side_headers)
            sidecar = _detect_seq_sidecar(side_headers, side_rows) if combined else None
            already_seq = any(r.get("sense_sequence") or r.get("antisense_sequence")
                              for r in rows)
            if sidecar and not already_seq:
                dup_c, s_seq, a_seq, s_ol, a_ol = sidecar
                s_ol_sql = f'"{s_ol}"' if s_ol else "NULL"
                a_ol_sql = f'"{a_ol}"' if a_ol else "NULL"
                log_trace(file_log, "SEQ-SIDECAR",
                          f"combined table — rescuing sequences: duplex={dup_c}, "
                          f"sense_seq={s_seq}, antisense_seq={a_seq}, "
                          f"sense_oligo={s_ol}, antisense_oligo={a_ol}")
                sidecar_sql = f"""
SELECT
    "{dup_c}" AS duplex_id,
    "{s_seq}" AS sense_sequence,
    "{a_seq}" AS antisense_sequence,
    {s_ol_sql} AS sense_oligo_id,
    {a_ol_sql} AS antisense_oligo_id,
    NULL AS cell_line, NULL AS dose_nM, NULL AS inhibition_percent,
    NULL AS value_sd, NULL AS replicate, NULL AS transfection_method,
    NULL AS target_gene_name, NULL AS patent_id, NULL AS source_file
FROM secondary_table
WHERE "{dup_c}" IS NOT NULL
""".strip()
                seq_rows = _execute_sql(sidecar_sql, csv_file, os.path.basename(csv_file),
                                        file_log, PRIMARY_FIELDS, _NUMERIC_FIELDS_PRIMARY)
                seq_rows = [_normalize_row(r) for r in seq_rows]
                seq_rows = _validate_rows(seq_rows, PRIMARY_FIELDS,
                                          _NUMERIC_FIELDS_PRIMARY,
                                          os.path.basename(csv_file), file_log)
                primary_rows.extend(seq_rows)
                print(f"  → +{len(seq_rows)} sequence row(s) rescued from combined table")

        # DUAL-ROUTING: a knockdown table can also carry an IC50 column in the
        # same rows (combined table). The primary schema has no IC50 field, so
        # those values would be lost. Detect a dedicated IC50 column and run a
        # second IC50 extraction over the same file, feeding the IC50 output.
        if table_type == "primary" and _has_ic50_column(_read_csv_headers(csv_file)):
            print("  Combined table — also extracting IC50 column...")
            log_trace(file_log, "DUAL-ROUTE", "primary table also has an IC50 column")
            ic50_sql = generate_sql_query(csv_file, context_text, file_log, "ic50")
            if ic50_sql and ic50_sql != "__SKIP__":
                ic50_extra = _execute_sql(
                    ic50_sql, csv_file, os.path.basename(csv_file),
                    file_log, IC50_FIELDS, _NUMERIC_FIELDS_IC50)
                ic50_extra = [_normalize_row(r) for r in ic50_extra]
                ic50_extra = _validate_rows(ic50_extra, IC50_FIELDS,
                                            _NUMERIC_FIELDS_IC50,
                                            os.path.basename(csv_file), file_log)
                for r in ic50_extra:
                    r["transfection_method"] = txn_method
                ic50_rows.extend(ic50_extra)
                print(f"  → +{len(ic50_extra)} IC50 row(s) from combined table")

    # ── Primary knockdown table: merge ───────────────────────────────────────
    print(f"\nMerging {len(primary_rows)} primary row(s)...")
    merged_primary = _merge_primary_rows(primary_rows)
    _write_csv(merged_primary, output_path, PRIMARY_FIELDS, _NUMERIC_FIELDS_PRIMARY)
    print(f"Done. primary_table          → {output_path}  ({len(merged_primary)} rows)")

    # ── IC50 table: drop rows that carry no IC50 value ───────────────────────
    # (e.g. single-dose screens mis-routed here by the classifier emit empty
    #  ic50_nM rows — those hold no IC50 information and are discarded.)
    #  Control rows are retained.
    ic50_rows = [r for r in ic50_rows if r.get("ic50_nM") is not None]
    _write_csv(ic50_rows, ic50_path, IC50_FIELDS, _NUMERIC_FIELDS_IC50)
    print(f"     primary_ic50_table      → {ic50_path}  ({len(ic50_rows)} rows)")

    # ── Viability table: drop rows with no viability value (controls kept) ───
    viability_rows = [r for r in viability_rows
                      if r.get("viability_percent") is not None]
    _write_csv(viability_rows, viability_path, VIABILITY_FIELDS, _NUMERIC_FIELDS_VIABILITY)
    print(f"     primary_cell_viability  → {viability_path}  ({len(viability_rows)} rows)")

    # ── Failed-tables manifest ───────────────────────────────────────────────
    # Always written (even when empty) so its presence is deterministic. Lists
    # every table that produced no usable output and why, so a targeted re-run
    # (which reuses cached SQL for everything that succeeded) can recover them.
    manifest_path = os.path.join(output_dir, f"failed_tables{suffix}.csv")
    _MANIFEST_FIELDS = ["patent_id", "source_file", "table_type", "stage", "reason"]
    with open(manifest_path, "w", newline="", encoding="utf-8") as mf:
        w = csv.DictWriter(mf, fieldnames=_MANIFEST_FIELDS)
        w.writeheader()
        w.writerows(_FAILED[0])
    if _FAILED[0]:
        print(f"     failed_tables           → {manifest_path}  "
              f"({len(_FAILED[0])} table(s) need re-running)")
    else:
        print(f"     failed_tables           → {manifest_path}  (none — all tables produced output)")

    # ── Value-validation manifest ────────────────────────────────────────────
    # Every output cell that was blocked because it did not fit its column.
    # Always written so its presence is deterministic.
    vmanifest_path = os.path.join(output_dir, f"validation_failures{suffix}.csv")
    _VFIELDS = ["patent_id", "source_file", "column", "value", "reason"]
    with open(vmanifest_path, "w", newline="", encoding="utf-8") as vf:
        w = csv.DictWriter(vf, fieldnames=_VFIELDS)
        w.writeheader()
        w.writerows(_VALIDATION_FAILURES[0])
    if _VALIDATION_FAILURES[0]:
        print(f"     validation_failures     → {vmanifest_path}  "
              f"({len(_VALIDATION_FAILURES[0])} value(s) blocked — review)")
    else:
        print(f"     validation_failures     → {vmanifest_path}  (none — all values fit their columns)")