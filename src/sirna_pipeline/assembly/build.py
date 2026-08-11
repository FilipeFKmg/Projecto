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
   viability_value      – viability as reported in the patent (raw, NOT rescaled;
                          interpret via viability_basis)
   viability_sd         – standard deviation of viability_value
   viability_basis      – what the value is relative to (controlled vocabulary,
                          e.g. fraction_of_non_targeting); 'unknown' if unstated
   viability_relative_to– reference compound the value is normalised to
                          (e.g. AD-1955); NULL if none/unknown
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
import json
import os
import re
import time
import traceback
from datetime import datetime

from core import *
from classification import *
from sql import *

__all__ = ['_detect_transfection_method', '_NON_GENE_TOKENS', '_CONTROL_GENES', '_resolve_patent_target_gene', '_VIABILITY_BASIS_VALUES', '_resolve_viability_basis', '_apply_target_gene', '_is_seq_only', '_is_oligo_map_only', '_merge_primary_rows', '_ml_clean', '_write_csv', '_filter_files_by_prefix', 'build_primary_table', '_GENE_CONTROL_RE', '_clean_gene_value', '_propagate_patent_gene', '_process_table_group']


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


# Upper-case tokens that look like a gene symbol but are assay / lab / format
# terms — used to reject such look-alikes when verifying a resolved gene.
_NON_GENE_TOKENS = frozenset({
    "RNA", "DNA", "SIRNA", "DSRNA", "MRNA", "SHRNA", "MIRNA", "CRNA", "NTC", "ASO",
    "GAPMER", "IC50", "EC50", "CC50", "TC50", "GI50", "KD", "KI", "PBS", "HBSS",
    "DMEM", "FBS", "FCS", "DMSO", "HELA", "HEPG2", "HEP3B", "HEK", "COS", "CHO",
    "NIH", "HUH", "HUH7", "PHH", "UTR", "ORF", "CDS", "NT", "BP", "KB", "MB", "AD",
    "SD", "SEM", "ID", "NO", "PD", "PK", "IV", "SC", "IP", "PO", "US", "EP", "WO",
    "JP", "CN", "KR", "DE", "GB", "FR", "EDTA", "ATP", "GTP", "PCR", "QPCR",
    "ELISA", "FACS", "GFP", "LNP", "GALNAC", "PS", "OME", "MOE", "LNA", "UNA",
    "FANA", "CET", "SEQ", "FIG", "TABLE", "NM", "UM", "PM", "MM", "FM", "UG", "MG",
    "NG", "PCT", "CSV", "XML", "API", "RT", "PBMC", "SAR", "WT", "KO", "UPLC",
    "HPLC", "LCMS", "PEG", "AAV", "CMV",
})

# Genes that are siRNA CONTROLS, not therapeutic targets — used to reject a
# resolver answer that mistakenly names a control gene.
_CONTROL_GENES = frozenset({
    "PLK1", "KIF11", "EG5", "GAPDH", "LUCIFERASE", "LUC", "PPIB", "SSB", "AHA1",
    "ACTB", "BACTIN", "HPRT", "HPRT1", "TUBB", "B2M", "SCRAMBLED", "NTC", "NEG",
})


def _resolve_patent_target_gene(context_texts: list[str], trace_file: str) -> str | None:
    """Resolve the SINGLE therapeutic target gene for a patent from its table
    titles/captions, using the LLM as an identifier.

    Free-text gene extraction by regex cannot tell the target gene from a control
    gene (PLK1, luciferase), a disease/cell-line word (uveal melanoma), an assay
    term (IC50), or an OCR typo — and a single screen table mixes target duplexes
    with control duplexes, so no one gene labels a whole table. Identifying the
    target is a judgment task, so the LLM reads the collected titles and names the
    one target. The answer is verified: it must be a plausible symbol, must not be
    a known control gene, and must actually appear in the titles (no
    hallucination). Returns None when unsure or multi-target, leaving the existing
    per-table genes and propagation untouched."""
    titles: list[str] = []
    for ct in context_texts:
        for ln in (ct or "").splitlines():
            ln = ln.strip()
            if ln:
                titles.append(ln[:200])
    if not titles:
        return None
    joined = "\n".join(dict.fromkeys(titles))[:6000]   # dedupe, preserve order, cap

    prompt = f"""The lines below are TITLES and captions of tables from ONE patent that
describes siRNA / dsRNA molecules. Identify the SINGLE human gene that the
siRNAs are designed to silence — the therapeutic target.

RULES:
- Output ONLY one JSON object: {{"gene": "SYMBOL"}} or {{"gene": null}}.
- Use the official upper-case gene symbol (e.g. ANGPTL3, GNAQ, PCSK9, TTR).
- IGNORE control siRNAs and their genes (PLK1, KIF11, luciferase, GAPDH,
  non-targeting / scrambled) — a control is NOT the target.
- IGNORE cell-line names, disease names (e.g. uveal melanoma), and assay terms
  (IC50, EC50, viability).
- If there are several distinct targets, or you cannot tell, return {{"gene": null}}.

Titles:
{joined}"""

    raw = _llm_chat(prompt, max_tokens=80)
    log_trace(trace_file, "PATENT GENE RESOLVER RAW", raw)
    obj = _parse_json_object(raw)
    if not obj:
        return None
    gene = obj.get("gene")
    if not isinstance(gene, str):
        return None
    gene = gene.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9]{1,9}", gene):
        return None                                   # not a plausible symbol
    if gene in _CONTROL_GENES or gene in _NON_GENE_TOKENS:
        return None                                   # it named a control / non-gene
    if gene not in joined.upper():
        return None                                   # not actually in the titles
    return gene


# Controlled vocabulary for viability_basis — a small fixed set so the column is
# groupable/filterable across the corpus (free text would fragment into many
# spellings and become useless for ML). The pipeline NEVER rescales the value;
# this label is what lets a downstream, versioned transform normalise correctly
# per row (e.g. fraction→percent only where basis says fraction).
_VIABILITY_BASIS_VALUES = (
    "fraction_of_non_targeting",   # value / non-targeting control; control ≈ 1.0
    "percent_of_non_targeting",    # value / non-targeting control × 100; ≈ 100
    "fraction_of_mock",            # relative to mock / untreated cells; ≈ 1.0
    "percent_of_mock",             # relative to mock / untreated cells; ≈ 100
    "raw",                         # absolute readout (RLU / fluorescence / OD / counts)
    "unknown",                     # context does not state a normalisation
)


_VIAB_SENTENCE_RE = re.compile(
    r"viab|viable|celltiter|cell ?titer|cytotox|cell surviv", re.IGNORECASE)
_VIAB_NORM_RE = re.compile(
    r"normali[sz]|relative to|expressed as|% ?viab", re.IGNORECASE)


def _scope_viability_context(context_text: str) -> str:
    """Return only the viability-relevant sentences of a table's context.

    A single table's context often describes SEVERAL assays — mRNA knockdown, IC50,
    and viability — each normalised to a DIFFERENT control. Feeding the whole text to
    _resolve_viability_basis lets a prominent knockdown/IC50 reference (typically
    AD-1955, stated first and repeatedly) outweigh the viability-specific one (often
    mock), so the basis gets mislabelled. Keeping only the sentences that actually
    discuss viability removes those competing normalisation statements, so the model
    sees just the viability assay's own reference.

    Conservative: only returns the narrowed text when it still contains a
    normalisation statement; otherwise it returns the full context unchanged, so the
    result is never worse than passing everything."""
    ctx = (context_text or "").strip()
    if not ctx:
        return ctx
    sentences = re.split(r"(?<=[.!?])\s+", ctx)
    kept = [s for s in sentences if _VIAB_SENTENCE_RE.search(s)]
    scoped = " ".join(kept).strip()
    if scoped and _VIAB_NORM_RE.search(scoped):
        return scoped
    return ctx


def _resolve_viability_basis(context_text: str,
                             trace_file: str) -> tuple[str, str | None]:
    """Identify WHAT a viability table's values are normalised against, as a
    label — never a transformed value.

    Mirrors _resolve_patent_target_gene: the LLM reads the assay description and
    picks from a fixed vocabulary; the named reference compound (e.g. 'AD-1955')
    is returned separately and verified to actually occur in the text (no
    hallucinated reference). Returns ('unknown', None) when the context is silent
    — for a foundation-model dataset an honest gap beats a guess, since a guessed
    basis would systematically mislabel a normalisation the model then learns."""
    ctx = _scope_viability_context(context_text)
    if not ctx:
        return "unknown", None

    allowed = ", ".join(_VIABILITY_BASIS_VALUES)
    prompt = f"""The text below describes how CELL VIABILITY was measured and normalised in
ONE siRNA/dsRNA patent assay. Identify what the reported viability numbers are
expressed RELATIVE TO. Do NOT compute or transform any value.

Return ONLY one JSON object:
  {{"basis": "<one of: {allowed}>", "relative_to": "<reference compound or null>"}}

Guidance:
- "normalized to / compared to a non-targeting (scrambled / negative-control)
  duplex" → fraction_of_non_targeting (values near 1.0) OR
  percent_of_non_targeting (values near 100). Judge fraction vs percent from the
  scale the text describes.
- "relative to mock / untreated / naive / vehicle cells" → fraction_of_mock or
  percent_of_mock (same fraction-vs-percent judgement).
- absolute readout (raw RLU / fluorescence / OD / cell counts) → raw.
- if no normalisation basis is stated → unknown.
- relative_to: the named control/reference duplex (e.g. "AD-1955"), else null.

Assay description:
{ctx[:3000]}"""

    raw = _llm_chat(prompt, max_tokens=80)
    log_trace(trace_file, "VIABILITY BASIS RAW", raw)
    obj = _parse_json_object(raw)
    if not isinstance(obj, dict):
        return "unknown", None

    basis = str(obj.get("basis") or "").strip().lower()
    if basis not in _VIABILITY_BASIS_VALUES:
        basis = "unknown"

    ref = obj.get("relative_to")
    if isinstance(ref, str):
        ref = ref.strip()
        # Reject empties and any reference the model invented (must be in text).
        if ref.lower() in ("", "null", "none", "n/a") or ref.upper() not in ctx.upper():
            ref = None
    else:
        ref = None

    return basis, ref


def _apply_target_gene(gene: str, *row_lists: list[dict]) -> int:
    """Set the patent's single target gene on every NON-CONTROL duplex.

    Control duplexes (detected by _CONTROL_ID_RE on the duplex_id) are left
    untouched — a non-targeting control silences nothing, and a positive control
    such as a PLK1 siRNA targets its own gene, not the patent's target. Because a
    patent has one therapeutic target, this overrides any per-table value that
    disagrees (e.g. a control gene wrongly read from a caption)."""
    n = 0
    for rows in row_lists:
        for r in rows:
            if _CONTROL_ID_RE.search(str(r.get("duplex_id") or "")):
                continue
            if r.get("target_gene_name") != gene:
                r["target_gene_name"] = gene
                n += 1
    return n


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


def _stage_dirs(root: str) -> dict[str, str]:
    """The run's outputs are split into ONE FOLDER PER STAGE, so finished data,
    the review pile, per-file drafts, and the decision trail never sit together:

        <root>/1_final_tables/    the three finished CSVs per patent (the dataset)
        <root>/2_review/          failed_tables / validation_failures / flagged_rows
        <root>/3_per_file_drafts/ each input file's extraction, before merging
        <root>/4_trace/           run log, per-table logs, gene log, sql_cache/

    So "the dataset" (stage 1), "what to check by hand" (stage 2), the rough
    per-file drafts (stage 3), and "how it decided" (stage 4) are each in their
    own place. Created on demand; safe to call repeatedly.
    """
    dirs = {
        "final":  os.path.join(root, "1_final_tables"),
        "review": os.path.join(root, "2_review"),
        "drafts": os.path.join(root, "3_per_file_drafts"),
        "trace":  os.path.join(root, "4_trace"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    return dirs


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
    # Everything this run writes is organised into one folder per stage.
    stages = _stage_dirs(output_dir)

    session_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    _SESSION_LOG[0] = os.path.join(stages["trace"], f"session_{session_ts}.log")

    # Enable the on-disk SQL cache so re-runs reuse already-generated SQL and
    # only the previously-failed tables consume API calls. Lives in the trace
    # stage; safe to delete to force a clean regeneration.
    _CACHE_DIR[0] = os.path.join(stages["trace"], "sql_cache")
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
                crash_manifest = os.path.join(stages["review"],
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


# A target_gene_name cell that is really a control / role label, not a gene.
# The LLM sometimes drops these into the gene column for control wells, e.g.
# "(+) control", "(-) control", "mock", "non-targeting". They must not be read
# as genes, nor be overwritten WITH the target gene (a control does not silence
# it: positive controls hit a different gene, negative controls hit nothing).
_GENE_CONTROL_RE = re.compile(
    r'^[\(\[]?\s*[+\-]?\s*[\)\]]?\s*'
    r'(control|ctrl|mock|pbs|untreated|na[iï]ve|vehicle|buffer|blank|'
    r'scrambl\w*|non[\s_\-]?targeting|neg(ative)?|pos(itive)?)\b',
    re.IGNORECASE,
)


def _clean_gene_value(value) -> str | None:
    """Return a tidy gene symbol, or None when the cell is blank or holds a
    control/role label instead of a gene.

    Trims stray surrounding whitespace and footnote dots (e.g. 'ANGPTL3 .' ->
    'ANGPTL3'), and maps control labels ('(+) control', 'mock', ...) to None."""
    if value is None:
        return None
    s = re.sub(r'^[\s.]+|[\s.]+$', '', str(value))     # drop edge spaces/dots
    s = re.sub(r'\s+', ' ', s)
    if not s or _GENE_CONTROL_RE.match(s):
        return None
    return s


def _propagate_patent_gene(*row_lists: list[dict]) -> None:
    """Normalise the gene column and fill a missing target_gene_name across the
    three tables of ONE patent.

    Two steps, both mutating the row dicts in place:

    1. CLEAN every gene cell with _clean_gene_value: footnote-marked symbols are
       repaired ('ANGPTL3 .' -> 'ANGPTL3') and control/role labels ('(+) control',
       'mock', ...) are blanked, because they are not genes.

    2. FILL the patent's single target gene onto experimental rows that are still
       blank — closing gaps in the IC50/viability tables (which get no other gene
       enrichment) and in any primary row the duplex-ID join did not reach.

    A row is treated as a CONTROL — and so is NEVER given the target gene — when
    its gene cell held a control label or its duplex_id is a known control
    (AD-1955, mock, PBS, ...). This keeps positive/negative-control wells from
    being mislabelled with the silenced target. Filling acts only when the
    experimental rows agree on one gene; otherwise a warning is printed and only
    the cleaning is applied.
    """
    flagged: list[tuple[dict, bool]] = []      # (row, is_control)
    real_genes: list[str] = []

    for rows in row_lists:
        for r in rows:
            raw     = r.get("target_gene_name")
            cleaned = _clean_gene_value(raw)
            is_control = (
                (raw not in (None, "") and cleaned is None)        # was a control label
                or bool(_CONTROL_ID_RE.search(str(r.get("duplex_id") or "")))
            )
            r["target_gene_name"] = cleaned                        # step 1: clean in place
            flagged.append((r, is_control))
            if cleaned is not None and not is_control:
                real_genes.append(cleaned)

    if not real_genes:
        return

    if len({g.casefold() for g in real_genes}) > 1:
        print(f"  [gene] multiple target genes present {sorted(set(real_genes))}; "
              f"cleaned the column but skipping gene fill.")
        return

    # One agreed gene: pick its most frequent spelling (ties → first seen).
    counts: dict[str, int] = {}
    for g in real_genes:
        counts[g] = counts.get(g, 0) + 1
    gene = max(counts, key=counts.get)

    filled = 0
    for r, is_control in flagged:                                  # step 2: fill
        if not is_control and r.get("target_gene_name") in (None, ""):
            r["target_gene_name"] = gene
            filled += 1
    if filled:
        print(f"  [gene] propagated '{gene}' to {filled} experimental row(s) "
              f"across all tables (control rows left blank).")


def _process_table_group(
    csv_files:    list[str],
    output_path:  str,
    per_file_dir: str | None,
    group_label:  str | None = None,
    group_idx:    int | None = None,
    group_total:  int | None = None,
) -> None:
    """Extract the three siRNA tables from one set of input files and write them,
    organised by stage: finished tables -> 1_final_tables, review files ->
    2_review, per-file drafts -> 3_per_file_drafts, logs -> 4_trace."""
    root   = os.path.dirname(os.path.abspath(output_path)) or "."
    stages = _stage_dirs(root)
    primary_stem = os.path.splitext(os.path.basename(output_path))[0]
    suffix = re.sub(r"^primary_table", "", primary_stem)
    # Stage 1 — the finished dataset.
    primary_path   = os.path.join(stages["final"], f"primary_table{suffix}.csv")
    ic50_path      = os.path.join(stages["final"], f"primary_ic50_table{suffix}.csv")
    viability_path = os.path.join(stages["final"], f"primary_cell_viability_table{suffix}.csv")
    # Stage 2/3/4 — review pile, per-file drafts, decision trail.
    review_dir = stages["review"]
    trace_dir  = stages["trace"]
    # Drafts always nest under the run folder; per_file_dir (kept for back-compat)
    # can still redirect them elsewhere if explicitly given.
    drafts_dir = per_file_dir or stages["drafts"]

    tag = f"{group_label} " if group_label else ""
    n_tables = len(csv_files)

    # Fresh failed-tables manifest for this patent group.
    _FAILED[0] = []
    # Fresh value-validation manifest for this patent group.
    _VALIDATION_FAILURES[0] = []

    primary_rows:    list[dict] = []
    ic50_rows:       list[dict] = []
    viability_rows:  list[dict] = []
    flagged_rows:    list[dict] = []   # quarantined rows (immune / in-vivo disagreement)
    context_texts:   list[str]  = []   # titles/captions, for patent-level gene resolution

    for t_i, csv_file in enumerate(csv_files, start=1):
        base     = os.path.splitext(os.path.basename(csv_file))[0]
        file_log = os.path.join(trace_dir, f"{base}.log")
        print(f"\n[{tag}table {t_i}/{n_tables}] Processing: {os.path.basename(csv_file)}")

        ctx_file     = csv_file.replace("_tables.csv", "_context.txt")
        context_text = ""
        if os.path.exists(ctx_file):
            with open(ctx_file, encoding="utf-8") as _f:
                context_text = _f.read()
        context_texts.append(context_text)

        # Decide the measurement TYPE(S). An LLM reads the title / NOTE lines /
        # column names (never the data values) and returns the set of
        # measurements present; the deterministic detectors are the fallback.
        # The base type drives the primary extractor; any other measurement is
        # picked up by the multi-routing step below. See _classify_measurements.
        headers      = _read_csv_headers(csv_file)
        measurements = _classify_measurements(headers, context_text, file_log)
        table_type   = measurements["base"]
        print(f"  Table type: {table_type}  "
              f"(measurements={sorted(measurements['types']) or 'none'}, "
              f"via {measurements['source']})")
        log_trace(file_log, "TABLE TYPE DETECTED",
                  f"base={table_type} types={sorted(measurements['types'])} "
                  f"source={measurements['source']}")

        # Deterministic cross-checks, advisory. If the LLM routed this table to
        # knockdown but its columns look like cytokine/immune readouts or an
        # in-vivo animal study, the signals disagree. Rather than silently trusting
        # the LLM (and polluting the in-vitro knockdown table) or silently dropping
        # the rows (a wrong rule could delete good data), we QUARANTINE the table's
        # rows: they go to flagged_rows.csv instead of the main tables, and a record
        # is added to the failed_tables manifest. Nothing is lost — the rows are
        # kept, just held aside for review — and the main dataset stays clean.
        flag_reasons: list[str] = []
        if "knockdown" in measurements["types"] and _looks_like_immune_table(headers):
            flag_reasons.append("columns look like immune/cytokine readouts "
                                "(IFN/TNF/IL…), not target knockdown")
            print("  ⚠ quarantined: knockdown vs immune-readout disagreement")
            log_trace(file_log, "TYPE DISAGREEMENT",
                      "LLM=knockdown but deterministic sniff=immune readouts")
        if "knockdown" in measurements["types"] and _looks_like_invivo_table(headers):
            flag_reasons.append("columns look like an in-vivo animal study "
                                "(mg/kg dosing, tissue, or species)")
            print("  ⚠ quarantined: in-vivo columns in a knockdown table")
            log_trace(file_log, "INVIVO DISAGREEMENT",
                      "LLM=knockdown but deterministic sniff=in-vivo study")
        flag_reason = "; ".join(flag_reasons)
        if flag_reason:
            _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                           table_type, "quarantined_review",
                           f"rows quarantined to flagged_rows.csv — {flag_reason}; "
                           "verify before reinstating into the dataset")

        # Flag A — IC50 / viability data possibly sitting in the KNOCKDOWN table.
        # Column-based, so it works even when the title has no words: the rows went
        # to the knockdown extractor, but a column marks IC50/EC50 or viability AND
        # that measurement was NOT separately extracted. A genuine mixed table (the
        # other type IS in `types`, handled by multi-routing) is deliberately left
        # alone, so this fires only on a real un-extracted mismatch. Flagged for
        # review, NOT quarantined: the knockdown rows may well be correct, so they
        # are surfaced rather than removed.
        if table_type == "primary" and "knockdown" in measurements["types"]:
            mismatch = None
            if _has_ic50_column(headers) and "ic50" not in measurements["types"]:
                mismatch = "IC50/EC50"
            elif _has_viability_column(headers) and "viability" not in measurements["types"]:
                mismatch = "viability"
            if mismatch:
                _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                               table_type, "wrong_type_in_knockdown_review",
                               f"routed to knockdown but a {mismatch} column is present "
                               f"and was not extracted as its own type — values in the "
                               f"knockdown table may actually be {mismatch}")
                print(f"  ⚠ flagged: un-extracted {mismatch} column in a "
                      "knockdown-routed table")
                log_trace(file_log, "TYPE MISMATCH",
                          f"knockdown-routed but has un-extracted {mismatch} column")

        # Flag B — KNOCKDOWN data possibly MISSING because the table went to
        # IC50/viability. The table was routed to ic50/viability, knockdown was NOT
        # among its types, yet a column is explicitly an inhibition/knockdown
        # readout — so knockdown data may never have been extracted. Nothing to
        # quarantine (the failure is absence, not a wrong row present), so this is a
        # manifest flag only.
        if (table_type in ("ic50", "viability")
                and "knockdown" not in measurements["types"]
                and _has_explicit_knockdown_column(headers)):
            _record_failed(_derive_patent_id(csv_file), os.path.basename(csv_file),
                           table_type, "missing_knockdown_review",
                           f"routed to {table_type} but has an explicit knockdown / "
                           "inhibition column — knockdown data may not have been "
                           "extracted; verify nothing was lost")
            print(f"  ⚠ flagged: knockdown column in a {table_type}-routed table "
                  "(possible missing data)")
            log_trace(file_log, "MISSING KNOCKDOWN",
                      f"{table_type}-routed but has an explicit knockdown column")

        txn_method = _detect_transfection_method(context_text)

        # Viability provenance (what the values are normalised against + the
        # reference compound). Resolved once per viability table and stamped on
        # its rows like transfection_method — a label, never a transformed value.
        # The value column itself stays raw. Skipped for non-viability tables so
        # no LLM call is wasted on them.
        viab_basis, viab_ref = "unknown", None
        if "viability" in measurements["types"]:
            viab_basis, viab_ref = _resolve_viability_basis(context_text, file_log)
            log_trace(file_log, "VIABILITY BASIS",
                      f"basis={viab_basis} relative_to={viab_ref}")

        print("  Generating SQL...")
        sql_query = generate_sql_query(
            csv_file, context_text, file_log, table_type,
            knockdown_expected="knockdown" in measurements["types"])

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
        if table_type == "viability":
            for r in rows:
                r["viability_basis"]        = viab_basis
                r["viability_relative_to"]  = viab_ref

        if rows:
            os.makedirs(drafts_dir, exist_ok=True)
            per_path = os.path.join(drafts_dir, f"{base}_{table_type}.csv")
            _write_csv(rows, per_path, fields, numeric_fields)
            print(f"  → per-file CSV: {per_path}")

        if flag_reason:
            for r in rows:
                r["flag_reason"] = flag_reason
            flagged_rows.extend(rows)
        elif table_type == "ic50":
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
        if table_type in ("primary", "ic50", "viability"):  # any table — rescue seqs even from IC50/viability tables
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
                if flag_reason:
                    for r in seq_rows:
                        r["flag_reason"] = flag_reason
                    flagged_rows.extend(seq_rows)
                else:
                    primary_rows.extend(seq_rows)
                print(f"  → +{len(seq_rows)} sequence row(s) rescued from combined table")

        # MULTI-ROUTING: a physical table can carry more than one measurement (a
        # dose-response screen may report BOTH % knockdown and IC50). The main
        # extraction above handled `table_type`; here we run an extra extraction
        # for every OTHER measurement the classifier found, so nothing is lost.
        # "knockdown" maps to the 'primary' extractor. A knockdown extraction is
        # added ONLY when the classifier saw real knockdown data — never
        # fabricated from an IC50 (that was the (100 - IC50)*100 bug).
        mset = measurements["types"]
        extra_types = []
        if table_type != "ic50"      and "ic50"      in mset: extra_types.append("ic50")
        if table_type != "viability" and "viability" in mset: extra_types.append("viability")
        if table_type != "primary"   and "knockdown" in mset: extra_types.append("primary")

        for xt in extra_types:
            print(f"  Combined table — also extracting [{xt}]...")
            log_trace(file_log, "MULTI-ROUTE", f"also extracting {xt}")
            x_sql = generate_sql_query(csv_file, context_text, file_log, xt)
            if not x_sql or x_sql == "__SKIP__":
                continue
            if xt == "ic50":
                x_fields, x_numeric = IC50_FIELDS, _NUMERIC_FIELDS_IC50
            elif xt == "viability":
                x_fields, x_numeric = VIABILITY_FIELDS, _NUMERIC_FIELDS_VIABILITY
            else:
                x_fields, x_numeric = PRIMARY_FIELDS, _NUMERIC_FIELDS_PRIMARY
            x_rows = _execute_sql(x_sql, csv_file, os.path.basename(csv_file),
                                  file_log, x_fields, x_numeric)
            x_rows = [_normalize_row(r) for r in x_rows]
            x_rows = _validate_rows(x_rows, x_fields, x_numeric,
                                    os.path.basename(csv_file), file_log)
            for r in x_rows:
                r["transfection_method"] = txn_method
            if xt == "viability":
                for r in x_rows:
                    r["viability_basis"]       = viab_basis
                    r["viability_relative_to"] = viab_ref
            if x_rows:
                os.makedirs(drafts_dir, exist_ok=True)
                xp = os.path.join(drafts_dir, f"{base}_{xt}.csv")
                _write_csv(x_rows, xp, x_fields, x_numeric)
            if flag_reason:
                for r in x_rows:
                    r["flag_reason"] = flag_reason
                flagged_rows.extend(x_rows)
            else:
                (ic50_rows if xt == "ic50" else
                 viability_rows if xt == "viability" else
                 primary_rows).extend(x_rows)
            print(f"  → +{len(x_rows)} {xt} row(s) from combined table")

    # ── Patent-level gene fill ───────────────────────────────────────────────
    # Each patent targets ONE gene, but it may be named only in some table titles
    # (and a screen table mixes target duplexes with control duplexes). First ask
    # the LLM to identify the single target gene from the collected titles — it
    # can tell the target from controls / cell lines / assay terms, which a regex
    # cannot — then stamp it on every NON-CONTROL duplex. Controls keep their own
    # value. If the resolver is unsure it returns None and we fall back to the
    # consensus propagation over whatever per-table genes already exist.
    target_gene = _resolve_patent_target_gene(context_texts, os.path.join(trace_dir, "_gene_resolver.log"))
    if target_gene:
        applied = _apply_target_gene(target_gene, primary_rows, ic50_rows, viability_rows)
        print(f"  [gene] resolved target gene '{target_gene}' — set on "
              f"{applied} non-control row(s)")
    else:
        print("  [gene] target gene unresolved — using per-table genes + propagation")
    # Clean control-label genes and fill any remaining blanks on non-control rows.
    _propagate_patent_gene(primary_rows, ic50_rows, viability_rows)

    # ── Primary knockdown table: merge ───────────────────────────────────────
    print(f"\nMerging {len(primary_rows)} primary row(s)...")
    merged_primary = _merge_primary_rows(primary_rows)
    _write_csv(merged_primary, primary_path, PRIMARY_FIELDS, _NUMERIC_FIELDS_PRIMARY)
    print(f"Done. primary_table          → {primary_path}  ({len(merged_primary)} rows)")

    # ── IC50 table: drop rows that carry no IC50 value ───────────────────────
    # (e.g. single-dose screens mis-routed here by the classifier emit empty
    #  ic50_nM rows — those hold no IC50 information and are discarded.)
    #  Control rows are retained.
    ic50_rows = [r for r in ic50_rows if r.get("ic50_nM") is not None]
    _write_csv(ic50_rows, ic50_path, IC50_FIELDS, _NUMERIC_FIELDS_IC50)
    print(f"     primary_ic50_table      → {ic50_path}  ({len(ic50_rows)} rows)")

    # ── Viability table: drop rows with no viability value (controls kept) ───
    viability_rows = [r for r in viability_rows
                      if r.get("viability_value") is not None]
    _write_csv(viability_rows, viability_path, VIABILITY_FIELDS, _NUMERIC_FIELDS_VIABILITY)
    print(f"     primary_cell_viability  → {viability_path}  ({len(viability_rows)} rows)")

    # ── Quarantined rows ─────────────────────────────────────────────────────
    # Rows from tables where the LLM said "knockdown" but a deterministic sniff
    # disagreed (immune readouts or an in-vivo study). Kept out of the tables
    # above so the in-vitro dataset stays clean, but written here in full (with
    # the reason) so nothing is lost and you can review / reinstate them. Only
    # written when non-empty to avoid a stray empty file in the common case.
    if flagged_rows:
        flagged_path = os.path.join(review_dir, f"flagged_rows{suffix}.csv")
        with open(flagged_path, "w", newline="", encoding="utf-8") as ff:
            w = csv.DictWriter(ff, fieldnames=_FLAGGED_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(flagged_rows)
        print(f"     flagged_rows            → {flagged_path}  "
              f"({len(flagged_rows)} row(s) quarantined for review)")

    # ── Failed-tables manifest ───────────────────────────────────────────────
    # Always written (even when empty) so its presence is deterministic. Lists
    # every table that produced no usable output and why, so a targeted re-run
    # (which reuses cached SQL for everything that succeeded) can recover them.
    manifest_path = os.path.join(review_dir, f"failed_tables{suffix}.csv")
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
    vmanifest_path = os.path.join(review_dir, f"validation_failures{suffix}.csv")
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
