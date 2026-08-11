"""classification.py — decide what a table measures (knockdown /
IC50 / viability) and run the deterministic safety sniffs (immune, in-vivo,
and the wrong-type / missing-knockdown flags). Imports core."""

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

__all__ = ['_VIABILITY_TITLE_RE', '_IC50_TITLE_RE', '_TABLE_TITLE_RE', '_TABLE_LINE_RE', '_classify_table', '_IC50_COL_RE', '_has_ic50_column', '_EC50_COL_RE', '_ic50_columns_present', '_guard_ic50_columns', '_KNOCKDOWN_NAMED_RE', '_AVG_VALUE_RE', '_has_viability_column', '_IMMUNE_COL_RE', '_looks_like_immune_table', '_INVIVO_COL_RE', '_INVITRO_CONTEXT_RE', '_looks_like_invivo_table', '_has_knockdown_data', '_has_explicit_knockdown_column', '_resolve_base_type', '_MEASUREMENT_LABELS', '_base_from_types', '_measurement_types_deterministic', '_build_table_type_prompt', '_llm_classify_types', '_classify_measurements', '_INVITRO_MARKER_RE', '_CLINICAL_MARKER_RE', '_skipped_table_has_activity', '_NONVALUE_COL_RE', '_detect_value_scale']


# ---------------------------------------------------------------------------
# Table-type detection
# ---------------------------------------------------------------------------

# Matches viability wording in a title ("cell viability") AND a snake_case column
# name ("viability_pct", "cell_viability"). Lookarounds treat _, -, space, and
# string ends as boundaries — a plain \b fails on underscores because underscore
# is a word character (so "viability_pct" would not match with \b).
_VIABILITY_TITLE_RE = re.compile(
    r'(?<![a-z0-9])(viab(?:le|ility)?|celltiter|cytotox\w*|% ?viable|'
    r'percent[\s_]+viable|cell[\s_]+(?:survival|death|viability))(?![a-z0-9])',
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


_IC50_COL_RE = re.compile(r'(?<![A-Za-z])ic\s*_?50', re.IGNORECASE)

def _has_ic50_column(headers: list[str]) -> bool:
    """True if the table carries a dedicated IC50 value column (e.g. 'ic50_nM')."""
    return any(_IC50_COL_RE.search(h) for h in headers)


# EC50 / ED50 potency column (the activator analogue of IC50). Counts as IC50-type
# evidence too, since these are routed to the same dose-response output.
_EC50_COL_RE = re.compile(r'(?<![A-Za-z])e[cd]\s*_?50', re.IGNORECASE)


def _ic50_columns_present(headers: list[str]) -> bool:
    """True only if a COLUMN actually holds IC50/EC50 potency values.

    An IC50 routing must be backed by such a column. A caption that merely mentions
    'IC50' is NOT sufficient evidence, because in EPO table layouts the IC50 caption
    of one table routinely sits adjacent to a DIFFERENT table — e.g. a multi-dose
    knockdown screen whose columns are '10nM_AVG', '0.1nM_AVG', .... Trusting the
    caption alone is exactly what copies those %-mRNA-remaining columns into the
    IC50 output as fabricated IC50 values."""
    return any(_IC50_COL_RE.search(h) or _EC50_COL_RE.search(h) for h in headers)


def _guard_ic50_columns(types: set[str], headers: list[str],
                        trace_file: str = "") -> set[str]:
    """Reconcile the 'ic50' routing with the table's actual columns, in BOTH
    directions. The deciding evidence is always a real IC50/EC50 value column —
    never the caption, which routinely bleeds in from a neighbouring table.

      • REMOVE a false IC50: tagged ic50 but no IC50/EC50 column exists → the claim
        came from caption bleed, so drop it (otherwise a knockdown %-remaining
        column gets mislabelled as IC50 nM).
      • ADD a missed IC50: an IC50/EC50 column IS present but ic50 was not tagged →
        the classifier overlooked real IC50 data, so add it. This is safe ONLY
        because IC50 column names vary little (ic50 / IC50 (nM) / ec50); it is
        deliberately NOT applied to knockdown or viability, whose column names are
        too varied for "no recognised column" to mean "no data".

    Any other measurement on the table (e.g. knockdown) is left untouched; if ic50
    is added to a table with no knockdown column, the knockdown pass simply skips
    it downstream. Both adjustments are logged so they stay auditable."""
    has_ic50_col = _ic50_columns_present(headers)
    if "ic50" in types and not has_ic50_col:
        log_trace(trace_file, "IC50 DROPPED — no IC50/EC50 column",
                  f"headers={headers}")
        print("  ⚠ ic50 claim not backed by an IC50/EC50 column — dropped "
              "(any knockdown reading is kept)")
        return {t for t in types if t != "ic50"}
    if "ic50" not in types and has_ic50_col:
        log_trace(trace_file, "IC50 ADDED — IC50/EC50 column present but unflagged",
                  f"headers={headers}")
        print("  ＋ ic50 added — an IC50/EC50 column is present but was not flagged")
        return types | {"ic50"}
    return types


# A column whose NAME marks it as knockdown/inhibition data (percent inhibition,
# knockdown, silencing, mRNA/message remaining, relative expression).
_KNOCKDOWN_NAMED_RE = re.compile(
    r'(inhib|knock[\s_]?down|silenc|remain|'
    r'(?:m|messen\w*|transcript)?rna[\s_]*(?:remain|level)|'
    r'percent[\s_]*(?:inhib|knock|silenc|remain)|'
    r'\bpct[\s_]*(?:inhib|knock|silenc|remain)|'
    r'(?:rel|relative|norm|normali[sz]ed)[\s_]*(?:expr|express|mrna|message|activity))',
    re.IGNORECASE,
)

# A dose-LABELLED average/value column (e.g. '10nM_AVG', '0.1nM_AVG', 'avg_10nm',
# 'mean', or a bare dose value like '10nM'/'500pM'). Knockdown screens routinely
# report activity in such columns with NO 'inhibition' keyword, so this is the
# second half of the knockdown-data test.
_AVG_VALUE_RE = re.compile(
    r'(\bavg\b|\bmean\b|_avg\b|\bavg_|_mean\b|\bmean_|'
    r'[\d.]+[\s_]?[np]m\b|\bconc[\s_]?\d)',
    re.IGNORECASE,
)


def _has_viability_column(headers: list[str]) -> bool:
    """True if a column name marks cell-viability data (viab/CellTiter/cytotox…)."""
    return any(_VIABILITY_TITLE_RE.search(h) for h in headers)


# Cytokine / innate-immune readout columns. A table whose value columns are these
# measures an IMMUNOSTIMULATION response (e.g. % IFN-alpha, % TNF-alpha, IL-6),
# NOT target knockdown. This is used only as a deterministic CROSS-CHECK against
# the LLM's table-type call: when the two disagree the table is flagged for human
# review (failed_tables manifest) — it never silently overrides the LLM, so a term
# this list does not know about simply means no flag, never a wrong drop.
_IMMUNE_COL_RE = re.compile(
    r'(?<![a-z0-9])('
    r'ifn|interferon|tnf|il[-_]?\d{1,2}|cytokine|chemokine|'
    r'cxcl\d|ccl\d|isg\d|oas\d?|pkr|mx[12]|ifit\d|'
    r'immunostim|innate'
    r')(?![a-z0-9])',
    re.IGNORECASE,
)


def _looks_like_immune_table(headers: list[str]) -> bool:
    """Deterministic sniff: do the columns look like cytokine / innate-immune
    readouts rather than target knockdown? Advisory only — used to cross-check the
    LLM, never to decide the table's fate."""
    return bool(headers) and any(_IMMUNE_COL_RE.search(h) for h in headers)


# In-vivo (animal study) column markers: dosing by body weight (mg/kg), a tissue
# the target mRNA was measured in (liver, kidney, serum…), or an animal/species.
# In-vivo knockdown is a DIFFERENT measurement from an in-vitro screen (mg/kg vs
# nM, tissue vs cell line) and usually does not belong in an in-vitro dataset.
# Like the immune sniff, this is advisory only: a disagreement with the LLM's
# call is flagged for review, never silently acted on. "day"/timepoint terms are
# deliberately excluded because in-vitro viability tables use day_x_ynm columns.
_INVIVO_COL_RE = re.compile(
    r'(?<![a-z0-9])('
    r'mg[-_/]?kg|mpk|'                                            # dose per body weight
    r'liver|hepatic|kidney|renal|spleen|serum|plasma|tumou?r|'    # tissues / matrices
    r'muscle|lung|jejunum|duodenum|'
    r'mouse|mice|murine|rat|cyno|cynomolgus|monkey|primate|nhp|'  # animals
    r'animal|in[-_]?vivo|subcutaneous'
    r')(?![a-z0-9])',
    re.IGNORECASE,
)

# In-vitro system markers. When one of these appears in a header, any animal/
# species word in that SAME header is describing the SOURCE of cultured cells
# (e.g. cynomolgus monkey hepatocytes), not a live-animal study — so it must not
# count as in-vivo evidence. Kept narrow on purpose: only unambiguous in-vitro
# systems, so true in-vivo tissue/matrix columns (liver, serum, mg/kg) still flag.
_INVITRO_CONTEXT_RE = re.compile(
    r'(?<![a-z])('
    r'hepatocyt|'                 # hepatocyte(s) — cultured primary cells
    r'in[-_ ]?vitro|'
    r'primary[-_ ]?cell|'
    r'cell[-_ ]?line|'
    r'cultured|'
    r'microsom'                   # microsomes — in-vitro metabolic prep
    r')',
    re.IGNORECASE,
)


def _looks_like_invivo_table(headers: list[str]) -> bool:
    """Deterministic sniff: do the columns look like an in-vivo animal study
    (mg/kg dosing, a tissue matrix, an animal/species) rather than an in-vitro
    screen? Advisory only — cross-checks the LLM, never decides the table's fate.

    A species/animal word is NOT in-vivo evidence when the SAME header also names
    an in-vitro system — most importantly 'hepatocyte'. 'IC50 in Cynomolgus monkey
    hepatocyte' is a cultured-primary-cell assay, not a live animal: the animal
    word only says where the cells came from. Without this, an in-vitro cyno/mouse
    hepatocyte column quarantines an entire otherwise-in-vitro table. Genuine
    in-vivo signals (mouse LIVER, monkey SERUM, mg/kg) carry no such cell marker
    and still flag."""
    for h in headers:
        if _INVIVO_COL_RE.search(h) and not _INVITRO_CONTEXT_RE.search(h):
            return True
    return False


def _has_knockdown_data(headers: list[str]) -> bool:
    """True if the table carries a genuine knockdown/inhibition measurement column.

    Knockdown value columns are too varied to enumerate by name (they range from
    'pct_inhibition' to a bare dose-labelled average like '10nM_AVG'), so we accept
    EITHER an explicit knockdown-named column OR a dose-labelled average/value
    column — while ignoring IC50/EC50 and viability columns, which are separate
    measurements with their own output tables. This is the test that stops a
    sequence/IC50-only table (no knockdown column) from being read as a knockdown
    screen and having a fake inhibition % invented from its IC50."""
    for h in headers:
        if _IC50_COL_RE.search(h) or _VIABILITY_TITLE_RE.search(h):
            continue
        if _KNOCKDOWN_NAMED_RE.search(h) or _AVG_VALUE_RE.search(h):
            return True
    return False


def _has_explicit_knockdown_column(headers: list[str]) -> bool:
    """True if a column is EXPLICITLY named as a knockdown/inhibition readout
    (inhibition, knockdown, silencing, % remaining…), as opposed to a bare
    dose-labelled average that IC50 and viability tables also carry. Used to flag a
    table routed to IC50/viability that may actually hold un-extracted knockdown
    data — a stricter signal than _has_knockdown_data to keep that flag precise."""
    return any(_KNOCKDOWN_NAMED_RE.search(h) for h in headers
               if not (_IC50_COL_RE.search(h) or _VIABILITY_TITLE_RE.search(h)))


def _resolve_base_type(headers: list[str], context_text: str) -> str:
    """Pick the BASE table type for the main extraction.

    The title/NOTE classifier is the prior; the COLUMNS overrule it only when they
    clearly disagree, which is deliberately conservative (when unsure we keep the
    title's answer, preserving previous behaviour):
      • a 'primary' default with NO knockdown column but an IC50 (or viability)
        column → 'ic50' (or 'viability');
      • an 'ic50'/'viability' title with NO matching column but a real knockdown
        column → 'primary'.
    Mixed tables (knockdown AND IC50, etc.) keep this base type and have the other
    measurement(s) extracted as well by the multi-routing step in the caller."""
    t = _classify_table(context_text)
    has_ic50 = _has_ic50_column(headers)
    has_viab = _has_viability_column(headers)
    has_kd   = _has_knockdown_data(headers)
    if t == "primary" and not has_kd:
        if has_ic50:
            return "ic50"
        if has_viab:
            return "viability"
    elif t == "ic50" and not has_ic50 and has_kd:
        return "primary"
    elif t == "viability" and not has_viab and has_kd:
        return "primary"
    return t


# ---------------------------------------------------------------------------
# LLM table-type classifier  (type/types only — never reads data values)
# ---------------------------------------------------------------------------
#
# A patent table is routed by WHICH measurement(s) it reports. The regex
# detectors above are a solid fallback but misread two recurring cases:
#   • a cell-VIABILITY table whose values sit in dose-named columns
#     ("1nM", "0.1nM") — the dose names trip the knockdown test and the table
#     is demoted to a knockdown screen (→ empty viability output);
#   • genuinely MIXED tables (e.g. a dose-response screen reporting BOTH
#     % knockdown AND an IC50) where a single base type loses one measurement.
#
# The LLM decides the measurement TYPE(S) from the title, the surrounding
# NOTE/context lines and the column NAMES only. It never sees or transcribes a
# data value, so the no-fabrication guarantee is unchanged: extraction stays
# fully deterministic (DuckDB SQL); the LLM only chooses which extractor(s) run.
_MEASUREMENT_LABELS = ("knockdown", "ic50", "viability")


def _base_from_types(types: set[str]) -> str:
    """Map a measurement-type set to the routing key the main loop branches on.

    Priority knockdown > ic50 > viability picks the PRIMARY extractor; any other
    measurement present is added by the multi-routing step. An empty set
    (sequence / other table) maps to 'primary' so the table still flows through
    the primary branch and its strand/sequence detectors — it is simply never
    forced into the knockdown extractor (no fabricated activity, no false
    0-row failure)."""
    if "knockdown" in types:
        return "primary"
    if "ic50" in types:
        return "ic50"
    if "viability" in types:
        return "viability"
    return "primary"


def _measurement_types_deterministic(headers: list[str], context_text: str) -> set[str]:
    """Rule-based fallback for the LLM classifier.

    Mirrors the column/title detectors but with the viability-vs-dose fix: a
    bare dose/avg column counts as knockdown evidence ONLY when there is no
    viability signal, because a viability screen reports its values in dose
    columns too. An explicit knockdown-NAMED column always counts."""
    title_type = _classify_table(context_text)            # primary | ic50 | viability
    has_ic50 = _has_ic50_column(headers) or title_type == "ic50"
    has_viab = _has_viability_column(headers) or title_type == "viability"
    has_kd_named = any(
        _KNOCKDOWN_NAMED_RE.search(h)
        for h in headers
        if not (_IC50_COL_RE.search(h) or _VIABILITY_TITLE_RE.search(h))
    )
    has_dose_or_avg = _has_knockdown_data(headers)        # named OR bare dose/avg

    types: set[str] = set()
    if has_viab:
        types.add("viability")
    if has_ic50:
        types.add("ic50")
    if has_kd_named:
        types.add("knockdown")
    elif has_dose_or_avg and not has_viab:
        # dose/avg value columns with no viability signal → a knockdown screen
        types.add("knockdown")
    return types


def _build_table_type_prompt(context_text: str, headers: list[str]) -> str:
    """Prompt the LLM to label a table's measurement type(s) from its title,
    NOTE/context lines and column NAMES only (no data values)."""
    # Keep the context compact: the title and any NOTE/caption lines carry
    # essentially all the semantic signal; trim the rest to bound tokens.
    lines = [ln.strip() for ln in context_text.splitlines() if ln.strip()]
    keep  = [ln for ln in lines
             if _TABLE_LINE_RE.match(ln) or ln.lower().startswith(("note:", "title:"))]
    ctx   = ("\n".join(keep) if keep else "\n".join(lines[:8]))[:1500]
    cols  = ", ".join(h for h in headers if h.strip()) or "(no column names)"

    return f"""\
You are routing a table extracted from an siRNA/dsRNA patent. Decide WHICH
quantitative measurement(s) it reports, using ONLY the title, the NOTE/context
lines and the COLUMN NAMES below. Do NOT infer from or transcribe any data value.

Allowed labels (return a JSON list — may be empty, may contain several):
  "knockdown"  - target-gene silencing efficacy: % inhibition, % knockdown,
                 % mRNA/message remaining, relative/normalised expression, or a
                 single-dose / dose-response in-vitro SCREEN whose value columns
                 are doses (e.g. "1nM", "10nM", "conc_1_nm", "10nM_avg").
  "ic50"       - a fitted potency value: IC50 / EC50 / ED50 (nM or pM). The
                 column name contains ic50/ec50/ed50.
  "viability"  - cell viability / cytotoxicity: CellTiter, % viable, cell
                 survival/death, normalised viability. The TITLE or NOTE says
                 "viability"/"cytotoxicity" even when the value columns are
                 named as doses (days x concentrations).

Return [] (empty) for tables that are none of the above, e.g. sequence
listings (sense/antisense/target sequences, SEQ ID NO, modified strands),
duplex-name/sample-name maps, abbreviation legends, immunostimulatory/cytokine
activity, or position tables.

Rules
- A table may report MORE THAN ONE measurement (e.g. a screen with both
  % knockdown columns AND an IC50 column -> ["knockdown","ic50"]). List all.
- VIABILITY is decided by the title/NOTE, NOT by the value columns: dose-named
  columns under a "cell viability" title are still "viability", never
  "knockdown".
- When the title is explicit it outranks an ambiguous column name.

Output ONLY a JSON object, no prose:
  {{"types": ["..."], "reason": "<short>"}}

## TITLE / NOTES / CONTEXT
{ctx}

## COLUMN NAMES
{cols}
"""


def _llm_classify_types(context_text: str, headers: list[str],
                        trace_file: str = "") -> set[str] | None:
    """Ask the LLM for the table's measurement type set. Returns a validated
    subset of _MEASUREMENT_LABELS (possibly empty), or None when the LLM is
    unavailable / unparseable / returns something unusable (→ caller falls back
    to the deterministic detectors)."""
    if not _CLIENTS:
        return None
    raw = _llm_chat(_build_table_type_prompt(context_text, headers), max_tokens=200)
    log_trace(trace_file, "LLM-TABLE-TYPE RAW", raw)
    obj = _parse_json_object(raw)
    if not isinstance(obj, dict) or not isinstance(obj.get("types"), list):
        return None
    _SYN = {"primary": "knockdown", "silencing": "knockdown", "inhibition": "knockdown",
            "ec50": "ic50", "ed50": "ic50", "potency": "ic50",
            "cytotoxicity": "viability", "viable": "viability"}
    types = {_SYN.get(str(t).strip().lower(), str(t).strip().lower()) for t in obj["types"]}
    return {t for t in types if t in _MEASUREMENT_LABELS}   # may be empty (other table)


def _type_cache_key(headers: list[str], context_text: str) -> str:
    """Stable key for a cached table-type decision. The decision depends ONLY on
    the headers and the title/caption (the LLM never sees data values), so those
    two inputs are a complete key — if either changes, we re-decide. NOTE: if the
    classification PROMPT itself is changed, delete the type_cache folder to force
    a clean re-decision (same rule as the SQL cache)."""
    h = hashlib.sha1()
    h.update("\x00".join(headers).encode("utf-8"))
    h.update(b"\x01")
    h.update(context_text.encode("utf-8"))
    return h.hexdigest()[:16]


def _read_type_cache(path: str) -> dict | None:
    """Load a cached decision, rebuilding the types set. None on any problem."""
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return {"types": set(obj["types"]), "base": obj["base"], "source": "cache"}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _write_type_cache(path: str, result: dict) -> None:
    """Persist a decision (types stored as a sorted list so it is JSON-clean)."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"types": sorted(result["types"]), "base": result["base"]}, f)
    except OSError:
        pass


def _classify_measurements(headers: list[str], context_text: str,
                           trace_file: str = "") -> dict:
    """Decide a table's measurement type(s) and routing base.

    LLM-first (type/types only — never reads data values); deterministic
    detectors as fallback. Returns
        {"types": set[str], "base": str, "source": "llm"|"fallback"|"cache"}
    with types ⊆ {knockdown, ic50, viability} and base ∈ {primary, ic50, viability}.

    The LLM decision is CACHED (a tiny JSON file per table, in the type_cache
    folder alongside sql_cache) so a re-run reuses it instead of re-hitting the
    API. Only a real LLM decision is cached — never a fallback, since a fallback
    means the API was down and the next run should try the LLM again. A cache entry
    counts as a hit ONLY if it pre-existed this run (see _cache_is_preexisting): a
    decision written for one table earlier in the SAME run is not reused by a later
    same-shaped table, so a single misclassification cannot propagate run-wide."""
    # Cache lookup (keyed on the inputs the LLM actually sees). Only a PRIOR-run
    # entry is a hit — a file this run just wrote is ignored here on purpose.
    cache_path = ""
    if _CACHE_DIR[0]:
        cache_dir  = os.path.join(os.path.dirname(_CACHE_DIR[0]), "type_cache")
        cache_path = os.path.join(cache_dir, f"{_type_cache_key(headers, context_text)}.json")
        hit = _read_type_cache(cache_path)
        if hit is not None and _cache_is_preexisting(cache_path):
            log_trace(trace_file, "TYPE CACHE HIT (prior run)", cache_path)
            # Re-apply the column guard even to a cached decision, so a stale entry
            # written before this guard existed is still corrected on read.
            types = _guard_ic50_columns(set(hit["types"]), headers, trace_file)
            return {"types": types, "base": _base_from_types(types), "source": "cache"}

    types  = _llm_classify_types(context_text, headers, trace_file)
    source = "llm"
    if types is None:
        types  = _measurement_types_deterministic(headers, context_text)
        source = "fallback"
    # Column-corroboration guard: an ic50 routing must be backed by an actual
    # IC50/EC50 column, never by a neighbouring table's IC50 caption.
    types  = _guard_ic50_columns(types, headers, trace_file)
    result = {"types": types, "base": _base_from_types(types), "source": source}

    # Persist only a confident LLM decision, so an API-outage fallback is retried.
    # The GUARDED types are written, so next run's cache is already corrected.
    if cache_path and source == "llm":
        _write_type_cache(cache_path, result)
    return result


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
