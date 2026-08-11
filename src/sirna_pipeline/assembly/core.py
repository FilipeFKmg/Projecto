"""core.py — shared foundation: config/schemas, the Groq client and key
rotation, CSV reading, ID/sequence canonicalisation, and value validation.
Imported by every other module; imports none of them."""

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
from groq import Groq

__all__ = ['_GROQ_MODEL', 'PRIMARY_FIELDS', 'IC50_FIELDS', 'VIABILITY_FIELDS', '_FLAGGED_FIELDS', '_SEQ_FIELDS', '_ASSAY_FIELDS', '_OLIGO_ID_FIELDS', '_ID_SUBSTRINGS', '_NUMERIC_FIELDS_PRIMARY', '_NUMERIC_FIELDS_IC50', '_NUMERIC_FIELDS_VIABILITY', '_CLIENTS', '_RATE_LIMITS', '_ACTIVE_IDX', '_SESSION_LOG', '_CACHE_DIR', '_CACHE_PREEXISTING', '_FAILED', '_VALIDATION_FAILURES', '_MAX_DOSE_NM', '_MAX_INHIBITION_PCT', '_MIN_INHIBITION_PCT', '_init_clients', '_active_client', '_active_key', '_is_available', '_record_rate_limit', '_next_available_idx', '_shortest_wait', '_parse_retry_after', 'log_trace', '_sql_cache_key', '_snapshot_existing_cache', '_cache_is_preexisting', '_record_failed', '_validate_rows', '_PLAIN_NUCL_RE', '_is_modified_sequence', '_best_sequence_value', '_CONTROL_ID_RE', '_derive_patent_id', '_strip_suffix', '_DUPLEX_CORE_RE', '_canonical_duplex_id', '_canonical_oligo_id', '_NOSPACE_COLS', '_normalize_row', '_read_csv_headers', '_looks_like_sequence', '_read_csv_sample', '_parse_json_object', '_llm_chat']


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
    "viability_value",
    "viability_sd",
    "viability_basis",
    "viability_relative_to",
    "transfection_method",
    "target_gene_name",
    "patent_id",
    "source_file",
]

# Schema for quarantined rows: every column any extractor can produce, plus the
# reason the row was held aside. A superset so a quarantined knockdown / IC50 /
# viability row all fit one review file without losing fields.
_FLAGGED_FIELDS = (
    ["flag_reason"]
    + list(dict.fromkeys(PRIMARY_FIELDS + IC50_FIELDS + VIABILITY_FIELDS))
)

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
_NUMERIC_FIELDS_VIABILITY  = {"day", "dose_nM", "viability_value", "viability_sd"}

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

# Snapshot of cache files that ALREADY existed on disk when this run started —
# i.e. entries produced by a PREVIOUS run. A cache entry counts as a hit ONLY if
# it is in this set. Entries written DURING the current run are deliberately NOT
# treated as hits, so two same-shaped tables in one run are each decided
# independently and a single (possibly wrong) decision can never propagate to the
# rest of the run. Re-running the same patent still reuses prior-run results.
# Wrapped in a one-element list so `from core import *` callers share one object.
_CACHE_PREEXISTING: list[set] = [set()]

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

# Plausible ceiling for a knockdown/inhibition percentage. 100% = complete
# knockdown; a reading can sit a little above that from assay noise, but a value
# in the hundreds or thousands is a mis-derived number (e.g. an IC50 wrongly
# pushed through a "(100 - value) * 100" formula), never a real measurement.
_MAX_INHIBITION_PCT = 200.0
# Lower bound. A negative inhibition is genuine gene upregulation and must be
# kept (e.g. a control fraction of 2.46 -> -146 %, observed in real data). But a
# value far below this floor cannot be real: it comes from a corrupt source cell
# whose decimal point was lost (e.g. "0.36" written as "036" -> read as 36 ->
# (1 - 36) * 100 = -3500 %). The floor is set well below the largest plausible
# upregulation so real outliers survive while corrupted cells are dropped. It is
# a backstop, not a full corruption filter: a dropped decimal that happens to
# stay in range cannot be detected here. Tune if a corpus shows stronger real
# upregulation.
_MIN_INHIBITION_PCT = -200.0


def _init_clients(api_keys: list[str] | str | None) -> None:
    # NOTE: these globals are shared across modules via `from core import *`, so we
    # MUST mutate the existing list/dict objects in place (clear + refill) rather
    # than rebind the names — a rebind would leave sql.py / the orchestrator holding
    # references to the original empty objects, breaking key rotation.
    if api_keys is None:
        env_key = os.environ.get("GROQ_API_KEY", "")
        keys = [env_key] if env_key else []
    elif isinstance(api_keys, str):
        keys = [k.strip() for k in api_keys.split(",") if k.strip()]
    else:
        keys = [k.strip() for k in api_keys if k.strip()]

    if not keys:
        raise ValueError("No Groq API key found. Set GROQ_API_KEY env var or pass --api_key.")

    _CLIENTS.clear()
    _CLIENTS.extend({"key": k, "client": Groq(api_key=k)} for k in keys)
    _RATE_LIMITS.clear()
    _ACTIVE_IDX[0] = 0
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


def _snapshot_existing_cache(*dirs: str) -> None:
    """Record every cache file already on disk BEFORE this run does any work.

    Call this ONCE at run start, after the cache directories are known. Only the
    files captured here count as cache hits (see _cache_is_preexisting): they are
    the output of a previous run, so reusing them simply resumes/skips work that
    already succeeded. Files this run creates afterwards are NOT in the snapshot,
    so a decision cached for one table is never silently reused for another table
    in the SAME run — which is what let a single misclassification spread across
    every structurally-identical table."""
    snap: set = set()
    for d in dirs:
        if d and os.path.isdir(d):
            for name in os.listdir(d):
                snap.add(os.path.abspath(os.path.join(d, name)))
    _CACHE_PREEXISTING[0] = snap


def _cache_is_preexisting(path: str) -> bool:
    """True only if `path` existed at run start (i.e. a prior-run cache entry).
    A file written earlier in THIS run is not pre-existing, so it is not reused
    within the run."""
    return os.path.abspath(path) in _CACHE_PREEXISTING[0]


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
    to the validation manifest. For inhibition % both an UPPER ceiling (non-
    physical mis-derived values such as an IC50 forced through a knockdown
    formula) and a LOWER floor (corrupt source cells with a dropped decimal point,
    e.g. "036" read as 36) are enforced; the floor sits well below the largest
    plausible upregulation so genuine negative-inhibition outliers are kept."""
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
            elif col == "inhibition_percent" and x > _MAX_INHIBITION_PCT:
                _flag(col, v, "inhibition % above physical ceiling — mis-derived "
                              "(e.g. an IC50 pushed through a knockdown formula)")
                row[col] = None
            elif col == "inhibition_percent" and x < _MIN_INHIBITION_PCT:
                _flag(col, v, "inhibition % below plausible floor — corrupt source "
                              "cell (e.g. a control fraction with a dropped decimal "
                              "point) pushed through the knockdown formula")
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

def _looks_like_sequence(v: str) -> bool:
    s = str(v).strip()
    if len(s) < 12:
        return False
    bases = sum(1 for c in s if c in "ACGUTacgut")
    return bases >= 8 and bases / len(s) > 0.4


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


def _parse_json_object(raw: str | None) -> dict | None:
    """Extract the first JSON object from an LLM reply, tolerating code fences."""
    if not raw:
        return None
    s = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    a, b = s.find("{"), s.rfind("}")
    if a == -1 or b == -1 or b < a:
        return None
    try:
        out = json.loads(s[a:b + 1])
        return out if isinstance(out, dict) else None
    except (ValueError, TypeError):
        return None


def _llm_chat(prompt: str, max_tokens: int = 512) -> str:
    """One chat completion via the shared Groq key pool, with the same
    rate-limit handling as the main SQL path. Returns raw text, or '' on
    repeated failure."""
    max_attempts = max(6, len(_CLIENTS) * 2)
    for _ in range(max_attempts):
        idx = _next_available_idx()
        if idx is None:
            time.sleep(_shortest_wait())
            idx = _next_available_idx() or 0
        _ACTIVE_IDX[0] = idx
        try:
            resp = _active_client().chat.completions.create(
                model=_GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0, max_tokens=max_tokens)
            return resp.choices[0].message.content.strip()
        except Exception as e:                       # noqa: BLE001
            err = str(e)
            if "429" in err or "rate_limit" in err.lower():
                _record_rate_limit(_active_key(), _parse_retry_after(err))
    return ""
