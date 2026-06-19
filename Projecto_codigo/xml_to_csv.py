"""
xml_to_csv.py — AI-enhanced edition with multi-key Groq rotation
=================================================================
Converts CALS-style XML patent files into TWO output files per XML:

  <base>_context.txt   – document title + extracted text paragraphs
  <base>_tables.csv    – SQL-normalised headers + full data rows (AI-powered via Groq)

AI model : llama-3.1-8b-instant  (Groq API, free-tier compatible)
Methodology: Header normalisation prompt adapted from the LLM data-extraction
             pipeline described in the supplementary material (Prompt 1).

Jupyter usage
-------------
    from xml_to_csv import convert_directory

    groq_api_key1 = "gsk_..."
    groq_api_key2 = "gsk_..."
    groq_api_key3 = "gsk_..."
    groq_api_key4 = "gsk_..."

    from xml_to_csv import convert_directory

    XML_DIR = "isolated_tables"
    CSV_DIR = "csv_output"

    convert_directory(
        XML_DIR,
        output_dir=CSV_DIR,
        api_keys=f"{groq_api_key1},{groq_api_key2},{groq_api_key3},{groq_api_key4}",
    )

CLI usage
---------
    python xml_to_csv.py <input_dir> [output_dir]
    (reads GROQ_API_KEY from the environment when no api_keys are passed)

Install dependency:
    pip install groq
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime

from groq import Groq


# ===========================================================================
# Key-rotation state  (module-level → persists across Jupyter calls in the
#                      same kernel session, so cooldown timers carry over)
# ===========================================================================

_CLIENTS:     list[dict]       = []   # [{"key": str, "client": Groq}, ...]
_RATE_LIMITS: dict[str, float] = {}   # api_key -> unix timestamp when usable again
_ACTIVE_IDX:  list[int]        = [0]  # mutable single-item list (avoids `global`)
_SESSION_LOG: list[str]        = [""]  # mutable box for the session-level debug log

_GROQ_MODEL        = "llama-3.1-8b-instant"
# Larger model used exclusively for Pass 1 (structural header repair).
# Empty cells in header rows signal a sparse / multi-level layout that
# needs reliable merging — the 70 B model handles this significantly better.
_REPAIR_GROQ_MODEL = "llama-3.3-70b-versatile"

# ---------------------------------------------------------------------------
# Tag sets used by the text-extraction logic
# ---------------------------------------------------------------------------

_TABLE_TAGS = frozenset({"tables", "table", "tgroup", "thead", "tbody", "row", "entry"})
_PARA_TAGS  = frozenset({"para", "p", "paragraph", "abstract", "body", "section", "sec"})


# ===========================================================================
# Client pool management
# ===========================================================================

def _init_clients(api_keys: list[str] | str | None) -> None:
    """
    Initialise (or re-initialise) the global client pool.

    Accepts
    -------
    api_keys : list of key strings
             | comma-separated string  "key1,key2,key3"
             | None  → falls back to the GROQ_API_KEY environment variable
    """
    global _CLIENTS, _RATE_LIMITS, _ACTIVE_IDX

    if api_keys is None:
        env_key = os.environ.get("GROQ_API_KEY", "")
        keys = [env_key] if env_key else []
    elif isinstance(api_keys, str):
        keys = [k.strip() for k in api_keys.split(",") if k.strip()]
    else:
        keys = [k.strip() for k in api_keys if k.strip()]

    if not keys:
        raise ValueError(
            "No Groq API key found. "
            "Pass api_keys= to convert_directory() or set the GROQ_API_KEY env var."
        )

    _CLIENTS     = [{"key": k, "client": Groq(api_key=k)} for k in keys]
    _RATE_LIMITS = {}
    _ACTIVE_IDX  = [0]
    print(f"  [Groq] {len(_CLIENTS)} API key(s) loaded.")


def _active_client() -> Groq:
    return _CLIENTS[_ACTIVE_IDX[0]]["client"]


def _active_key() -> str:
    return _CLIENTS[_ACTIVE_IDX[0]]["key"]


def _is_available(key: str) -> bool:
    """True if the key is not currently in its cooldown window."""
    return _RATE_LIMITS.get(key, 0.0) <= time.time()


def _record_rate_limit(key: str, seconds: float) -> None:
    """Mark *key* as unavailable for *seconds* from now."""
    _RATE_LIMITS[key] = time.time() + max(seconds, 1.0)


def _next_available_idx() -> int | None:
    """
    Return the index of the first non-rate-limited client, cycling through
    all clients in round-robin order starting after the currently active one.
    Returns None when every key is still cooling down.
    """
    n = len(_CLIENTS)
    for offset in range(1, n + 1):
        idx = (_ACTIVE_IDX[0] + offset) % n
        if _is_available(_CLIENTS[idx]["key"]):
            return idx
    # The current key itself may have cooled down in the meantime
    if _is_available(_active_key()):
        return _ACTIVE_IDX[0]
    return None


def _shortest_wait() -> float:
    """
    Seconds until the soonest rate-limited key becomes available.
    Returns 0.0 if at least one key is already free.
    A 0.5 s safety margin is added to avoid re-hitting the limit immediately.
    """
    now = time.time()
    pending = [exp for exp in _RATE_LIMITS.values() if exp > now]
    return (min(pending) - now + 0.5) if pending else 0.0


def _parse_retry_after(error_str: str) -> float:
    """
    Extract a wait duration (seconds) from a Groq rate-limit error message.

    Handles the formats:
        "Please try again in 19m9.12s"
        "Please try again in 1h2m30s"
        "Please try again in 83.5s"
        "retry_after: 60"

    Falls back to 60 s when no hint is present.
    """
    patterns = [
        (r"in\s+(\d+)h(\d+)m(\d+(?:\.\d+)?)s", "hr_min_sec"),  # "in 1h2m30s"
        (r"in\s+(\d+)h(\d+(?:\.\d+)?)\s*s",     "hr_sec"),      # "in 1h30.5s"
        (r"in\s+(\d+)h",                          "hr"),          # "in 2h"
        (r"in\s+(\d+)m(\d+(?:\.\d+)?)s",          "min_sec"),     # "in 1m23.5s"
        (r"in\s+(\d+(?:\.\d+)?)\s*s",             "sec"),         # "in 83.5s"
        (r"retry.after[\":\s]+(\d+)",              "sec"),         # "retry_after: 60"
    ]
    for pattern, fmt in patterns:
        m = re.search(pattern, error_str, re.IGNORECASE)
        if m:
            if fmt == "hr_min_sec":
                return float(m.group(1)) * 3600 + float(m.group(2)) * 60 + float(m.group(3))
            if fmt == "hr_sec":
                return float(m.group(1)) * 3600 + float(m.group(2))
            if fmt == "hr":
                return float(m.group(1)) * 3600
            if fmt == "min_sec":
                return float(m.group(1)) * 60 + float(m.group(2))
            return float(m.group(1))
    return 60.0   # conservative default


# ===========================================================================
# Trace / logging
# ===========================================================================

def log_trace(trace_file: str, label: str, content: str) -> None:
    """
    Append a timestamped entry to *trace_file* AND the session log.
    Writes to every unique non-empty path; no-op when both are empty.
    """
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] [{label}]\n{content}\n{'-' * 60}\n"
    seen: set[str] = set()
    for path in [trace_file, _SESSION_LOG[0]]:
        if path and path not in seen:
            seen.add(path)
            with open(path, "a", encoding="utf-8") as f:
                f.write(entry)


# ===========================================================================
# XML helpers
# ===========================================================================

def element_text(element) -> str:
    """Return clean, single-string text for an XML element."""
    if element is None:
        return ""
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        parts.append(element_text(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(" ".join(parts).split())


def get_text(element) -> str:
    """Recursively collect all text inside an element, stripping whitespace."""
    return element_text(element)


def get_title(table_el) -> str:
    """Extract the table title as a clean string."""
    title_el = table_el.find("title")
    if title_el is None:
        return ""
    return element_text(title_el).strip()


def is_abbreviation_table(title: str) -> bool:
    """Return True if the table is an abbreviations legend (skip it)."""
    return "abbreviation" in title.lower()


# ===========================================================================
# Row-expansion for morerows (CALS-style)
# ===========================================================================

def _is_spanning_row(row_el, ncols: int) -> bool:
    """
    Return True when a <row> is a full-width annotation (not a real header row).

    A row is considered a full-width annotation only when it occupies the
    *entire* table width as a single logical cell — i.e. it has exactly one
    <entry> in a multi-column table, OR its single spanning entry explicitly
    covers all columns (namest=col1 / nameend=colN).

    What we must NOT flag as spanning:
    - A row that has several real per-column entries PLUS one entry that uses
      namest/nameend to form a group-label over a sub-range of columns.
      Example (table_01 header row 1):
          Duplex ID | SID | Sense strand (S) | AS ID | Antisense strand (AS)
          | % of mRNA remained conc. of siRNA [spans cols 6-8] | IC50 (nM)
      This row has 7 entries; only one of them spans.  It is a legitimate
      two-level header row, not an annotation.

    Detection rules (in order):
    1. Single entry, multi-column table → annotation.
    2. All entries together carry namest/nameend AND together they cover the
       full column range → annotation (e.g. a single full-width caption split
       across a few explicit col-spans).
    3. Otherwise → real header row (even if some entries span sub-ranges).
    """
    entries = row_el.findall("entry")

    # Rule 1 – trivially a single full-width cell
    if len(entries) == 1 and ncols > 1:
        return True

    # Rule 2 – every entry uses explicit column spanning AND
    # collectively they cover all columns (no plain per-column entries)
    if ncols > 0 and all(e.get("namest") or e.get("nameend") for e in entries):
        return True

    # Rule 3 – mixed row: some entries are plain, some may span sub-ranges.
    # This is a real (possibly multi-level) header row.
    return False


def expand_rows(tbody_or_rows, ncols: int = 0,
                colspec_map: dict | None = None) -> list[list[str]]:
    """
    Expand <row>/<entry> elements (including morerows spanning) into a plain
    2-D list of strings.

    tbody_or_rows : a <tbody>/<thead> element  OR  a plain list of <row> elements.
    ncols         : total column count of the parent tgroup.  0 = unknown / not needed.
    """
    if tbody_or_rows is None:
        return []

    # Accept either a list of elements or a parent element to search inside
    if isinstance(tbody_or_rows, list):
        raw_rows = tbody_or_rows
    else:
        raw_rows = tbody_or_rows.findall("row")
    if not raw_rows:
        return []

    max_cols       = max(len(r.findall("entry")) for r in raw_rows)
    grid:          list[list[str]]                       = []
    pending_spans: dict[tuple[int, int], tuple[str, int]] = {}

    for row_el in raw_rows:
        row_cells:  list[str] = []
        entry_iter = iter(row_el.findall("entry"))
        col = 0

        while col < max_cols:
            span_key = (len(grid), col)
            if span_key in pending_spans:
                value, remaining = pending_spans.pop(span_key)
                row_cells.append(value)
                if remaining > 1:
                    pending_spans[(len(grid) + 1, col)] = (value, remaining - 1)
                col += 1
                continue

            try:
                entry = next(entry_iter)
            except StopIteration:
                row_cells.append("")
                col += 1
                continue

            value    = element_text(entry).strip()
            morerows = int(entry.get("morerows", 0))

            # ── Column span (namest / nameend) ───────────────────────────────
            colspan = 1
            if colspec_map is not None:
                namest  = entry.get("namest")
                nameend = entry.get("nameend")
                if namest and nameend:
                    s = _resolve_colname(namest,  colspec_map)
                    e = _resolve_colname(nameend, colspec_map)
                    if s is not None and e is not None and e >= s:
                        colspan = e - s + 1

            for k in range(colspan):
                row_cells.append(value)
                if morerows > 0:
                    pending_spans[(len(grid) + 1, col + k)] = (value, morerows)
            col += colspan

        grid.append(row_cells)

    return grid


def expand_thead(thead, ncols: int,
                 colspec_map: dict | None = None) -> tuple[list[str], list[list[str]]]:
    """
    Split a <thead> into:
      - annotations : list of plain strings (spanning / caption rows)
      - header_rows : 2-D list of strings (real column-name rows only)

    Spanning rows (full-width notes like "Numbering is based on …") are
    collected as annotations so they can be written to the context file
    instead of appearing as column headers in the CSV.
    """
    if thead is None:
        return [], []

    annotations: list[str]       = []
    real_rows:   list[list[str]] = []
    pending_spans: dict[tuple[int, int], tuple[str, int]] = {}

    for row_el in thead.findall("row"):
        if _is_spanning_row(row_el, ncols):
            # Collect the text of every entry and join as one annotation string
            texts = [element_text(e).strip() for e in row_el.findall("entry")]
            combined = " ".join(t for t in texts if t)
            if combined:
                annotations.append(combined)
        else:
            # Real column-name row — expand normally
            row_cells:  list[str] = []
            entry_iter = iter(row_el.findall("entry"))
            col = 0
            while col < ncols:
                span_key = (len(real_rows), col)
                if span_key in pending_spans:
                    value, remaining = pending_spans.pop(span_key)
                    row_cells.append(value)
                    if remaining > 1:
                        pending_spans[(len(real_rows) + 1, col)] = (value, remaining - 1)
                    col += 1
                    continue
                try:
                    entry = next(entry_iter)
                except StopIteration:
                    row_cells.append("")
                    col += 1
                    continue
                value    = element_text(entry).strip()
                morerows = int(entry.get("morerows", 0))

                # ── Column span (namest / nameend) ───────────────────────────
                colspan = 1
                if colspec_map is not None:
                    namest  = entry.get("namest")
                    nameend = entry.get("nameend")
                    if namest and nameend:
                        s = _resolve_colname(namest,  colspec_map)
                        e = _resolve_colname(nameend, colspec_map)
                        if s is not None and e is not None and e >= s:
                            colspan = e - s + 1

                for k in range(colspan):
                    row_cells.append(value)
                    if morerows > 0:
                        pending_spans[(len(real_rows) + 1, col + k)] = (value, morerows)
                col += colspan
            real_rows.append(row_cells)

    return annotations, real_rows


# ===========================================================================
# Multi-level header merge  (Fix 1)
# ===========================================================================

def merge_multilevel_headers(header_rows: list[list[str]]) -> list[str]:
    """
    Collapse a two-or-more-row header grid into a single flat list of
    qualified column names, column-by-column.

    Problem solved
    --------------
    Some tables use a two-row header layout:

        Row 1 (group labels):   ""      ""      "1 nM"   ""      "0.01 nM"  ""
        Row 2 (leaf names):  "Duplex"  "Avg"   "STDEV"  "Avg"   "STDEV"

    Because both STDEV cells are identical strings, de-duplicating before
    the LLM call produces only one "STDEV" entry, so both end up with the
    same SQL name.

    This function instead concatenates non-empty cells top-to-bottom for each
    column position before anything reaches the LLM:

        col 4  →  "1 nM"  +  "STDEV"  →  "1 nM STDEV"
        col 6  →  "0.01 nM"  +  "STDEV"  →  "0.01 nM STDEV"

    The LLM then receives distinct strings and produces stdev_1_nM and
    stdev_0_01_nM respectively.

    Columns that already carry the concentration in row 2 (e.g. "Avg. 1 nM")
    are unaffected because row 1 is empty for them and nothing is prepended.

    If *header_rows* has only one row the function returns it unchanged, so
    the change is a no-op for the common single-row case.

    Returns a single flat list of merged header strings (one per column).
    """
    if not header_rows:
        return []
    if len(header_rows) == 1:
        return list(header_rows[0])

    # Determine width from the widest row
    ncols = max(len(r) for r in header_rows)

    merged: list[str] = []
    for col in range(ncols):
        parts = []
        prev: str = ""
        for row in header_rows:
            if col < len(row):
                cell = row[col].strip()
                # Skip exact consecutive duplicates: they arise from morerows=N
                # expansion which copies the same text into every spanned row.
                # Without this guard "Duplex ID" (morerows=1) would merge as
                # "Duplex ID Duplex ID" and confuse the LLM normaliser.
                if cell and cell != prev:
                    parts.append(cell)
                    prev = cell
        merged.append(" ".join(parts))
    return merged


# ===========================================================================
# Table extraction
# ===========================================================================

def _fix_ocr_concentration(text: str) -> str:
    """
    Fix common OCR errors in concentration labels embedded in tbody pseudo-header rows.

    Patterns seen in practice:
        "O.lnM"   → "0.1nM"    (capital O mistaken for 0, l mistaken for 1)
        "O.OOlnM" → "0.001nM"
        "O.OlnM"  → "0.01nM"

    Only fixes the numeric portion preceding "nM" / "nm" so normal text is untouched.
    """
    def _fix_match(m: re.Match) -> str:
        num = m.group(1).replace("O", "0").replace("l", "1").replace("I", "1")
        return num + m.group(2)

    # Match: digit-or-OCR-digit, optional dot + more digits/OCR, then nM
    return re.sub(r"([0O][0Ol.I]+)([nN][mM])", _fix_match, text)


def build_colspec_map(tgroup_el) -> dict:
    """
    Build a {colname: 0-based-column-index} mapping from <colspec> children
    of a <tgroup>.  Used to resolve namest/nameend column-span attributes.

    Falls back to positional ordering when <colnum> is absent, and also
    accepts the common 'colN' naming convention without explicit colnum.
    """
    result: dict[str, int] = {}
    if tgroup_el is None:
        return result
    for pos, spec in enumerate(tgroup_el.findall("colspec")):
        name   = spec.get("colname", "")
        colnum = spec.get("colnum")
        if colnum is not None:
            idx = int(colnum) - 1   # colnum is 1-based in CALS
        else:
            idx = pos               # positional fallback
        if name:
            result[name] = idx
    return result


def _resolve_colname(colname: str, colspec_map: dict) -> int | None:
    """
    Resolve a CALS column name to a 0-based column index.

    Tries the colspec map first; falls back to parsing 'colN' numerically.
    """
    if colname in colspec_map:
        return colspec_map[colname]
    m = re.match(r"^col(\d+)$", colname, re.IGNORECASE)
    if m:
        return int(m.group(1)) - 1
    return None


def extract_tables(xml_file: str) -> list[dict]:
    """
    Parse one XML file and return a list of table dicts (one per parent <table>
    element, combining all its <tgroup> children into a single row list):

      {
        "title":       str,
        "annotations": [str, ...],   # spanning caption rows → go to context
        "headers":     [row, ...],   # real column-name rows only (AI-normalised)
        "rows":        [row, ...],   # data rows
      }

    Three additional behaviours compared to the naïve per-tgroup approach:

    1. **Pseudo-header rows in <tbody>**: Some patent XMLs place the real column
       label row inside <tbody> (not <thead>) using namest/nameend spanning attrs.
       These are detected by _is_spanning_row() and promoted to the headers list
       so the LLM can normalise them.  OCR artefacts in concentration labels
       (e.g. "O.lnM" → "0.1nM") are also corrected at this stage.

    2. **Fill-down for empty group-label cells**: When the first column of a data
       row is empty but the row clearly contains data in other columns, the last
       non-empty value seen in column 0 is propagated forward.  This restores the
       cell-line / group label that CALS morerows=0 tables leave blank after the
       first occurrence.

    3. **Footnote tgroups**: Some patent XMLs append a trailing <tgroup> with no
       <thead> whose entire <tbody> consists only of full-width spanning rows
       (footnotes, legends, method notes).  These tgroups carry no column headers
       and no data rows — only annotation text.  They are detected by the absence
       of <thead> combined with every row being a spanning row, and their text is
       routed to annotations (→ context file) rather than treated as data or
       pseudo-headers.
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()

    tables = []
    for tables_el in root.iter("tables"):
        for table_el in tables_el.findall("table"):
            title = get_title(table_el)
            if is_abbreviation_table(title):
                continue

            # ── Combine all tgroups belonging to the same <table> element ───
            combined_annotations: list[str]       = []
            combined_headers:     list[list[str]] = []
            combined_rows:        list[list[str]] = []
            last_col0:            str             = ""   # fill-down tracker

            for tgroup in table_el.findall("tgroup"):
                ncols       = int(tgroup.get("cols", 0))
                colspec_map = build_colspec_map(tgroup)
                thead  = tgroup.find("thead")
                tbody  = tgroup.find("tbody")

                # Spanning rows from <thead> → annotations
                annotations, header_rows = expand_thead(thead, ncols, colspec_map)
                combined_annotations.extend(annotations)

                # Only take the first tgroup's real headers to avoid duplicates
                if not combined_headers and header_rows:
                    combined_headers = header_rows

                if tbody is None:
                    continue

                all_tbody_rows = tbody.findall("row")

                # ── Detect footnote-only tgroups (Issue 1 fix) ───────────────
                # Pattern: no <thead>, and every row in <tbody> is a spanning
                # row (full-width footnote / legend text, no actual data).
                # Route their text to annotations and skip data/header processing.
                if thead is None and all_tbody_rows:
                    all_spanning = all(
                        _is_spanning_row(r, ncols) for r in all_tbody_rows
                    )
                    if all_spanning:
                        for row_el in all_tbody_rows:
                            texts = [
                                element_text(e).strip()
                                for e in row_el.findall("entry")
                            ]
                            note = " ".join(t for t in texts if t)
                            if note:
                                combined_annotations.append(note)
                        continue   # skip data/pseudo-header logic for this tgroup

                # ── Detect pseudo-header rows at the top of <tbody> ──────────
                # Some documents embed the column label row in <tbody> using
                # namest/nameend spanning attributes (instead of <thead>).
                pseudo_header_els: list = []
                data_row_els:      list = []
                for row_el in all_tbody_rows:
                    if not data_row_els and _is_spanning_row(row_el, ncols):
                        pseudo_header_els.append(row_el)
                    else:
                        data_row_els.append(row_el)

                if pseudo_header_els and not combined_headers:
                    for row_el in pseudo_header_els:
                        cells = [
                            _fix_ocr_concentration(element_text(e).strip())
                            for e in row_el.findall("entry")
                        ]
                        combined_headers.append(cells)

                # ── Detect a non-spanning label row at position 0 of <tbody> ─
                #
                # Pattern seen in this XML (and others like it):
                #   <thead> carries sparse group labels:
                #       ["", "", "", "1nM", "0.01nM"]
                #   <tbody> row 0 carries the real leaf labels:
                #       ["Duplex", "1nM AVG", "0.01nM AVG", "STDEV", "STDEV"]
                #
                # _is_spanning_row() returns False for that tbody row because
                # it has 5 ordinary <entry> elements (no namest/nameend), so
                # the existing pseudo-header logic misses it entirely.  The row
                # then lands in data_row_els and gets written as a data row,
                # producing a CSV with a sparse header and a rogue label row.
                #
                # Detection heuristic (conservative):
                #   1. No spanning pseudo-headers were found above.
                #   2. There are at least 2 rows left (label row + ≥1 data row).
                #   3. Every non-empty cell in the first row is non-numeric.
                #   4. The first cell does NOT look like a duplex/compound ID
                #      (avoids promoting an actual first data row).
                #   5. At least one cell in the next 1–4 rows IS numeric,
                #      confirming this is a quantitative data table.
                _bare_num_re = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")
                _id_re       = re.compile(r"^(AD|SID)-\d+", re.IGNORECASE)

                if not pseudo_header_els and len(data_row_els) >= 2:
                    first_row_el  = data_row_els[0]
                    first_cells   = [
                        element_text(e).strip()
                        for e in first_row_el.findall("entry")
                    ]
                    first_nonempty = [c for c in first_cells if c]
                    all_text       = (
                        first_nonempty
                        and not any(_bare_num_re.match(c) for c in first_nonempty)
                    )
                    first_not_id = not (
                        first_cells and first_cells[0]
                        and _id_re.match(first_cells[0])
                    )
                    has_num_after = any(
                        _bare_num_re.match(element_text(e).strip())
                        for row in data_row_els[1:5]
                        for e in row.findall("entry")
                        if element_text(e).strip()
                    )

                    if all_text and first_not_id and has_num_after:
                        promoted = [
                            _fix_ocr_concentration(c) for c in first_cells
                        ]
                        combined_headers.append(promoted)
                        data_row_els = data_row_els[1:]

                # ── Expand data rows ─────────────────────────────────────────
                data_rows = expand_rows(data_row_els, ncols, colspec_map)

                # ── Fill-down: propagate last non-empty column-0 value ───────
                for row in data_rows:
                    if not row:
                        continue
                    if row[0]:                   # new label found
                        last_col0 = row[0]
                    elif last_col0 and any(c for c in row[1:]):
                        row[0] = last_col0       # fill down into empty cell

                combined_rows.extend(data_rows)

            if not combined_rows:
                continue

            tables.append({
                "title":       title,
                "annotations": combined_annotations,
                "headers":     combined_headers,
                "rows":        combined_rows,
            })

    return tables


# ===========================================================================
# Text content extraction
# ===========================================================================

def extract_text_content(xml_file: str, table_annotations: list[str] | None = None,
                         table_titles: list[str] | None = None) -> dict:
    """
    Extract the document-level title and prose paragraphs, excluding anything
    that lives inside table containers.

    table_annotations : optional list of spanning-row strings collected by
                        extract_tables().  When provided they are appended to
                        the paragraphs so they end up in the context file
                        (each one prefixed with "NOTE: ").
    table_titles      : optional list of table title strings collected by
                        extract_tables().  When provided they are inserted
                        into the paragraphs (before annotations) so each
                        table title appears in the context file.

    Returns { "title": str, "paragraphs": list[str] }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Build parent map once for O(n) ancestor lookup
    parent_map: dict = {child: parent for parent in root.iter() for child in parent}

    def has_table_ancestor(el) -> bool:
        current = el
        while current in parent_map:
            current = parent_map[current]
            if current.tag in _TABLE_TAGS:
                return True
        return False

    # ── Document title ──────────────────────────────────────────────────────
    doc_title = ""
    for el in root.iter("title"):
        if not has_table_ancestor(el):
            doc_title = element_text(el).strip()
            if doc_title:
                break

    # ── Paragraphs ──────────────────────────────────────────────────────────
    paragraphs: list[str] = []
    seen: set[str]        = set()

    for tag in _PARA_TAGS:
        for el in root.iter(tag):
            if has_table_ancestor(el):
                continue
            text = element_text(el).strip()
            if text and text not in seen:
                paragraphs.append(text)
                seen.add(text)

    # Fallback: bare text nodes when no paragraph tags are found
    if not paragraphs:
        for el in root.iter():
            if el.tag in _TABLE_TAGS | {"title"}:
                continue
            if el.text:
                text = el.text.strip()
                if len(text) > 40 and text not in seen:
                    paragraphs.append(text)
                    seen.add(text)

    # ── Table titles ──────────────────────────────────────────────────────────
    if table_titles:
        for title in table_titles:
            title = title.strip()
            if title and title not in seen:
                paragraphs.append(title)
                seen.add(title)

    # ── Table annotation rows (spanning captions from thead) ─────────────────
    if table_annotations:
        for note in table_annotations:
            note = note.strip()
            if note and note not in seen:
                annotated = f"NOTE: {note}"
                paragraphs.append(annotated)
                seen.add(annotated)

    return {"title": doc_title, "paragraphs": paragraphs}


# ===========================================================================
# AI header repair  (Pass 1 — structure)
# ===========================================================================

def _build_repair_prompt(header_rows: list[list[str]],
                         sample_rows: list[list[str]],
                         table_title: str = "") -> str:
    """
    Build a prompt that asks the LLM to collapse a multi-row header grid
    (plus the first few data rows for context) into a single flat list of
    human-readable column names.

    The LLM is explicitly told that some "data rows" at the top may actually
    be misclassified header rows (a known CALS/patent-XML parsing artefact),
    and that it should recognise and use them as headers.

    *table_title*, when non-empty, is injected as a single line at the top of
    the prompt so the model can use the subject (e.g. "In vitro metabolic
    stability in liver cytosol") to disambiguate ambiguous leaf-label strings
    like "Avg" or "SD" that appear across many different table types.

    This runs *before* normalize_headers_with_ai so the downstream SQL
    normalisation receives clean, unambiguous strings rather than raw
    multi-level fragments or OCR-corrupted labels.
    """
    ncols = max((len(r) for r in header_rows), default=0)
    if ncols == 0:
        return ""

    def fmt_row(row: list[str], width: int) -> str:
        padded = list(row) + [""] * (width - len(row))
        return " | ".join(f"{str(c):25s}" for c in padded)

    header_block = "\n".join(
        f"  header_row[{i}]: {fmt_row(r, ncols)}"
        for i, r in enumerate(header_rows)
    )

    sample_block = ""
    if sample_rows:
        sample_block = (
            "\nFirst data rows (some may actually be misclassified header rows):\n"
        )
        for i, r in enumerate(sample_rows[:4]):
            sample_block += f"  data_row[{i}]: {fmt_row(r, ncols)}\n"

    title_line = f"Table title: {table_title}\n\n" if table_title.strip() else ""

    return f"""\
You are a data engineer cleaning up table headers extracted from a patent XML file.

{title_line}The XML parser extracted the following header row(s) and first data rows for a
table with {ncols} columns. Patent XML tables often have multi-level headers
(group labels above leaf labels), and the parser sometimes misclassifies the
second header row as the first data row.

Your job: produce exactly ONE flat list of {ncols} human-readable column names,
one per column, by:
  a) Merging information across all header rows.
  b) Also checking the first data rows — if a data row looks like a label row
     (contains column names, units, or concentrations rather than numbers),
     treat it as an additional header row and merge it too.

## Rules
1. Output ONLY a JSON array of exactly {ncols} strings. No markdown, no preamble.
   Start with [ and end with ].
2. For each column, combine the group label (if any) with the leaf label:
   Example: group "Day 3", leaf "1nM" → "Day 3 1nM".
3. If a column repeats the same non-empty label across rows (morerows span),
   use it once.
4. Correct obvious OCR errors:
   "O.lnM" → "0.1nM",  "O.OOlnM" → "0.001nM",  "DuplexlD" → "Duplex ID"
5. Empty or missing labels → empty string "" in the output array.
6. Do NOT invent information not present in the header or label rows.

## Common patterns to recognise
- Sparse group-label row: ['', '', 'Day 3', 'Day 5', 'Day 7', '', '', ...]
  followed by a data_row[0] with leaf labels: ['Cell line', 'Duplex ID', '1nM', '0.1nM', ...]
  → merge them: ["Cell line", "Duplex ID", "Day 3 1nM", "Day 3 0.1nM", ...]
- Column-span group labels (the same group name repeated across its columns):
  header_row[0]: ['', 'Transfection (Hep3b)', 'Transfection (Hep3b)', 'Transfection (Hep3b)', 'Transfection (Hep3b)', 'Free Uptake (PCH)', 'Free Uptake (PCH)', 'Free Uptake (PCH)', 'Free Uptake (PCH)']
  header_row[1]: ['', '10nM', '10nM', '0.1nM', '0.1nM', '10nM', '10nM', '500nM', '500nM']
  header_row[2]: ['', 'Avg', 'SD', 'Avg', 'SD', 'Avg', 'SD', 'Avg', 'SD']
  → merge: ["", "Transfection (Hep3b) 10nM Avg", "Transfection (Hep3b) 10nM SD",
             "Transfection (Hep3b) 0.1nM Avg", "Transfection (Hep3b) 0.1nM SD",
             "Free Uptake (PCH) 10nM Avg", "Free Uptake (PCH) 10nM SD",
             "Free Uptake (PCH) 500nM Avg", "Free Uptake (PCH) 500nM SD"]
- Two misaligned header rows (morerows expansion artefact): align by column position.
- First data row is a label row (no numbers, only names/units): treat as header.

## Extracted rows
{header_block}
{sample_block}
## Required output
A single JSON array of {ncols} strings.
"""


def repair_headers_with_ai(
    header_rows:  list[list[str]],
    sample_rows:  list[list[str]],
    trace_file:   str = "",
    table_title:  str = "",
) -> list[str] | None:
    """
    Use the Groq LLM to collapse a multi-row header grid into a single flat
    list of human-readable column names.

    Returns the repaired list (length == ncols), or None if the LLM could not
    be reached or returned an unusable result (caller falls back to
    merge_multilevel_headers).

    This is Pass 1 (structure repair).  The repaired strings are then passed
    to normalize_headers_with_ai (Pass 2 / SQL normalisation) as usual.

    Only called when there is more than one header row — single-row headers
    are already clean and need no structural repair.
    """
    if not header_rows:
        return None

    ncols = max(len(r) for r in header_rows)
    if ncols == 0:
        return None

    # ── Decide whether to invoke the AI or skip ──────────────────────────────
    #
    # Empty cells in any of the first 1-3 header rows are a reliable signal
    # of a sparse / multi-level layout (group labels above leaf labels, or a
    # morerows artefact).  In that case we ALWAYS call the AI — the stronger
    # _REPAIR_GROQ_MODEL handles merging far better than the rule-based fallback.
    #
    # Only skip the AI when the header is already a complete single row (no
    # empty cells in the first 3 rows) AND the first sample row looks like real
    # numeric data, meaning no structural repair is needed.
    _bare_num = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")

    has_empty_in_headers = any(
        str(cell).strip() == ""
        for row in header_rows[:3]
        for cell in row
    )

    if not has_empty_in_headers and len(header_rows) == 1 and sample_rows:
        first_row_cells = [str(c).strip() for c in sample_rows[0]]
        if any(_bare_num.match(c) for c in first_row_cells if c):
            return None   # clean single-row header + numeric data → no repair needed

    if not _CLIENTS:
        return None

    prompt = _build_repair_prompt(header_rows, sample_rows, table_title)
    if not prompt:
        return None

    max_tokens = max(256, ncols * 25 + 100)
    max_attempts = max(6, len(_CLIENTS) * 2)

    for attempt in range(1, max_attempts + 1):
        current_key    = _active_key()
        current_client = _active_client()

        log_trace(trace_file, f"REPAIR ATTEMPT {attempt}",
                  f"Using key ...{current_key[-6:]}\n{prompt}")

        try:
            response = current_client.chat.completions.create(
                model=_REPAIR_GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=max_tokens,
            )
            raw = response.choices[0].message.content.strip()

            log_trace(trace_file, f"REPAIR ATTEMPT {attempt} - RAW OUTPUT", raw)

            # Extract the JSON array from the response
            arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
            if not arr_match:
                raise ValueError("No JSON array found in LLM response.")

            repaired: list = json.loads(arr_match.group(0))

            if not isinstance(repaired, list):
                raise ValueError("LLM returned non-list JSON.")

            # Pad or truncate to exactly ncols
            repaired = [str(x) for x in repaired]
            if len(repaired) < ncols:
                repaired += [""] * (ncols - len(repaired))
            else:
                repaired = repaired[:ncols]

            log_trace(trace_file, f"REPAIR ATTEMPT {attempt} - SUCCESS",
                      f"Repaired {ncols} column(s):\n" +
                      "\n".join(f"  [{i:2d}] {h!r}" for i, h in enumerate(repaired)))
            return repaired

        except Exception as e:
            error_str = str(e)
            log_trace(trace_file, f"REPAIR ATTEMPT {attempt} - ERROR", error_str)

            if "429" in error_str or "rate_limit_exceeded" in error_str:
                wait_seconds = _parse_retry_after(error_str)
                _record_rate_limit(current_key, wait_seconds if wait_seconds > 0 else 60.0)
                next_idx = _next_available_idx()
                if next_idx is not None:
                    _ACTIVE_IDX[0] = next_idx
                    continue
                sleep_secs = _shortest_wait()
                time.sleep(sleep_secs)
                next_idx = _next_available_idx()
                if next_idx is not None:
                    _ACTIVE_IDX[0] = next_idx
                continue

            break

    log_trace(trace_file, "REPAIR FALLBACK",
              "repair_headers_with_ai failed; merge_multilevel_headers will be used.")
    return None


# ===========================================================================
# AI header normalisation — with key rotation + retry  (Pass 2 — SQL names)
# ===========================================================================

def basic_sql_normalize(header: str) -> str:
    """
    Rule-based fallback (no AI).  Mirrors the Column Name Rules from
    supplementary Prompt 1:  lowercase, underscores, % → pct, # → num.
    """
    s = header.lower().strip()
    s = s.replace("%", "_pct").replace("#", "_num")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _build_prompt(headers: list[str], include_codegen: bool = False) -> str:
    """Build the normalisation prompt for siRNA/dsRNA patent table headers.

    When include_codegen=True the prompt requests TWO outputs:
      1. A JSON mapping  (original header → SQL identifier)
      2. A standalone Python function ``llm_normalize(header: str) -> str``
         that encodes the exact rules applied, so results are reproducible
         without calling the LLM again.

    When include_codegen=False (default) only the JSON mapping is requested,
    saving ~400-700 tokens per call against Groq's TPM limit.

    NOTE: headers are passed raw (un-normalised) so the LLM can also repair
    obvious OCR artefacts (run-together words, transposed characters, etc.)
    as part of the normalisation step.  See the OCR section in the prompt.
    """
    numbered = "\n".join(f"{i + 1}. {h}" for i, h in enumerate(headers))
    base_prompt = f"""\
You are a data engineer preparing column headers extracted from siRNA/dsRNA patent tables for loading into a SQL database.

Convert each of the following column headers to a SQL-friendly identifier.

## Rules
- **Preserve meaning exactly** – do NOT drop, merge, or rephrase terms.
- **SQL-compatible** – lowercase; replace spaces, hyphens, dots, slashes and
  any other non-alphanumeric character with a single underscore; no leading
  or trailing underscores; no consecutive underscores.
- **Special characters**
    %  or "percent"  →  suffix  _pct      e.g. "% AS"  →  pct_as
    #  or "number"   →  suffix  _num
    Parentheses / brackets: remove the brackets but keep the text inside,
    separated by underscores.
    Apostrophes (as in 5' / 3') → remove, keep the digit.
    Dots in abbreviations (e.g. "SEQ ID NO.") → remove the dot.
    "St.Dev" / "Stdev" / "STDEV" → stdev   (standardise spelling)
    "Avg."  → avg
    "SD"    → sd
- **Numeric-only or unit-only headers** (e.g. "1 nM", "50 nM")
    → prepend  conc_  →  conc_1_nm, conc_50_nm
- **Multi-level / qualified headers** produced by collapsing a group-label
  row on top of a leaf-name row (e.g. "1 nM STDEV", "0.01 nM STDEV"):
    Treat the group label as a qualifier suffix, e.g.:
      "1 nM STDEV"    → stdev_1_nM
      "0.01 nM STDEV" → stdev_0_01_nM
      "500 nM Avg"    → avg_500_nM
      "1 nM Avg"      → avg_1_nM
- **OCR artefact correction** – some headers are OCR-corrupted: run-together
  words, missing spaces, transposed characters.  Recognise and repair these
  before normalising.  Examples:
      "senseoligon_ame"   → sense_oligo_name
      "antisoligoname"    → antisense_oligo_name
      "DuplexlD"          → duplex_id   (capital-I mistaken for lowercase-l)
      "Dupiex ID"         → duplex_id   (i/l OCR swap)
      Any run-together words that clearly represent a known siRNA field name
      should be split and normalised accordingly.
- **Implicit duplex_id column** – if a header is empty, blank, or clearly a
  positional placeholder (e.g. "_col_0", "col0", "") AND the first data
  column contains values matching the pattern AD-\\d+ (Alnylam duplex IDs),
  map that header to  duplex_id.
- **Recognised siRNA/biology abbreviations – keep exactly as-is (case-folded)**:
    SEQ ID NO → seq_id_no
    IC50      → ic50
    ED50      → ed50
    nM        → nm
    OMe / 2OMe / 2'OMe → 2OMe
    dTdT      → dTdT
    dTsdT     → dTsdT  (phosphorothioate overhang; keep the s)
    mRNA      → mrna
    ssRNA     → ssrna
    HBsAg     → hbsag
    ELISA     → elisa
    P-ORF     → p_orf
    S-ORF     → s_orf
    ORF       → orf
    HBV       → hbv
    GNAQ      → gnaq
    NM_xxxxxx → keep verbatim (RefSeq accession), replace dot with underscore
- **Long annotation / spanning rows** (the ones that say "Numbering for target
  sequences is based on …") – map them to a short, descriptive snake_case label
  that starts with  note_  e.g.:
    "Numbering for target sequences is based on Human GNAQ NM_002072"
    → note_numbering_human_gnaq_nm_002072

## Examples
  "SEQ ID NO"                           →  seq_id_no
  "SEQ ID NO."                          →  seq_id_no
  "SEQ ID NO:"                          →  seq_id_no
  "S E Q ID NO:"                        →  seq_id_no
  "SEQUENCE (5'-3')"                    →  sequence_5_3
  "Sense Sequence (5' to 3')"           →  sense_sequence_5_to_3
  "Antisense Sequence (5' to 3')"       →  antisense_sequence_5_to_3
  "Modified sequence 5' to 3'"         →  modified_sequence_5_to_3
  "Unmodified sequence 5' to 3'"       →  unmodified_sequence_5_to_3
  "Strand"                              →  strand
  "Start of target sequence"            →  start_of_target_sequence
  "Duplex ID"                           →  duplex_id
  "Duplex Name"                         →  duplex_id
  "Duplex Name Duplex Name"             →  duplex_id
  "duplex_num"                          →  duplex_id
  "Sense Oligo Name"                    →  sense_oligo_name
  "Antisense Oligo Name"                →  antisense_oligo_name
  "Sense Strand ID"                     →  sense_strand_id
  "Antisense Strand ID"                 →  antisense_strand_id
  "senseoligon_ame"                     →  sense_oligo_name
  "antisoligoname"                      →  antisense_oligo_name
  "DuplexlD"                            →  duplex_id
  "IC50 (nM)"                           →  ic50_nM
  "IC50 (nM) at 24 hr"                  →  ic50_nM_at_24hr
  "IC50 (nM) at 72 hr"                  →  ic50_nM_at_72hr
  "P-ORF1 IC50_A (nM)"                  →  p_orf1_ic50_a_nM
  "P-ORF1 IC50_B (nM)"                  →  p_orf1_ic50_b_nM
  "P-ORF1 IC50_Combine (nM)"            →  p_orf1_ic50_combine_nM
  "S-ORF2 IC50_A (nM)"                  →  s_orf2_ic50_a_nM
  "S Ag ELISA ED50 (nM)"                →  s_ag_elisa_ed50_nM
  "HBsAg ELISA"                         →  hbsag_elisa
  "% Target Remaining"                  →  pct_target_remaining
  "% AS"                                →  pct_as
  "% SS"                                →  pct_ss
  "% parent remaining at 24hr incubation" → pct_parent_remaining_at_24hr_incubation
  "1 nM"                                →  conc_1_nM
  "50 nM"                               →  conc_50_nM
  "1 nM STDEV"                          →  stdev_1_nM
  "0.01 nM STDEV"                       →  stdev_0_01_nM
  "500 nM Avg"                          →  avg_500_nM
  "5 nM Avg"                            →  avg_5_nM
  "500 nM STDEV"                        →  stdev_500_nM
  "5 nM STDEV"                          →  stdev_5_nM
  "STDEV 1 nM"                          →  stdev_1_nM
  "STDEV 0.01 nM"                       →  stdev_0_01_nM
  "Avg. 500 nM"                         →  avg_500_nM
  "Avg. 5 nM"                           →  avg_5_nM
  "Avg. 0.1 nM"                         →  avg_0_1_nm
  "Avg.10 nM"                           →  avg_10_nM
  "SD 0.1 nM"                           →  sd_0_1_nM
  "SD 10 nM"                            →  sd_10_nM
  "STDEV"                               →  stdev
  "St.Dev error"                        →  stdev_error
  "Combine d"                           →  combined
  "A549 cells (1nM conc.)"              →  a549_cells_1nM
  "A375 cells (0.1nM conc.)"            →  a375_cells_0_1nM
  "OMM1.3 cells (10nM conc.)"           →  omm1_3_cells_10nM
  "Hep3B cells screen"                  →  hep3b_cells_screen
  "Dual luciferase HBV reporter cells"  →  dual_luciferase_hbv_reporter_cells
  "In vitro metabolic stability"        →  in_vitro_metabolic_stability
  "Cytosol"                             →  cytosol
  "Endo-lysosome"                       →  endo_lysosome
  "Type"                                →  type
  "Target"                              →  target
  "Sample Name"                         →  sample_name
  "Position in NM_000040.1"             →  position_in_nm_000040_1
  "Assay Development"                   →  assay_development
  "Optimized Assay"                     →  optimized_assay
  "Numbering for target sequences is based on Human GNAQ NM_002072"
                                        →  note_numbering_human_gnaq_nm_002072
  "Transfection (Hep3b) 10nM Avg"      →  transfection_hep3b_10nM_avg
  "Transfection (Hep3b) 10nM SD"       →  transfection_hep3b_10nM_sd
  "Transfection (Hep3b) 0.1nM Avg"     →  transfection_hep3b_0_1nM_avg
  "Transfection (Hep3b) 0.1nM SD"      →  transfection_hep3b_0_1nM_sd
  "Free Uptake (PCH) 10nM Avg"         →  free_uptake_pch_10nM_avg
  "Free Uptake (PCH) 10nM SD"          →  free_uptake_pch_10nM_sd
  "Free Uptake (PCH) 500nM Avg"        →  free_uptake_pch_500_nM_avg
  "Free Uptake (PCH) 500nM SD"         →  free_uptake_pch_500_nM_sd

## Headers to convert
{numbered}

## Required output — JSON mapping
A single valid JSON object: keys = original headers, values = SQL identifiers.
No markdown fences, no preamble.  Start the line with  {{  and end it with  }}
"""
    if not include_codegen:
        return base_prompt

    return base_prompt + """
## Additional output — Reproducible Python function
Immediately after the JSON object, output a Python code block (```python ... ```)
containing a self-contained function with this exact signature:

    def llm_normalize(header: str) -> str:
        \"\"\"
        Deterministic Python equivalent of the LLM normalisation applied to
        this batch.  Generated automatically — do NOT edit by hand.
        Reproduces every mapping in the JSON without calling the LLM.
        \"\"\"
        ...

Rules for the function:
- The function MUST reproduce every mapping in the JSON exactly.
- Start with an explicit lookup table (a dict literal) containing every
  original-header → sql-identifier pair from the JSON, so the mapping is
  100% transparent and auditable.
- After the lookup table, add a fallback block that implements the general
  rules (lowercase, underscores, pct/num substitutions, etc.) using only
  the Python standard library (re, str methods).  This handles any header
  not in the lookup table.
- No imports outside the standard library.  No calls to external services.
- The function must be runnable as-is: copy-paste into a Python REPL and
  call llm_normalize("IC50 (nM)") → "ic50_nM".

This function is the audit trail that proves the LLM followed the rules
and did not take shortcuts.  It will be saved beside the output CSV."""


def _extract_json_and_code(raw: str) -> tuple[str, str]:
    """
    Split the LLM response into two parts:

      1. The JSON mapping object  (everything from the first '{' to its matching '}')
      2. The Python code block    (content inside the first ```python ... ``` fence)

    Returns (json_str, python_code).  Either part may be an empty string if
    the LLM omitted it.

    Truncation recovery: if max_tokens was hit mid-JSON the response ends
    without a closing '}'.  In that case we salvage every complete key/value
    pair that was emitted and return a partial (but valid) JSON object, so the
    caller can still use what the model produced and fill remaining headers with
    the rule-based fallback instead of discarding the entire response.
    """
    # ── Part 1: JSON object ──────────────────────────────────────────────────
    json_str = ""
    brace_start = raw.find("{")
    if brace_start != -1:
        depth = 0
        for i, ch in enumerate(raw[brace_start:], start=brace_start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    json_str = raw[brace_start: i + 1]
                    break

        # ── Truncation recovery ──────────────────────────────────────────────
        # If we never found a closing '}' (depth > 0 still), the response was
        # cut off.  Try to salvage all complete "key": "value" pairs using regex.
        if not json_str and brace_start != -1:
            fragment = raw[brace_start:]
            pairs = re.findall(
                r'"([^"]+)"\s*:\s*"([^"]*)"(?:\s*,)?\s*',
                fragment,
            )
            if pairs:
                salvaged = {k: v for k, v in pairs}
                try:
                    json_str = json.dumps(salvaged)
                except Exception:
                    json_str = ""

    # ── Part 2: Python code block ────────────────────────────────────────────
    python_code = ""
    py_match = re.search(r"```python\s*\n(.*?)```", raw, re.DOTALL | re.IGNORECASE)
    if py_match:
        python_code = py_match.group(1).strip()

    return json_str, python_code


def _verify_codegen(python_code: str, mapping: dict[str, str]) -> list[str]:
    """
    Execute the generated ``llm_normalize`` function in an isolated namespace
    and verify it reproduces every entry in *mapping*.

    Returns a list of discrepancy strings (empty → all good).
    This is the guardrail: if the LLM wrote lazy/wrong code the mismatch
    is caught here and logged, so the problem is visible rather than silent.
    """
    if not python_code:
        return ["No Python code was generated by the LLM."]

    namespace: dict = {}
    try:
        exec(compile(python_code, "<llm_generated>", "exec"), namespace)  # noqa: S102
    except Exception as exc:
        return [f"Generated code failed to compile/execute: {exc}"]

    fn = namespace.get("llm_normalize")
    if fn is None:
        return ["Generated code does not define llm_normalize()."]

    discrepancies = []
    for original, expected in mapping.items():
        try:
            got = fn(original)
        except Exception as exc:
            discrepancies.append(f"  llm_normalize({original!r}) raised {exc}")
            continue
        if got != expected:
            discrepancies.append(
                f"  llm_normalize({original!r}) → {got!r}  (expected {expected!r})"
            )
    return discrepancies


def _write_codegen_file(
    python_code:   str,
    mapping:       dict[str, str],
    discrepancies: list[str],
    codegen_path:  str,
    model:         str,
) -> None:
    """
    Write a fully self-contained Python script to *codegen_path*.

    When run directly next to its matching XML file, the script:
      1. Infers the XML filename from its own name
         (<base>_llm_normalize.py  →  <base>.xml).
      2. Parses the XML using embedded copies of the CALS extraction helpers
         (no dependency on xml_to_csv.py or any third-party package).
      3. Applies the AI mapping frozen at generation time via the
         llm_normalize() lookup table — no API call needed.
      4. Writes  <base>_tables.csv  beside itself, reproducing the exact
         same output that xml_to_csv.py produced originally.
      5. Verifies all frozen mappings and reports any discrepancies.
    """
    import inspect as _inspect

    ts       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    py_base  = os.path.basename(codegen_path)          # e.g. "foo_llm_normalize.py"
    xml_base = os.path.splitext(py_base)[0]            # "foo_llm_normalize"
    if xml_base.endswith("_llm_normalize"):
        xml_base = xml_base[: -len("_llm_normalize")]  # "foo"

    # ── Frozen mapping as a Python dict literal ───────────────────────────────
    mapping_lines = ["_MAPPING = {\n"]
    for orig, sql in sorted(mapping.items()):
        mapping_lines.append(f"    {orig!r}: {sql!r},\n")
    mapping_lines.append("}\n")
    mapping_literal = "".join(mapping_lines)

    # ── Embed the XML-parsing helpers via inspect.getsource() ─────────────────
    # This guarantees the embedded code is always in sync with the live module.
    _funcs_to_embed = [
        element_text,
        get_text,
        get_title,
        is_abbreviation_table,
        _is_spanning_row,
        expand_rows,
        expand_thead,
        _fix_ocr_concentration,
        extract_tables,
        write_tables_headers_csv,
    ]
    embedded_helpers = "\n\n".join(_inspect.getsource(fn) for fn in _funcs_to_embed)

    # ── Discrepancy comment ───────────────────────────────────────────────────
    if discrepancies:
        disc_comment = (
            "# WARNING — verification discrepancies detected at generation time:\n"
            + "\n".join(f"#   {d}" for d in discrepancies)
            + "\n#\n"
            "# The lookup table is authoritative; the fallback rule may differ\n"
            "# for headers not in the table.\n"
        )
    else:
        disc_comment = (
            "# All mappings verified against the LLM JSON output at generation time.\n"
        )

    # ── Assemble the script via plain string concatenation ────────────────────
    # (avoids any f-string / escaping issues with the embedded source code)
    parts = []

    parts.append('"""\n')
    parts.append(f"{py_base} — auto-generated by xml_to_csv.py\n")
    parts.append("=" * 62 + "\n")
    parts.append(f"Generated : {ts}\n")
    parts.append(f"Model     : {model}\n")
    parts.append(f"Headers   : {len(mapping)}\n")
    parts.append("\n")
    parts.append("Self-contained reproducibility script.\n")
    parts.append(f"Place this file in the same directory as  {xml_base}.xml  and run:\n")
    parts.append("\n")
    parts.append(f"    python {py_base}\n")
    parts.append("\n")
    parts.append("It will:\n")
    parts.append(f"  1. Parse  {xml_base}.xml  using embedded CALS helpers (no external deps).\n")
    parts.append("  2. Apply the frozen AI header mapping (llm_normalize lookup table).\n")
    parts.append(f"  3. Write  {xml_base}_tables.csv  beside this script.\n")
    parts.append(f"  4. Verify all {len(mapping)} header mapping(s) and report any discrepancies.\n")
    parts.append("\n")
    parts.append("Standard-library only — no pip installs required.\n")
    parts.append('"""\n')
    parts.append("\n")
    parts.append("from __future__ import annotations\n")
    parts.append("import csv\n")
    parts.append("import os\n")
    parts.append("import re\n")
    parts.append("import sys\n")
    parts.append("import xml.etree.ElementTree as ET\n")
    parts.append("\n\n")

    parts.append("# " + "=" * 70 + "\n")
    parts.append("# Frozen AI header mapping\n")
    parts.append("# " + "=" * 70 + "\n")
    parts.append("\n")
    parts.append(mapping_literal)
    parts.append("\n\n")

    parts.append("def llm_normalize(header: str) -> str:\n")
    parts.append('    """\n')
    parts.append("    Apply the frozen AI mapping to one header string.\n")
    parts.append("    Exact lookup first; rule-based fallback for anything not in the table.\n")
    parts.append('    """\n')
    parts.append("    if header in _MAPPING:\n")
    parts.append("        return _MAPPING[header]\n")
    parts.append("    # fallback: lowercase, underscores, % → _pct, # → _num\n")
    parts.append("    s = header.lower().strip()\n")
    parts.append('    s = s.replace("%", "_pct").replace("#", "_num")\n')
    parts.append('    s = re.sub(r"[^a-z0-9]+", "_", s)\n')
    parts.append('    s = re.sub(r"_+", "_", s).strip("_")\n')
    parts.append("    return s\n")
    parts.append("\n\n")

    parts.append("# " + "=" * 70 + "\n")
    parts.append("# Embedded CALS XML-parsing helpers (copied verbatim from xml_to_csv.py)\n")
    parts.append("# " + "=" * 70 + "\n")
    parts.append("\n")
    parts.append(embedded_helpers)
    parts.append("\n\n")

    parts.append("# " + "=" * 70 + "\n")
    parts.append("# Main — parse XML, apply mapping, write CSV\n")
    parts.append("# " + "=" * 70 + "\n")
    parts.append("\n")
    parts.append("def _reproduce(xml_file: str, csv_file: str) -> None:\n")
    parts.append("    tables = extract_tables(xml_file)\n")
    parts.append("    if not tables:\n")
    parts.append('        print(f"No data tables found in {xml_file}")\n')
    parts.append("        return\n")
    parts.append("    for tbl in tables:\n")
    parts.append('        tbl["sql_headers"] = [\n')
    parts.append("            [llm_normalize(cell) if cell.strip() else \"\" for cell in hrow]\n")
    parts.append('            for hrow in tbl["headers"]\n')
    parts.append("        ]\n")
    parts.append("    write_tables_headers_csv(tables, csv_file)\n")
    parts.append('    total_rows = sum(len(t["rows"]) for t in tables)\n')
    parts.append('    print(f"Written: {csv_file}  ({len(tables)} table(s), {total_rows} data row(s))")\n')
    parts.append("\n\n")

    parts.append("# " + "=" * 70 + "\n")
    parts.append("# Verification — confirm llm_normalize() reproduces every frozen mapping\n")
    parts.append("# " + "=" * 70 + "\n")
    parts.append(disc_comment)
    parts.append("\n")
    parts.append("def _verify() -> bool:\n")
    parts.append("    failures = []\n")
    parts.append("    for orig, expected in _MAPPING.items():\n")
    parts.append("        got = llm_normalize(orig)\n")
    parts.append("        if got != expected:\n")
    parts.append("            failures.append(f\"  {orig!r} -> {got!r}  (expected {expected!r})\")\n")
    parts.append("    if failures:\n")
    parts.append('        print("VERIFICATION FAILED:")\n')
    parts.append("        for f in failures:\n")
    parts.append("            print(f)\n")
    parts.append("        return False\n")
    parts.append(f'    print("All {len(mapping)} mapping(s) verified OK.")\n')
    parts.append("    return True\n")
    parts.append("\n\n")

    parts.append('if __name__ == "__main__":\n')
    parts.append("    here     = os.path.dirname(os.path.abspath(__file__))\n")
    parts.append(f'    xml_name = "{xml_base}.xml"\n')
    parts.append("    xml_path = os.path.join(here, xml_name)\n")
    parts.append(f'    csv_path = os.path.join(here, "{xml_base}_tables.csv")\n')
    parts.append("\n")
    parts.append("    ok = _verify()\n")
    parts.append("\n")
    parts.append("    if not os.path.exists(xml_path):\n")
    parts.append('        print(f"XML file not found: {xml_path}")\n')
    parts.append("        sys.exit(1 if not ok else 0)\n")
    parts.append("\n")
    parts.append("    _reproduce(xml_path, csv_path)\n")
    parts.append("    sys.exit(0 if ok else 1)\n")

    with open(codegen_path, "w", encoding="utf-8") as fh:
        fh.writelines(parts)


def normalize_headers_with_ai(
    headers:              list[str],
    trace_file:           str  = "",
    codegen_path:         str  = "",
    create_headers_file:  bool = False,
) -> dict[str, str]:
    """
    Normalise column headers to SQL identifiers via the Groq API.

    In addition to returning the JSON mapping, the LLM is now asked to emit
    a standalone Python function (``llm_normalize``) that encodes the exact
    rules it applied.  This function is written to *codegen_path* (inside the
    debug directory) so anyone can audit or re-run the normalisation without
    calling the LLM.  A self-test block is appended to the generated file and
    run immediately as a guardrail — if the LLM wrote lazy or incorrect code
    the discrepancies are logged rather than silently propagated.

    Key-rotation logic
    ------------------
    1. On a 429 / rate_limit_exceeded error the current key is put into
       cooldown and the next available key is activated immediately.
    2. If ALL keys are cooling down, we sleep until the soonest one wakes up
       (the wait time is parsed from the API error message, including hours),
       then retry.
    3. Non-rate-limit errors break the loop immediately.
    4. After all attempts are exhausted, basic_sql_normalize() is the fallback.
    """
    if not headers:
        return {}
    if not _CLIENTS:
        raise RuntimeError(
            "Client pool is empty — call convert_directory() with api_keys= first."
        )

    prompt       = _build_prompt(headers, include_codegen=create_headers_file)
    # Each header entry in the JSON response costs ~25-35 tokens on average
    # (quoted key + quoted value + punctuation).  Give a comfortable margin.
    # When codegen is requested, the Python function adds ~400-600 extra tokens.
    _tokens_per_header = 35
    _codegen_overhead  = 700
    max_tokens = len(headers) * _tokens_per_header + (
        _codegen_overhead if create_headers_file else 150
    )
    # Clamp: Groq rejects max_tokens > model context; never go below 256
    max_tokens = max(256, min(max_tokens, 4096))
    max_attempts = max(8, len(_CLIENTS) * 3)

    for attempt in range(1, max_attempts + 1):
        current_key    = _active_key()
        current_client = _active_client()

        log_trace(trace_file, f"ATTEMPT {attempt}",
                  f"Using key ...{current_key[-6:]}")

        # ── Log LLM input ────────────────────────────────────────────────────
        log_trace(
            trace_file,
            f"ATTEMPT {attempt} - LLM INPUT",
            f"Script  : normalize_headers_with_ai\n"
            f"Model   : {_GROQ_MODEL}\n"
            f"Headers : {len(headers)}\n"
            f"{'─' * 40}\n"
            f"{prompt}",
        )

        try:
            response = current_client.chat.completions.create(
                model=_GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,    # deterministic → reproducible
                max_tokens=max_tokens,
            )
            raw = response.choices[0].message.content.strip()

            # ── Log raw LLM output ───────────────────────────────────────────
            log_trace(
                trace_file,
                f"ATTEMPT {attempt} - LLM OUTPUT (raw)",
                raw,
            )

            # ── Split response into JSON mapping + Python code ────────────────
            json_str, python_code = _extract_json_and_code(raw)

            if not json_str:
                raise ValueError("LLM response contained no JSON object.")

            mapping: dict[str, str] = json.loads(json_str)

            # Fill any gaps the model left
            for h in headers:
                if h not in mapping:
                    mapping[h] = basic_sql_normalize(h)

            # ── Guardrail: verify the generated code reproduces the mapping ───
            # Only run when codegen was actually requested; an absent python_code
            # block is expected (not a discrepancy) when create_headers_file=False.
            if create_headers_file:
                discrepancies = _verify_codegen(python_code, mapping)
                if discrepancies:
                    print(f"  [Codegen] ⚠  {len(discrepancies)} verification discrepancy(ies) "
                          f"— see log and generated file.")
                    log_trace(
                        trace_file,
                        f"ATTEMPT {attempt} - CODEGEN VERIFICATION FAILED",
                        "\n".join(discrepancies),
                    )
                else:
                    print(f"  [Codegen] ✓  Generated llm_normalize() verified OK "
                          f"({len(mapping)} header(s)).")
                    log_trace(
                        trace_file,
                        f"ATTEMPT {attempt} - CODEGEN VERIFICATION OK",
                        f"All {len(mapping)} mapping(s) reproduced correctly.",
                    )
            else:
                discrepancies = []

            # ── Write the generated Python file (only when opted-in) ─────────
            if create_headers_file and codegen_path:
                _write_codegen_file(
                    python_code   = python_code,
                    mapping       = mapping,
                    discrepancies = discrepancies,
                    codegen_path  = codegen_path,
                    model         = _GROQ_MODEL,
                )
                print(f"  [Codegen] Written → {os.path.basename(codegen_path)}")
                log_trace(
                    trace_file,
                    f"ATTEMPT {attempt} - CODEGEN FILE",
                    f"Saved to: {codegen_path}\n"
                    f"Python code present: {bool(python_code)}\n"
                    f"Discrepancies      : {len(discrepancies)}",
                )
            else:
                log_trace(
                    trace_file,
                    f"ATTEMPT {attempt} - CODEGEN FILE SKIPPED",
                    "create_headers_file=False — headers .py file not written.",
                )

            log_trace(trace_file, f"ATTEMPT {attempt} - SUCCESS",
                      f"{len(mapping)} header(s) mapped.\n"
                      f"Mapping:\n"
                      + "\n".join(f"  {k!r:40s} → {v!r}" for k, v in sorted(mapping.items())))
            return mapping

        except Exception as e:
            tb_str    = traceback.format_exc()
            error_str = str(e)
            print(f"  -> API error: {e}")
            log_trace(trace_file, f"ATTEMPT {attempt} - API ERROR",
                      f"{error_str}\n\n--- traceback ---\n{tb_str}")

            # ── Rate-limit handling ────────────────────────────────────────────
            if "429" in error_str or "rate_limit_exceeded" in error_str:
                # Record how long THIS key must cool down
                wait_seconds = _parse_retry_after(error_str)
                if wait_seconds > 0:
                    _record_rate_limit(current_key, wait_seconds)
                    print(f"  -> Key ...{current_key[-6:]} rate-limited for "
                          f"{wait_seconds / 60:.1f} min.")
                else:
                    _record_rate_limit(current_key, 60.0)

                # Try to switch to another key that is already available
                next_idx = _next_available_idx()
                if next_idx is not None:
                    _ACTIVE_IDX[0] = next_idx
                    new_key = _active_key()
                    print(f"  -> Switching to key ...{new_key[-6:]} (available now).")
                    log_trace(trace_file, f"ATTEMPT {attempt} - KEY ROTATION",
                              f"Switched to key ...{new_key[-6:]}.")
                    continue   # retry immediately with the new key

                # All keys are cooling down → sleep until the soonest wakes up
                sleep_secs = _shortest_wait()
                print(f"  -> All keys rate-limited. Sleeping "
                      f"{sleep_secs / 60:.1f} min ({sleep_secs:.0f} s) "
                      f"until next key is available …")
                log_trace(trace_file, f"ATTEMPT {attempt} - ALL KEYS RATE LIMITED",
                          f"Sleeping {sleep_secs:.0f} s.")
                time.sleep(sleep_secs)

                # Activate the key that just became free
                next_idx = _next_available_idx()
                if next_idx is not None:
                    _ACTIVE_IDX[0] = next_idx
                continue   # retry

            # Non-rate-limit error: do not loop endlessly
            break

    print("  [Groq] All attempts exhausted. Using rule-based fallback.")
    log_trace(trace_file, "FALLBACK", "basic_sql_normalize used for all headers.")
    return {h: basic_sql_normalize(h) for h in headers}


# ===========================================================================
# Output writers
# ===========================================================================

def write_context_file(content: dict, filepath: str) -> None:
    """
    Write a plain-text context file:
        TITLE: <title>
        ============================================================

        <paragraph 1>

        <paragraph 2>
        ...
    """
    with open(filepath, "w", encoding="utf-8") as f:
        if content["title"]:
            f.write(f"TITLE: {content['title']}\n")
            f.write("=" * 60 + "\n\n")
        for para in content["paragraphs"]:
            f.write(para + "\n\n")


def write_tables_headers_csv(tables: list[dict], filepath: str) -> None:
    """
    Write a semicolon-delimited CSV with:
      1. SQL-normalised column header row(s)  <- AI-normalised, no annotations
         If no headers could be determined, a synthetic _col_0, _col_1 … row
         is written so the CSV is never silently headerless.
      2. All data rows verbatim from the XML
      3. One blank separator row between tables

    Table titles are NOT written here — they are already routed to the
    context file by extract_text_content() via the table_titles argument.
    Writing them here too would pollute the CSV with non-data rows.

    Spanning annotation rows ("Numbering for target sequences is based on …")
    are NOT written here either — they have already been routed to the context
    file by extract_tables() + extract_text_content().
    """
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        for tbl in tables:
            sql_headers = tbl.get("sql_headers", [])
            raw_headers = tbl.get("headers", [])
            data_rows   = tbl.get("rows", [])

            # ── 1. Column header row(s) ──────────────────────────────────────
            header_rows = sql_headers or raw_headers
            if header_rows:
                for hrow in header_rows:
                    writer.writerow(hrow)
            else:
                # No headers found in the XML — synthesise positional names
                # so the CSV always has a header row (never silently headerless).
                ncols = max((len(r) for r in data_rows), default=1)
                writer.writerow([f"_col_{i}" for i in range(ncols)])

            # ── 2. Data rows ─────────────────────────────────────────────────
            for drow in data_rows:
                writer.writerow(drow)

            # ── 3. Blank separator between tables ────────────────────────────
            writer.writerow([])


# ===========================================================================
# Main entry point
# ===========================================================================

def convert_directory(
    input_dir:            str,
    output_dir:           str | None = None,
    api_keys:             list[str] | str | None = None,
    create_headers_file:  bool = False,
) -> None:
    """
    Scan *input_dir* for XML files and produce two outputs per file:

      <base>_context.txt             – document title + text paragraphs
      <base>_tables.csv              – SQL-normalised table headers + data rows
      <base>_llm_normalize.py       – (optional) reproducible Python function
                                       placed beside the original XML file when
                                       create_headers_file=True.  Encodes every
                                       normalisation rule the LLM applied
                                       (audit trail / guardrail).

    A trace log is also written to debug/ inside *output_dir*.

    The generated ``<base>_llm_normalize.py`` file is runnable standalone:

        python <base>_llm_normalize.py

    It will verify all mappings and exit 0 if they are correct, non-zero if
    any discrepancy is detected — making it a self-contained reproducibility
    check that catches any LLM shortcutting at generation time.

    Parameters
    ----------
    input_dir            : directory containing the XML files.
    output_dir           : where to write output files (default: same as input_dir).
    api_keys             : a Groq API key, a list of keys, or a comma-separated string.
                           Falls back to the GROQ_API_KEY environment variable if omitted.
    create_headers_file  : when True, write ``<base>_llm_normalize.py`` beside each
                           processed XML file (same directory as *input_dir*).
                           Defaults to False — no file is created.

    Jupyter example
    ---------------
        groq_api_key1 = "gsk_..."
        groq_api_key2 = "gsk_..."
        groq_api_key3 = "gsk_..."
        groq_api_key4 = "gsk_..."

        from xml_to_csv import convert_directory

        XML_DIR = "isolated_tables"
        CSV_DIR = "csv_output"

        convert_directory(
            XML_DIR,
            output_dir=CSV_DIR,
            api_keys=f"{groq_api_key1},{groq_api_key2},{groq_api_key3},{groq_api_key4}",
            create_headers_file=True,   # optional: write <base>_llm_normalize.py beside XML
        )
    """
    # ── Initialise client pool ───────────────────────────────────────────────
    _init_clients(api_keys)

    if output_dir is None:
        output_dir = input_dir
    else:
        os.makedirs(output_dir, exist_ok=True)

    # ── Debug directory ──────────────────────────────────────────────────────
    debug_dir   = os.path.join(output_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)

    session_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_log     = os.path.join(debug_dir, f"session_{session_ts}.log")
    _SESSION_LOG[0] = session_log   # all log_trace() calls mirror here automatically

    xml_files = glob.glob(os.path.join(input_dir, "*.xml"))

    if not xml_files:
        print(f"No XML files found in '{input_dir}'.")
        return

    log_trace(session_log, "SESSION START",
              f"{len(xml_files)} XML file(s) | {len(_CLIENTS)} Groq key(s)\n"
              f"Output dir : {output_dir}\n"
              f"Debug dir  : {debug_dir}")

    for xml_file in sorted(xml_files):
        base        = os.path.splitext(os.path.basename(xml_file))[0]
        ctx_path    = os.path.join(output_dir, f"{base}_context.txt")
        tables_path = os.path.join(output_dir, f"{base}_tables.csv")

        file_log = os.path.join(debug_dir, f"{base}.log")
        with open(file_log, "w", encoding="utf-8") as _fl:
            _fl.write(f"=== DEBUG LOG: {base}.xml | "
                      f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")

        print(f"\nProcessing: {base}.xml")
        log_trace(file_log, f"FILE: {base}.xml", "Starting extraction.")

        # ── Extract raw content ──────────────────────────────────────────────
        try:
            tables = extract_tables(xml_file)
            # Collect all spanning annotation strings from every table so they
            # can be appended to the context file instead of the CSV.
            all_annotations = [
                note
                for tbl  in tables
                for note in tbl.get("annotations", [])
            ]
            all_titles = [
                tbl["title"]
                for tbl in tables
                if tbl.get("title", "").strip()
            ]
            text_content = extract_text_content(
                xml_file,
                table_annotations=all_annotations,
                table_titles=all_titles,
            )
        except Exception as exc:
            print(f"  ERROR reading file: {exc}")
            log_trace(file_log, f"FILE: {base}.xml - READ ERROR",
                      f"{exc}\n\n--- traceback ---\n{traceback.format_exc()}")
            continue

        # ── Header processing: two AI passes ────────────────────────────────
        #
        # Pass 1 (repair_headers_with_ai) — structure repair, per table.
        #   Multi-row headers (group labels + leaf labels) are collapsed into
        #   a single flat list of qualified human-readable column names by the
        #   LLM.  This handles any layout the XML parser produces — Day3/Day5/Day7
        #   over concentration rows, morerows spanning, OCR artefacts, etc. —
        #   without any brittle rule-based heuristics.
        #   Only called for tables that have more than one header row.
        #   Falls back to merge_multilevel_headers if the LLM is unavailable.
        #
        # Pass 2 (normalize_headers_with_ai) — SQL normalisation, batched per file.
        #   Takes the clean human-readable strings from Pass 1 and converts them
        #   to SQL-compatible identifiers.  Same as before.

        for tbl_idx, tbl in enumerate(tables):
            raw_header_rows = tbl.get("headers", [])
            # Pass 1: ask Groq to repair/merge headers.
            # repair_headers_with_ai returns None for clean single-row headers
            # (where the first data row has numeric values), so there is no
            # wasted API call for tables that are already well-formed.
            repaired = repair_headers_with_ai(
                header_rows = raw_header_rows,
                sample_rows = tbl.get("rows", [])[:4],
                trace_file  = file_log,
                table_title = tbl.get("title", ""),
            )
            if repaired is not None:
                tbl["headers"] = [repaired]
                log_trace(file_log, f"REPAIR TABLE {tbl_idx}",
                          f"AI repair applied:\n" +
                          "\n".join(f"  [{i:2d}] {h!r}" for i, h in enumerate(repaired)))
            else:
                tbl["headers"] = [merge_multilevel_headers(raw_header_rows)]
                log_trace(file_log, f"REPAIR TABLE {tbl_idx}",
                          "No repair needed (or fallback); merge_multilevel_headers used.")

        # merged_flat[i] is the (now single-row) header list for tables[i]
        merged_flat: list[list[str]] = [
            list(tbl["headers"][0]) if tbl.get("headers") else []
            for tbl in tables
        ]

        # Unique raw (un-normalised) merged strings to send to the LLM
        unique_headers: list[str] = sorted({
            cell
            for mf in merged_flat
            for cell in mf
            if cell.strip()
        })

        if unique_headers:
            print(f"  [Pass 2] Normalising {len(unique_headers)} unique header(s) to SQL via Groq …")
            codegen_path = (
                os.path.join(input_dir, f"{base}_llm_normalize.py")
                if create_headers_file
                else ""
            )
            mapping = normalize_headers_with_ai(
                unique_headers,
                trace_file=file_log,
                codegen_path=codegen_path,
                create_headers_file=create_headers_file,
            )
        else:
            mapping = {}

        # ── Apply mapping + Fix 2 (duplicate SQL names) + Fix 4 (duplex_id) ──
        #
        # Fix 2: After the LLM assigns SQL names we scan each table's flat
        # header list for repeated values and suffix subsequent occurrences
        # with their column index so SQL never sees two identical column names.
        #
        # Fix 4: If the first data column contains AD-\d+ style duplex IDs
        # but no header was found (empty / positional placeholder), replace
        # that column's SQL name with duplex_id.

        _AD_RE = re.compile(r"^AD-\d+", re.IGNORECASE)

        for tbl_idx, tbl in enumerate(tables):
            mf      = merged_flat[tbl_idx]   # flat merged headers for this table
            data    = tbl.get("rows", [])

            # Resolve each merged header to its SQL name via the LLM mapping
            sql_flat: list[str] = [
                mapping.get(cell, basic_sql_normalize(cell)) if cell.strip() else ""
                for cell in mf
            ]

            # Fix 4 – detect missing duplex_id from first data column
            if sql_flat and data:
                first_col_values = [row[0] for row in data if row and row[0].strip()]
                ad_hits = sum(1 for v in first_col_values if _AD_RE.match(v))
                if (
                    ad_hits >= max(1, len(first_col_values) // 2)
                    and sql_flat[0] != "duplex_id"   # ← was: sql_flat[0] in ("", "_col_0", "col0", "col_0")
                    and "duplex_id" not in sql_flat   # don't add a second duplex_id column
                ):
                    sql_flat[0] = "duplex_id"

            # Fix 5 – normalise any header that contains the word "duplex" -> duplex_id
            #
            # "Duplex Name" maps correctly via the LLM prompt, but when
            # merge_multilevel_headers collapses two header rows that both carry
            # "Duplex Name" in the same column, the result is
            # "Duplex Name Duplex Name".  The LLM prompt has no example for that
            # string, so basic_sql_normalize produces "duplex_name_duplex_name".
            # More broadly, any header whose SQL form contains the word "duplex"
            # (e.g. "duplex", "duplex_name", "duplex_name_duplex_name",
            # "duplex_id", "duplex_num", …) should be normalised to "duplex_id".
            # This covers both the rule-based fallback and any unexpected LLM
            # variant that still carries the word "duplex".
            _DUPLEX_RE = re.compile(r"(^|_)duplex(_|$)", re.IGNORECASE)
            for fix5_idx, name in enumerate(sql_flat):
                if name != "duplex_id" and _DUPLEX_RE.search(name):
                    log_trace(
                        file_log,
                        f"FIX5 TABLE {tbl_idx}",
                        f"Renamed '{name}' at col {fix5_idx} to 'duplex_id'.",
                    )
                    sql_flat[fix5_idx] = "duplex_id"

            # Fix 2 – de-conflict duplicate SQL names within the same table
            seen_names: dict[str, int] = {}
            deduped_flat: list[str] = []
            for col_idx, name in enumerate(sql_flat):
                if not name:
                    deduped_flat.append(name)
                    continue
                if name not in seen_names:
                    seen_names[name] = col_idx
                    deduped_flat.append(name)
                else:
                    unique_name = f"{name}_{col_idx}"
                    log_trace(
                        file_log,
                        f"FIX2 TABLE {tbl_idx}",
                        f"Duplicate SQL name '{name}' at col {col_idx} "
                        f"renamed to '{unique_name}'.",
                    )
                    deduped_flat.append(unique_name)

            # Store as a single sql_headers row (the merged representation)
            tbl["sql_headers"] = [deduped_flat]

        # ── Write outputs ────────────────────────────────────────────────────
        write_context_file(text_content, ctx_path)
        print(f"  -> {os.path.basename(ctx_path)}"
              f"  ({len(text_content['paragraphs'])} paragraph(s))")

        if tables:
            write_tables_headers_csv(tables, tables_path)
            print(f"  -> {os.path.basename(tables_path)}"
                  f"  ({len(tables)} table(s))")
        else:
            print(f"  SKIP tables file (no data tables found in {base}.xml)")

    log_trace(session_log, "SESSION END", "All files processed.")
    print(f"\nDone.  Debug logs → {debug_dir}/")