# EPO siRNA Patent Intelligence Pipeline

An automated pipeline for extracting, curating, and harmonising patent metadata from the **European Patent Office (EPO) Open Patent Services (OPS) API**, with a focus on the competitive landscape of **RNA interference (siRNA/RNAi)** therapeutics.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Repository Structure](#repository-structure)
- [Search Strategy](#search-strategy)
- [API Constraints and How They Are Handled](#api-constraints-and-how-they-are-handled)
- [Output Files and Columns](#output-files-and-columns)
- [Prerequisites and Installation](#prerequisites-and-installation)
- [Quick Start](#quick-start)
- [Detailed Usage](#detailed-usage)
  - [Phase 1a — Extraction by Classification Codes](#phase-1a--extraction-by-classification-codes-epo_api_codespy)
  - [Phase 1b — Extraction by Keywords](#phase-1b--extraction-by-keywords-epo_api_termspy)
  - [Phase 2 — Bibliographic Metadata](#phase-2--bibliographic-metadata-epo_api_metadatapy)
  - [Comparing Extractions](#comparing-extractions-compare_patentspy)
- [EPO OPS API Credentials](#epo-ops-api-credentials)
- [Known Issues and Edge Cases](#known-issues-and-edge-cases)

---

## Overview

This project solves a specific challenge in patent intelligence: extracting a **complete, deduplicated, and analysis-ready** dataset of siRNA/RNAi patents from the EPO, while respecting the strict technical constraints of the EPO OPS API (rate limits, hard pagination caps, token expiry, and weekly data quotas).

The pipeline is designed to answer questions such as:
- Who are the dominant assignees in siRNA therapeutics over the last five years?
- Which classification codes are most actively patented?
- How does a broad keyword search compare to a classification-code search in terms of patent family coverage?

---

## Pipeline Architecture

The pipeline is split into two sequential phases.

```
┌──────────────────────────────────────────────────────────────┐
│                        PHASE 1 — ID Extraction               │
│                                                              │
│   epo_api_codes.py  ──┐                                      │
│   (CPC/IPC codes)     ├──► CSV: Patent_ID, Family_ID, Country│
│                        │                                      │
│   epo_api_terms.py  ──┘                                      │
│   (Keyword search)                                           │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                     PHASE 2 — Metadata Fetch                  │
│                                                              │
│   epo_api_Metadata.py ──► CSV: Full bibliographic metadata   │
│   (/biblio endpoint)        Title, Abstract, Applicant,      │
│                             Dates, IPC/CPC codes, ...        │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                      OPTIONAL — Comparison                    │
│                                                              │
│   compare_patents.py ──► Only-in-A, Only-in-B, Shared CSVs  │
└──────────────────────────────────────────────────────────────┘
```

Phase 1 is **intentionally lightweight**: it makes only search queries (no `/biblio` calls) and produces a clean, deduplicated list of patent family IDs. This minimises API quota consumption and gives you a checkpoint before the more expensive Phase 2.

---

## Repository Structure

```
├── epo_api_codes.py       # Phase 1a: ID extraction using CPC/IPC classification codes
├── epo_api_terms.py       # Phase 1b: ID extraction using keyword/title-abstract search
├── epo_api_Metadata.py    # Phase 2: Full bibliographic metadata fetch via /biblio
├── compare_patents.py     # Utility: Compare two Phase 1 CSVs by patent Family_ID
└── run_code.ipynb         # Jupyter Notebook with end-to-end worked examples
```

---

## Search Strategy

### Method 1 — Classification Codes (`epo_api_codes.py`)

Uses CPC (Cooperative Patent Classification) and IPC (International Patent Classification) codes to identify relevant patents. These codes are assigned by patent examiners and function as precise technical hashtags.

**Why search at `C12N15/11` rather than `C12N15/113`?**
The specific subclasses created for non-coding RNA and RNAi therapies (e.g. `C12N15/113`) were only formally introduced in 2006. Searching one level up at `C12N15/11` captures pioneering patents filed before this date, ensuring no historical prior art is missed.

**Active exclusions:** Aptamers (`C12N15/115`) and Immunomodulatory agents (`C12N15/117`) are excluded from every query to keep the dataset focused on siRNA/RNAi.

**Applicant filtering:** An optional `applicant_filter` parameter restricts results to patents from a specific company (e.g. `"Alnylam*"`). Setting `only_applicant=True` builds the query purely from the company name, bypassing the classification codes entirely — useful for fetching a complete company portfolio regardless of technology area.

### Method 2 — Keywords (`epo_api_terms.py`)

Searches for siRNA-related terms exclusively in the **Title and Abstract** fields of patent documents (using the EPO CQL `ta=` parameter).

Example terms include: `siRNA*`, `RNAi*`, `"RNA interference"`, `"short interfering RNA"`, `dsRNA*`.

Each term is run as an independent query so that the results can be merged and deduplicated, and so that each individual query stays below the EPO's 2,000-result hard cap.

### Patent Family Deduplication

A single invention is routinely filed in multiple jurisdictions (US, EP, WO, JP, etc.). The EPO groups all equivalent documents under a single `Family_ID` (EPO Simple Family).

Both Phase 1 scripts deduplicate results by `Family_ID` so that **each invention counts exactly once**. When multiple documents from the same family are found, the pipeline selects one representative using the following jurisdiction priority (lower = preferred):

| Priority | Office |
|----------|--------|
| 1        | WO     |
| 2        | US     |
| 3        | EP     |
| 4        | GB     |
| 5        | AU     |
| 6        | CA     |
| 7        | NZ     |
| 8        | IE     |

English-publishing offices are ranked highest so that titles and abstracts fetched in Phase 2 are preferentially available in English.

---

## API Constraints and How They Are Handled

| Constraint | How it is handled |
|---|---|
| **2,000-result search cap** | Each query is split by year. Years with >2,000 results are sliced by month; months with >2,000 results are sliced by day. |
| **Token expiry (900 s)** | A global token cache tracks token age and auto-refreshes with a 50-second safety margin before expiry. |
| **10 req/min rate limit** | Conservative `time.sleep()` calls are placed between every paginated request and every batch fetch. |
| **100-ID `/biblio` batch limit** | Phase 2 sends batches of exactly 100 IDs. Failed batches are retried up to 3 times, then each ID is retried individually to isolate failures. |
| **Missing abstracts** | If `/biblio` returns no abstract, a fallback call is made to the dedicated `/abstract` endpoint, preferring English. |
| **Weekly 4 GB data quota** | `check_epo_quota()` reads live quota headers before any run and prints a dashboard. The run continues regardless, but you are warned if the quota is exceeded. |
| **Yearly data loss on long runs** | Each year's results are autosaved to `EPO_IDs_AutoSave_{year}.csv` immediately after processing. |
| **Leading-zero Family_ID inconsistency** | `compare_patents.py` normalises all Family_IDs by stripping leading zeros before any comparison, so `"84545942"` and `"084545942"` are treated as the same family. |

---

## Output Files and Columns

### Phase 1 output (both `epo_api_codes.py` and `epo_api_terms.py`)

The output filename is generated automatically based on the parameters used:

| Scenario | Filename |
|---|---|
| Codes only | `EPO_siRNA_IDs_{start}_{end}_codes_only.csv` |
| Codes + applicant filter | `EPO_siRNA_IDs_{start}_{end}_codes_and_{Applicant}.csv` |
| Applicant portfolio only | `EPO_siRNA_IDs_{start}_{end}_only_applicant_{Applicant}.csv` |
| Terms only | `EPO_siRNA_IDs_{start}_{end}_terms_only.csv` |
| Terms + applicant filter | `EPO_siRNA_IDs_{start}_{end}_terms_and_{Applicant}.csv` |

**Columns:**

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier, e.g. `US.7056704.B2` |
| `Family_ID` | EPO Simple Family identifier |
| `Country` | Two-letter office code, e.g. `US`, `EP`, `WO` |

### Phase 2 output (`epo_api_Metadata.py`)

The output filename appends `_metadata` to the Phase 1 filename, e.g. `EPO_siRNA_IDs_2022_2025_terms_only_metadata.csv`.

**Columns:**

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier |
| `Country` | Two-letter office code |
| `Number` | Patent number without punctuation |
| `Kind` | Kind code, e.g. `B2`, `A1` |
| `Family_ID` | EPO Simple Family identifier |
| `Priority_Date` | Earliest priority date (`YYYYMMDD`) |
| `Publication_Date` | Publication date (`YYYYMMDD`) |
| `Applicant` | Pipe-separated list of applicant names with country codes |
| `Title` | Invention title (English preferred) |
| `Abstract` | Full abstract text (English preferred, HTML-stripped) |
| `IPCs` | Comma-separated IPC classification codes |
| `CPCs` | Comma-separated CPC classification codes |

All CSVs use `;` as the separator and `utf-8-sig` encoding for Excel compatibility.

---

## Prerequisites and Installation

Python 3.9+ is required. Install dependencies with:

```bash
pip install pandas requests numpy
```

To run the notebook interactively:

```bash
pip install jupyter
jupyter notebook run_code.ipynb
```

---

## Quick Start

1. Obtain EPO OPS API credentials at [ops.epo.org](https://ops.epo.org).
2. Open `run_code.ipynb`.
3. Set your `CONSUMER_KEY` and `CONSUMER_SECRET` in the first cell.
4. Run the cells sequentially: Phase 1 → Phase 2 → (optional) comparison.

---

## Detailed Usage

### Phase 1a — Extraction by Classification Codes (`epo_api_codes.py`)

```python
import epo_api_codes

CONSUMER_KEY    = "your_key"
CONSUMER_SECRET = "your_secret"

# Optional: check your weekly quota before starting
epo_api_codes.check_epo_quota(CONSUMER_KEY, CONSUMER_SECRET)

# Extract all siRNA-related patent families, 2015–2025
df_ids = epo_api_codes.download_patent_ids(
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
    start_year      = 2015
## API Constraints and How They Are Handled

| Constraint | How it is handled |
|---|---|
| **2,000-result search cap** | Each query is split by year. Years with >2,000 results are sliced by month; months with >2,000 results are sliced by day. |
| **Token expiry (900 s)** | A global token cache tracks token age and auto-refreshes with a 50-second safety margin before expiry. |
| **10 req/min rate limit** | Conservative `time.sleep()` calls are placed between every paginated request and every batch fetch. |
| **100-ID `/biblio` batch limit** | Phase 2 sends batches of exactly 100 IDs. Failed batches are retried up to 3 times, then each ID is retried individually to isolate failures. |
| **Missing abstracts** | If `/biblio` returns no abstract, a fallback call is made to the dedicated `/abstract` endpoint, preferring English. |
| **Weekly 4 GB data quota** | `check_epo_quota()` reads live quota headers before any run and prints a dashboard. The run continues regardless, but you are warned if the quota is exceeded. |
| **Yearly data loss on long runs** | Each year's results are autosaved to `EPO_IDs_AutoSave_{year}.csv` immediately after processing. |
| **Leading-zero Family_ID inconsistency** | `compare_patents.py` normalises all Family_IDs by stripping leading zeros before any comparison, so `"84545942"` and `"084545942"` are treated as the same family. |

---

## Output Files and Columns

### Phase 1 output (both `epo_api_codes.py` and `epo_api_terms.py`)

The output filename is generated automatically based on the parameters used:

| Scenario | Filename |
|---|---|
| Codes only | `EPO_siRNA_IDs_{start}_{end}_codes_only.csv` |
| Codes + applicant filter | `EPO_siRNA_IDs_{start}_{end}_codes_and_{Applicant}.csv` |
| Applicant portfolio only | `EPO_siRNA_IDs_{start}_{end}_only_applicant_{Applicant}.csv` |
| Terms only | `EPO_siRNA_IDs_{start}_{end}_terms_only.csv` |
| Terms + applicant filter | `EPO_siRNA_IDs_{start}_{end}_terms_and_{Applicant}.csv` |

**Columns:**

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier, e.g. `US.7056704.B2` |
| `Family_ID` | EPO Simple Family identifier |
| `Country` | Two-letter office code, e.g. `US`, `EP`, `WO` |

### Phase 2 output (`epo_api_Metadata.py`)

The output filename appends `_metadata` to the Phase 1 filename, e.g. `EPO_siRNA_IDs_2022_2025_terms_only_metadata.csv`.

**Columns:**

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier |
| `Country` | Two-letter office code |
| `Number` | Patent number without punctuation |
| `Kind` | Kind code, e.g. `B2`, `A1` |
| `Family_ID` | EPO Simple Family identifier |
| `Priority_Date` | Earliest priority date (`YYYYMMDD`) |
| `Publication_Date` | Publication date (`YYYYMMDD`) |
| `Applicant` | Pipe-separated list of applicant names with country codes |
| `Title` | Invention title (English preferred) |
| `Abstract` | Full abstract text (English preferred, HTML-stripped) |
| `IPCs` | Comma-separated IPC classification codes |
| `CPCs` | Comma-separated CPC classification codes |

All CSVs use `;` as the separator and `utf-8-sig` encoding for Excel compatibility.

---

## Prerequisites and Installation

Python 3.9+ is required. Install dependencies with:

```bash
pip install pandas requests numpy
```

To run the notebook interactively:

```bash
pip install jupyter
jupyter notebook run_code.ipynb
```

---

## Quick Start

1. Obtain EPO OPS API credentials at [ops.epo.org](https://ops.epo.org).
2. Open `run_code.ipynb`.
3. Set your `CONSUMER_KEY` and `CONSUMER_SECRET` in the first cell.
4. Run the cells sequentially: Phase 1 → Phase 2 → (optional) comparison.

---

## Detailed Usage

### Phase 1a — Extraction by Classification Codes (`epo_api_codes.py`)

```python
import epo_api_codes

CONSUMER_KEY    = "your_key"
CONSUMER_SECRET = "your_secret"

# Optional: check your weekly quota before starting
epo_api_codes.check_epo_quota(CONSUMER_KEY, CONSUMER_SECRET)

# Extract all siRNA-related patent families, 2015–2025
df_ids = epo_api_codes.download_patent_ids(
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
    start_year      = 2015,
    end_year        = 2025,
    applicant_filter = None,   # None = all applicants
    only_applicant   = False,  # False = use CPC/IPC codes (default)
)

# Narrow to one company's full portfolio (codes are ignored)
df_alnylam = epo_api_codes.download_patent_ids(
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
    start_year      = 2022,
    end_year        = 2025,
    applicant_filter = "Alnylam*",  # Wildcard supported
    only_applicant   = True,        # Build query from applicant name only
)
```

**`download_patent_ids` parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `consumer_key` | str | required | EPO OPS API key |
| `consumer_secret` | str | required | EPO OPS API secret |
| `start_year` | int | required | First year of the extraction range |
| `end_year` | int | required | Last year of the extraction range (inclusive) |
| `applicant_filter` | str or None | `None` | Applicant name to filter by. Wildcards accepted (e.g. `"Alnylam*"`). `None` returns all applicants. |
| `only_applicant` | bool | `False` | If `True`, builds the query solely from the applicant name, ignoring all CPC/IPC codes. Use this to retrieve a complete company portfolio. |

---

### Phase 1b — Extraction by Keywords (`epo_api_terms.py`)

The interface is identical to `epo_api_codes.py`. Replace the import and the function will search Title & Abstract instead of classification codes.

```python
import epo_api_terms

df_terms = epo_api_terms.download_patent_ids(
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
    start_year      = 2022,
    end_year        = 2025,
)
```

---

### Phase 2 — Bibliographic Metadata (`epo_api_Metadata.py`)

Reads the CSV produced by Phase 1 and fetches full metadata via the `/biblio` endpoint.

```python
from epo_api_Metadata import fetch_biblio_from_csv

df_metadata = fetch_biblio_from_csv(
    ids_csv         = "EPO_siRNA_IDs_2022_2025_terms_only.csv",
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
)
```

Phase 2 is not subject to the 2,000-result search cap because it makes direct `/biblio` calls rather than search queries. However, the `/biblio` endpoint accepts a maximum of **100 patent IDs per request**, so the script automatically splits your ID list into batches of 100 and processes them sequentially. The overall practical limit on total patents fetchable in one run is your weekly 4 GB data quota.

---

### Comparing Extractions (`compare_patents.py`)

Useful for understanding the overlap between a codes-based extraction and a terms-based extraction, or between two different applicant filters.

```python
from compare_patents import compare

only_codes, only_terms, shared = compare(
    "EPO_siRNA_IDs_2022_2025_codes_only.csv",
    "EPO_siRNA_IDs_2022_2025_terms_only.csv",
)
```

**Returns three DataFrames and saves three CSVs automatically:**

| Return value | Saved as | Description |
|---|---|---|
| `only_a` | `patentes_only_{label_a}.csv` | Families found exclusively in file A |
| `only_b` | `patentes_only_{label_b}.csv` | Families found exclusively in file B |
| `shared` | `patentes_in_commum_{label_a} and {label_b}.csv` | Families present in both files |

Set `save=False` to suppress CSV output and work with the DataFrames directly.

---

## EPO OPS API Credentials

1. Register at [ops.epo.org](https://ops.epo.org/3.2/). A free account includes a **4 GB weekly data quota**.
2. Create an application under your account to receive a `Consumer Key` and `Consumer Secret`.
3. Tokens are short-lived (900 seconds). The scripts manage token renewal automatically — you never need to call the auth endpoint manually.

---

## Known Issues and Edge Cases

**Family ID leading zeros:** The EPO OPS API occasionally returns the same family under two representations — with and without a leading zero (e.g. `"84545942"` vs `"084545942"`). `compare_patents.py` normalises both sides before comparison. Phase 2 also includes a final deduplication pass to catch any residual inconsistencies introduced by the `/biblio` endpoint.

**Missing abstracts:** Some patents, particularly older ones or those from certain jurisdictions, have no abstract in the EPO database. These are flagged with the placeholder value `"No abstract available in EPO database"` in the output.

**Non-Latin titles:** Phase 2's deduplication logic deprioritises records with non-Latin-script titles (e.g. Chinese or Japanese) in favour of English equivalents when both are available for the same family.

**Day-level truncation:** If a single calendar day produces more than 2,000 results for a given query — an extremely rare edge case — the API hard cap will truncate results and a `[WARNING]` is printed. No automatic workaround exists at sub-day granularity.

**Rate limiting (HTTP 429 / 503):** The scripts back off automatically with exponential waits. A `403` response (quota exceeded mid-run) triggers a 10-minute pause before retrying.
