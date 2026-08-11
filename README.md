# EPO siRNA Patent Intelligence Pipeline

An automated pipeline that goes from the **European Patent Office (EPO) Open Patent Services (OPS) API** to a structured, analysis-ready dataset of **siRNA/RNAi** activity data extracted from patent full text.

The pipeline answers two different kinds of question. The first four stages answer landscape questions: who is patenting siRNA, under which classification codes, and how well different search strategies cover the field. The last three stages go inside the documents and pull out the experimental tables, producing one row per measurement (duplex, cell line, dose, percent knockdown, IC50, viability).

---

## Table of contents

- [Overview](#overview)
- [Pipeline architecture](#pipeline-architecture)
- [Repository structure](#repository-structure)
- [Prerequisites and installation](#prerequisites-and-installation)
- [Quick start](#quick-start)
- [API credentials](#api-credentials)
- [Stage 1: patent identifier extraction](#stage-1-patent-identifier-extraction)
- [Stage 2: bibliographic metadata](#stage-2-bibliographic-metadata)
- [Stage 3: tier classification](#stage-3-tier-classification)
- [Stage 4: full-text XML download](#stage-4-full-text-xml-download)
- [Stage 5: table isolation](#stage-5-table-isolation)
- [Stage 6: XML to CSV and header normalisation](#stage-6-xml-to-csv-and-header-normalisation)
- [Stage 7: primary table assembly](#stage-7-primary-table-assembly)
- [API constraints and how they are handled](#api-constraints-and-how-they-are-handled)
- [Known issues and edge cases](#known-issues-and-edge-cases)

---

## Overview

Patents hold a large amount of siRNA activity data that never reaches public databases: duplex sequences, target genes, cell lines, doses, percent knockdown, IC50 values and viability screens. The data sits in long free-text documents and in tables whose layout differs from one patent to the next.

Two problems have to be solved to get at it. The first is retrieval, staying inside the technical limits of the OPS API: rate limits, a hard 2000-result cap per query, 900 second tokens and a 4 GB weekly data quota. The second is structure, turning heterogeneous CALS-style patent tables into one fixed schema.

**Design rule for the LLM stages:** the language model never touches the data values. It only reads column headers and context, and writes the mapping (a DuckDB `SELECT`). DuckDB then executes that mapping locally against the CSV. Every value in the final dataset therefore comes from the patent, not from a model.

---

## Pipeline architecture

```
STAGE 1  identifier extraction                       EPO OPS search
  epo_api_codes.py    CPC/IPC classification codes ─┐
  epo_api_terms.py    title/abstract keywords      ─┴─► ID CSV
                                                        (Patent_ID, Family_ID, Country)
                            |
STAGE 2  bibliographic metadata                      EPO OPS /biblio
  epo_api_Metadata.py                              ──► *_metadata.csv
                            |
STAGE 3  tier classification                         local, rule based
  epo_filter.py                                    ──► *_metadata_filtered.csv
                            |
STAGE 4  full-text XML download                      EPO OPS + EPS
  xml_download.py                                  ──► eps_xmls/*.xml
                                                       successful_downloads.csv
                                                       not_in_eps.csv
                            |
STAGE 5  table isolation                             local, BeautifulSoup
  table.py                                         ──► isolated_tables/*.xml
                            |
STAGE 6  XML to CSV + header normalisation           Groq LLM
  xml_to_csv.py                                    ──► csv_output/*_tables.csv
                                                       csv_output/*_context.txt
                            |
STAGE 7  primary table assembly                      Groq LLM + DuckDB
  xml_to_primary_table.py (+ core, classification, sql)
                                                   ──► 1_final_tables/  knockdown, IC50, viability
                                                       2_review/        manifests
                                                       3_per_file_drafts/
                                                       4_trace/
```

Stages 1 to 4 consume EPO quota. Stages 6 and 7 consume Groq quota. Stages 3 and 5 are local and free.

Stage 1 is intentionally lightweight: search queries only, no `/biblio` calls, so it gives a cheap checkpoint before the expensive stages.

---

## Repository structure

```
epo_api_codes.py          Stage 1a: ID extraction using CPC/IPC classification codes
epo_api_terms.py          Stage 1b: ID extraction using title/abstract keywords
epo_api_Metadata.py       Stage 2: bibliographic metadata via /biblio
epo_filter.py             Stage 3: rule-based tier classification
xml_download.py           Stage 4: full-text XML download from the EPS
table.py                  Stage 5: isolates the experimental tables from each XML
xml_to_csv.py             Stage 6: table XML to CSV, with LLM header normalisation
xml_to_primary_table.py   Stage 7: assembles the three final tables
core.py                   Schemas, Groq client and key rotation, validation. Imports nothing else
classification.py         Decides what each table measures. Imports core
sql.py                    Builds and runs the DuckDB SQL. Imports core and classification
run_code.ipynb            Notebook running the whole pipeline end to end
```

All files must sit in the same folder. `xml_to_primary_table.py` imports `core`, `classification` and `sql` at load time.

---

## Prerequisites and installation

Python 3.9 or later.

```bash
pip install requests pandas numpy beautifulsoup4 lxml groq duckdb openpyxl
```

| Package | Needed by |
|---|---|
| `requests` | Stages 1, 2, 4 (EPO OPS and EPS) |
| `pandas`, `numpy` | all stages |
| `beautifulsoup4`, `lxml` | Stages 5 and 6 (XML parsing) |
| `groq` | Stages 6 and 7 |
| `duckdb` | Stage 7 |
| `openpyxl` | optional, Excel I/O for manual inspection |

To run the notebook:

```bash
pip install jupyter
jupyter notebook run_code.ipynb
```

---

## Quick start

1. Get EPO OPS credentials at [ops.epo.org](https://ops.epo.org) and a Groq API key at [console.groq.com](https://console.groq.com).
2. Export them, so no key ends up inside the notebook file:
   ```bash
   export EPO_CONSUMER_KEY=your_key
   export EPO_CONSUMER_SECRET=your_secret
   export GROQ_API_KEYS=key1,key2,key3
   ```
3. Open `run_code.ipynb` and run the Credentials cell. If a variable is not set, it prompts for the value instead.
4. Run the sections in order. Each one reads what the previous one wrote.

Stages can also be run on their own, as long as their input files are already in the working directory. Stages 4 to 7 are slow: EPS requests are spaced 8 seconds apart, and the Groq free tier limits how fast the LLM calls run.

---

## API credentials

**EPO OPS.** Register at [ops.epo.org](https://ops.epo.org/3.2/) and create an application to get a Consumer Key and a Consumer Secret. A free account includes a 4 GB weekly data quota. Access tokens live 900 seconds and the scripts renew them automatically, so the auth endpoint is never called by hand.

**Groq.** `GROQ_API_KEYS` accepts one key or several separated by commas, and the scripts rotate between them, moving to the next key when one hits its rate limit. Free-tier limits apply per account, so extra keys only raise throughput if they come from different accounts. When no key is passed to a function, `GROQ_API_KEY` is read from the environment instead.

**The European Publication Server** (Stage 4) is open and needs no credentials.

---

## Stage 1: patent identifier extraction

`epo_api_codes.py` and `epo_api_terms.py`, entry point `download_patent_ids` in both.

This stage collects family identifiers only. Each output row is `Patent_ID`, `Family_ID`, `Country`.

### Method 1: classification codes (`epo_api_codes.py`)

Classification codes are assigned by patent examiners, which makes them a structured and fairly reliable signal. Six independent queries are issued:

| Field | Code | Meaning | Operator |
|---|---|---|---|
| CPC | `C12N15/11` | Fragments of DNA or RNA | `/low`, all subgroups included |
| CPC | `A61K31/7088` | Medicinal preparations with three or more nucleotides | `/low` |
| IPC | `C12N15/113` | Non-coding nucleic acids regulating gene expression (siRNA, RNAi) | exact |
| IPC | `A61K31/713` | Medicinal preparations with double-stranded nucleic acids | exact |
| IPC | `C12N15/11` | Fragments of DNA or RNA | exact |
| IPC | `A61K31/7088` | Medicinal preparations with three or more nucleotides | exact |

**Why the CPC queries start at `C12N15/11` and not `C12N15/113`.** The subgroups for non-coding RNA and RNAi therapies, `C12N15/113` among them, only exist since 2006. Querying the parent with `/low` captures every subgroup plus the pioneering filings from before that date, so no historical prior art is lost.

**Exclusions.** Every query in both Stage 1 scripts carries `NOT (cpc="C12N15/115" OR cpc="C12N15/117")`, removing aptamers and immunomodulatory agents.

### Method 2: keywords (`epo_api_terms.py`)

Searches siRNA terms in the **title and abstract fields only**, through the CQL `ta=` parameter. Restricting the field stops incidental mentions deep in the body text from inflating the results.

27 independent queries, one per term, including `siRNA*`, `RNAi*`, `dsRNA*`, `iRNA*`, `oligonucleotide*`, `"RNA interference"`, `"small interfering RNA"`, `"short interfering RNA"`, `"double stranded RNA"`, `"RNA duplex"` and the "ribonucleic acid" spellings of each. One query per term keeps every query below the 2000-result cap and allows deduplicated merging afterwards.

### Family deduplication

One invention is normally filed in several jurisdictions. The EPO groups equivalent documents under one `Family_ID` (EPO Simple Family). Both scripts deduplicate by `Family_ID` so that **each invention counts once**, then pick one representative document by country priority, lowest score winning.

`epo_api_terms.py`, eight entries, anything else scoring 99:

| Score | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---|---|---|---|---|---|---|---|---|
| Office | EP | US | WO | GB | AU | CA | NZ | IE |

`epo_api_codes.py`, 35 entries: EP (1), US (2), WO (3), CA (6), then the EPC national offices alphabetically (AT 7 through SK 30), then GB (31), IE (32), CH (33), AU (34), NZ (35).

EP leads because the project is built on EPO full text; US and WO follow because they publish in English, which keeps Stage 2 titles and abstracts readable.

**Cross-year awareness.** Extraction runs year by year, and a `seen_families` dictionary carries forward the best country score already found, so a family met again later is only replaced when the new year offers a strictly better representative.

### Usage

```python
import os
import epo_api_codes

CONSUMER_KEY    = os.environ["EPO_CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["EPO_CONSUMER_SECRET"]

# Optional: weekly quota dashboard
epo_api_codes.check_epo_quota(CONSUMER_KEY, CONSUMER_SECRET)

df_ids = epo_api_codes.download_patent_ids(
    consumer_key     = CONSUMER_KEY,
    consumer_secret  = CONSUMER_SECRET,
    start_year       = 2015,
    end_year         = 2025,
    applicant_filter = None,    # None = every applicant
    only_applicant   = False,   # False = query built from CPC/IPC codes
)

# One company's complete portfolio, classification codes ignored
df_alnylam = epo_api_codes.download_patent_ids(
    consumer_key     = CONSUMER_KEY,
    consumer_secret  = CONSUMER_SECRET,
    start_year       = 2022,
    end_year         = 2025,
    applicant_filter = "Alnylam*",
    only_applicant   = True,
)
```

`epo_api_terms.py` has the identical signature. Both return the deduplicated DataFrame and write the CSV at the same time.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `consumer_key` | str | required | EPO OPS key |
| `consumer_secret` | str | required | EPO OPS secret |
| `start_year` | int | required | First year of the range |
| `end_year` | int | required | Last year, inclusive |
| `applicant_filter` | str or None | `None` | Applicant name, wildcards accepted. `None` returns every applicant |
| `only_applicant` | bool | `False` | If `True`, the query is built from the applicant name alone. Returns an empty DataFrame when `applicant_filter` is `None` |

### Output

| Scenario | Filename |
|---|---|
| Codes only | `EPO_siRNA_IDs_{start}_{end}_codes_only.csv` |
| Codes plus applicant filter | `EPO_siRNA_IDs_{start}_{end}_codes_and_{Applicant}.csv` |
| Terms only | `EPO_siRNA_IDs_{start}_{end}_terms_only.csv` |
| Terms plus applicant filter | `EPO_siRNA_IDs_{start}_{end}_terms_and_{Applicant}.csv` |
| Applicant portfolio (`only_applicant=True`) | `EPO_siRNA_IDs_{start}_{end}_only_applicant_{Applicant}.csv` |

The wildcard is stripped from the applicant name, so `"Alnylam*"` gives `..._only_applicant_Alnylam.csv`. Because the years are part of the name, changing them changes the filename every later stage has to read.

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier, for example `US.7056704.B2` |
| `Family_ID` | EPO Simple Family identifier |
| `Country` | Two-letter office code |

Each year is also autosaved to `EPO_IDs_AutoSave_{year}.csv` as it completes, so an interrupted multi-year run is not lost.

---

## Stage 2: bibliographic metadata

`epo_api_Metadata.py`, entry point `fetch_biblio_from_csv`.

```python
from epo_api_Metadata import fetch_biblio_from_csv

df_metadata = fetch_biblio_from_csv(
    ids_csv         = "EPO_siRNA_IDs_2022_2025_terms_only.csv",
    consumer_key    = CONSUMER_KEY,
    consumer_secret = CONSUMER_SECRET,
)
```

Reads a Stage 1 CSV and fetches the full record for every ID through `/biblio`. The output filename is the input with `_metadata` inserted before the extension.

How a long run is protected:

- IDs go out in batches of exactly 100, the `/biblio` hard limit.
- A failing batch is retried up to 3 times, then each ID in it is retried alone, so one bad record cannot cost the other 99.
- When `/biblio` returns no abstract, a second call is made to the dedicated `/abstract` endpoint, English preferred.
- 429 and 503 responses count as strikes. Three strikes trigger a 10 minute cooldown, and the pause between batches lengthens automatically while the server is under strain.
- A final pass deduplicates by `Family_ID`, preferring records with a usable English abstract and a Latin-script title, then the oldest priority date.

Stage 2 is not affected by the 2000-result cap, since it makes direct `/biblio` calls rather than search queries. The real limit is the weekly 4 GB quota.

| Column | Description |
|---|---|
| `Patent_ID` | DocDB identifier |
| `Country` | Two-letter office code |
| `Number` | Patent number without punctuation |
| `Kind` | Kind code, for example `B2`, `A1` |
| `Family_ID` | EPO Simple Family identifier |
| `Priority_Date` | Earliest priority date, `YYYYMMDD` |
| `Publication_Date` | Publication date, `YYYYMMDD` |
| `Applicant` | Applicant names with country codes, pipe separated |
| `Title` | Invention title, English preferred |
| `Abstract` | Abstract text, English preferred, HTML stripped |
| `IPCs` | IPC codes, comma separated |
| `CPCs` | CPC codes, comma separated |

---

## Stage 3: tier classification

`epo_filter.py`, entry point `apply_filters`. Local and rule based, no API calls.

```python
from epo_filter import apply_filters

result_df = apply_filters(
    raw_data        = "EPO_siRNA_IDs_2022_2025_terms_only_metadata.csv",
    output_filename = "EPO_siRNA_IDs_2022_2025_terms_only_metadata_filtered.csv",
    csv_sep         = ";",
    csv_encoding    = "utf-8-sig",
)
```

`raw_data` accepts a DataFrame already in memory or a path to a CSV.

Every patent is sorted into one of eight tiers from two signals: siRNA wording in the title and abstract, and the structural `C12N15/113` classification anchor. Tiers are tested in priority order and each patent gets the highest one it qualifies for, so they never overlap.

| Tier | Criterion | Recommended action |
|---|---|---|
| 1 | Wording **and** anchor | Core dataset, include without review |
| 2 | Wording only, anchor absent | High confidence, include. A missing CPC code is not disqualifying |
| 3 | Anchor only, no wording (abstract missing, non-English or uninformative) | Check on Espacenet before including |
| 4A | Wording or anchor, **plus** a competing-technology term (aptamer, antisense oligonucleotide, CRISPR) | Mixed technology, review to find the primary one |
| 4B | Diagnostic or biomarker language, no therapeutic application | Likely out of scope for a therapeutics analysis |
| 5 | Agricultural, veterinary or pest-control application **with** the anchor | Depends on scope (RNAi in plants and insects) |
| 6 | No wording and no anchor | Likely irrelevant, lowest priority |
| 7 | Agricultural or veterinary application **without** the anchor | Likely irrelevant |

**Nothing is deleted.** Every input record appears in the output with its tier, so curation stays transparent and reversible. Rows are grouped by tier, and each group is preceded by a separator row that is blank except for `Patent_ID`, which holds `--- TIER N ... (count patents) ---`. That makes the file readable by eye but means **you must drop the separator rows, or filter on the `Tier` column, before further processing**.

Output columns, those present in the input plus the derived ones:

| Column | Description |
|---|---|
| `Patent_ID`, `Priority_Date`, `Publication_Date`, `Applicant`, `Title`, `Abstract`, `IPCs`, `CPCs`, `Family_ID` | carried through from Stage 2 |
| `Espacenet_Link` | direct deep link to the Espacenet bibliographic record |
| `Tier` | the assigned tier label |
| `Has_Efficacy_Data` | `YES (EFFICACY)` when the abstract mentions IC50, EC50, ED50, KD, a percent knockdown or a residual-mRNA phrase. These are the patents most likely to carry usable tables |
| `Needs_Espacenet_Review` | `YES (REVIEW)` when the record has no abstract and a non-Latin title |
| `Warning` | carried through if the input has it |

The console prints a per-tier histogram plus the counts of records needing review and records with efficacy signals.

> **Limitation.** Keyword and CPC rules cannot handle negation or ambiguous phrasing. Replacing the rule for tiers 3, 4A and 4B with LLM-assisted abstract classification is the planned next iteration.

---

## Stage 4: full-text XML download

`xml_download.py`, entry point `download_eps_xmls_with_ops`.

```python
from xml_download import download_eps_xmls_with_ops

download_eps_xmls_with_ops(
    csv_filename     = "EPO_siRNA_IDs_2022_2025_only_applicant_Alnylam.csv",
    consumer_key     = CONSUMER_KEY,
    consumer_secret  = CONSUMER_SECRET,
    output_directory = "eps_xmls",
)
```

Downloads full text (description, claims and the experimental tables) from the **European Publication Server**, which is what Stage 5 reads. The input CSV needs `Patent_ID` and `Family_ID`.

**It works on the whole family, not a single publication.** Family members are not copies of each other: divisionals, continuations, and even the A (laid-open application) and B (granted) versions of the same application can carry different experimental data. Claims and sometimes the description are amended during prosecution, so an A document is not a substitute for its B. Rather than guessing which relative is best, the module downloads every EP member it can and leaves the comparison to a later step.

For each patent in the input CSV:

1. **The whole family** is fetched from OPS, all members and all countries, using both the `/equivalents` service and the `famn=<Family_ID>` search.
2. **Non-EP members** (US, WO, JP and so on) go straight to `not_in_eps.csv`. Only EP publications can have full text on the EPS, so they are never requested.
3. **Every EP publication number is probed across all kind codes** `A1, A2, A3, A4, B1, B2, B3`, and every document that exists is saved. The probe does not stop at the first hit, because OPS often reports only one kind per member even when both an application and a grant were published. EP numbers with no full text for any kind code are logged to `not_in_eps.csv` with the reason.

A document counts as full text only when the XML actually contains `<description>`, `<claims>` or `<table>`.

**Filenames** follow `EP{number}NW{kind}.xml`, for example `EP2723758NWB1.xml`. Files already in the output directory are reused without a request, so an interrupted run can simply be restarted.

**Throttling.** A strict 8 second pause follows every EPS request, successful or not. A full family sweep is deliberately slow.

Two CSVs are written to the working directory, both `;` separated:

| `successful_downloads.csv` | |
|---|---|
| `Downloaded_File` | the saved filename |
| `Member_Patent_ID` | the document ID, for example `EP2723758NWB1` |
| `Kind` | kind code actually retrieved |
| `Relationship` | `requested patent` or `family member` |
| `Requested_Patent` | the ID from the input CSV that led here |
| `Family_ID` | family identifier |

| `not_in_eps.csv` | |
|---|---|
| `Member_Patent_ID`, `Country`, `Kind` | the member with no full text |
| `Requested_Patent`, `Family_ID` | where it came from |
| `Reason` | `Non-EP member`, `no full XML on EPS for any kind code`, or `No family members returned by OPS` |

---

## Stage 5: table isolation

`table.py`, entry point `extract_tables_from_patent`. Local, no API calls.

```python
from table import extract_tables_from_patent
import glob, os

for path in sorted(glob.glob("eps_xmls/*.xml")):
    extract_tables_from_patent(path, output_dir="isolated_tables")
```

Each full-text XML is parsed with BeautifulSoup and `lxml`. The script finds the `EXAMPLES` heading, the standard EPO boundary between the general description and the experimental section, and takes every top-level `table` or `tables` element after it that sits inside `<description>`. A patent with no `EXAMPLES` heading, or no tables after it, is skipped with a message.

For each table, the **five preceding paragraphs** are copied along with it, since they normally carry the assay conditions, cell line and setup needed to read the numbers. Paragraphs from before `EXAMPLES` or outside the description are dropped, and any table nested inside a copied paragraph is stripped out so the context holds text only. Table plus context are written as one self-contained XML file.

Output filenames follow `<patent_id>_table_<NN>[_T<num>][_in_vitro].xml`:

| Part | Meaning |
|---|---|
| `<patent_id>` | base name of the source XML, for example `EP2723758NWB1` |
| `<NN>` | zero-padded position of the table in the document, so ordering is stable and names unique |
| `T<num>` | the real table number read from the `<title>`, for example `T18b`. Omitted when the title has no recognisable `Table <N>` label |
| `_in_vitro` | added when the title mentions any of: antisense strand, cells, in vitro, sense strand, transfection, single dose, dose response, modified sequences, antisense sequence, sense sequence |

`descriptor_words=N` appends N descriptor words from the title after the table number, giving names like `T18b_ic_50_pm`. The default of 0 is recommended: descriptors read as clutter and can cut mid-identifier.

---

## Stage 6: XML to CSV and header normalisation

`xml_to_csv.py`, entry point `convert_directory`. Uses the Groq API.

```python
from xml_to_csv import convert_directory

convert_directory(
    "isolated_tables",
    output_dir          = "csv_output",
    api_keys            = GROQ_API_KEYS,   # str, list, or None to read GROQ_API_KEY
    create_headers_file = False,
)
```

Two problems are solved here: getting the data out of the XML, and turning the
heterogeneous, often multi-level headers of patent tables into clean SQL
identifiers.

Each input XML produces two files in `output_dir`:

- `<base>_context.txt`: the table title and the context paragraphs kept in
  Stage 5, plus any full-width annotation rows (method notes, footnotes,
  spanning captions) that are not column names.
- `<base>_tables.csv`: the table data with normalised headers.

### Structural repairs, before anything reaches the LLM

Patent tables use the CALS model: a table is split into `<tgroup>` blocks,
column names belong in `<thead>`, data belongs in `<tbody>`, and a cell can
span columns (`namest`, `nameend`) or rows (`morerows`). Patent drafters break
those conventions constantly, and a reader that trusts the markup produces a
broken CSV.

**1. Column names stored as data.** Some patents put the real label row inside
`<tbody>` rather than `<thead>`, so the column names would become the first row
of measurements. A row is treated as a label row when it is one cell wide
across the whole table, or when every one of its cells spans a range of
columns. A row that mixes ordinary cells with one spanning cell is left alone,
because that is a normal two-level header:

```
Duplex ID | SID | Sense strand | AS ID | % mRNA remaining [spans 3 columns] | IC50 (nM)
```

**2. Header split across `thead` and `tbody`.** A common variant puts the group
labels in `<thead>` and the leaf labels in the first `<tbody>` row:

```
<thead>      ""       ""         "1nM"        "0.01nM"   ""
<tbody> #0   "Duplex" "1nM AVG"  "0.01nM AVG" "STDEV"    "STDEV"
```

That `tbody` row has ordinary cells, so repair 1 misses it. It is promoted only
when every conservative check passes: no spanning header was found, at least
two rows remain, every non-empty cell in the row is non-numeric, the first cell
does not look like a duplex or compound ID, and at least one cell in the next
few rows is numeric.

**3. Blank group labels.** In CALS a label covering several rows is written
once and the cells below it are left empty, so only the first row of each block
keeps its cell line. The last non-empty value in column 0 is carried down into
any later row that is blank there but has data elsewhere:

```
before             after
Hep3B  25  81      Hep3B  25  81
""     10  62      Hep3B  10  62
""      1  30      Hep3B   1  30
```

**4. Footnotes that look like a table.** Some patents append a final `<tgroup>`
with no `<thead>` whose rows are all full-width text: legends, method notes,
footnote markers. It has no columns and no data, and its text goes to the
context file.

Full-width rows inside a normal table are sorted by length. A short one such as
`HeLa day 3` is a section divider, and when a table stacks two or more labelled
sections the label is kept as a trailing `section` column so the condition is
not lost. A long one ending in a period is a caption and goes to the context
file.

**5. OCR damage in concentration labels.** Scanned headers turn `0.1nM` into
`O.lnM`, a capital O for the zero and a lowercase l for the one. Only the
numeric part immediately before `nM` is repaired, so ordinary text is never
altered.

Tables whose title contains "abbreviation" are skipped entirely.

### Header normalisation

**The data is the ground truth for the column count.** Every data row in a CALS
table has one cell per column, so the header must end up with exactly that many
names. Any other count means a column was dropped, added or reordered, which
silently misaligns every value in the table. The whole strategy below is built
around that check.

**Step 1, deterministic merge (`merge_multilevel_headers`).** This runs first
and owns the grid. It concatenates the header rows column by column, so it can
never drop, add or reorder a column:

```
header row 1:  ""        "1 nM"       "1 nM"        "0.01 nM"     "0.01 nM"
header row 2:  "Duplex"  "Avg"        "STDEV"       "Avg"         "STDEV"
merged:        "Duplex"  "1 nM Avg"   "1 nM STDEV"  "0.01 nM Avg" "0.01 nM STDEV"
```

This is also why both `STDEV` columns survive: as bare strings they are
identical and would collapse into one SQL name. Exact consecutive duplicates
within a column are skipped, so a `morerows=1` cell that repeats `Duplex ID`
down two header rows merges to `Duplex ID`, not `Duplex ID Duplex ID`.

If the merged width equals the data width, it is accepted and no LLM call is
made for this table.

**Step 2, LLM re-fusion, only on a mismatch
(`repair_headers_with_ai`, `llama-3.3-70b-versatile`).** When the spans cannot
account for every data column, which happens with broken CALS markup or a
header the parser found inside `<tbody>`, the 70B model is given the header
grid, the table title and up to four sample rows, and returns one flat list of
column names. It is accepted only if its length also matches the data width.

**Step 3, unresolved.** If neither method aligns, no shifted header is shipped.
The table gets positional names `_col_0`, `_col_1`, … , a `[WARN]` is printed
and the reason is written to the debug log for manual review.

**Step 4, SQL names (`normalize_headers_with_ai`, `llama-3.1-8b-instant`).**
The clean human-readable strings become SQL identifiers: lowercase,
underscores, `%` to `_pct`, `#` to `_num`, brackets removed but their contents
kept, `5'`/`3'` apostrophes dropped, and siRNA-specific mappings.

```
"Duplex"         → duplex_id
"1 nM STDEV"     → stdev_1_nM
"0.01 nM Avg"    → avg_0_01_nM
"% AS"           → pct_as
"SEQ ID NO."     → seq_id_no
"IC50 (nM)"      → ic50_nm
"50 nM"          → conc_50_nm      (unit-only headers get a conc_ prefix)
"DuplexlD"       → duplex_id       (OCR repair, capital I for lowercase l)
```

The call is batched over the unique headers of the whole file, not per table,
to save tokens against the Groq TPM limit. If the API cannot be reached, the
rule-based `basic_sql_normalize` takes over, which applies the lowercase,
underscore, `_pct` and `_num` rules but none of the siRNA-specific mappings.

**Deterministic fixes after the LLM.** Each one exists because the model was
seen to get that case wrong:

| Fix | Problem | Result |
|---|---|---|
| Day/time qualifier | `"Day 3 1nM"` normalised to `conc_1_nm`, collapsing three timepoints into one name | The header is re-normalised by rule, keeping the day: `day_3_1nm` |
| duplex_id by value | The duplex column sits under a spanning label such as `Conc. (in nM)` and gets no usable header. It is not always the first column either, a cell-line column can precede it | Every column is scanned. The first whose values are mostly `AD-\d+` becomes `duplex_id`, unless the table already has one |
| duplex_id by name | Two header rows both carrying `Duplex Name` merge to `Duplex Name Duplex Name`, which the prompt has no example for | Any SQL name containing the word `duplex` becomes `duplex_id` |
| Duplicate names | Two columns resolve to the same identifier, for example two `stdev` | Later occurrences get their column index appended: `stdev`, `stdev_4` |

**Avg/SD plausibility check.** A width check catches a dropped or duplicated
column, but not a swap: if two columns trade names, every count still matches
while the values are wrong. Paired `*_avg` and `*_sd` columns are therefore
compared, and a warning is logged when the SD median exceeds the Avg median,
since a standard deviation should be smaller than its mean. It only warns and
never edits the data, so a false positive is harmless.

**Debug trail.** A `debug/` folder is created inside `output_dir` with one
session log per run and one log per file, recording which header path was taken
for each table, the raw LLM input and output, and every fix applied.

**Optional audit file.** With `create_headers_file=True`, a
`<base>_llm_normalize.py` is written next to each source XML. It is runnable
standalone, encodes the exact mapping rules the LLM applied, and exits non-zero
if any mapping does not hold, which catches an LLM shortcut at generation time.

> **Model deprecation.** Both model IDs are deprecated by Groq, with a
> decommission date of 2026-08-16 noted in the source.

---

## Stage 7: primary table assembly

`xml_to_primary_table.py`, entry point `build_primary_table`, supported by `core.py`, `classification.py` and `sql.py`. Uses the Groq API and DuckDB.

```python
from xml_to_primary_table import build_primary_table

build_primary_table(
    "csv_output",
    api_keys      = GROQ_API_KEYS,
    file_prefixes = ["EP2723758NWB1", "EP2999785NWB1"],   # optional filter
)
```

| Parameter | Default | Description |
|---|---|---|
| `input_dir` | required | Folder holding the `*_tables.csv` and `*_context.txt` files from Stage 6 |
| `output_path` | `"primary_table.csv"` | Name and location of the knockdown table. Its folder becomes the run root, and the IC50 and viability tables inherit any suffix in the stem |
| `api_keys` | `None` | Groq key, list of keys or comma-separated string. Falls back to `GROQ_API_KEY` |
| `per_file_dir` | `None` | Kept for backwards compatibility. Overrides where per-file drafts are written |
| `file_prefixes` | `None` | Only process table files whose basename starts with one of these prefixes. Matching is case-insensitive, and a prefix that matches nothing is reported |

### How a table becomes rows

1. **Routing.** `classification.py` decides what each table measures, using the paired `_context.txt` and the headers: knockdown, IC50 or viability. Deterministic safety checks run alongside, flagging immune-response and in-vivo tables and tables whose content does not match their route.
2. **Mapping.** `sql.py` asks the LLM (`llama-3.3-70b-versatile`) for a DuckDB `SELECT` that maps this file's specific columns onto the fixed schema, including detectors for sparse sequence layouts where a duplex ID appears once and applies to several rows.
3. **Execution.** DuckDB runs that SQL locally against the CSV, so no value is ever produced by the model. A no-fabricated-values guard verifies this.
4. **Validation.** Every value is checked against its column: numeric fields must be numeric, dose and IC50 must sit between 0 and 10⁷ nM (10 mM), inhibition must sit between -200 % and 200 %, since a negative value is genuine upregulation and must be kept, and sequence fields must look like sequences. A failing cell is blanked and recorded rather than silently corrupting the table.

### Merging (knockdown table only)

Rows sharing `(patent_id, duplex_id, cell_line, dose_nM)` are merged into one. Annotations and measurements take the first non-null value, and `source_file` accumulates every contributing filename. Rows carrying only sequences or oligo IDs, with no measurement, enrich matching activity rows and are then dropped, so no annotation-only records remain. Both sequence forms are kept: `sense_sequence` and `antisense_sequence` hold the modified form when available, `*_sequence_unmodified` the plain form. IC50 and viability tables are not merged, since each of their rows is an independent condition.

### Output layout

Everything is written under the folder of `output_path`, one subfolder per stage of the run:

```
1_final_tables/     the dataset: primary_table*.csv, primary_ic50_table*.csv,
                    primary_cell_viability_table*.csv
2_review/           failed_tables*.csv, validation_failures*.csv, flagged_rows*.csv
3_per_file_drafts/  each input file's extraction, before merging
4_trace/            session log, per-table logs, gene log, sql_cache/
```

So the finished data, the pile that needs a human, the rough drafts and the decision trail never sit together. With `file_prefixes` set and `output_path` left at its default, each output set is labelled with the matched prefix, for example `primary_table_EP2723758NWB1.csv`.

| Table | Columns |
|---|---|
| `primary_table` | `duplex_id`, `sense_sequence`, `antisense_sequence`, `sense_sequence_unmodified`, `antisense_sequence_unmodified`, `sense_oligo_id`, `antisense_oligo_id`, `cell_line`, `dose_nM`, `inhibition_percent`, `value_sd`, `replicate`, `transfection_method`, `target_gene_name`, `patent_id`, `source_file` |
| `primary_ic50_table` | `duplex_id`, `cell_line`, `timepoint_hrs`, `replicate`, `ic50_nM`, `ic50_unit_original`, `transfection_method`, `target_gene_name`, `patent_id`, `source_file` |
| `primary_cell_viability_table` | `duplex_id`, `cell_line`, `day`, `dose_nM`, `viability_value`, `viability_sd`, `viability_basis`, `viability_relative_to`, `transfection_method`, `target_gene_name`, `patent_id`, `source_file` |

Review files in `2_review/`:

- `failed_tables*.csv`: tables that produced no output, with the reason.
- `validation_failures*.csv`: individual cells that were blanked, with the value and the rule it broke.
- `flagged_rows*.csv`: rows quarantined instead of being merged into the dataset, for example immune-response or in-vivo disagreements. They use a superset schema, so a knockdown, IC50 or viability row all fit one review file without losing fields.

**Resilience.** The SQL generated for each table is cached in `4_trace/sql_cache/`, keyed by a SHA hash of the table content and the prompt. If a run stops, for example on a sustained rate limit, restarting reuses the cached SQL for finished tables and only calls the API for the rest. Deleting the cache folder forces clean regeneration.

---

## API constraints and how they are handled

| Constraint | How it is handled |
|---|---|
| **2000-result search cap** (Stage 1) | Each query runs year by year. A year above 2000 results is sliced by month, a month above 2000 by day. Results are paginated 90 at a time through `X-OPS-Range` |
| **Token expiry after 900 s** | A module-level cache refreshes the token at 850 seconds, a 50 second safety margin |
| **About 10 requests per minute** | Fixed pauses: 3 seconds between count queries, 8 seconds between result pages. Shorter pauses were tested and triggered a block |
| **`/biblio` accepts 100 IDs** (Stage 2) | Batches of exactly 100, 3 retries per batch, then per-ID retries |
| **Missing abstracts** | Fallback call to the `/abstract` endpoint, English preferred |
| **Weekly 4 GB quota** | `check_epo_quota()` prints a dashboard before a run. It always returns success, so the run continues either way and you act on the warning |
| **Server strain** (Stage 2) | 429 and 503 count as strikes, 3 strikes trigger a 10 minute cooldown, and the inter-batch pause lengthens automatically |
| **HTTP 403** | Treated as a rate limit or temporary block: 10 minutes in the count helper, 15 minutes in the paginated fetch, forced cooldown in Stage 2 |
| **HTTP 429 and 503** | Progressive back-off of 300, 600, 900 and 1200 seconds |
| **Data loss on long runs** | Stage 1 autosaves each year to `EPO_IDs_AutoSave_{year}.csv` |
| **EPS throttling** (Stage 4) | Strict 8 second pause after every request. Already-downloaded files are reused without a request, so runs resume cheaply |
| **Groq rate limits** (Stages 6 and 7) | Several API keys are rotated, moving to the next when one is limited. Stage 7 caches generated SQL on disk so a restart only re-queries what failed |

---

## Known issues and edge cases

**Groq model deprecation.** Groq announced in June 2026 the deprecation of `llama-3.3-70b-versatile` and `llama-3.1-8b-instant` for the free and developer tiers, suggesting `openai/gpt-oss-120b` and `openai/gpt-oss-20b` as replacements. The model IDs live in `xml_to_csv.py` and `core.py`. Check the Groq deprecations page before starting a long run.

**Separator rows in the Stage 3 output.** The filtered CSV is meant to be read by a human, so every tier block is preceded by a blank row. Drop those rows, or filter on `Tier`, before feeding the file to anything downstream.

**`only_applicant=True` drops the exclusions.** An applicant-only query is built from the company name alone, so the aptamer and immunomodulatory exclusions do not apply and the output covers the whole portfolio, not only the siRNA work. That is intended for the Alnylam reference corpus, but worth remembering for any other company.

**A and B documents are not interchangeable.** Stage 4 keeps both when both exist, because claims and sometimes experimental data are amended during prosecution. If you ever substitute one for the other, treat the tables as unverified.

**Missing abstracts.** Some patents, mostly older ones or from certain offices, have no abstract in the EPO database. These carry the placeholder `No abstract available in EPO database`.

**Non-Latin titles.** Stage 2 deduplication demotes records whose title is in a non-Latin script, so an English equivalent from the same family wins when one exists.

**Day-level truncation.** If a single calendar day returns more than 2000 results for one query, which is very rare, the cap truncates and a `[WARNING]` is printed. There is no workaround below day granularity.

**Patents without an `EXAMPLES` heading are skipped** in Stage 5. Their experimental tables, if any, are never extracted. The skip is printed but not written to a manifest.
