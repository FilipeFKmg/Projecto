"""
epo_filter.py
================
Patent classification pipeline for EPO siRNA datasets.

PURPOSE
-------
Reads a raw EPO patent DataFrame (or a CSV file path) and classifies
every patent into one of eight tiers based on the strength of the
siRNA signal found in the text (title + abstract) and/or the CPC/IPC
classification codes.

DESIGN PHILOSOPHY
-----------------
- NO patents are ever deleted.
  Every record ends up in the output CSV so the researcher can perform
  manual curation with full context.
- Tiers are mutually exclusive and applied in priority order:
  higher-confidence tiers (1, 2, 3) are assigned first; ambiguous or
  likely-irrelevant patents fall into lower tiers (4–7).
- The output CSV uses ';' as separator and includes dummy header rows
  between tiers so it can be opened directly in Excel for review.

TIER DEFINITIONS
----------------
  TIER 1  — siRNA confirmed by BOTH text signals AND CPC/IPC code C12N15/113
              → Highest confidence; likely core dataset
  TIER 2  — siRNA confirmed by text signals only (no CPC anchor)
              → High confidence; CPC may simply be missing
  TIER 3  — siRNA confirmed by CPC anchor only (no text signal)
              → Check abstract manually; text may be missing or in another language
  TIER 4A — siRNA text OR CPC Anchor found BUT also contains a "forbidden" competing technology term
              → Mixed-technology patent; needs manual review
  TIER 4B — Diagnostic/biomarker-only patent with no therapeutic language
              → Likely out of scope; review manually
  TIER 5  — Agriculture/veterinary/pest-control patent WITH a CPC anchor
              → Possibly relevant (RNAi in plants/insects); review manually
  TIER 6  — No siRNA text signal and no CPC anchor
              → Likely irrelevant; lowest priority for review
  TIER 7  — Agriculture/veterinary patent WITHOUT a CPC anchor
              → Likely irrelevant; lowest priority for review

USAGE
-----
    from epo_filter import apply_filters

    # Option A: pass a pandas DataFrame directly
    result_df = apply_filters(my_dataframe)

    # Option B: pass a CSV file path (string or pathlib.Path)
    result_df = apply_filters("EPO_siRNA_RAW_2016_2016.csv")

    # Option C: override CSV reading options
    result_df = apply_filters("data/patents.csv", csv_sep=',', csv_encoding='utf-8')

    # Option D: customise the output file name
    result_df = apply_filters("patents.csv", output_filename="classified_2016.csv")
"""

import pandas as pd
import numpy as np
from pathlib import Path


def apply_filters(
    raw_data: "pd.DataFrame | str | Path",
    output_filename: str = "EPO_siRNA_FILTERED_FINAL.csv",
    csv_sep: str = ";",
    csv_encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    """
    Classify EPO patents into tiered sections and save a curated CSV.

    Parameters
    ----------
    raw_data : pd.DataFrame | str | Path
        Either a pandas DataFrame already in memory, or a file-system path
        (string or pathlib.Path) pointing to a CSV file to be read.
    output_filename : str, optional
        Name / path of the output CSV file.
        Default: 'EPO_siRNA_FILTERED_FINAL.csv'
    csv_sep : str, optional
        Column separator used when *reading* a CSV path.
        Default: ';'  (matches the EPO bulk-export format)
    csv_encoding : str, optional
        Encoding used when *reading* a CSV path.
        Default: 'utf-8-sig'  (handles the BOM that Excel-exported CSVs add)

    Returns
    -------
    pd.DataFrame
        The classified DataFrame with dummy separator rows included,
        exactly as written to the output CSV.
        Returns an empty DataFrame if the input is empty.

    Raises
    ------
    FileNotFoundError
        If a file path is supplied but the file does not exist on disk.
    """

    # ------------------------------------------------------------------
    # STEP 0 — INPUT RESOLUTION
    # If the caller passed a file path instead of a DataFrame, read it.
    # This block is a no-op when a DataFrame is passed directly.
    # ------------------------------------------------------------------
    if isinstance(raw_data, (str, Path)):
        csv_path = Path(raw_data)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        print(f"[INFO] Reading CSV from disk: {csv_path}")
        raw_data = pd.read_csv(
            csv_path,
            sep=csv_sep,
            encoding=csv_encoding,
            low_memory=False,  # avoids dtype-guessing warnings on large files
        )

    print("\n=== STARTING PATENT CLASSIFICATION ===")
    print("[INFO] No patents will be deleted — all records go to the output CSV.")

    # Guard: abort early if the input is completely empty
    if raw_data.empty:
        print("[ERROR] Input DataFrame is empty. Nothing to classify.")
        return pd.DataFrame()

    df = raw_data.copy()
    initial_count = len(df)

    # ------------------------------------------------------------------
    # STEP 1 — BUILD ESPACENET DEEP-LINK
    # Constructs a direct URL to the Espacenet bibliographic record for
    # each patent, using the Country, Number, and Kind columns.
    # Only built when all three columns are present.
    # ------------------------------------------------------------------
    if all(col in df.columns for col in ["Country", "Number", "Kind"]):
        df["Espacenet_Link"] = (
            "https://worldwide.espacenet.com/publicationDetails/biblio?CC="
            + df["Country"].fillna("").astype(str)
            + "&NR=" + df["Number"].fillna("").astype(str)
            + "&KC=" + df["Kind"].fillna("").astype(str)
            + "&FT=D"
        )
    else:
        # Columns missing in this dataset version — leave blank
        df["Espacenet_Link"] = ""

    # Combined text field used by all regex signals below.
    # Using both title and abstract maximises recall.
    df["Search_Text"] = df["Title"].fillna("") + " " + df["Abstract"].fillna("")

    # ------------------------------------------------------------------
    # STEP 2 — MANUAL REVIEW FLAG: Unreadable / Non-Latin Records
    # Flags patents whose abstract is missing AND whose title contains
    # non-ASCII characters (e.g., Japanese Kanji, Cyrillic).
    # These records cannot be classified by text alone and must be
    # opened manually in Espacenet.
    # ------------------------------------------------------------------
    mask_no_abstract = (
        df["Abstract"].fillna("").str.contains("No abstract available", case=False)
        | (df["Abstract"].fillna("").str.strip() == "")
    )
    # Regex [^\x00-\x7F] matches any character outside the standard ASCII range
    mask_non_latin_title = df["Title"].fillna("").str.contains(
        r"[^\x00-\x7F]", regex=True
    )
    df["Needs_Espacenet_Review"] = mask_no_abstract & mask_non_latin_title

    # ==================================================================
    # CLASSIFICATION SIGNALS
    # Each signal is a boolean Series aligned with df's index.
    # ==================================================================

    # ------------------------------------------------------------------
    # SIGNAL 1 — CPC / IPC ANCHOR
    # C12N 15/113 is the official taxonomic code for siRNA / RNAi in both
    # the Cooperative Patent Classification (CPC) and the International
    # Patent Classification (IPC). A match here is a strong structural
    # signal regardless of what the abstract says.
    # ------------------------------------------------------------------
    mask_anchor = (
        df["CPCs"].fillna("").str.contains(r"C12N15/113", regex=False)
        | df["IPCs"].fillna("").str.contains(r"C12N15/113", regex=False)
    )

    # ------------------------------------------------------------------
    # SIGNAL 2 — siRNA TEXT SIGNAL
    # Matches a broad set of siRNA-related terms in the combined
    # Title + Abstract field.  Uses word boundaries (\b) where possible
    # to avoid spurious matches (e.g., 'RISC' should not match 'CISCO').
    # ------------------------------------------------------------------
    sirna_patterns = [
        r"\bsiRNA\b",
        r"\bsi-RNA\b",
        r"\bRNAi\b",
        r"\bdsRNA\b",
        r"\bdsRNAi\b",
        r"\bds-RNA\b",
        r"\bRISC\b",                              # RNA-induced silencing complex
        r"\bDicer\b",                             # enzyme central to siRNA processing
        r"small\s+interfering\s+R[Nn][Aa]",
        r"short\s+interfering\s+R[Nn][Aa]",
        r"RNA\s+interference",
        r"RNA[\s-]induced\s+silenc",
        r"double[\s-]stranded\s+R[Nn][Aa]",
        r"double[\s-]stranded\s+ribonucleic",
        r"small\s+interfering\s+ribonucleic",
        r"short\s+interfering\s+ribonucleic",
        r"ribonucleic\s+acid\s+interference",
        r"interfering\s+ribonucleic",
        r"\bRNA\s+duplex\b",
        r"RNAi\s+(?:agent|molecule|therapeuti|oligonucleotide)",
        r"siRNA\s+(?:agent|molecule|therapeuti|delivery|construct)",
        r"gene\s+silenc\w+",                      # gene silencing / gene-silenced etc.
        r"RNA\s+silenc\w+",
        r"post.transcriptional\s+silenc",         # '.' intentionally matches space or hyphen
        r"sequence.specific\s+silenc",
    ]
    regex_sirna = "|".join(sirna_patterns)
    mask_sirna_text = df["Search_Text"].str.contains(
        regex_sirna, case=False, na=False, regex=True
    )

    # ------------------------------------------------------------------
    # SIGNAL 3 — FORBIDDEN / COMPETING TECHNOLOGY TERMS
    # These terms indicate that the patent's primary technology is NOT
    # siRNA, even if siRNA is mentioned incidentally.
    # ------------------------------------------------------------------
    forbidden_pattern = (
        r"\b(?:"
        r"CRISPR|Cas9|Cas12|Cas13|"
        r"antisense|ASO\s*(?:therapy|treatment|oligonucleotide)\b|"
        r"microRNA|miRNA|miR-\d+|"
        r"circRNA|circular\s+RNA|"
        r"aptamer|"
        r"mRNA\s+vacc|mRNA\s+therap"
        r")\b"
    )
    mask_forbidden = df["Search_Text"].str.contains(
        forbidden_pattern, case=False, na=False, regex=True
    )

    # ------------------------------------------------------------------
    # SIGNAL 4 — PURE DIAGNOSTIC / BIOMARKER
    # Patents that describe only a diagnostic, detection, or biomarker
    # application and contain NO therapeutic language are out of scope
    # for a therapeutic siRNA dataset.
    # ------------------------------------------------------------------
    diagnostic_pattern = (
        r"\b(?:biomarker|prognostic\s+marker|diagnostic\s+kit|detection\s+method|"
        r"assay\s+kit|sequencing\s+method|ELISA\s+kit)\b"
    )
    mask_diag = df["Search_Text"].str.contains(
        diagnostic_pattern, case=False, na=False, regex=True
    )
    # A patent is "purely diagnostic" only if it has NO treatment language
    mask_no_treatment = ~df["Search_Text"].str.contains(
        r"\b(?:treat|therap|drug|medicine|medicament)\b",
        case=False,
        na=False,
        regex=True,
    )
    mask_pure_diag = mask_diag & mask_no_treatment

    # ------------------------------------------------------------------
    # SIGNAL 5 — AGRICULTURE / VETERINARY / PEST CONTROL
    # Covers plant science, crop protection, animal agriculture, and
    # model organisms (e.g., C. elegans, Drosophila) which are typically
    # out of scope for a human-therapeutics siRNA patent database.
    # ------------------------------------------------------------------
    agri_pattern = (
        r"\b(?:"
        r"crop|weed|maize|corn(?!ea)|soybean|wheat|barley|tobacco|"
        r"tomato(?!\s+cell)|potato|cotton|canola|rapeseed|sugarcane|"
        r"Arabidopsis|thaliana|"
        r"herbicide|pesticide|insecticide|fungicide|nematicide|"
        r"nematode|aphid|whitefly|thrips|locust|"
        r"livestock|poultry|swine|porcine|ovine|equine|caprine|"
        r"cattle|sheep(?!\s+cell)|goat|turkey|aquaculture|"
        r"Fusarium|Botrytis|Phytophthora|Pythium|powdery\s+mildew|"
        r"Agrobacterium|biopesticide|pest\s+control|biocontrol|"
        r"filamentous\s+fung|yeast\s+strain|fungal\s+strain|"
        r"Aspergillus|Trichoderma|Neurospora|Pichia\s+pastoris|"
        r"Saccharomyces\s+cerevisiae|"
        r"C\.\s*elegans|Caenorhabditis|"
        r"Drosophila\s+melanogaster|"
        r"Xenopus\s+laevis|"
        r"insect\s+cell(?!s?\s+line)|"
        r"Sf9\s+cell|baculovirus\s+express"
        r")\b"
    )
    mask_agri = df["Search_Text"].str.contains(
        agri_pattern, case=False, na=False, regex=True
    )

    # ------------------------------------------------------------------
    # SIGNAL 6 — QUANTITATIVE EFFICACY (The "Holy Grail" metric)
    # Looks for IC50/EC50 values or percentages related to gene knockdown.
    # ------------------------------------------------------------------
    efficacy_pattern = (
        r"(?:"
        r"\b(?:IC50|EC50|ED50|KD)\b|"                            # Standard metric abbreviations
        r"\b\d{1,3}(?:\.\d+)?\s*%\s*(?:knockdown|silencing|inhibition|reduction|decrease)\b|" # e.g., "95% knockdown"
        r"(?:mRNA|expression|target|protein)\s+(?:level\s+)?(?:was\s+)?(?:reduced|decreased|inhibited)\s+by\s+(?:at\s+least\s+)?\d{1,3}\s*%|" # e.g., "mRNA level was reduced by 80%"
        r"(?:residual|remaining)\s+(?:mRNA|expression|protein)\b" # Specifically targets "remaining mRNA"
        r")"
    )
    mask_efficacy = df["Search_Text"].str.contains(
        efficacy_pattern, case=False, na=False, regex=True
    )
    
    # Create a new column to flag these high-value patents
    df["Has_Efficacy_Data"] = mask_efficacy

    # ==================================================================
    # TIER ASSIGNMENT
    # Conditions and labels are listed in priority order (highest first).
    # The loop applies them in REVERSE order so that when two conditions
    # overlap, the HIGHEST priority label wins (last write wins).
    # ==================================================================
    conditions = [
        # TIER 1 — siRNA confirmed by both text AND CPC/IPC code
        (mask_sirna_text & mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),

        # TIER 2 — siRNA confirmed by text only (no CPC anchor present)
        (mask_sirna_text & ~mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),

        # TIER 3 — CPC anchor present but no text signal (abstract may be missing)
        (~mask_sirna_text & mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),

        # TIER 4A — siRNA text OR CPC Anchor confirmed BUT a competing technology term also present
        ((mask_sirna_text | mask_anchor) & mask_forbidden & ~mask_pure_diag),

        # TIER 4B — Pure diagnostic / biomarker with no therapeutic indication
        (mask_pure_diag),

        # TIER 5 — Agriculture/vet patent that still has the CPC anchor
        (mask_agri & mask_anchor & ~mask_pure_diag),

        # TIER 6 — No siRNA text and no CPC anchor; not agricultural
        (~mask_sirna_text & ~mask_anchor & ~mask_agri & ~mask_pure_diag),

        # TIER 7 — Agriculture/vet patent WITHOUT a CPC anchor
        (mask_agri & ~mask_anchor & ~mask_pure_diag),
    ]

    tier_labels = [
        "TIER 1 — siRNA Confirmed (Text + CPC)",
        "TIER 2 — siRNA Confirmed (Text only)",
        "TIER 3 — siRNA by CPC only (Check Abstract)",
        "TIER 4A — Mixed Tech (siRNA/CPC + Forbidden Term)",
        "TIER 4B — Diagnostic/Biomarker only (Review)",
        "TIER 5 — Agri/Vet with siRNA CPC (Review)",
        "TIER 6 — No siRNA Signal (Likely Irrelevant)",
        "TIER 7 — Agri/Vet without siRNA (Likely Irrelevant)",
    ]

    # Default all rows to UNCLASSIFIED before the priority loop
    df["Tier"] = "UNCLASSIFIED"

    # Apply in reverse so the first condition in the list wins
    for condition, label in zip(reversed(conditions), reversed(tier_labels)):
        df.loc[condition, "Tier"] = label

    # Drop the helper column — it was only needed for classification
    df = df.drop(columns=["Search_Text"], errors="ignore")

    # ==================================================================
    # MAKE FLAGS VISUALLY STAND OUT IN THE CSV
    # ==================================================================
    # Replace standard 'True' with eye-catching text and clear out 'False'
    df["Needs_Espacenet_Review"] = df["Needs_Espacenet_Review"].replace({
        True: "YES (REVIEW)", 
        False: ""
    })
    
    df["Has_Efficacy_Data"] = df["Has_Efficacy_Data"].replace({
        True: "YES (EFFICACY)", 
        False: ""
    })

    # ==================================================================
    # OUTPUT CONSTRUCTION
    # The final CSV is organised into sections, one per tier.
    # Each section is preceded by a single "dummy" row whose Patent_ID
    # cell contains the tier name and record count, making it easy to
    # navigate in Excel without any extra tooling.
    # ==================================================================

    # Only include columns that actually exist in this dataset
    desired_columns = [
        "Patent_ID",
        "Priority_Date",
        "Publication_Date",
        "Applicant",
        "Title",
        "Abstract",
        "Espacenet_Link",
        "Tier",
        "Has_Efficacy_Data", 
        "Needs_Espacenet_Review",
        "Warning",          # optional column; included if present in source data
        "IPCs",
        "CPCs",
        "Family_ID",
    ]
    output_columns = [c for c in desired_columns if c in df.columns]

    sections = []
    for label in tier_labels:
        tier_df = df[df["Tier"] == label].copy()

        # Skip tiers that have no patents (keeps the CSV clean)
        if tier_df.empty:
            continue

        # Build the separator row — blank everywhere except Patent_ID
        separator_row = {col: "" for col in output_columns}
        separator_row["Patent_ID"] = (
            f"--- {label.upper()} ({len(tier_df)} patents) ---"
        )
        sections.append(pd.DataFrame([separator_row]))
        sections.append(tier_df[output_columns])

    final_df = pd.concat(sections, ignore_index=True)

    # Write with semicolon separator and BOM so Excel opens it correctly
    final_df.to_csv(output_filename, index=False, sep=";", encoding="utf-8-sig")

    # ==================================================================
    # SUMMARY REPORT
    # ==================================================================
    # ANSI escape codes for terminal colors
    COLOR_RED = "\033[91m"
    COLOR_GREEN = "\033[92m"
    COLOR_RESET = "\033[0m"

    print(f"\n{'=' * 55}")
    print("[SUCCESS] Classification complete.")
    print(f"  Total input patents : {initial_count}")
    print()
    for label in tier_labels:
        count = (df["Tier"] == label).sum()
        bar = "█" * (count // 100)           # 1 block per 100 patents
        print(f"  {label[:48]:<48} {count:>5}  {bar}")
    print()

    needs_review_count = (df["Needs_Espacenet_Review"] == "YES (REVIEW)").sum()
    if needs_review_count > 0:
        print(
            f"{COLOR_RED} There are {needs_review_count} patent(s) "
            f"flagged as Needs_Espacenet_Review (unreadable title / missing abstract){COLOR_RESET}\n"
        )
        
    efficacy_count = (df["Has_Efficacy_Data"] == "YES (EFFICACY)").sum()
    if efficacy_count > 0:
        print(
            f"{COLOR_GREEN} There are {efficacy_count} patent(s) "
            f"flagged with Quantitative Efficacy Data{COLOR_RESET}\n"
        )

    print(f"  Output saved to: {output_filename}")
    print(f"{'=' * 55}")

    return final_df