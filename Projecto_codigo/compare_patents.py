"""
compare_patents.py
=============================================================================
A reusable module for comparing two EPO patent CSV files by Family_ID.

WHAT IT DOES
    Given two extraction CSVs (each one row per patent family, with a
    "Family_ID" column), it works out which families are:
        - exclusive to file A,
        - exclusive to file B,
        - present in both files,
    then prints a summary and (optionally) writes the three result sets to
    their own CSV files.

USAGE (in a Jupyter notebook or any Python script):

    from compare_patents import compare

    only_a, only_b, shared = compare(
        "EPO_siRNA_IDs_2022_2025_True_Alnylam.csv",
        "EPO_siRNA_IDs_2022_2025_terms_only.csv"
    )

RETURNS:
    Three pandas DataFrames:
        only_a  — patent families present ONLY in file A
        only_b  — patent families present ONLY in file B
        shared  — patent families present in BOTH files

OUTPUT FILES (written automatically to the output folder):
    Every output filename contains BOTH labels so that running several
    comparisons in a row never overwrites earlier results (see the
    "OVERWRITE FIX" note below). For files A = "codes_only" and
    B = "terms_only" the names are:

        patents_only_in_codes_only_vs_terms_only.csv
        patents_only_in_terms_only_vs_codes_only.csv
        patents_common_codes_only_and_terms_only.csv
=============================================================================
"""

import sys
import os
import pandas as pd


# =============================================================================
# INTERNAL HELPERS
# Functions prefixed with "_" are implementation details. They are not meant
# to be imported or called directly from a notebook; only compare() is.
# =============================================================================

def _load_csv(filepath: str) -> pd.DataFrame:
    """
    Loads a semicolon-separated EPO patent CSV into a DataFrame.

    Why dtype=str:
        Family_IDs are numeric-looking but should be matched as plain text.
        Forcing every column to string keeps them as exact, comparable strings
        and stops pandas from coercing them into ints/floats, which could
        change how they are represented and break exact matching.

    Raises:
        SystemExit if the file does not exist or has no "Family_ID" column.
        (SystemExit stops the notebook cell cleanly with a readable message
        instead of a long traceback.)
    """
    # Fail early and clearly if the path is wrong, rather than letting pandas
    # raise a less obvious error further down.
    if not os.path.exists(filepath):
        sys.exit(f"[ERROR] File not found: {filepath}\n"
                 f"        Make sure the CSV is in the same folder as this script.")

    # sep=";"            -> EPO CSVs are semicolon-separated, not comma.
    # encoding="utf-8-sig" -> handles the BOM that Excel/our exporter adds.
    # dtype=str          -> see the docstring note above (match IDs as text).
    df = pd.read_csv(filepath, sep=";", encoding="utf-8-sig", dtype=str)

    # Remove accidental whitespace from column names (common after Excel
    # round-trips), so " Family_ID" still matches "Family_ID".
    df.columns = df.columns.str.strip()

    # The whole comparison is keyed on this column, so refuse to continue
    # without it and show the user which columns were actually found.
    if "Family_ID" not in df.columns:
        sys.exit(f"[ERROR] Column 'Family_ID' not found in: {filepath}\n"
                 f"        Found columns: {list(df.columns)}")

    return df


def _normalise_ids(df: pd.DataFrame) -> pd.Series:
    """
    Returns a Series of Family_IDs cleaned for matching.

    Cleaning only strips surrounding whitespace, so that a stray space never
    causes two otherwise-identical IDs to be treated as different families.

    Important: these cleaned values are used ONLY internally for building the
    comparison sets. They are NEVER written to an output file — the output
    keeps the original Family_ID exactly as it appeared in the source CSV.
    """
    return df["Family_ID"].str.strip()


def _short_label(filepath: str) -> str:
    """
    Derives a short, human-readable label from a file path, used in both the
    printed summary and the output filenames.

    How it works:
        Takes the filename without its extension, splits it on "_", and drops
        the common EPO prefix segments (EPO, siRNA, IDs, YYYY) by keeping
        everything from the 5th segment onward.

    Example:
        "EPO_siRNA_IDs_2022_2025_terms_only.csv"
            split -> ['EPO','siRNA','IDs','2022','2025','terms','only']
            [4:]  -> ['2025','terms','only']
            join  -> "2025_terms_only"
    """
    stem = os.path.splitext(os.path.basename(filepath))[0]  # drop folder + ".csv"
    parts = stem.split("_")

    # Keep everything after the first four underscore-separated segments.
    # If the name is shorter than expected, fall back to using all parts so we
    # never return an empty label.
    meaningful = parts[4:] if len(parts) > 4 else parts
    return "_".join(meaningful) if meaningful else stem


# =============================================================================
# PUBLIC API
# This is the only function you should call from a notebook.
# =============================================================================

def compare(
    file_a: str,
    file_b: str,
    save: bool = True,
    out_dir: str = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compares two EPO patent CSV files by Family_ID.

    Parameters
    ----------
    file_a : str
        Path to the first CSV file (e.g. the Alnylam-filtered extraction).
    file_b : str
        Path to the second CSV file (e.g. the CPC/terms-based extraction).
    save : bool, optional
        If True (default), writes the three result DataFrames to CSV files.
        Set to False to skip saving and only get the DataFrames back.
    out_dir : str, optional
        Folder where the output CSVs are written (default: current folder).
        Created automatically if it does not exist. Useful for keeping the
        outputs of different comparison runs in separate folders.

    Returns
    -------
    only_a : pd.DataFrame
        Patent families present ONLY in file_a.
    only_b : pd.DataFrame
        Patent families present ONLY in file_b.
    shared : pd.DataFrame
        Patent families present in BOTH files (one row per family, taken
        from file_a).

    Examples
    --------
    >>> from compare_patents import compare
    >>> only_a, only_b, shared = compare(
    ...     "EPO_siRNA_IDs_2022_2025_True_Alnylam.csv",
    ...     "EPO_siRNA_IDs_2022_2025_terms_only.csv"
    ... )
    >>> shared.head()
    """

    # ------------------------------------------------------------------
    # 1. Load both files into DataFrames
    # ------------------------------------------------------------------
    print(f"Loading files...")
    df_a = _load_csv(file_a)
    df_b = _load_csv(file_b)

    # Short labels drive both the printed summary and the output filenames.
    label_a = _short_label(file_a)
    label_b = _short_label(file_b)

    # ------------------------------------------------------------------
    # 2. Clean Family_IDs and build comparison sets
    # ------------------------------------------------------------------
    # Add a temporary helper column "_fid" holding the cleaned ID. We keep it
    # on the DataFrame (instead of a separate variable) so we can filter the
    # original rows by it later and still output the untouched Family_ID.
    df_a["_fid"] = _normalise_ids(df_a)
    df_b["_fid"] = _normalise_ids(df_b)

    # Turn the cleaned IDs into Python sets so we can use fast set algebra.
    # dropna() guards against any rows where the ID was missing/blank.
    ids_a = set(df_a["_fid"].dropna())
    ids_b = set(df_b["_fid"].dropna())

    only_in_a = ids_a - ids_b   # families exclusive to file A
    only_in_b = ids_b - ids_a   # families exclusive to file B
    in_both   = ids_a & ids_b   # families present in both

    # ------------------------------------------------------------------
    # 3. Build the output DataFrames
    #    We filter the ORIGINAL rows using the "_fid" set membership, then
    #    drop the helper column so the outputs only contain real columns.
    # ------------------------------------------------------------------
    only_a = (
        df_a[df_a["_fid"].isin(only_in_a)]   # keep A-rows whose family is A-only
        .drop(columns="_fid")
        .reset_index(drop=True)
    )

    only_b = (
        df_b[df_b["_fid"].isin(only_in_b)]   # keep B-rows whose family is B-only
        .drop(columns="_fid")
        .reset_index(drop=True)
    )

    # For shared families we keep one row per family, taken from file A and
    # ranked by the jurisdiction priority already applied during extraction
    # (drop_duplicates with keep="first" preserves the first/best occurrence).
    shared = (
        df_a[df_a["_fid"].isin(in_both)]
        .drop(columns="_fid")
        .drop_duplicates(subset="Family_ID", keep="first")
        .sort_values("Family_ID")
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # 4. Print a readable summary of the comparison
    # ------------------------------------------------------------------
    print(f"""
  PATENT FAMILY COMPARISON  (by Family_ID)

  File A — {label_a:<20}: {len(df_a):>6,} rows  |  {len(ids_a):>5,} unique families
  File B — {label_b:<20}: {len(df_b):>6,} rows  |  {len(ids_b):>5,} unique families

  Only in A  ({label_a}) : {len(only_in_a):>5,} families  →  {len(only_a):>5,} rows
  Only in B  ({label_b}) : {len(only_in_b):>5,} families  →  {len(only_b):>5,} rows
  In both files              : {len(in_both):>5,} families  →  {len(shared):>5,} rows

  Total unique families      : {len(ids_a | ids_b):>5,}

""")

    # ------------------------------------------------------------------
    # 5. Save CSVs (unless the caller opted out)
    #
    # ------------------------------------------------------------------
    if save:
        # Make sure the destination folder exists (no-op if out_dir == ".").
        os.makedirs(out_dir, exist_ok=True)

        # "only_in_A_vs_B" reads naturally: families in A that are absent from B.
        out_a = os.path.join(
            out_dir, f"patents_only_in_{label_a}_vs_{label_b}.csv"
        )
        # "only_in_B_vs_A": families in B that are absent from A.
        out_b = os.path.join(
            out_dir, f"patents_only_in_{label_b}_vs_{label_a}.csv"
        )
        # The shared file already carried both labels, so it never collided,
        # but we keep the same both-labels convention for consistency.
        out_shared = os.path.join(
            out_dir, f"patents_common_{label_a}_and_{label_b}.csv"
        )

        only_a.to_csv(out_a,      sep=";", index=False, encoding="utf-8-sig")
        only_b.to_csv(out_b,      sep=";", index=False, encoding="utf-8-sig")
        shared.to_csv(out_shared, sep=";", index=False, encoding="utf-8-sig")

        print(f"[SAVED] {len(only_a):>5,} rows  →  {out_a}")
        print(f"[SAVED] {len(only_b):>5,} rows  →  {out_b}")
        print(f"[SAVED] {len(shared):>5,} rows  →  {out_shared}")
        print()

    return only_a, only_b, shared
