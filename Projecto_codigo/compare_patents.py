"""
compare_patents.py
=============================================================================
A reusable module for comparing two EPO patent CSV files by Family_ID.

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

OUTPUT FILES (written automatically to the same folder):
    patentes_so_<label_a>.csv
    patentes_so_<label_b>.csv
    patentes_em_comum.csv

KNOWN ISSUE HANDLED:
    The EPO OPS API sometimes returns the same Family_ID with and without a
    leading zero depending on the query type (e.g. "84545942" vs "084545942").
    This module normalises both sides by stripping leading zeros before any
    comparison so that identical families always match.
=============================================================================
"""

import sys
import os
import pandas as pd


# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def _load_csv(filepath: str) -> pd.DataFrame:
    """
    Loads a semicolon-separated EPO patent CSV into a DataFrame.

    Uses dtype=str throughout to prevent pandas from silently coercing
    numeric Family_IDs — for example, turning "084545942" into 84545942.0
    and dropping the leading zero in the process.

    Raises:
        SystemExit if the file is not found or is missing the Family_ID column.
    """
    if not os.path.exists(filepath):
        sys.exit(f"[ERROR] File not found: {filepath}\n"
                 f"        Make sure the CSV is in the same folder as this script.")

    df = pd.read_csv(filepath, sep=";", encoding="utf-8-sig", dtype=str)

    # Remove accidental whitespace from column names (common after Excel exports)
    df.columns = df.columns.str.strip()

    if "Family_ID" not in df.columns:
        sys.exit(f"[ERROR] Column 'Family_ID' not found in: {filepath}\n"
                 f"        Found columns: {list(df.columns)}")

    return df


def _normalise_ids(df: pd.DataFrame) -> pd.Series:
    """
    Returns a Series of normalised Family_IDs for set comparisons.

    Normalisation strips surrounding whitespace and leading zeros so that
    "84545942" and "084545942" are treated as the same family.
    The normalised values are never written to any output file — they are
    only used internally for matching.
    """
    return df["Family_ID"].str.strip().str.lstrip("0")


def _short_label(filepath: str) -> str:
    """
    Derives a short human-readable label from a file path for use in
    output filenames and summary output.

    Example: "EPO_siRNA_IDs_2022_2025_True_Alnylam.csv" → "True_Alnylam"
    """
    stem = os.path.splitext(os.path.basename(filepath))[0]  # drop .csv
    parts = stem.split("_")
    # Drop the common EPO prefix parts (EPO, siRNA, IDs, YYYY, YYYY)
    # and keep everything after the fourth underscore segment
    meaningful = parts[4:] if len(parts) > 4 else parts
    return "_".join(meaningful) if meaningful else stem


# =============================================================================
# PUBLIC API
# =============================================================================

def compare(
    file_a: str,
    file_b: str,
    save: bool = True,
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
        If True (default), writes the three result DataFrames to CSV files
        in the current directory. Set to False to skip saving.

    Returns
    -------
    only_a : pd.DataFrame
        Patent families present ONLY in file_a.
    only_b : pd.DataFrame
        Patent families present ONLY in file_b.
    shared : pd.DataFrame
        Patent families present in BOTH files (one row per family, source = A).

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
    # 1. Load both files
    # ------------------------------------------------------------------
    print(f"Loading files...")
    df_a = _load_csv(file_a)
    df_b = _load_csv(file_b)

    label_a = _short_label(file_a)
    label_b = _short_label(file_b)

    # ------------------------------------------------------------------
    # 2. Normalise Family_IDs and build comparison sets
    #    (strips leading zeros — see module docstring for why this matters)
    # ------------------------------------------------------------------
    df_a["_fid"] = _normalise_ids(df_a)
    df_b["_fid"] = _normalise_ids(df_b)

    ids_a = set(df_a["_fid"].dropna())
    ids_b = set(df_b["_fid"].dropna())

    only_in_a = ids_a - ids_b   # families exclusive to file A
    only_in_b = ids_b - ids_a   # families exclusive to file B
    in_both   = ids_a & ids_b   # families present in both

    # ------------------------------------------------------------------
    # 3. Build output DataFrames
    #    The helper column "_fid" is dropped so outputs stay clean
    # ------------------------------------------------------------------
    only_a = (
        df_a[df_a["_fid"].isin(only_in_a)]
        .drop(columns="_fid")
        .reset_index(drop=True)
    )

    only_b = (
        df_b[df_b["_fid"].isin(only_in_b)]
        .drop(columns="_fid")
        .reset_index(drop=True)
    )

    # For shared families we keep one row per family from file A,
    # ranked by the jurisdiction priority already applied during extraction
    shared = (
        df_a[df_a["_fid"].isin(in_both)]
        .drop(columns="_fid")
        .drop_duplicates(subset="Family_ID", keep="first")
        .sort_values("Family_ID")
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # 4. Print summary
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
    # ------------------------------------------------------------------
    if save:
        out_a      = f"patentes_only_{label_a}.csv"
        out_b      = f"patentes_only_{label_b}.csv"
        out_shared = f"patentes_in_commum_{label_a} and {label_b}.csv"

        only_a.to_csv(out_a,      sep=";", index=False, encoding="utf-8-sig")
        only_b.to_csv(out_b,      sep=";", index=False, encoding="utf-8-sig")
        shared.to_csv(out_shared, sep=";", index=False, encoding="utf-8-sig")

        print(f"[SAVED] {len(only_a):>5,} rows  →  {out_a}")
        print(f"[SAVED] {len(only_b):>5,} rows  →  {out_b}")
        print(f"[SAVED] {len(shared):>5,} rows  →  {out_shared}")
        print()

    return only_a, only_b, shared
