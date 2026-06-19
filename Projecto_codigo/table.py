import os
import copy
import re
import argparse
from bs4 import BeautifulSoup


def _title_slug(title_text, descriptor_words=0):
    """Turn a table <title> like 'Table 18b: IC 50 (pM) in 3 cell lines' into a
    short, filesystem-safe provenance tag.

    descriptor_words=0 (default) -> just the patent table number, e.g. 'T18b'.
      Clean and unambiguous: you can look the number up directly in the patent.
    descriptor_words>0 -> append that many descriptor words, e.g. with 3:
      'T18b_ic_50_pm'. More words read as clutter and can cut mid-identifier,
      so the number-only default is recommended.

    Returns '' when the title is empty or has no recognisable 'Table <n>' label,
    so the caller falls back to the positional index alone.
    """
    if not title_text:
        return ""
    m = re.match(r"\s*table\s+([0-9]+[a-z]?)\s*[:.\-]*\s*(.*)", title_text, re.IGNORECASE)
    if not m:
        return ""
    num = m.group(1)
    if descriptor_words <= 0:
        return f"T{num}"
    desc = re.sub(r"[^a-z0-9]+", "_", m.group(2).lower()).strip("_")
    desc = "_".join(desc.split("_")[:descriptor_words])
    return f"T{num}_{desc}".strip("_")[:60]


def extract_tables_from_patent(xml_filepath, output_dir='extracted_tables',
                               descriptor_words=0):
    """
    Parses a patent XML file, isolates each table into its own XML file,
    and includes the 5 preceding paragraphs (excluding any table content inside them).
    Only processes tables that fall after the EXAMPLES heading.

    Stop boundary: The closing </description> tag (if present).

    Files are named  <base>_table_<NN>[_T<num>][_in_vitro].xml  where <NN> is the
    positional index (stable ordering / uniqueness) and T<num> is the patent's
    real table number parsed from the <title> (see _title_slug). descriptor_words
    optionally appends a short descriptor.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    base_name = os.path.splitext(os.path.basename(xml_filepath))[0]

    with open(xml_filepath, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'xml')

    def find_heading(text):
        return next(
            (h for h in soup.find_all('heading')
             if h.get_text(strip=True).lower() == text.lower()),
            None
        )

    examples_heading = find_heading('examples')
    if not examples_heading:
        print(f"  [SKIP] No EXAMPLES heading found in {xml_filepath}")
        return

    description_tag = soup.find('description')

    ps_after_examples = {id(p) for p in examples_heading.find_all_next('p')}

    all_tables_after = examples_heading.find_all_next(['tables', 'table'])
    top_level_tables = [
        t for t in all_tables_after
        if not t.find_parent(['tables', 'table'])
        and (not description_tag or description_tag in t.parents)
    ]

    if not top_level_tables:
        print(f"  [SKIP] No tables found in EXAMPLES section of {xml_filepath}")
        return

    for idx, table in enumerate(top_level_tables):
        previous_ps = table.find_all_previous('p', limit=5)[::-1]
        previous_ps = [
            p for p in previous_ps
            if id(p) in ps_after_examples
            and (not description_tag or description_tag in p.parents)
        ]

        # --- Title handling ---
        title_tag = table.find('title')
        # Use a SPACE separator so tokens split across markup
        # (e.g. <b>Table</b> <b>1</b>) are not concatenated into "Table1",
        # which would break both the keyword check and the slug.
        title_raw  = title_tag.get_text(separator=" ", strip=True) if title_tag else ""
        title_text = title_raw.lower()

        good_words = ["antisense strand", "cells", "in vitro", "sense strand",
                      "transfection", "single dose", "dose response", "modified sequences",
                      "antisense sequence", "sense sequence"]

        suffix = ""
        for word in good_words:
            if word in title_text:
                suffix = "_in_vitro"
                break

        # Build the output XML document
        out_soup = BeautifulSoup('<extracted_data></extracted_data>', 'xml')
        root = out_soup.extracted_data

        for p in previous_ps:
            p_copy = copy.copy(p)
            for nested_table in p_copy.find_all(['tables', 'table']):
                nested_table.decompose()
            root.append(p_copy)

        root.append(copy.copy(table))

        # Filename = patent id + positional index (stable ordering / uniqueness)
        # + the patent's real table number from <title> (provenance) + suffix.
        slug = _title_slug(title_raw, descriptor_words=descriptor_words)
        slug_part = f"_{slug}" if slug else ""
        out_filename = os.path.join(
            output_dir,
            f"{base_name}_table_{idx + 1:02d}{slug_part}{suffix}.xml"
        )
        with open(out_filename, 'w', encoding='utf-8') as out_file:
            out_file.write(out_soup.prettify())

        print(f"  Saved: {out_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Isolate patent tables into individual XML files.")
    parser.add_argument("xml_filepath", help="Path to the patent XML file.")
    parser.add_argument("output_dir", nargs="?", default="extracted_tables",
                        help="Directory for the isolated table XML files.")
    parser.add_argument("--descriptor-words", type=int, default=0,
                        help="Append this many descriptor words after the table number "
                             "(0 = table number only, the clean default).")
    args = parser.parse_args()
    extract_tables_from_patent(args.xml_filepath, args.output_dir,
                               descriptor_words=args.descriptor_words)
