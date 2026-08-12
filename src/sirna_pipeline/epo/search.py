"""
EPO patent identifier extraction, Phase 1 of the siRNA pipeline
=============================================================================

WHAT THIS MODULE DOES
---------------------
It asks the European Patent Office (EPO) which patents exist about siRNA and
writes their identifiers to a CSV file. Nothing else. No titles, no abstracts,
no full text. Those come later, in Phase 2 (`biblio.py`) and Phase 4
(`fulltext.py`).

Keeping Phase 1 this small is deliberate. The EPO free tier allows only 4 GB of
downloads per week, so the first pass collects the smallest possible amount of
information: just enough to know which patents exist and which of them are
duplicates of each other.

Output columns:
    Patent_ID  the publication identifier, e.g. "US7056704B2"
    Family_ID  the EPO family identifier, e.g. "27615316"
    Country    the two-letter office code, e.g. "US", "EP", "WO"


PATENT VOCABULARY FOR NEWCOMERS
-------------------------------
Publication
    One published document. A single invention is usually published several
    times: first as an application (kind code A1, A2 ...), later as a granted
    patent (kind code B1, B2 ...). Each publication has its own identifier.

Kind code
    The letter and digit at the end of an identifier. "A" means application,
    "B" means granted patent. So US7056704B2 is a granted US patent.

Patent family
    The same invention filed in several countries, plus all its applications
    and grants. One invention filed in Europe, the United States and under the
    international PCT route produces at least three publications that all share
    one Family_ID. For our purposes they describe the same experiments, so we
    keep only one member per family. Choosing which member to keep is what
    COUNTRY_PRIORITY below is for.

CPC and IPC classification codes
    Subject codes assigned by patent examiners, similar to library shelf marks.
    IPC is the international scheme, CPC is a more detailed joint EPO and USPTO
    extension of it. The main siRNA code is C12N15/113 (RNA interference).
    Because an examiner assigns them, they are reliable but slow to appear:
    a recent application may not be classified yet.

CQL
    Contextual Query Language, the query syntax the EPO search service accepts.
    The fields this module uses:
        cpc=      CPC classification code
        cpc=/low  CPC code including everything below it in the hierarchy
        ipc=      IPC classification code
        ta=       words appearing in the title or the abstract
        pa=       applicant name (the company that filed the patent)
        pd=       publication date, either a year or a `within "start end"` range

OPS
    Open Patent Services, the EPO REST API this module talks to.


THE THREE SEARCH STRATEGIES
---------------------------
The same invention can be found in different ways, and each way misses
something different. That is why `download_patent_ids` takes a `strategy`
argument instead of this module having one fixed behaviour.

    strategy="codes"
        Searches by CPC and IPC classification code. High precision: an
        examiner decided this document is about RNA interference. Misses recent
        applications that have not been classified yet.

    strategy="terms"
        Searches for siRNA wording in the title and abstract. High recall:
        catches documents the moment they are published, whatever the examiner
        later decides. Also catches false positives, for example a patent that
        merely mentions siRNA as prior art in its abstract.

    strategy="applicant"
        Searches by company name only, ignoring subject matter entirely. Used
        to pull one company's complete portfolio (Alnylam, for example) as a
        reference set to measure the other two strategies against.

    strategy="codes+terms"
        Runs the code queries and the term queries in one pass and merges the
        results into a single family-deduplicated file. Use this when you want
        one corpus rather than a comparison of the two approaches.

`applicant_filter` is separate from the strategy. Passing it together with
"codes" or "terms" narrows that subject search to one company. Passing
strategy="applicant" makes the company name the whole query.


HOW THE SEARCH GETS AROUND THE EPO LIMITS
-----------------------------------------
The OPS search service enforces three hard limits, and the whole shape of this
module is a response to them.

1. A query never returns more than 2000 results, however many actually match.
   Two defences:

   Independent queries. Every classification code and every search term is sent
   as its own query rather than joined with OR into one big query. Six small
   queries returning 800 results each give 4800 records. One combined query
   matching the same documents would be truncated at 2000.

   Date slicing. Each query is first run for one publication year. If that year
   still returns more than 2000, it is split into twelve monthly windows, and
   any month still over the limit is split into daily windows. A single day
   over 2000 results cannot be split further, so the module logs a warning and
   accepts the truncation.

2. A single response page holds at most 100 records, so results are paginated.

3. Roughly 10 requests per minute. Every request is followed by a sleep. The
   values in the SLEEP_ constants below are deliberately conservative: shorter
   pauses have triggered a temporary ban in practice.

Because independent queries overlap heavily (one patent carries several
classification codes and matches several search terms), the same family arrives
many times. `_deduplicate_by_family` keeps exactly one publication per family.


HOW TO CHANGE WHAT IS SEARCHED FOR
----------------------------------
Everything specific to siRNA sits in section 1, right below the imports:
CPC_CODES, IPC_CODES, EXCLUDED_CPC_CODES and SEARCH_TERMS. Editing those four
lists is the only change needed to widen or narrow the search. Nothing further
down the file mentions siRNA.


TYPICAL USE
-----------
    from sirna_pipeline.epo import search

    search.check_epo_quota(KEY, SECRET)

    df = search.download_patent_ids(
        consumer_key=KEY,
        consumer_secret=SECRET,
        start_year=2022,
        end_year=2025,
        strategy="codes",
    )
"""

# `from __future__ import annotations` makes Python treat every type hint as
# plain text instead of evaluating it. It is what allows the modern
# `str | None` syntax to work on Python versions older than 3.10.
from __future__ import annotations

import base64
import calendar
import os
import re
import time
import urllib.parse

import pandas as pd
import requests


# =============================================================================
# 1. WHAT TO SEARCH FOR
#
# This is the only section that is specific to siRNA. Edit these four lists to
# change the subject of the search; the rest of the module is generic.
# =============================================================================

# CPC codes, searched with the "/low" modifier so that every subgroup below the
# code is included automatically. C12N15/11 covers DNA and RNA fragments, and
# the siRNA code C12N15/113 sits underneath it, so "/low" picks it up.
CPC_CODES = [
    "C12N15/11",     # DNA or RNA fragments, includes C12N15/113 (RNA interference)
    "A61K31/7088",   # medicinal preparations containing nucleic acids
]

# IPC codes, searched without hierarchy expansion, so subgroups are listed
# explicitly. IPC is coarser than CPC and is applied by more offices, which
# catches documents a CPC-only search would miss.
IPC_CODES = [
    "C12N15/113",    # RNA interference and small interfering RNA
    "A61K31/713",    # medicinal preparations containing double-stranded RNA
    "C12N15/11",     # parent group of C12N15/113
    "A61K31/7088",   # parent group of A61K31/713
]

# Codes attached to two neighbouring technologies that use similar language but
# a different mechanism. They are excluded from every subject query so they do
# not enter the corpus:
#   C12N15/115  aptamers, short nucleic acids that bind a protein directly
#   C12N15/117  immunomodulatory nucleic acids, which act on immune receptors
EXCLUDED_CPC_CODES = [
    "C12N15/115",
    "C12N15/117",
]

# Title and abstract terms. Patent drafters write the same concept in many
# ways, so the spelled-out forms ("ribonucleic acid") sit here alongside the
# abbreviations, in singular and plural.
#
# Two syntax rules apply:
#   * a trailing "*" is a wildcard, so siRNA* also matches siRNAs
#   * a term containing a space must be wrapped in double quotes, otherwise the
#     EPO reads it as two separate words
SEARCH_TERMS = [
    "siRNA*", "RNAi*", "dsRNA*", "iRNA*", "dsRNAi*", "oligonucleotide*",
    '"si-RNA"', '"ds RNA"',
    '"RNA interference"', '"interfering RNA"',
    '"small interfering RNA"', '"small interfering RNAs"',
    '"short interfering RNA"', '"short interfering RNAs"',
    '"double stranded RNA"', '"double stranded RNAs"',
    '"RNA duplex"', '"RNA duplexes"',
    '"small interfering ribonucleic acid"', '"small interfering ribonucleic acids"',
    '"short interfering ribonucleic acid"', '"short interfering ribonucleic acids"',
    '"double stranded ribonucleic acid"', '"double stranded ribonucleic acids"',
    '"ribonucleic acid interference"',
    '"interfering ribonucleic acid"', '"interfering ribonucleic acids"',
]


# =============================================================================
# 2. API SETTINGS AND LIMITS
#
# These describe the EPO service itself and should only change if the EPO
# changes its terms of use.
# =============================================================================

AUTH_URL   = "https://ops.epo.org/3.2/auth/accesstoken"
SEARCH_URL = "https://ops.epo.org/3.2/rest-services/published-data/search"

MAX_RESULTS_PER_QUERY = 2000   # hard server-side cap, results beyond it are lost
RESULTS_PER_PAGE      = 100    # hard server-side cap on one response page
MAX_ATTEMPTS          = 4      # retries per failed request before giving up

TOKEN_REFRESH_SECONDS = 850    # tokens expire at 900s, refresh early for safety
SLEEP_BETWEEN_PAGES   = 8      # pause after every page of results
SLEEP_AFTER_COUNT     = 3      # pause after every count-only request
SLEEP_ON_RETRY        = 20     # pause after a transient failure
SLEEP_ON_FORBIDDEN    = 600    # pause after HTTP 403, which signals a rate ban

WEEKLY_QUOTA_MB = 4000         # free tier allowance, used only for the report

# Access tokens are valid for 900 seconds. This module-level dictionary holds
# the current token so that hundreds of requests share one token instead of
# authenticating each time. `_get_valid_token` refreshes it when it ages out.
TOKEN_CACHE = {"token": None, "timestamp": 0}


# =============================================================================
# 3. COUNTRY PRIORITY
#
# One patent family contains the same invention published by several offices.
# Only one member is kept, and this table decides which. Lower number wins.
#
# The ranking follows what the later pipeline stages need:
#   EP first, because Phase 4 downloads full-text XML from the European
#      Publication Server, which only serves EP documents.
#   US second, because US full text is obtainable from the USPTO if the family
#      has no EP member.
#   WO third, since PCT publications are in English and widely mirrored.
#   Then the remaining offices, roughly by how likely structured full text is
#      to be available.
#
# A country absent from this table gets the fallback score 99, so it is chosen
# only when the family contains nothing better.
# =============================================================================
COUNTRY_PRIORITY = {
    "EP": 1,  "US": 2,  "WO": 3,  "CA": 6,  "AT": 7,  "BE": 8,  "BG": 9,
    "CY": 10, "CZ": 11, "DK": 12, "EE": 13, "ES": 14, "FR": 15,
    "GR": 16, "HR": 17, "IT": 18, "LT": 19, "LU": 20, "MC": 21,
    "MD": 22, "ME": 23, "NO": 24, "PL": 25, "PT": 26, "RO": 27,
    "RS": 28, "SE": 29, "SK": 30, "GB": 31, "IE": 32, "CH": 33,
    "AU": 34, "NZ": 35,
}

UNKNOWN_COUNTRY_SCORE = 99

VALID_STRATEGIES = ("codes", "terms", "codes+terms", "applicant")


# =============================================================================
# 4. AUTHENTICATION AND QUOTA
# =============================================================================
def _get_valid_token(consumer_key: str, consumer_secret: str) -> str:
    """
    Return a valid EPO access token, generating a new one when needed.

    The EPO uses OAuth2 client credentials: the key and secret are exchanged
    for a token that is valid for 900 seconds, and the token is what every
    later request carries. Since a full extraction makes thousands of requests
    over several hours, the token is cached module-wide and refreshed once it
    is older than TOKEN_REFRESH_SECONDS.

    Args:
        consumer_key:    EPO OPS consumer key.
        consumer_secret: EPO OPS consumer secret.

    Returns:
        The access token string, ready to be placed in an Authorization header.
    """
    age = time.time() - TOKEN_CACHE["timestamp"]
    if TOKEN_CACHE["token"] is None or age > TOKEN_REFRESH_SECONDS:
        print("\n[AUTH] Generating a new EPO access token...")
        auth_string  = f"{consumer_key}:{consumer_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()

        response = requests.post(
            AUTH_URL,
            headers={
                "Authorization": f"Basic {encoded_auth}",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=30,
        )
        response.raise_for_status()

        TOKEN_CACHE["token"]     = response.json()["access_token"]
        TOKEN_CACHE["timestamp"] = time.time()

    return TOKEN_CACHE["token"]


def check_epo_quota(consumer_key: str, consumer_secret: str) -> tuple:
    """
    Print how much of the weekly EPO download allowance has been used.

    The free tier allows 4 GB per week. The EPO reports current consumption in
    the response headers of any request, so this function sends one deliberately
    tiny query (a single result) and reads the headers rather than the body.

    Running this before a long extraction is optional but recommended: an
    extraction that runs out of quota halfway through wastes hours.

    Args:
        consumer_key:    EPO OPS consumer key.
        consumer_secret: EPO OPS consumer secret.

    Returns:
        (True, weekly_pct). The first element is always True so that a failed
        quota check never blocks an extraction; the second is the percentage of
        the weekly allowance already consumed.
    """
    print("=" * 45)
    print("      EPO OPS API - STATUS DASHBOARD")
    print("=" * 45)

    weekly_pct = 0.0

    try:
        token = _get_valid_token(consumer_key, consumer_secret)

        # One-result probe query, chosen only because it is guaranteed to be
        # valid. The result itself is discarded; the headers are the point.
        probe_query = urllib.parse.quote('cpc="C12N15/113"')
        response = requests.get(
            f"{SEARCH_URL}?q={probe_query}",
            headers={"Authorization": f"Bearer {token}", "X-OPS-Range": "1-1"},
            timeout=30,
        )
        response.raise_for_status()

        headers = response.headers
        traffic = headers.get("X-Throttling-Control", "")

        # The throttling header reports the current request rate per service
        search_match = re.search(r"search=\w+:(\d+)", traffic)
        search_rpm   = int(search_match.group(1)) if search_match else 0

        # The EPO uses either header name depending on the account type
        used_bytes_raw = (
            headers.get("X-RegisteredQuotaPerWeek-Used")
            or headers.get("X-IndividualQuotaPerHour-Used")
            or "0"
        )

        try:
            used_mb       = int(used_bytes_raw) / (1024 * 1024)
            weekly_pct    = (used_mb / WEEKLY_QUOTA_MB) * 100
            remaining_pct = 100 - weekly_pct
        except ValueError:
            used_mb       = 0.0
            weekly_pct    = 0.0
            remaining_pct = 100.0

        print("\n[DATA CONSUMPTION]")
        print(f"Weekly Volume Used: {weekly_pct:.4f}% ({used_mb:.2f} MB / {WEEKLY_QUOTA_MB} MB)")
        print(f"Quota Remaining:    {remaining_pct:.4f}%")
        print(f"Current API Load:   {search_rpm} requests/minute")
        print("\n" + "-" * 45)

        if weekly_pct >= 100:
            print("[WARNING] Weekly 4 GB quota exceeded. Script will try to run anyway.")
        elif "overloaded" in traffic.lower():
            print("[WARNING] Server overloaded. Script will try to run anyway.")
        else:
            print("[OK] Server clear. Safe to proceed with extraction.")
        print("-" * 45)

    except Exception as exc:
        print(f"\n[QUOTA CHECK ERROR] Could not read quota, proceeding anyway: {exc}")

    return True, weekly_pct


# =============================================================================
# 5. SMALL HELPERS
# =============================================================================
def _clean_val(node) -> str:
    """
    Pull a plain string out of an EPO JSON value node.

    The EPO wraps text values in a dictionary with a single "$" key, so the
    country code "US" arrives as {"$": "US"}. This unwraps it, and passes
    through anything that is already a plain value.
    """
    if isinstance(node, dict):
        return node.get("$", "")
    return str(node) if node else ""


def _as_list(node) -> list:
    """
    Force an EPO JSON node into a list.

    When a response contains several documents the EPO returns a list; when it
    contains exactly one it returns a bare dictionary instead. Every loop over
    EPO results has to handle both, so it is done here once.
    """
    if node is None:
        return []
    if isinstance(node, dict):
        return [node]
    if isinstance(node, list):
        return node
    return []


# =============================================================================
# 6. QUERY CONSTRUCTION
#
# Two steps. `build_independent_queries` produces the list of subject queries
# implied by the strategy, one entry per code or per term. `_build_cql` then
# completes a single query by adding the applicant filter and the date window.
# =============================================================================
def _exclusion_clause() -> str:
    """
    Build the CQL fragment that removes the neighbouring technologies.

    Returns a string such as:
        ' NOT (cpc="C12N15/115" OR cpc="C12N15/117")'

    An empty string is returned when EXCLUDED_CPC_CODES is empty, so the rest
    of the query building works unchanged if the exclusions are removed.
    """
    if not EXCLUDED_CPC_CODES:
        return ""
    joined = " OR ".join(f'cpc="{code}"' for code in EXCLUDED_CPC_CODES)
    return f" NOT ({joined})"


def build_independent_queries(strategy: str) -> list[str]:
    """
    Build the list of subject queries for a strategy.

    Every entry in the returned list is sent to the EPO as its own query, which
    is what keeps each response below the 2000-result cap. The date window and
    any applicant filter are added later by `_build_cql`.

    Args:
        strategy: one of "codes", "terms", "codes+terms", "applicant".

    Returns:
        A list of CQL fragments. For strategy="applicant" the list is a single
        empty string: there is no subject condition, but the extraction loop
        still has to run exactly once.

    Raises:
        ValueError: if the strategy name is not recognised.
    """
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"Unknown strategy {strategy!r}. "
            f"Valid options are: {', '.join(VALID_STRATEGIES)}."
        )

    # Applicant-only searches have no subject condition. The single empty
    # string is a placeholder that makes the caller's loop execute once.
    if strategy == "applicant":
        return [""]

    exclusions = _exclusion_clause()
    queries: list[str] = []

    if strategy in ("codes", "codes+terms"):
        # "/low" expands the code to include everything below it in the CPC
        # hierarchy, so C12N15/11 also brings in C12N15/113 and its siblings.
        for code in CPC_CODES:
            queries.append(f'(cpc=/low "{code}"{exclusions})')
        # IPC has no hierarchy expansion here, so subgroups are listed one by one.
        for code in IPC_CODES:
            queries.append(f'(ipc="{code}"{exclusions})')

    if strategy in ("terms", "codes+terms"):
        # ta= searches the title and the abstract only, not the full text.
        for term in SEARCH_TERMS:
            queries.append(f"(ta={term}{exclusions})")

    return queries


def _build_cql(base_query: str, date_condition: str, applicant_filter: str | None) -> str:
    """
    Assemble one complete CQL query string.

    Three shapes are produced, depending on what is supplied:
        applicant only:  pa="Alnylam*" AND pd=2024
        subject only:    (cpc=/low "C12N15/11" NOT (...)) AND pd=2024
        both combined:   (cpc=...) AND pa="Alnylam*" AND pd=2024

    Args:
        base_query:       one entry from `build_independent_queries`, possibly
                          an empty string for an applicant-only search.
        date_condition:   the date fragment, e.g. 'pd=2024' or
                          'pd within "20240101 20240131"'.
        applicant_filter: applicant name, or None. A trailing "*" acts as a
                          wildcard, which matters because one company files
                          under several legal entity names.

    Returns:
        The full query string, not yet URL-encoded.
    """
    parts = []
    if base_query:
        parts.append(base_query)
    if applicant_filter:
        parts.append(f'pa="{applicant_filter}"')
    parts.append(date_condition)
    return " AND ".join(parts)


# =============================================================================
# 7. COUNTING RESULTS
#
# Every date window is counted before anything is downloaded. The count is what
# decides whether a window can be fetched directly or has to be sliced further,
# and a count request costs almost no quota.
# =============================================================================
def get_total_results_count(cql_query: str, consumer_key: str, consumer_secret: str) -> int:
    """
    Return how many patents match a query, without downloading any of them.

    The trick is the X-OPS-Range: 1-1 header, which asks for a single result.
    The EPO still reports the true total in the @total-result-count field, so
    the count arrives for the bandwidth cost of one record.

    Failures are handled by status code:
        400  the query syntax was rejected. Permanent, so return 0 immediately.
        403  rate limit or ban. Wait ten minutes, then retry.
        404  usually a transient server hiccup, sometimes a genuinely empty
             window. Retried, and treated as empty if it persists.
        429, 503  server busy. Wait longer on each attempt.

    Args:
        cql_query:       complete query string including the date condition.
        consumer_key:    EPO OPS consumer key.
        consumer_secret: EPO OPS consumer secret.

    Returns:
        The number of matching patents, or 0 if the query matched nothing or
        failed on every attempt.
    """
    url = f"{SEARCH_URL}?q={urllib.parse.quote(cql_query)}"

    for attempt in range(MAX_ATTEMPTS):
        try:
            token    = _get_valid_token(consumer_key, consumer_secret)
            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept":        "application/json",
                    "X-OPS-Range":   "1-1",
                },
                timeout=30,
            )

            if response.status_code == 200:
                payload = (
                    response.json()
                    .get("ops:world-patent-data", {})
                    .get("ops:biblio-search", {})
                )
                total = int(payload.get("@total-result-count", 0))
                time.sleep(SLEEP_AFTER_COUNT)
                return total

            if response.status_code == 400:
                print("[SYNTAX ERROR 400] EPO rejected the query syntax. Skipping.")
                return 0

            if response.status_code == 404:
                if attempt < MAX_ATTEMPTS - 1:
                    print(f"[WARNING 404] Slice not found "
                          f"(attempt {attempt + 1}/{MAX_ATTEMPTS}). Retrying...")
                    time.sleep(SLEEP_ON_RETRY)
                    continue
                # Still missing after every retry, so treat the window as empty
                return 0

            if response.status_code == 403:
                print(f"[SECURITY 403] Forbidden. Cooling down for {SLEEP_ON_FORBIDDEN}s...")
                time.sleep(SLEEP_ON_FORBIDDEN)

            elif response.status_code in (429, 503):
                wait_time = 300 * (attempt + 1)
                print(f"[OVERLOAD {response.status_code}] API busy. Waiting {wait_time}s...")
                time.sleep(wait_time)

            else:
                time.sleep(15)

        except Exception as exc:
            if attempt == MAX_ATTEMPTS - 1:
                print(f"[ERROR] Connection failed after {MAX_ATTEMPTS} attempts: {exc}")
            time.sleep(SLEEP_ON_RETRY)

    print("[CRITICAL] EPO server consistently rejecting count requests. Skipping this slice.")
    return 0


# =============================================================================
# 8. DOWNLOADING IDENTIFIERS
# =============================================================================
def _parse_publication_page(documents: list) -> list[dict]:
    """
    Turn one page of EPO search results into simple records.

    Each document in the response carries its family identifier as an attribute
    and several identifiers in different formats. Only the "docdb" format is
    used, because it splits the identifier into country, number and kind code
    instead of returning one opaque string.

    Args:
        documents: the ops:publication-reference nodes from one response page.

    Returns:
        A list of dicts with keys 'id', 'family_id' and 'country'. Documents
        without a usable docdb identifier are skipped.
    """
    records = []

    for doc in documents:
        # The family identifier is an attribute on the reference node itself,
        # not a nested child element.
        family_id = doc.get("@family-id", "").strip()

        # A document carries its identifier in up to three formats
        # (docdb, epodoc, original). Only docdb is structured.
        ids = _as_list(doc.get("document-id", doc.get("ops:document-id")))
        docdb_node = next(
            (d for d in ids if d.get("@document-id-type") == "docdb"), None
        )
        if not docdb_node:
            continue

        country = _clean_val(docdb_node.get("country", ""))
        # Raw document numbers sometimes contain stray spaces or dots
        number = _clean_val(docdb_node.get("doc-number", "")).replace(" ", "").replace(".", "")
        # The kind code is optional. When absent, it is left off rather than
        # appended as an empty string.
        kind = _clean_val(docdb_node.get("kind", ""))

        if not (country and number):
            continue

        patent_id = f"{country}{number}{kind}" if kind else f"{country}{number}"
        records.append({
            "id":        patent_id,
            "family_id": family_id,
            "country":   country,
        })

    return records


def fetch_ids_for_query(
    cql_query: str,
    total_patents: int,
    consumer_key: str,
    consumer_secret: str,
) -> list[dict]:
    """
    Download every identifier matching one query, page by page.

    The caller has already counted the results and made sure the query fits
    inside the 2000-result cap. This function only walks the pages.

    Pagination uses the X-OPS-Range header with 1-based positions, so the first
    page is "1-100", the second "101-200" and so on. A page that keeps failing
    after every retry ends the pagination for this query, and whatever was
    collected so far is returned rather than discarded.

    Args:
        cql_query:       complete query string including the date condition.
        total_patents:   result count from `get_total_results_count`, used to
                         know where to stop.
        consumer_key:    EPO OPS consumer key.
        consumer_secret: EPO OPS consumer secret.

    Returns:
        A list of dicts with keys 'id', 'family_id' and 'country'. Duplicates
        within this query are already removed; duplicates across queries are
        resolved later by `_deduplicate_by_family`.
    """
    # Defensive cap: the server never returns more than 2000 for one query,
    # whatever count the caller passed in.
    total_patents = min(total_patents, MAX_RESULTS_PER_QUERY)

    encoded_query = urllib.parse.quote(cql_query)
    records: list[dict] = []
    seen_ids: set[str] = set()
    start_index = 1

    while start_index <= total_patents:
        end_index = min(start_index + RESULTS_PER_PAGE - 1, total_patents)
        documents = []
        page_ok   = False

        for attempt in range(MAX_ATTEMPTS):
            token   = _get_valid_token(consumer_key, consumer_secret)
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept":        "application/json",
                "X-OPS-Range":   f"{start_index}-{end_index}",
            }

            try:
                response = requests.get(
                    f"{SEARCH_URL}?q={encoded_query}", headers=headers, timeout=30
                )

                if response.status_code == 200:
                    biblio = (
                        response.json()
                        .get("ops:world-patent-data", {})
                        .get("ops:biblio-search", {})
                    )
                    documents = _as_list(
                        biblio.get("ops:search-result", {}).get("ops:publication-reference")
                    )

                    # Log requested against received. A mismatch is the visible
                    # symptom of server-side truncation.
                    ops_range = biblio.get("ops:range", {})
                    print(f"[INFO] Requested: {start_index}-{end_index} | "
                          f"Received: {ops_range.get('@begin', 0)}-{ops_range.get('@end', 0)} | "
                          f"Total expected: {total_patents}")

                    if not documents and attempt < MAX_ATTEMPTS - 1:
                        # HTTP 200 with an empty body is usually transient
                        time.sleep(SLEEP_ON_RETRY)
                        continue

                    if not documents:
                        print(f"[WARNING] HTTP 200 but no documents found after "
                              f"{MAX_ATTEMPTS} attempts.")
                        break

                    page_ok = True
                    break

                if response.status_code == 400:
                    # Rejected syntax is permanent. Return what was collected.
                    print("[SYNTAX ERROR 400] EPO rejected the query. Skipping block.")
                    return records

                if response.status_code == 404:
                    if start_index == 1 and attempt == MAX_ATTEMPTS - 1:
                        # A 404 on the very first page means the query matched
                        # nothing at all.
                        print(f"[WARNING 404] No results found after {MAX_ATTEMPTS} attempts.")
                        return records
                    print(f"[WARNING 404] Page {start_index} not found "
                          f"(attempt {attempt + 1}/{MAX_ATTEMPTS}). Retrying...")
                    time.sleep(SLEEP_ON_RETRY)
                    continue

                if response.status_code == 403:
                    print(f"[WARNING 403] Access denied. Waiting 15 min "
                          f"(attempt {attempt + 1}/{MAX_ATTEMPTS})...")
                    time.sleep(900)

                elif response.status_code in (429, 503):
                    time.sleep(300 * (attempt + 1))

                else:
                    time.sleep(15)

            except Exception:
                # Network-level failure such as a timeout or a reset connection
                time.sleep(SLEEP_ON_RETRY)

        if not page_ok:
            # Every retry for this page failed. Stop paginating this query and
            # keep whatever was already collected.
            break

        for record in _parse_publication_page(documents):
            if record["id"] not in seen_ids:
                seen_ids.add(record["id"])
                records.append(record)

        start_index += RESULTS_PER_PAGE
        time.sleep(SLEEP_BETWEEN_PAGES)

    return records


# =============================================================================
# 9. DATE SLICING
#
# The 2000-result cap applies per query, and the only way to make a query
# smaller without changing its subject is to narrow its date window.
# =============================================================================
def search_year_with_slicing(
    base_query: str,
    year: int,
    consumer_key: str,
    consumer_secret: str,
    applicant_filter: str | None = None,
) -> list[dict]:
    """
    Collect every identifier for one query in one publication year.

    The window is narrowed only as far as necessary, because every narrowing
    step costs extra count requests:

        1. Count the whole year. If it is empty, stop. If it fits under 2000,
           fetch it in one pass.
        2. Otherwise count each of the twelve months and fetch the ones that
           fit.
        3. For any month still over 2000, count and fetch day by day.

    A single day over 2000 results cannot be narrowed further. The EPO silently
    truncates it, so the loss is logged as a warning and execution continues.

    Args:
        base_query:       one entry from `build_independent_queries`.
        year:             publication year to cover.
        consumer_key:     EPO OPS consumer key.
        consumer_secret:  EPO OPS consumer secret.
        applicant_filter: optional applicant name to narrow the query.

    Returns:
        A list of dicts with keys 'id', 'family_id' and 'country'.
    """
    def build(date_condition: str) -> str:
        return _build_cql(base_query, date_condition, applicant_filter)

    # --- Level 1, the whole year ---------------------------------------
    year_cql = build(f"pd={year}")
    year_total = get_total_results_count(year_cql, consumer_key, consumer_secret)

    if year_total == 0:
        return []

    if year_total <= MAX_RESULTS_PER_QUERY:
        print(f"[INFO] Year {year}: {year_total} results. Fetching yearly...")
        return fetch_ids_for_query(year_cql, year_total, consumer_key, consumer_secret)

    # --- Level 2, month by month ---------------------------------------
    records: list[dict] = []
    print(f"[INFO] Year {year}: {year_total} results (>{MAX_RESULTS_PER_QUERY}). "
          f"Slicing by month...")

    for month in range(1, 13):
        # calendar.monthrange returns (weekday of the 1st, number of days), so
        # index 1 gives the correct last day including February in leap years.
        last_day = calendar.monthrange(year, month)[1]

        # The EPO date range syntax needs YYYYMMDD on both sides
        month_start = f"{year}{month:02d}01"
        month_end   = f"{year}{month:02d}{last_day}"

        month_cql   = build(f'pd within "{month_start} {month_end}"')
        month_total = get_total_results_count(month_cql, consumer_key, consumer_secret)

        if month_total == 0:
            continue

        if month_total <= MAX_RESULTS_PER_QUERY:
            print(f"[INFO] Year {year}, month {month}: fetching {month_total} results...")
            records.extend(
                fetch_ids_for_query(month_cql, month_total, consumer_key, consumer_secret)
            )
            continue

        # --- Level 3, day by day, the last available narrowing ---------
        print(f"[INFO] Year {year}, month {month}: {month_total} results "
              f"(>{MAX_RESULTS_PER_QUERY}). Slicing by day...")

        for day in range(1, last_day + 1):
            day_str   = f"{year}{month:02d}{day:02d}"
            day_cql   = build(f'pd within "{day_str} {day_str}"')
            day_total = get_total_results_count(day_cql, consumer_key, consumer_secret)

            if day_total > MAX_RESULTS_PER_QUERY:
                # No deeper slicing exists. The surplus is lost server-side.
                print(f"[WARNING] Year {year}, month {month}, day {day}: {day_total} "
                      f"results exceed {MAX_RESULTS_PER_QUERY}. API cap truncating results.")

            if day_total > 0:
                print(f"[INFO] Year {year}, month {month}, day {day}: "
                      f"fetching {day_total} results...")
                records.extend(
                    fetch_ids_for_query(day_cql, day_total, consumer_key, consumer_secret)
                )

    return records


# =============================================================================
# 10. FAMILY DEDUPLICATION
# =============================================================================
def _deduplicate_by_family(id_records: list, seen_families: dict | None = None) -> tuple:
    """
    Keep one publication per patent family.

    Independent queries overlap heavily, so the same invention arrives many
    times: once per matching classification code, once per matching search
    term, and once per country it was filed in. This collapses all of that to
    one row per family, choosing the member from the highest-priority country
    according to COUNTRY_PRIORITY.

    Cross-year awareness: a family already kept in an earlier year is skipped,
    unless the current year contains a strictly better member. That happens
    when, for example, an EP grant is published two years after the US
    application that was picked first.

    Publications with no family identifier are kept individually, since there
    is no way to tell whether they duplicate anything.

    Args:
        id_records:    records from Phase 1, each with 'id', 'family_id',
                       'country'.
        seen_families: mapping of family identifier to the best country score
                       already kept in a previous year. Pass None for the first
                       year.

    Returns:
        (kept_ids, families_this_year) where kept_ids is a list of patent
        identifier strings and families_this_year maps each family identifier
        processed here to the country score of the member that was kept.
    """
    if seen_families is None:
        seen_families = {}

    family_map: dict[str, dict] = {}   # family_id -> best record found so far
    no_family:  dict[str, bool] = {}   # patent_id -> True, for records with no family

    for record in id_records:
        family_id = record.get("family_id", "").strip()
        patent_id = record.get("id", "")
        country   = record.get("country", "")

        if not family_id:
            if patent_id and patent_id not in seen_families:
                no_family[patent_id] = True
            continue

        new_score = COUNTRY_PRIORITY.get(country, UNKNOWN_COUNTRY_SCORE)

        # Already kept in an earlier year. Skip it unless this member comes
        # from a better country than the one already stored.
        if family_id in seen_families and new_score >= seen_families[family_id]:
            continue

        # Within this call, keep the best country seen for the family
        if family_id not in family_map:
            family_map[family_id] = record
        else:
            current_score = COUNTRY_PRIORITY.get(
                family_map[family_id]["country"], UNKNOWN_COUNTRY_SCORE
            )
            if new_score < current_score:
                family_map[family_id] = record

    kept_ids = [rec["id"] for rec in family_map.values()] + list(no_family.keys())

    families_this_year = {
        fam_id: COUNTRY_PRIORITY.get(rec["country"], UNKNOWN_COUNTRY_SCORE)
        for fam_id, rec in family_map.items()
    }
    # Records with no family are tracked under their own identifier so they are
    # not collected again in a later year.
    for patent_id in no_family:
        families_this_year[patent_id] = UNKNOWN_COUNTRY_SCORE

    return kept_ids, families_this_year


# =============================================================================
# 11. OUTPUT FILE NAMING
# =============================================================================
def _strategy_tag(strategy: str, applicant_filter: str | None) -> str:
    """
    Build the part of the output filename that records how the search was run.

    The tag makes a directory of results self-describing, and lets several
    strategies be run over the same years without overwriting each other:

        codes_only, terms_only, codes_and_terms
        only_applicant_Alnylam
        codes_and_Alnylam, terms_and_Alnylam

    The wildcard "*" is stripped from the applicant name, since it is valid in
    a query but not in a filename.
    """
    applicant = re.sub(r"\*", "", applicant_filter) if applicant_filter else ""

    if strategy == "applicant":
        return f"only_applicant_{applicant}"

    subject = {
        "codes":       "codes",
        "terms":       "terms",
        "codes+terms": "codes_and_terms",
    }[strategy]

    if applicant:
        return f"{subject}_and_{applicant}"
    # "codes+terms" already reads as a description, the other two need "_only"
    # to say that no applicant filter was applied.
    if strategy == "codes+terms":
        return subject
    return f"{subject}_only"


# =============================================================================
# 12. MAIN ENTRY POINT
# =============================================================================
def download_patent_ids(
    consumer_key: str,
    consumer_secret: str,
    start_year: int,
    end_year: int,
    strategy: str = "codes",
    applicant_filter: str | None = None,
    output_dir: str = ".",
    autosave: bool = True,
) -> pd.DataFrame:
    """
    Run a full Phase 1 extraction and write the identifier CSV.

    What happens, year by year:
        1. Every subject query for the chosen strategy is run for that year,
           slicing the date window whenever a query exceeds the 2000-result cap.
        2. The records are deduplicated by patent family, taking into account
           the families already kept in earlier years.
        3. The surviving rows are written to a per-year autosave file, so a run
           interrupted after six hours is not lost.
    After the last year, all years are concatenated and deduplicated once more
    by family, and the final CSV is written.

    Args:
        consumer_key:     EPO OPS consumer key.
        consumer_secret:  EPO OPS consumer secret.
        start_year:       first publication year to cover, inclusive.
        end_year:         last publication year to cover, inclusive.
        strategy:         how to search. One of:
                            "codes"        CPC and IPC classification codes
                            "terms"        title and abstract keywords
                            "codes+terms"  both, merged into one corpus
                            "applicant"    company name only, no subject filter
        applicant_filter: company name to restrict the search to, for example
                          "Alnylam*". With "codes" or "terms" it narrows the
                          subject search; with "applicant" it is the whole
                          query and is therefore required. A trailing "*"
                          matches all of a company's legal entity names.
        output_dir:       directory for the CSV files. Created if missing.
        autosave:         write one CSV per year as the run progresses.

    Returns:
        A DataFrame with columns Patent_ID, Family_ID and Country, one row per
        patent family. Empty if the search found nothing.

    Raises:
        ValueError: if the strategy is unknown, if strategy="applicant" is used
                    without an applicant name, or if the year range is reversed.

    Examples:
        Full siRNA landscape from classification codes:
            download_patent_ids(KEY, SECRET, 2022, 2025, strategy="codes")

        Keyword search over a longer period:
            download_patent_ids(KEY, SECRET, 2001, 2026, strategy="terms")

        One company's complete portfolio:
            download_patent_ids(KEY, SECRET, 2022, 2025,
                                strategy="applicant",
                                applicant_filter="Alnylam*")

        That company's siRNA-classified patents only:
            download_patent_ids(KEY, SECRET, 2022, 2025,
                                strategy="codes",
                                applicant_filter="Alnylam*")
    """
    # --- Validate the arguments before spending any API quota --------------
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"Unknown strategy {strategy!r}. "
            f"Valid options are: {', '.join(VALID_STRATEGIES)}."
        )
    if strategy == "applicant" and not applicant_filter:
        raise ValueError(
            'strategy="applicant" needs an applicant_filter, '
            'for example applicant_filter="Alnylam*".'
        )
    if end_year < start_year:
        raise ValueError(f"end_year ({end_year}) is before start_year ({start_year}).")

    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)

    independent_queries = build_independent_queries(strategy)
    tag = _strategy_tag(strategy, applicant_filter)

    print(f"\n=== STARTING EPO ID EXTRACTION ({start_year} - {end_year}) ===")
    print(f"[INFO] Strategy: {strategy}")
    print(f"[INFO] {len(independent_queries)} independent query conditions.")
    if strategy != "applicant" and EXCLUDED_CPC_CODES:
        print(f"[INFO] Exclusions active: {', '.join(EXCLUDED_CPC_CODES)} "
              f"(Aptamers, Immunomodulatory)")
    if applicant_filter:
        print(f"[INFO] Applicant filter: {applicant_filter}")

    try:
        yearly_frames = []

        # Families kept so far, across every year processed. Maps family
        # identifier to the country score of the member that was kept, so a
        # later year can replace it only with something better.
        seen_families_global: dict[str, int] = {}

        for year in range(start_year, end_year + 1):
            print(f"\n[INFO] Processing year {year}...")

            # Run every independent query for this year and pool the records
            year_records = []
            for index, base_query in enumerate(independent_queries, start=1):
                label = base_query[:80] if base_query else "(applicant only)"
                print(f"  -> Query {index}/{len(independent_queries)}: {label}...")
                year_records.extend(
                    search_year_with_slicing(
                        base_query, year, consumer_key, consumer_secret, applicant_filter
                    )
                )

            print(f"[INFO] Raw records accumulated (including cross-query duplicates): "
                  f"{len(year_records)}")

            kept_ids, new_families = _deduplicate_by_family(
                year_records, seen_families=seen_families_global
            )
            seen_families_global.update(new_families)

            print(f"[INFO] After family deduplication: {len(kept_ids)} unique IDs")
            print(f"[INFO] Records filtered (cross-query duplicates + prior-year skips): "
                  f"{len(year_records) - len(kept_ids)}")
            print(f"[INFO] Families tracked globally so far: {len(seen_families_global)}")

            if not kept_ids:
                continue

            # Recover the full record for each identifier that survived
            winners = set(kept_ids)
            unique_records: dict[str, dict] = {}
            for record in year_records:
                if record["id"] in winners and record["id"] not in unique_records:
                    unique_records[record["id"]] = record

            df_year = pd.DataFrame(list(unique_records.values())).rename(columns={
                "id":        "Patent_ID",
                "family_id": "Family_ID",
                "country":   "Country",
            })

            # A missing family identifier would create empty cells, and later
            # stages group on this column. Placeholders keep it populated and
            # unique so these rows survive the final deduplication.
            missing_family = df_year["Family_ID"].eq("")
            if missing_family.sum() > 0:
                print(f"  [WARNING] {missing_family.sum()} patents have no family ID, "
                      f"assigning placeholders.")
                df_year.loc[missing_family, "Family_ID"] = [
                    f"UNKNOWN_FAM_{i}_{year}" for i in range(missing_family.sum())
                ]

            if autosave:
                autosave_path = os.path.join(
                    output_dir, f"EPO_IDs_AutoSave_{tag}_{year}.csv"
                )
                df_year.to_csv(autosave_path, index=False, sep=";", encoding="utf-8-sig")
                print(f"  [AUTOSAVE] Year {year}: {len(df_year)} IDs saved to {autosave_path}")

            yearly_frames.append(df_year)

        if not yearly_frames:
            print("\n[WARNING] No patents found in the specified year range.")
            return pd.DataFrame()

        # --- Final cross-year deduplication --------------------------------
        # The per-year pass already skips known families, but the same family
        # can still appear twice: the EPO occasionally reports a slightly
        # different family identifier for the same invention between calls.
        # Sorting by country priority and keeping the first row per family
        # guarantees the best member wins.
        final_df = pd.concat(yearly_frames, ignore_index=True)
        final_df["Priority"] = final_df["Country"].map(COUNTRY_PRIORITY).fillna(
            UNKNOWN_COUNTRY_SCORE
        )
        final_df = final_df.sort_values("Priority")
        final_df = final_df.drop_duplicates(subset="Family_ID", keep="first")
        final_df = final_df.drop(columns=["Priority"])

        out_path = os.path.join(
            output_dir, f"EPO_siRNA_IDs_{start_year}_{end_year}_{tag}.csv"
        )
        final_df.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")

        elapsed = time.time() - start_time
        print(f"\n[SUCCESS] {len(final_df)} unique patent families saved to: {out_path}")
        print(f"[TIME] Extraction completed in {int(elapsed // 60)}m {int(elapsed % 60)}s")

        return final_df

    except Exception as exc:
        # A long extraction should never end in a bare traceback: the per-year
        # autosave files are still on disk and can be concatenated by hand.
        print(f"\n[CRITICAL ERROR] {exc}")
        return pd.DataFrame()
