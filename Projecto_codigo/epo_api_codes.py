"""
EPO Patent ID Extraction Tool — Phase 1 Only
=============================================================================
Collects patent IDs and family IDs from the EPO OPS API for siRNA technology.
This script intentionally makes NO bibliographic (/biblio) calls — it only
collects the minimal identifiers needed to deduplicate by patent family and
prepare a clean ID list for Phase 2 (epo_fetch.py).

Output columns:
    Patent_ID  — docdb identifier, e.g. "US.7056704.B2"
    Family_ID  — EPO patent family identifier (groups equivalent patents)
    Country    — two-letter country/office code, e.g. "US", "EP", "WO"

Search strategy:
    - Independent Query Architecture: each CPC/IPC code and text term is
      executed as a separate query and the results are merged. This avoids
      the 2000-result cap per query by narrowing each individual query.
    - Deep Slicing: queries that exceed 2000 results are automatically split
      by month, and further by day if needed, until each slice fits within
      the 2000-result API hard limit.
    - Family deduplication: across all queries and all years, only one
      representative per patent family is kept, preferring English-publishing
      jurisdictions (WO > US > EP > GB > AU > CA > NZ > IE).
    - Exclusions: Aptamers (C12N15/115) and Immunomodulatory (C12N15/117)
      are excluded from every query.
"""

import time
import base64
import requests
import urllib.parse
import pandas as pd
import calendar
import re

# =============================================================================
# GLOBAL TOKEN CACHE
# EPO access tokens expire after 900 seconds. The cache stores the current
# token and its timestamp so it can be refreshed automatically before expiry.
# =============================================================================
TOKEN_CACHE = {'token': None, 'timestamp': 0}

# =============================================================================
# COUNTRY PRIORITY TABLE
# Used to select the best representative from a patent family.
# Lower score = higher priority. English-publishing jurisdictions come first
# so that titles and abstracts fetched in Phase 2 are more
#  likely to be available in English
# =============================================================================
COUNTRY_PRIORITY = {
    'WO': 1, 'US': 2, 'EP': 3, 'GB': 4,
    'AU': 5, 'CA': 6, 'NZ': 7, 'IE': 8,
}


# =============================================================================
# 1. SERVER AND QUOTA CHECK
# =============================================================================
def check_epo_quota(consumer_key: str, consumer_secret: str):
    """
    Authenticates with the EPO OPS API and prints the current weekly data
    consumption against the free 4 GB weekly quota.

    Returns:
        tuple: (True, weekly_pct) where weekly_pct is the percentage of the
               weekly quota already consumed. Always returns True so the caller
               can proceed regardless of quota status.
    """
    print("=" * 45)
    print("      EPO OPS API - STATUS DASHBOARD")
    print("=" * 45)

    auth_string  = f"{consumer_key}:{consumer_secret}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    weekly_pct   = 0.0

    try:
        # Authenticate and get a short-lived access token
        token_res = requests.post(
            "https://ops.epo.org/3.2/auth/accesstoken",
            headers={
                'Authorization': f'Basic {encoded_auth}',
                'Content-Type':  'application/x-www-form-urlencoded',
            },
            data={'grant_type': 'client_credentials'},
            timeout=30
        )
        token_res.raise_for_status()
        token = token_res.json()['access_token']

        # Fire a minimal test query (1 result only) to read the quota headers
        encoded_test_query = urllib.parse.quote("cpc=C12N15/113")
        response = requests.get(
            f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_test_query}",
            headers={'Authorization': f'Bearer {token}', 'X-OPS-Range': '1-1'},
            timeout=30
        )
        response.raise_for_status()

        h       = response.headers
        traffic = h.get('X-Throttling-Control', '')

        # Parse current request rate from the throttling header
        search_match = re.search(r'search=\w+:(\d+)', traffic)
        search_rpm   = int(search_match.group(1)) if search_match else 0

        # Read quota consumption — EPO may use either header name
        weekly_used_bytes = (
            h.get('X-RegisteredQuotaPerWeek-Used') or
            h.get('X-IndividualQuotaPerHour-Used') or
            '0'
        )

        try:
            bytes_used      = int(weekly_used_bytes)
            weekly_used_mb  = bytes_used / (1024 * 1024)
            weekly_pct      = (weekly_used_mb / 4000) * 100
            remaining_pct   = 100 - weekly_pct
        except ValueError:
            weekly_used_mb = 0.0
            remaining_pct  = 100.0
            weekly_pct     = 0.0

        print(f"\n[DATA CONSUMPTION]")
        print(f"Weekly Volume Used: {weekly_pct:.4f}% ({weekly_used_mb:.2f} MB / 4000 MB)")
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

    except Exception as e:
        print(f"\n[QUOTA CHECK ERROR] Could not read quota, proceeding anyway: {e}")

    return True, weekly_pct


# =============================================================================
# 2. HELPER FUNCTIONS
# =============================================================================
def _get_valid_token(consumer_key: str, consumer_secret: str) -> str:
    """
    Returns a valid EPO OPS access token, refreshing it automatically if it
    is missing or has been alive for more than 850 seconds (tokens expire at
    900 seconds, so this gives a 50-second safety margin).
    """
    current_time = time.time()
    if TOKEN_CACHE['token'] is None or (current_time - TOKEN_CACHE['timestamp']) > 850:
        print("\n[AUTH] Generating a new EPO access token...")
        auth_string  = f"{consumer_key}:{consumer_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()

        response = requests.post(
            "https://ops.epo.org/3.2/auth/accesstoken",
            headers={
                'Authorization': f'Basic {encoded_auth}',
                'Content-Type':  'application/x-www-form-urlencoded',
            },
            data={'grant_type': 'client_credentials'},
            timeout=30
        )
        response.raise_for_status()

        TOKEN_CACHE['token']     = response.json()['access_token']
        TOKEN_CACHE['timestamp'] = time.time()

    return TOKEN_CACHE['token']


def _clean_val(node) -> str:
    """
    Extracts a plain string from an EPO JSON value node.
    EPO JSON represents text values as {'$': 'actual text'} dicts.
    If the node is already a string (or other type), it is cast directly.
    """
    if isinstance(node, dict):
        return node.get('$', '')
    return str(node) if node else ""


def get_total_results_count(cql_query: str, consumer_key: str, consumer_secret: str) -> int:
    """
    Returns the total number of patents matching a CQL query without
    downloading any records.

    Strategy: sends X-OPS-Range: 1-1 (one result only) so the server
    returns the @total-result-count metadata field at minimal bandwidth
    cost. This count is used by search_with_slicing to decide whether
    the query needs to be broken into monthly or daily windows before
    any actual records are fetched.

    Retry policy: up to 4 attempts with escalating waits on transient
    server errors (404, 429, 503). Permanent errors (400, 403) either
    return 0 immediately or wait for a cooldown before retrying.

    Args:
        cql_query:       Full CQL query string, including date filter.
        consumer_key:    EPO OPS API consumer key.
        consumer_secret: EPO OPS API consumer secret.

    Returns:
        int: Total result count, or 0 if the query returns no results
             or fails after all retries.
    """
    
    encoded_query = urllib.parse.quote(cql_query)
    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"

    for attempt in range(4):
        try:
            token    = _get_valid_token(consumer_key, consumer_secret)
            response = requests.get(
                url,
                headers={
                    'Authorization': f'Bearer {token}',
                    'Accept':        'application/json',
                    'X-OPS-Range':   '1-1',
                },
                timeout=30
            )

            if response.status_code == 200:
                data  = response.json().get('ops:world-patent-data', {}).get('ops:biblio-search', {})
                total = int(data.get('@total-result-count', 0))
                time.sleep(3)   # Respect the ~10 req/min rate limit
                return total

            elif response.status_code == 400:
                # Permanent error the CQL querie rejected by the server by an syntax error; no point retrying
                print(f"[SYNTAX ERROR 400] EPO rejected the query syntax. Skipping.")
                return 0

            elif response.status_code == 404:
                # Transient — load balancer hiccup or empty date window; retry up to 4 times
                if attempt < 3:
                    print(f"[WARNING 404] Slice not found (attempt {attempt+1}/4). Retrying...")
                    time.sleep(20)
                    continue
                # Still 404 after 4 attempts, we will treat it genuinely empty
                return 0

            elif response.status_code == 403:
                # Rate limit or access ban — wait 10 minutes before retrying
                print(f"[SECURITY 403] Forbidden. Cooling down for 600s...")
                time.sleep(600)

            elif response.status_code in [429, 503]:
                # Server overloaded — back off progressively (300s, 600s, 900s, 1200s) 
                wait_time = 300 * (attempt + 1)
                print(f"[OVERLOAD {response.status_code}] API busy. Waiting {wait_time}s...")
                time.sleep(wait_time)

            else:
                time.sleep(15)

        except Exception as e:
            if attempt == 3:
                print(f"[ERROR] Connection failed after 4 attempts: {e}")
            time.sleep(20)

    # All 4 attempts exhausted without a valid response, we will skip this slice
    print("[CRITICAL] EPO server consistently rejecting count requests. Skipping this slice.")
    return 0

# =============================================================================
# 3. FAMILY DEDUPLICATION
# =============================================================================
def _deduplicate_by_family(id_records: list, seen_families: dict = None) -> tuple:
    """
    Selects the single best patent representative for each patent family from
    a list of raw records collected across all independent queries.

    Selection rule: COUNTRY_PRIORITY (US=1 > EP=2 > WO=3 > ...). This keeps
    English-language documents as the preferred family member, reducing the
    need for translation fallbacks in Phase 2.

    Cross-year awareness: if seen_families is provided (a dict mapping
    family_id to the best country score already fetched in a previous year),
    families are skipped unless the current year has a strictly better
    representative — e.g. a US continuation filed after a CN priority patent.

    Args:
        id_records:     List of dicts {'id', 'family_id', 'country'} from Phase 1.
        seen_families:  Dict {family_id -> best_country_score} built across years.
                        Pass None or {} for the first year.

    Returns:
        tuple:
            - list[str]: Patent ID strings ready for Phase 2 (/biblio calls).
            - dict:      {family_id -> best_country_score} for updating the
                         global tracker after this year's processing.
    """
    if seen_families is None:
        seen_families = {}

    family_map = {}   # family_id -> best record seen so far this call
    no_family  = {}   # patent_id -> True for records with no family_id

    for rec in id_records:
        fam_id  = rec.get('family_id', '').strip()
        pat_id  = rec.get('id', '')
        country = rec.get('country', '')

        if not fam_id:
            # No family ID — skip if already seen in a prior year
            if pat_id and pat_id not in seen_families:
                no_family[pat_id] = True
            continue

        new_score = COUNTRY_PRIORITY.get(country, 99)

        # Skip families already processed in a prior year unless this record
        # comes from a higher-priority jurisdiction than what we already have
        if fam_id in seen_families:
            if new_score >= seen_families[fam_id]:
                continue
            # Fall through — better representative found this year

        # Within this call, keep the highest-priority country per family
        if fam_id not in family_map:
            family_map[fam_id] = rec
        else:
            existing_score = COUNTRY_PRIORITY.get(family_map[fam_id]['country'], 99)
            if new_score < existing_score:
                family_map[fam_id] = rec

    result_ids     = [rec['id'] for rec in family_map.values()] + list(no_family.keys())
    processed_fams = {fid: COUNTRY_PRIORITY.get(rec['country'], 99)
                      for fid, rec in family_map.items()}
            
    # Register no-family patents into the global tracker with a sentinel score
    for pat_id in no_family.keys():
        processed_fams[pat_id] = 99

    return result_ids, processed_fams

def download_patent_ids(
    consumer_key:    str,
    consumer_secret: str,
    start_year:      int,
    end_year:        int,
    applicant_filter: str = None,
    only_applicant: bool = False
) -> pd.DataFrame:
    """
    Phase 1 extraction: collects patent IDs and family IDs from the EPO OPS
    API without making any bibliographic (/biblio) calls.
    """
    start_time = time.time()

    # -------------------------------------------------------------------------
    # Build the independent query list.
    # -------------------------------------------------------------------------

    if only_applicant:
        if not applicant_filter:
            print("\n[ERROR] 'applicant_filter' must be provided if 'only_applicant' is True.")
            # Cannot perform a search for a company without a name.
            return pd.DataFrame()
            
        # Adds a placeholder to ensure the extraction loop runs once when searching by applicant.
        independent_queries = [""]
    else:
        
        # Excludes CPC codes unrelated to siRNA (Aptamers and Immunomodulatory) from each query
        exclusions = ' NOT (cpc="C12N15/115" OR cpc="C12N15/117")'
        
        independent_queries = []

        # CPC codes
        for code in ["C12N15/11", "A61K31/7088"]:
            independent_queries.append(f'(cpc=/low "{code}"{exclusions})')

        # IPC codes
        for code in ["C12N15/113", "A61K31/713", "C12N15/11", "A61K31/7088"]:
            independent_queries.append(f'(ipc="{code}"{exclusions})')

    print(f"\n=== STARTING EPO ID EXTRACTION ({start_year} - {end_year}) ===")
    print(f"[INFO] {len(independent_queries)} independent query conditions.")
    if not only_applicant:
        print(f"[INFO] Exclusions active: C12N15/115 (Aptamers), C12N15/117 (Immunomodulatory)")
    if applicant_filter:
        print(f"[INFO] Applicant filter: {applicant_filter} (Only Applicant: {only_applicant})")

    # =========================================================================
    # PHASE 1A — PAGINATED ID COLLECTION
    # Fetches patent IDs page by page (90 per page, up to 2000 per query).
    # Returns a list of dicts: [{'id', 'family_id', 'country'}, ...]
    # =========================================================================
    def search_patent_ids(cql_query: str, total_patents: int) -> list:
    """
    Downloads all patent IDs matching cql_query by paginating through the
    EPO OPS /search endpoint in blocks of 100 results.

    The EPO API enforces two hard limits:
      - 2,000 results maximum per query (enforced by search_with_slicing upstream)
      - 100 results maximum per page

    We use 100 per page, the maximum the API allows, to minimise the number
    of requests needed and reduce total extraction time.

    Args:
        cql_query:     Full CQL query string, already including the date filter.
        total_patents: Result count from get_total_results_count(), used to
                       calculate page boundaries and know when to stop paginating.

    Returns:
        list[dict]: One dict per patent with keys 'id', 'family_id', 'country'.
                    Cross-query duplicates are possible and resolved upstream
                    by _deduplicate_by_family.
    """
    # Defensive cap: even if the caller passes a count above 2,000,
    # the API will never return more than 2,000 results per query
        total_patents = min(total_patents, 2000)
        
        encoded_query     = urllib.parse.quote(cql_query)
        extracted_records = []
        seen_ids          = set()   # Guards against duplicate IDs within this single query's pages
        start_index       = 1       # EPO pagination is 1-based, not 0-based

        # Paginate through all results in blocks of 100 until every record is collected
        while start_index <= total_patents:
            # Calculate the end of this page without overshooting the total
            end_index        = min(start_index + 99, total_patents)
            success_in_block = False

            for attempt in range(4):
                token   = _get_valid_token(consumer_key, consumer_secret)
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Accept':        'application/json',
                    'X-OPS-Range':   f'{start_index}-{end_index}',
                }

                try:
                    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"
                    res = requests.get(url, headers=headers, timeout=30)

                    if res.status_code == 200:
                        # Unwrap the nested EPO JSON response structure
                        json_resp  = res.json()
                        wpd        = json_resp.get('ops:world-patent-data', {})
                        biblio     = wpd.get('ops:biblio-search', {})
                        search_res = biblio.get('ops:search-result', {})
                        test_docs  = search_res.get('ops:publication-reference', [])

                        # Log requested vs received range — a mismatch signals the API
                        # returned fewer records than expected, which may indicate
                        # truncation or a server-side pagination issue
                        ops_range = biblio.get('ops:range', {})
                        begin_pos = int(ops_range.get('@begin', 0))
                        end_pos   = int(ops_range.get('@end', 0))
                        print(f"[INFO] Requested: {start_index}-{end_index} | "
                              f"Received: {begin_pos}-{end_pos} | "
                              f"Total expected: {total_patents}")

                        if not test_docs and attempt < 3:
                            # HTTP 200 but empty page, maybe an server issue, retry
                            time.sleep(20)
                            continue

                        if attempt == 3 and not test_docs:
                            # Still empty after 4 attempts, we give up on this page but warn
                            print("[WARNING] HTTP 200 but no documents found after 4 attempts.")
                            break

                        success_in_block = True
                        break

                    elif res.status_code == 400:
                        # Permanent error — malformed CQL rejected by the server.
                        # Return whatever records were collected before this failure
                        print(f"[SYNTAX ERROR 400] EPO rejected the query. Skipping block.")
                        return extracted_records

                    elif res.status_code == 404:
                        if start_index == 1:
                            # 404 on the very first page means probably no results exist for this query, but retrying to be sure
                            if attempt < 3:
                                time.sleep(20)
                                continue
                            else:
                                print(f"[WARNING 404] No results found after 4 attempts.")
                                return extracted_records
                        else:
                            # Transient 404 mid-pagination — load balancer hiccup, retry
                            print(f"[WARNING 404] Page {start_index} not found "
                                  f"(attempt {attempt+1}/4). Retrying...")
                            time.sleep(20)
                            continue

                    elif res.status_code == 403:
                        # Rate limit or IP ban, we will wait 15 minutes then let the retry loop continue
                        print(f"[WARNING 403] Access denied. Waiting 15 min "
                              f"(attempt {attempt+1}/4)...")
                        time.sleep(900)

                    elif res.status_code in [429, 503]:
                        # Server overloaded — back off progressively (300s, 600s, 900s, 1200s)
                        time.sleep(300 * (attempt + 1))

                    else:
                        # Unexpected status code — brief pause before retrying
                        time.sleep(15)

                except Exception:
                    # Network-level failure (timeout, connection reset, etc.), we will pause and retry
                    time.sleep(20)

            if not success_in_block:
                # All 4 attempts exhausted for this page — abort pagination for this query
                # and return however many records were successfully collected before the failure
                break

            # ── Parse the successfully retrieved page of documents ──────────────────
    
            # EPO returns a single dict instead of a list when there is only one result
            # on the page — normalise to a list so the loop below always works uniformly
            if isinstance(test_docs, dict):
                test_docs = [test_docs]

            for doc in test_docs:
                # family_id is stored as an attribute on the ops:publication-reference
                # node itself, not as a nested child element
                family_id = doc.get('@family-id', '').strip()

                # Each document can carry multiple document-id nodes in different formats
                # (docdb, epodoc, original). We only want the 'docdb' format, which
                # provides the structured country / number / kind breakdown we need.
                dids = doc.get('document-id', doc.get('ops:document-id', []))
                if isinstance(dids, dict):
                    # Same single-item normalisation as test_docs above
                    dids = [dids]

                # Extract only the docdb-format identifier — skip epodoc and original
                docdb_node = next(
                    (d for d in dids if d.get('@document-id-type') == 'docdb'), None
                )
                
                if docdb_node:
                    country = _clean_val(docdb_node.get('country', ''))
                    
                    # Strip spaces and dots that occasionally appear in raw doc-numbers
                    number  = _clean_val(docdb_node.get('doc-number', '')).replace(" ", "").replace(".", "")

                    # Kind code (e.g. A1, B2) is optional — omit it if absent rather
                    # than appending an empty string to the patent ID
                    kind    = _clean_val(docdb_node.get('kind', ''))

                    if country and number:
                        # Assemble the docdb patent ID in "CC.NNNNNN.KK" style
                        pat_id = f"{country}{number}{kind}" if kind else f"{country}{number}"
                    
                        # Deduplicate within this query's pages — cross-query duplicates
                        # are resolved later by _deduplicate_by_family
                        if pat_id not in seen_ids:
                            seen_ids.add(pat_id)
                            extracted_records.append({
                                'id':        pat_id,
                                'family_id': family_id,
                                'country':   country,
                            })
                            
            # Advance to the next page
            start_index += 90
            time.sleep(8)   # Conservative pause to stay within the ~10 req/min rate limit, tried 4 seconds got banned

        return extracted_records

    # =========================================================================
    # PHASE 1B — DATE SLICING
    # Wraps search_patent_ids with automatic year → month → day slicing to
    # handle queries that exceed the 2000-result API hard limit.
    # =========================================================================
    def search_with_slicing(base_cql: str, year: int) -> list:
        
        # Assembles the final CQL query string by combining the base code query,
        # an optional applicant filter, and the date window for the current slice.
        # Three modes are supported:
        #   1. only_applicant  — ignores base_cql entirely; searches by applicant name only
        #   2. applicant_filter (hybrid) — ANDs the code query with the applicant name
        #   3. codes only      — base_cql + date window, no applicant constraint
        def _build_cql(date_condition: str) -> str:
            if only_applicant:
                return f'pa="{applicant_filter}" AND {date_condition}'
            elif applicant_filter:
                return f'{base_cql} AND pa="{applicant_filter}" AND {date_condition}'
            else:
                return f'{base_cql} AND {date_condition}'

        # Probe the total result count for the full year before fetching.
        # This is a lightweight count-only call (no records downloaded) that
        # determines whether a direct fetch is possible or slicing is required.
        year_cql = _build_cql(f'pd={year}')
        total = get_total_results_count(year_cql, consumer_key, consumer_secret)

        # Nothing published this year for this query — skip entirely
        if total == 0:
            return []
            
        # Total patents in the year fits within the 2,000-result API hard limit, fetching the full year in one pass
        if total <= 2000:
            print(f"[INFO] Year {year}: {total} results. Fetching yearly...")
            return search_patent_ids(year_cql, total)

        # Year total exceeds the 2,000-result API hard limit — switch to monthly slicing.
        # Each month is probed independently: if a month also exceeds 2,000, it will
        # be sliced further by day in the next stage.
        ids = []
        print(f"[INFO] Year {year}: {total} results (>2000). Slicing by month...")
        for month in range(1, 13):
            # Resolve the exact last day of the month to handle variable month lengths
            # and leap years correctly (e.g. February = 28 or 29 days)
            last_day = calendar.monthrange(year, month)[1]
            
            # Build the date range strings in the YYYYMMDD format required by the EPO CQL syntax
            m_start  = f"{year}{month:02d}01"
            m_end    = f"{year}{month:02d}{last_day}"

            # Probe this month's result count before attempting any record download
            month_cql = _build_cql(f'pd within "{m_start} {m_end}"')
            month_total = get_total_results_count(month_cql, consumer_key, consumer_secret)

            if month_total == 0:
                # No patents published this month for this query — skip to the next month
                continue
                
            elif month_total <= 2000:
                # Month fits within the API limit, we will fetch all records in one pass
                print(f"[INFO] Year {year}, month {month}: fetching {month_total} results...")
                ids.extend(search_patent_ids(month_cql, month_total))
            
            else:
                # Month still exceeds 2,000, we will apply the final slicing level: day by day.
                # If a single day exceeds 2,000, the API will hard-truncate at 2,000
                # and there is no deeper slicing level to fall back to.
                print(f"[INFO] Year {year}, month {month}: {month_total} results (>2000). Slicing by day...")
                for day in range(1, last_day + 1):
                    # Build a single-day date range: pd within "YYYYMMDD YYYYMMDD"
                    day_str = f"{year}{month:02d}{day:02d}"

                    # Probe this day's count before fetching
                    day_cql = _build_cql(f'pd within "{day_str} {day_str}"')
                    day_total = get_total_results_count(day_cql, consumer_key, consumer_secret)

                    if day_total > 2000:
                        # Hard ceiling reached, the results will be silently truncated by the API.
                        # This is logged as a warning but execution continues, accepting the loss.
                        print(f"[WARNING] Year {year}, month {month}, day {day}: "
                              f"{day_total} results exceed 2000. API cap truncating results.")

                    if day_total > 0:
                        # Day has results within (or truncated by) the API limit are fetched
                        print(f"[INFO] Year {year}, month {month}, day {day}: fetching {day_total} results...")
                        ids.extend(search_patent_ids(day_cql, day_total))

        return ids

    # =========================================================================
    # MAIN LOOP — iterate over years, collect IDs, deduplicate, save
    # =========================================================================
    try:
        # Accumulates one DataFrame per year before final concatenation and deduplication
        all_yearly_dfs = []

        # Tracks patent families across the entire date range to prevent the same family
        # from appearing in multiple years' output. Updated incrementally after each year.
        # Stores {family_id: best_country_priority_score}.
        seen_families_global = {}

        for current_year in range(start_year, end_year + 1):
            print(f"\n[INFO] Processing year {current_year}...")

            # Each independent query is fetched independently and merged here.
            # search_with_slicing handles the 2,000-result API cap by cascading
            # from yearly → monthly → daily windows until every slice fits.
            all_records_year = []
            for idx, base_query in enumerate(independent_queries):
                print(f"  -> Query {idx+1}/{len(independent_queries)}: {base_query[:80]}...")
                all_records_year.extend(search_with_slicing(base_query, current_year))

            print(f"[INFO] Raw records accumulated (including cross-query duplicates): "
                  f"{len(all_records_year)}")

            # Keep only the best representative per patent family, skipping families
            # already output in prior years (unless a higher-priority jurisdiction is found)
            total_ids_year, new_families = _deduplicate_by_family(
                all_records_year,
                seen_families=seen_families_global
            )

            # Merge this year's families into the global tracker for subsequent years
            seen_families_global.update(new_families)

            print(f"[INFO] After family deduplication: {len(total_ids_year)} unique IDs")
            print(f"[INFO] Records filtered (cross-query duplicates + prior-year skips): "
                  f"{len(all_records_year) - len(total_ids_year)}")
            print(f"[INFO] Families tracked globally so far: {len(seen_families_global)}")

            if not total_ids_year:
                continue

            winning_set = set(total_ids_year)
            year_records_dict = {}

            for r in all_records_year:
                # If the ID won a spot AND we haven't added it to our final records yet
                if r['id'] in winning_set and r['id'] not in year_records_dict:
                    year_records_dict[r['id']] = r

            year_records = list(year_records_dict.values())

            df_year = pd.DataFrame(year_records).rename(columns={
                'id':        'Patent_ID',
                'family_id': 'Family_ID',
                'country':   'Country',
            })

            # Assign placeholder family IDs to patents with no family ID
            # so the CSV never has empty cells in that column
            mask_no_family = df_year['Family_ID'].eq('')
            if mask_no_family.sum() > 0:
                print(f"  [WARNING] {mask_no_family.sum()} patents have no family ID — "
                      f"assigning placeholders.")
                df_year.loc[mask_no_family, 'Family_ID'] = [
                    f"UNKNOWN_FAM_{i}_{current_year}"
                    for i in range(mask_no_family.sum())
                ]

            # Yearly autosave — protects against data loss on long runs
            autosave_name = f"EPO_IDs_AutoSave_{current_year}.csv"
            df_year.to_csv(autosave_name, index=False, sep=';', encoding='utf-8-sig')
            print(f"  [AUTOSAVE] Year {current_year}: {len(df_year)} IDs saved to {autosave_name}")

            all_yearly_dfs.append(df_year)

        # ------------------------------------------------------------------
        # Concatenate all years and apply a final cross-year deduplication.
        # This catches the edge case where the same family_id appears in
        # multiple years with a different best representative (e.g. because
        # Phase 1 family_ids occasionally differ between API calls).
        # ------------------------------------------------------------------
        if not all_yearly_dfs:
            print("\n[WARNING] No patents found in the specified year range.")
            return pd.DataFrame()

        final_df = pd.concat(all_yearly_dfs, ignore_index=True)

        # Create a priority column based on the COUNTRY_PRIORITY dictionary
        final_df['Priority'] = final_df['Country'].map(COUNTRY_PRIORITY).fillna(99)

        # Sort so the highest priority (lowest number) is at the top
        final_df = final_df.sort_values('Priority')

        # Now keep='first' will safely keep the highest priority representative
        final_df = final_df.drop_duplicates(subset='Family_ID', keep='first')

        # Clean up the temporary column
        final_df = final_df.drop(columns=['Priority'])
        
        # Check if applicant_filter is not None before regex substitution
        applicant = re.sub(r"\*", "", applicant_filter) if applicant_filter else ""

        # Save the final CSV
        if only_applicant:
            out_filename = f'EPO_siRNA_IDs_{start_year}_{end_year}_only_applicant_{applicant}.csv'
        elif applicant_filter:
            # Handles the hybrid case where both codes and applicant are used
            out_filename = f'EPO_siRNA_IDs_{start_year}_{end_year}_codes_and_{applicant}.csv'
        else:
            out_filename = f'EPO_siRNA_IDs_{start_year}_{end_year}_codes_only.csv'
        final_df.to_csv(out_filename, index=False, sep=';', encoding='utf-8-sig')

        elapsed = time.time() - start_time
        print(f"\n[SUCCESS] {len(final_df)} unique patent families saved to: {out_filename}")
        print(f"[TIME] Extraction completed in {int(elapsed // 60)}m {int(elapsed % 60)}s")

        return final_df

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        return pd.DataFrame()
