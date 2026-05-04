"""
EPO Patent Metadata Fetch Tool — Phase 2
=============================================================================
Reads a CSV of patent IDs produced by epo_api_ID.py (Phase 1) and fetches
full bibliographic metadata from the EPO OPS /biblio endpoint.

This script makes no search queries. It only calls /biblio with explicit
patent IDs, so the 2000-result search cap does not apply. The total number
of patents that can be fetched is limited only by the weekly 4 GB quota.

Input:
    A CSV file with at minimum a 'Patent_ID' column, as produced by
    epo_api_ID.py. Separator must be ';'.

Output columns:
    Patent_ID        - docdb identifier, e.g. "US7056704B2"
    Country          - two-letter country/office code
    Number           - patent number without punctuation
    Kind             - kind code, e.g. "B2", "A1"
    Family_ID        - EPO simple family identifier
    Priority_Date    - earliest priority date (YYYYMMDD)
    Publication_Date - publication date (YYYYMMDD)
    Applicant        - pipe-separated list of applicant names
    Title            - invention title (English preferred)
    Abstract         - abstract text (English preferred)
    IPCs             - comma-separated IPC classification codes
    CPCs             - comma-separated CPC classification codes

API behaviour:
    - /biblio accepts up to 100 patent IDs per request (comma-separated in URL).
    - If a batch of 100 fails, each ID in that batch is retried individually.
    - If an abstract is missing from /biblio, a fallback call to /abstract is made.
    - Rate limit: ~10 requests/minute. Conservative sleeps are included.
"""

import time
import base64
import requests
import pandas as pd
import numpy as np
import re

# =============================================================================
# GLOBAL TOKEN CACHE
# EPO access tokens expire after 900 seconds. The cache stores the current
# token and its timestamp so it can be refreshed automatically before expiry.
# =============================================================================
TOKEN_CACHE = {'token': None, 'timestamp': 0}

# =============================================================================
# 1. AUTHENTICATION
# =============================================================================
def _get_valid_token(consumer_key: str, consumer_secret: str) -> str:
    """
    Returns a valid EPO OPS access token, refreshing it automatically if it
    is missing or has been alive for more than 850 seconds (tokens expire at
    900 seconds, giving a 50-second safety margin).
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
            data={'grant_type': 'client_credentials'}
        )
        response.raise_for_status()

        TOKEN_CACHE['token']     = response.json()['access_token']
        TOKEN_CACHE['timestamp'] = time.time()

    return TOKEN_CACHE['token']


# =============================================================================
# 2. HELPERS FUNCTION
# =============================================================================
def _clean_val(node) -> str:
    """
    Extracts a plain string from an EPO JSON value node.
    EPO JSON represents text values as {'$': 'actual text'} dicts.
    If the node is already a non-dict type, it is cast to string directly.
    """
    if isinstance(node, dict):
        return node.get('$', '')
    return str(node) if node else ""


def clean_text_for_csv(text: str) -> str:
    """
    Strips HTML tags, normalises whitespace, and removes citation markers
    (e.g. [0001]) from abstract and title text before writing to CSV.
    """
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', '', text)
    clean = clean.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    clean = re.sub(r'\[\d{4}\]', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def _normalise_docdb_id(raw_id: str) -> str:
    """
    Converts a concatenated docdb ID like 'CA3224904A1'
    into the dotted format 'CA.3224904.A1' expected by the /biblio endpoint.
    """
    match = re.match(r'^([A-Z]{2})(\d+)([A-Z]\d*)$', raw_id.strip())
    if match:
        return f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
    return raw_id  # already dotted or unrecognised — leave as-is

# =============================================================================
# 3. ABSTRACT FALLBACK
# =============================================================================
def fetch_abstract_fallback(single_id: str, token: str) -> str:
    """
    Attempts to retrieve an abstract from the dedicated /abstract endpoint
    when /biblio returns no abstract for a patent.

    Prefers English. Falls back to the first available language if no English
    abstract is found. Returns a placeholder string if the endpoint fails.

    Args:
        single_id: Patent docdb ID string, e.g. "US.7056704.B2".
        token:     Valid EPO OPS bearer token.

    Returns:
        str: Cleaned abstract text, or "No abstract available in EPO database".
    """
    abs_url = (
        f"https://ops.epo.org/3.2/rest-services/published-data/"
        f"publication/docdb/{single_id}/abstract"
    )
    try:
        res = requests.get(
            abs_url,
            headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'},
            timeout=15
        )
        if res.status_code == 200:
            data     = res.json()
            wpd      = data.get('ops:world-patent-data', {})
            ex_docs  = wpd.get('exchange-documents', {})
            ex_doc   = ex_docs.get('exchange-document', {})
            if isinstance(ex_doc, list):
                ex_doc = ex_doc[0]

            abs_node = ex_doc.get('abstract', [])
            if isinstance(abs_node, dict):
                abs_node = [abs_node]

            # Prefer English abstract
            for a in abs_node:
                if isinstance(a, dict) and a.get('@lang', '').lower() == 'en':
                    p_node = a.get('p')
                    if isinstance(p_node, list):
                        return clean_text_for_csv(" ".join([_clean_val(p) for p in p_node]))
                    return clean_text_for_csv(_clean_val(p_node))

            # Fallback to first available language
            if abs_node:
                first  = abs_node[0]
                p_node = first.get('p') if isinstance(first, dict) else None
                if p_node:
                    text = (
                        " ".join([_clean_val(p) for p in p_node])
                        if isinstance(p_node, list)
                        else _clean_val(p_node)
                    )
                    return clean_text_for_csv(text)
                return clean_text_for_csv(_clean_val(first))

    except Exception as e:
        print(f"  [WARNING] Abstract fallback failed for {single_id}: {type(e).__name__}: {e}")
        pass

    return "No abstract available in EPO database"


# =============================================================================
# 4. JSON METADATA PARSER
# =============================================================================
def _parse_json_metadata(json_data: dict, results_list: list) -> None:
    """
    Parses a /biblio JSON response and appends one record per patent document
    to results_list.

    Handles both single-document responses (dict) and multi-document batch
    responses (list) transparently.

    Confirmed JSON structure from API:
        ops:world-patent-data
          exchange-documents
            exchange-document       <- one per patent in the batch
              @family-id
              abstract              <- list of {lang, p} nodes
              bibliographic-data
                invention-title
                publication-reference
                priority-claims
                parties
                patent-classifications

    Args:
        json_data:    Parsed JSON response from a /biblio request.
        results_list: List to append parsed records to (mutated in place).
    """

    data    = json_data.get('ops:world-patent-data', {})
    ex_docs = data.get('exchange-documents', {})
    docs    = ex_docs.get('exchange-document', [])

    # A single-patent response returns a dict instead of a list
    if isinstance(docs, dict):
        docs = [docs]

    for doc in docs:
        bib = doc.get('bibliographic-data', {})

        # Title: prefer English, fall back to first available
        t_node   = bib.get('invention-title', [])
        if isinstance(t_node, dict):
            t_node = [t_node]
        title_en = next(
            (_clean_val(t) for t in t_node
             if isinstance(t, dict) and t.get('@lang', '').lower() == 'en'),
            None
        )
        title = title_en if title_en else (_clean_val(t_node[0]) if t_node else "No title")

        # Abstract: sits on exchange-document, not on bibliographic-data.
        # Prefer English, fall back to first available language.
        abs_node      = doc.get('abstract', [])
        if isinstance(abs_node, dict):
            abs_node  = [abs_node]
        abstract_text = ""

        for a in abs_node:
            if isinstance(a, dict) and a.get('@lang', '').lower() == 'en':
                p_node = a.get('p')
                if p_node:
                    if isinstance(p_node, list):
                        abstract_text = " ".join([_clean_val(p) for p in p_node])
                    else:
                        abstract_text = _clean_val(p_node)
                else:
                    abstract_text = _clean_val(a)
                break

        if not abstract_text.strip() and abs_node:
            first_abs = abs_node[0]
            if isinstance(first_abs, dict):
                p_node = first_abs.get('p')
                if p_node:
                    if isinstance(p_node, list):
                        abstract_text = " ".join([_clean_val(p) for p in p_node])
                    else:
                        abstract_text = _clean_val(p_node)
                else:
                    abstract_text = _clean_val(first_abs)
            else:
                abstract_text = _clean_val(first_abs)

        if not abstract_text.strip():
            abstract_text = "No abstract available in EPO database"
        else:
            abstract_text = clean_text_for_csv(abstract_text)

        # Applicants: prefer English name form, fall back to first available
        parties         = bib.get('parties', {})
        applicants_wrap = parties.get('applicants', {})
        app_node        = applicants_wrap.get('applicant', [])
        if isinstance(app_node, dict):
            app_node = [app_node]
        applicants = []
        for app in app_node:
            app_name_wrap = app.get('applicant-name', {})
            names = app_name_wrap.get('name', [])
            if isinstance(names, (dict, str)):
                names = [names]
            name = next(
                (_clean_val(n) for n in names
                 if isinstance(n, dict) and n.get('@lang', '').lower() == 'en'),
                None
            )
            if not name and names:
                name = _clean_val(names[0])
            if name:
                applicants.append(name)

        # Publication reference: extract from the docdb-typed document-id node
        p_ref  = bib.get('publication-reference', {})
        d_ids  = p_ref.get('document-id', [])
        if isinstance(d_ids, dict):
            d_ids = [d_ids]
        pub_date, country, doc_number, kind = "", "", "", ""
        for d in d_ids:
            if d.get('@document-id-type') == 'docdb':
                pub_date   = _clean_val(d.get('date', ''))
                country    = _clean_val(d.get('country', ''))
                doc_number = _clean_val(d.get('doc-number', '')).replace(".", "")
                kind       = _clean_val(d.get('kind', ''))

        # Application reference: extract filing date to use as the true priority date fallback
        a_ref = bib.get('application-reference', {})
        a_d_ids = a_ref.get('document-id', [])
        if isinstance(a_d_ids, dict):
            a_d_ids = [a_d_ids]
        app_date = ""
        for d in a_d_ids:
            if d.get('@document-id-type') == 'docdb':
                app_date = _clean_val(d.get('date', ''))

        # Priority date: take the earliest date across all priority claims
        pri_claims     = bib.get('priority-claims', {})
        pri_claim_list = pri_claims.get('priority-claim', [])
        if isinstance(pri_claim_list, dict):
            pri_claim_list = [pri_claim_list]
        dates_found = []
        for claim in pri_claim_list:
            c_d_ids = claim.get('document-id', [])
            if isinstance(c_d_ids, dict):
                c_d_ids = [c_d_ids]
            for c_d in c_d_ids:
                if _clean_val(c_d.get('date')):
                    dates_found.append(_clean_val(c_d.get('date')))
                    
        # Use the earliest priority claim. If none exist, fallback to the filing (application) date.
        pri_date = min(dates_found) if dates_found else (app_date if app_date else pub_date)

        # Classifications: separate CPC and IPC codes.
        # Codes with unrecognised scheme are added to both sets as a safe fallback.
        cpcs_set, ipcs_set = set(), set()
        c_wrap = bib.get('patent-classifications', {})
        c_list = c_wrap.get('patent-classification', [])
        if isinstance(c_list, dict):
            c_list = [c_list]
        for c in c_list:
            sec  = _clean_val(c.get('section', ''))
            cls  = _clean_val(c.get('class', ''))
            subc = _clean_val(c.get('subclass', ''))
            mg   = _clean_val(c.get('main-group', ''))
            sg   = _clean_val(c.get('subgroup', ''))
            full_code = f"{sec}{cls}{subc}{mg}/{sg}".strip("/")
            if len(full_code) > 2:
                scheme_node = c.get('classification-scheme', {})
                scheme_type = (
                    _clean_val(scheme_node.get('@scheme', '')).upper()
                    if isinstance(scheme_node, dict) else ""
                )
                if 'CPC' in scheme_type:
                    cpcs_set.add(full_code)
                elif 'IPC' in scheme_type:
                    ipcs_set.add(full_code)
                else:
                    cpcs_set.add(full_code)
                    ipcs_set.add(full_code)

        results_list.append({
            'Patent_ID':        f"{country}{doc_number}{kind}",
            'Country':          country,
            'Number':           doc_number,
            'Kind':             kind,
            'Family_ID':        doc.get('@family-id', ''),
            'Priority_Date':    pri_date,
            'Publication_Date': pub_date,
            'Applicant':        " | ".join(applicants),
            'Title':            title,
            'Abstract':         abstract_text,
            'IPCs':             ", ".join(sorted(ipcs_set)),
            'CPCs':             ", ".join(sorted(cpcs_set)),
        })


# =============================================================================
# 5. MAIN FETCH FUNCTION
# =============================================================================
def fetch_biblio_from_csv(
    ids_csv:         str,
    consumer_key:    str,
    consumer_secret: str,
) -> pd.DataFrame:
    """
    Phase 2: reads a CSV of patent IDs and fetches full bibliographic metadata
    for each one via the EPO OPS /biblio endpoint.

    Batch strategy:
        - Requests are sent in batches of 100 IDs (the /biblio hard limit).
        - If a batch fails after 3 attempts, each ID is retried individually
          so one bad patent does not cause the whole batch to be lost.
        - If /biblio returns no abstract, a fallback call to /abstract is made.

    Deduplication:
        A final pass deduplicates by Family_ID, preferring records with good
        English text (non-empty abstract, Latin-script title) and then the
        oldest priority date within each family.

    Args:
        ids_csv:         Path to the Phase 1 output CSV (separator ';',
                         must contain a 'Patent_ID' column).
        consumer_key:    EPO OPS API key.
        consumer_secret: EPO OPS API secret.

    Returns:
        pd.DataFrame with full bibliographic metadata, or an empty DataFrame
        on failure. Also saves the result to a CSV file derived from the
        input filename (e.g. "EPO_IDs_2000_2024_metadata.csv").
    """
    start_time = time.time()

    # Load the patent ID list from the Phase 1 output CSV
    try:
        df_ids  = pd.read_csv(ids_csv, sep=';', dtype=str)
        id_list = df_ids['Patent_ID'].dropna().tolist()
        id_list = [_normalise_docdb_id(pid) for pid in id_list]
    except Exception as e:
        print(f"[CRITICAL ERROR] Could not read input CSV '{ids_csv}': {e}")
        return pd.DataFrame()

    print(f"\n=== STARTING EPO METADATA FETCH ===")
    print(f"[INFO] Input file : {ids_csv}")
    print(f"[INFO] Patent IDs : {len(id_list)}")
    print(f"[INFO] Batches : {(len(id_list) + 99) // 100} x 100 IDs per batch")

    results = []

    # Adaptive throttle state — tracks server stress signals across batches
    throttle_strikes  = 0    # Counts 429/503 responses across batches
    STRIKE_LIMIT      = 3    # Cooldown triggers after this many strikes
    BASE_SLEEP        = 8    # Normal inter-batch pause (seconds)
    current_sleep     = BASE_SLEEP
    MAX_SLEEP         = 30   # Ceiling for the escalating inter-batch pause
    COOLDOWN_DURATION = 600  # Full cooldown when strike limit is reached (seconds)

    for i in range(0, len(id_list), 100):
        batch      = id_list[i:i + 100]
        batch_num  = i // 100 + 1
        url        = (
            f"https://ops.epo.org/3.2/rest-services/published-data/"
            f"publication/docdb/{','.join(batch)}/biblio"
        )

        
        success    = False
        batch_strikes = 0

        # Batch attempt — up to 3 retries before falling back to individual
        for attempt in range(3):
            try:
                token = _get_valid_token(consumer_key, consumer_secret)
                request_start = time.time()
                res   = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'Accept':        'application/json',
                    },
                    timeout=30
                )

                response_time = time.time() - request_start

                if res.status_code == 200:

                    if response_time > 10:
                        batch_strikes    += 1
                        throttle_strikes += 1
                        print(f"  [SLOW] Batch {batch_num} took {response_time:.1f}s "
                          f"(throttle strike {throttle_strikes})")

                    current_len = len(results)

                    _parse_json_metadata(res.json(), results)

                    # Attempt /abstract fallback for any patent with no abstract
                    for idx in range(current_len, len(results)):
                        if results[idx]['Abstract'] == "No abstract available in EPO database":
                            docdb_id = (
                                f"{results[idx]['Country']}."
                                f"{results[idx]['Number']}."
                                f"{results[idx]['Kind']}"
                            ).strip(".")
                            results[idx]['Abstract'] = fetch_abstract_fallback(docdb_id, token)
                            time.sleep(5)

                    success = True
                    break

                elif res.status_code == 404:
                    print(f"  [WARNING 404] Batch {batch_num} not found — "
                          f"falling back to {len(batch)} individual requests.")
                    break
                elif res.status_code == 403:
                    print(f"  [WARNING 403] Access denied. Forcing full cooldown (attempt {attempt+1}/3)...")
                    throttle_strikes = STRIKE_LIMIT  # Force cooldown after this batch
                    time.sleep(600)
                elif res.status_code in [429, 503]:
                    batch_strikes    += 1
                    throttle_strikes += 1
                    wait_time = 60 * (attempt + 1)
                    print(f"  [THROTTLE {res.status_code}] Strike {throttle_strikes} — waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  [WARNING {res.status_code}] Unexpected response on batch {batch_num} "
                          f"(attempt {attempt+1}/3) — pausing 15s before retry.")
                    time.sleep(15)

            except Exception as e:
                print(f"  [BATCH ERROR] Attempt {attempt+1}/3 failed for batch "
                      f"starting {batch[0]}: {type(e).__name__}: {e}")
                time.sleep(20)

        # Individual fallback: isolates whichever patent caused the batch failure
        if not success:
            print(f"  [INFO] Batch {batch_num} failed — retrying {len(batch)} IDs individually...")

            pre_batch_len = len(results)

            for single_id in batch:
                try:
                    token = _get_valid_token(consumer_key, consumer_secret)
                    s_url = (
                        f"https://ops.epo.org/3.2/rest-services/published-data/"
                        f"publication/docdb/{single_id}/biblio"
                    )
                    s_res = requests.get(
                        s_url,
                        headers={
                            'Authorization': f'Bearer {token}',
                            'Accept':        'application/json',
                        },
                        timeout=20
                    )

                    if s_res and s_res.status_code == 200:
                        current_len = len(results)
                        _parse_json_metadata(s_res.json(), results)

                        if len(results) > current_len:
                            last_entry = results[-1]
                            if last_entry['Abstract'] == "No abstract available in EPO database":
                                docdb_id = (
                                    f"{last_entry['Country']}."
                                    f"{last_entry['Number']}."
                                    f"{last_entry['Kind']}"
                                ).strip(".")
                                last_entry['Abstract'] = fetch_abstract_fallback(docdb_id, token)
                                time.sleep(5)

                    time.sleep(5)

                except Exception as e:
                    # Log but continue — one failed ID must not stop the run
                    print(f"  [SILENT LOSS] Could not fetch {single_id}: {e}")

            lost = len(batch) - (len(results) - pre_batch_len)
            if lost > 0:
                print(f"  [WARNING] Batch {batch_num} — {lost}/{len(batch)} IDs "
                      f"could not be fetched and are permanently lost.")

        print(f"[INFO] Batch {batch_num} complete — {len(results)} records fetched so far.")

        # Adaptive inter-batch pause — escalates when server shows strain,
        # recovers gradually when batches succeed cleanly
        if batch_strikes > 0:
            current_sleep = min(current_sleep * 2, MAX_SLEEP)
            print(f"  [ADAPTIVE] Sleep escalated to {current_sleep}s "
                  f"({batch_strikes} stress signal(s) this batch).")
        else:
            current_sleep = max(BASE_SLEEP, current_sleep - 2)

        # Full cooldown when strike limit is reached — resets session state
        if throttle_strikes >= STRIKE_LIMIT:
            print(f"\n[COOLDOWN] {throttle_strikes} throttle strikes — "
                  f"pausing {COOLDOWN_DURATION // 60} min to reset EPO session...")
            time.sleep(COOLDOWN_DURATION)
            throttle_strikes = 0
            current_sleep    = BASE_SLEEP
            print(f"[COOLDOWN] Resuming from batch {batch_num + 1}...\n")
        else:
            time.sleep(current_sleep)

    # Build DataFrame
    if not results:
        print("\n[WARNING] No metadata fetched.")
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Assign placeholder Family_IDs to any patent missing both Family_ID and Patent_ID
    df['Family_ID'] = df['Family_ID'].replace('', np.nan)
    truly_empty     = df['Family_ID'].isna() & df['Patent_ID'].eq('')
    if truly_empty.sum() > 0:
        print(f"  [WARNING] {truly_empty.sum()} records with no Family_ID or Patent_ID "
              f"— assigning placeholders.")
        df.loc[truly_empty, 'Family_ID'] = [
            f"UNKNOWN_FAM_{j}" for j in range(truly_empty.sum())
        ]
    df['Family_ID'] = df['Family_ID'].fillna(df['Patent_ID'])

    # Final deduplication by Family_ID.
    # Catches cases where /biblio returns a family_id that differs from Phase 1
    # (e.g. EPO database inconsistencies).
    # Priority: good English text first, then oldest priority date.
    has_bad_abstract = df['Abstract'].fillna('').str.contains(
        'No abstract available', case=False
    )
    has_non_latin    = df['Title'].fillna('').str.contains(
        r'[^\x00-\x7F]', regex=True
    )
    df['Good_Text']  = ~(has_bad_abstract | has_non_latin)

    df = df.sort_values(['Good_Text', 'Priority_Date'], ascending=[False, True])
    df = df.drop_duplicates('Family_ID', keep='first')
    df = df.drop(columns=['Good_Text'])

    # Save output CSV with '_metadata' suffix derived from the input filename
    out_filename = ids_csv.replace('.csv', '_metadata.csv')
    df.to_csv(out_filename, index=False, sep=';', encoding='utf-8-sig')

    elapsed = time.time() - start_time
    print(f"\n[SUCCESS] {len(df)} patents saved to: {out_filename}")
    print(f"[TIME] Fetch completed in {int(elapsed // 60)}m {int(elapsed % 60)}s")

    return df
