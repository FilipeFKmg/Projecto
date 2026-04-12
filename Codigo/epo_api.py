"""
EPO Patent Extraction Tool for siRNA Technology
-------------------------------------------------------------------------
Automated tool for patent extraction from the European Patent Office (EPO) 
OPS API, specifically focused on siRNA technology.

Main Features:
- OAuth2 authentication with caching and automatic renewal.
- API limit handling (Throttling) with controlled delays.
- 'Time-Slicing' strategy: Splits searches by month to bypass the 2000 results limit.
- Error Recovery (Fallback): Processes IDs individually if a batch fails (404 Error).
- Post-processing Filtering: Removes agricultural/veterinary terms and flags competing technologies.
"""

import time
import base64
import requests
import urllib.parse
import pandas as pd
import numpy as np
import calendar

# --- GLOBAL CACHE ---
# Stores the token to avoid unnecessary authentication requests for each call.
TOKEN_CACHE = {'token': None, 'timestamp': 0}

def _get_valid_token(consumer_key: str, consumer_secret: str) -> str:
    """
    Manages authentication with the EPO OPS API.
    
    Checks if the cached token is still valid (15 minutes/900s validity).
    If expired or non-existent, requests a new one via OAuth2.
    
    Args:
        consumer_key (str): EPO account consumer key.
        consumer_secret (str): EPO account consumer secret.
        
    Returns:
        str: Valid access token for the Authorization header.
    """
    current_time = time.time()
    if TOKEN_CACHE['token'] is None or (current_time - TOKEN_CACHE['timestamp']) > 900:
        print("\n[SECURITY] Generating a new EPO Access Token...")
        url = "https://ops.epo.org/3.2/auth/accesstoken"
        auth_string = f"{consumer_key}:{consumer_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        response = requests.post(
            url,
            headers={'Authorization': f'Basic {encoded_auth}', 'Content-Type': 'application/x-www-form-urlencoded'},
            data={'grant_type': 'client_credentials'}
        )
        response.raise_for_status()
        
        TOKEN_CACHE['token'] = response.json()['access_token']
        TOKEN_CACHE['timestamp'] = time.time()
        
    return TOKEN_CACHE['token']

def _clean_val(node) -> str:
    """
    Safely extracts text values from EPO API JSON nodes.
    
    The EPO API often returns dictionaries where the value is in the '$' key.
    
    Args:
        node: The data node (can be dict, str, or None).
        
    Returns:
        str: The cleaned value or an empty string.
    """
    if isinstance(node, dict):
        return node.get('$', '')
    return str(node) if node else ""

def format_cql_term(term: str) -> str:
    """
    Formats a search term for CQL (Contextual Query Language) syntax.
    
    Handles single words, wildcards, and compound phrases, ensuring the 
    EPO search engine processes correctly without parsing errors.
    
    Args:
        term (str): Search term (e.g., 'siRNA*' or '"RNA interference"').
        
    Returns:
        str: Formatted string for the 'ta=' (Title or Abstract) field.
    """
    # Preserves terms explicitly enclosed in quotes
    if term.startswith('"') and term.endswith('"'):
        return f'ta={term}'
        
    # Single word or word with wildcard
    if " " not in term:
        return f'ta={term}'
    
    # Compound phrase: Joins words with the AND operator
    inner_query = " AND ".join(term.split())
    return f'ta=({inner_query})'

def get_total_results_count(cql_query: str, consumer_key: str, consumer_secret: str) -> int:
    """
    Queries the API only to obtain the total number of results for a query.
    
    Essential for deciding if the search needs to be sliced by time.
    
    Args:
        cql_query (str): The complete CQL query string.
        consumer_key/secret (str): Access credentials.
        
    Returns:
        int: Total patents found.
    """
    token = _get_valid_token(consumer_key, consumer_secret)
    encoded_query = urllib.parse.quote(cql_query)
    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"
    
    try:
        res = requests.get(url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'X-OPS-Range': '1-1'}, timeout=15)
        time.sleep(2) # Courtesy delay to avoid blocks
        
        if res.status_code == 200:
            data = res.json().get('ops:world-patent-data', {}).get('ops:biblio-search', {})
            return int(data.get('@total-result-count', 0))
    except Exception as e:
        print(f"      [WARNING] Could not verify total count: {e}")
    return 0

def extract_epo_patents(consumer_key: str, consumer_secret: str, start_year: int, end_year: int, applicant_filter: str = None) -> pd.DataFrame:
    """
    Main function for patent data extraction and processing.
    
    Executes the full flow: ID Search -> Pagination -> Metadata Retrieval -> 
    Data Cleaning -> CSV Export.
    
    Args:
        consumer_key (str): API Key.
        consumer_secret (str): API Secret.
        start_year (int): Search start year.
        end_year (int): Search end year.
        applicant_filter (str, optional): Name of a specific company/applicant.
        
    Returns:
        pd.DataFrame: DataFrame containing all processed patents.
    """
    start_time = time.time()
    
    # --- TERM CONFIGURATION ---
    search_terms = [
        "siRNA*", "RNAi*", "dsRNA*", "si-RNA*", "ds-RNA*", "iRNA*",
        '"RNA interference"', '"RNA-interference"', '"interfering RNA"',
        '"small interfering RNA"', '"small interfering RNAs"', '"short interfering RNA"',
        '"short interfering RNAs"', '"small-interfering RNA"', '"short-interfering RNA"',
        '"double-stranded RNA"', '"double stranded RNA"', '"double-stranded RNAs"',
        '"double stranded RNAs"', '"RNA duplex"', '"RNA duplexes"',
        '"small interfering ribonucleic acid"', '"small-interfering ribonucleic acid"',
        '"short interfering ribonucleic acid"', '"short-interfering ribonucleic acid"',
        '"double-stranded ribonucleic acid"', '"double stranded ribonucleic acid"',
        '"ribonucleic acid interference"', '"interfering ribonucleic acid"',
        '"small interfering ribonucleic acids"', '"small-interfering ribonucleic acids"',
        '"short interfering ribonucleic acids"', '"short-interfering ribonucleic acids"',
        '"double-stranded ribonucleic acids"', '"double stranded ribonucleic acids"',
        '"interfering ribonucleic acids"',
    ]
    
    competitor_terms = 'CRISPR|Cas9|Cas13|antisense|ASO|microRNA|miRNA|circRNA|circular RNA|aptamer'
    agri_vet_terms = 'plant|crop|seed|weed|agriculture|botanical|maize|corn|soybean|wheat|herbicide|pesticide|veterinary|canine|bovine|porcine'
    cpc_query = f'(cpc=C12N15/113 OR cpc=A61K31/713)'
    
    print(f"=== STARTING EPO EXTRACTION ({start_year} - {end_year}) ===")
    
    # --- 2. INTERNAL EXTRACTION FUNCTIONS ---
    def search_patent_ids(cql_query: str) -> list:
        """Searches for patent IDs respecting the API pagination limit (100 per call, max 2000)."""
        encoded_query = urllib.parse.quote(cql_query)
        extracted_ids = []
        start_index = 1
        
        while start_index <= 2000:
            end_index = start_index + 99 
            success_in_block = False
            res = None 
            
            for attempt in range(3):
                token = _get_valid_token(consumer_key, consumer_secret)
                headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'X-OPS-Range': f'{start_index}-{end_index}'}
                
                try:
                    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"
                    res = requests.get(url, headers=headers, timeout=30)
                    
                    if res.status_code == 200:
                        success_in_block = True
                        break 
                    elif res.status_code == 404:
                        return extracted_ids # End of results
                    else:
                        time.sleep(15 * (attempt + 1))
                except (requests.exceptions.RequestException, Exception) as e:
                    print(f"      [RETRY] Connection error: {e}")
                    time.sleep(20)
                    
            if not success_in_block or res is None: 
                break

            data = res.json().get('ops:world-patent-data', {}).get('ops:biblio-search', {})
            docs = data.get('ops:search-result', {}).get('ops:publication-reference', [])
            if not docs: break
            if isinstance(docs, dict): docs = [docs]

            for doc in docs:
                dids = doc.get('document-id', doc.get('ops:document-id', []))
                if isinstance(dids, dict): dids = [dids]
                
                docdb_node = next((d for d in dids if d.get('@document-id-type') == 'docdb'), None)
                if docdb_node:
                    country = _clean_val(docdb_node.get('country', ''))
                    number = _clean_val(docdb_node.get('doc-number', ''))
                    kind = _clean_val(docdb_node.get('kind', ''))
                    if country and number:
                        extracted_ids.append(f"{country}.{number}.{kind}".strip("."))

            start_index += 100
            time.sleep(6) 
            
        return extracted_ids

    def fetch_details(id_list: list) -> list:
        """Obtains detailed metadata for a list of IDs using batch processing."""
        print(f"\n[INFO] Extracting detailed metadata for {len(id_list)} unique patents...")
        results = []
        
        for i in range(0, len(id_list), 50):
            batch = id_list[i:i+50]
            url = f"https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/{','.join(batch)}/biblio"
            
            success = False
            for attempt in range(2):
                try:
                    token = _get_valid_token(consumer_key, consumer_secret)
                    res = requests.get(url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'}, timeout=30)
                    
                    if res.status_code == 200:
                        _parse_json_metadata(res.json(), results)
                        success = True
                        break
                    elif res.status_code == 404:
                        break # 404 in the batch means an ID is broken. Move to fallback.
                    else:
                        time.sleep(10)
                except (requests.exceptions.RequestException, Exception):
                    time.sleep(15)
            
            # FALLBACK: Process individually if the batch fails to isolate the problematic ID
            if not success:
                print(f"      [FALLBACK] Batch {i//50 + 1} failed. Isolating problematic IDs one by one...")
                for single_id in batch:
                    try:
                        token = _get_valid_token(consumer_key, consumer_secret)
                        s_url = f"https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/{single_id}/biblio"
                        s_res = requests.get(s_url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'}, timeout=15)
                        if s_res and s_res.status_code == 200:
                            _parse_json_metadata(s_res.json(), results)
                        time.sleep(4)
                    except Exception:
                        pass 
            
            time.sleep(6) 
        return results

    def _parse_json_metadata(json_data: dict, results_list: list):
        """Transforms complex nested JSON from the API into a flat dictionary for pandas."""
        data = json_data.get('ops:world-patent-data', {})
        ex_docs = data.get('ops:exchange-documents') or data.get('exchange-documents') or {}
        docs = ex_docs.get('ops:exchange-document') or ex_docs.get('exchange-document') or []
        if isinstance(docs, dict): docs = [docs]

        for doc in docs:
            bib = doc.get('ops:bibliographic-data', doc.get('bibliographic-data', {}))
            
            # Title (Prioritize English)
            t_node = bib.get('ops:invention-title', bib.get('invention-title', []))
            if isinstance(t_node, dict): t_node = [t_node]
            title_en = next((_clean_val(t) for t in t_node if isinstance(t, dict) and t.get('@lang', '').lower() == 'en'), None)
            title = title_en if title_en else (_clean_val(t_node[0]) if t_node else "No title")

            # Applicants
            app_node = bib.get('parties', {}).get('applicants', {}).get('applicant', [])
            if isinstance(app_node, dict): app_node = [app_node]
            applicants = []
            for app in app_node:
                names = app.get('applicant-name', {}).get('name', [])
                if isinstance(names, dict) or isinstance(names, str): names = [names]
                name = next((_clean_val(n) for n in names if isinstance(n, dict) and n.get('@lang', '').lower() == 'en'), None)
                if not name and names: name = _clean_val(names[0])
                if name: applicants.append(name)
            
            # Identifiers and Dates
            p_ref = bib.get('ops:publication-reference', bib.get('publication-reference', {}))
            d_ids = p_ref.get('ops:document-id', p_ref.get('document-id', []))
            if isinstance(d_ids, dict): d_ids = [d_ids]
            
            pub_date, country, doc_number, kind = "", "", "", ""
            for d in d_ids:
                if d.get('@document-id-type') == 'docdb':
                    pub_date = _clean_val(d.get('date', ''))
                    country = _clean_val(d.get('country', ''))
                    doc_number = _clean_val(d.get('doc-number', '')).replace(".", "")
                    kind = _clean_val(d.get('kind', ''))

            # CPC Codes
            cpcs_set = set()
            c_wrap = bib.get('ops:patent-classifications', bib.get('patent-classifications', {}))
            c_list = c_wrap.get('ops:patent-classification', c_wrap.get('patent-classification', []))
            if isinstance(c_list, dict): c_list = [c_list]
            for c in c_list:
                full_code = f"{_clean_val(c.get('section',''))}{_clean_val(c.get('class',''))}{_clean_val(c.get('subclass',''))}{_clean_val(c.get('main-group',''))}/{_clean_val(c.get('subgroup',''))}"
                if len(full_code) > 2: cpcs_set.add(full_code)

            results_list.append({
                'Patent_ID': f"{country}{doc_number}{kind}", 
                'Country': country, 
                'Number': doc_number, 
                'Kind': kind,
                'Family_ID': doc.get('@family-id', ''), 
                'Publication_Date': pub_date,
                'Applicant': " | ".join(applicants),
                'Title': title, 
                'CPCs': ", ".join(sorted(list(cpcs_set)))
            })

    # --- 3. MAIN EXECUTION FLOW ---
    try:
        # Groups terms into blocks of 2 to avoid excessively long URLs
        block_size = 2 
        term_blocks = [search_terms[i:i + block_size] for i in range(0, len(search_terms), block_size)]
        total_ids = []
        
        for current_year in range(start_year, end_year + 1):
            print(f"\n[INFO] Processing year {current_year}...")
            
            for idx, block in enumerate(term_blocks):
                text_query = " OR ".join([format_cql_term(term) for term in block])
                base_cql = f'({text_query}) AND {cpc_query}'
                if applicant_filter: base_cql += f' AND pa="{applicant_filter}"'
                
                # Slicing Logic: Decides whether to search the full year or divide by month
                year_cql = f'{base_cql} AND pd={current_year}'
                total_in_year = get_total_results_count(year_cql, consumer_key, consumer_secret)
                
                if total_in_year == 0:
                    print(f"  -> Block {idx+1}/{len(term_blocks)}: 0 results. Skipping.")
                    continue
                elif total_in_year <= 2000:
                    print(f"  -> Block {idx+1}/{len(term_blocks)}: {total_in_year} results. Fetching yearly...")
                    total_ids.extend(search_patent_ids(year_cql))
                else:
                    # If it exceeds 2000, slices search month-by-month to ensure full capture
                    print(f"  -> Block {idx+1}/{len(term_blocks)}: {total_in_year} results (>2000). Slicing by month...")
                    for month in range(1, 13):
                        last_day = calendar.monthrange(current_year, month)[1]
                        period_start = f"{current_year}{month:02d}01"
                        period_end   = f"{current_year}{month:02d}{last_day}"
                        
                        month_cql = f'{base_cql} AND pd within "{period_start} {period_end}"'
                        total_ids.extend(search_patent_ids(month_cql))
                        
        total_ids = list(set(total_ids)) # Deduplicates IDs found across multiple terms
        
        if not total_ids:
            print("\n[WARNING] No patents found.")
            return pd.DataFrame()
            
        final_dataset = fetch_details(total_ids)
        df = pd.DataFrame(final_dataset)
        
        # --- 4. POST-PROCESSING AND EXPORT ---
        if not df.empty:
            # Filters agricultural/veterinary patents
            df = df[~df['Title'].str.contains(agri_vet_terms, case=False, na=False, regex=True)]
            
            # Sorts by date and keeps only the first patent of each family (avoids global duplicates)
            df = df.sort_values('Publication_Date').drop_duplicates('Family_ID', keep='first')
            
            # Generates direct link to Espacenet
            df['Espacenet_Link'] = "https://worldwide.espacenet.com/publicationDetails/biblio?CC=" + df['Country'] + "&NR=" + df['Number'] + "&KC=" + df['Kind'] + "&FT=D"
            
            # Signals if title contains competing technologies (Warning)
            df['Warning'] = np.where(df['Title'].str.contains(competitor_terms, case=False, na=False, regex=True), 'Check Tech', '')

            # Reorganizes final columns
            cols = ['Patent_ID', 'Publication_Date', 'Applicant', 'Title', 'Espacenet_Link', 'Warning', 'CPCs', 'Family_ID']
            df = df[[c for c in cols if c in df.columns]]

            # Export to CSV (';' separator for Excel compatibility)
            filename = f'EPO_siRNA_{start_year}_{end_year}.csv'
            df.to_csv(filename, index=False, sep=';', encoding='utf-8-sig')
            print(f"\n[SUCCESS] Exported {len(df)} unique patent families to: {filename}")
            
            elapsed = time.time() - start_time
            print(f"[TIME] Total execution: {int(elapsed // 60)}m {int(elapsed % 60)}s")
            return df

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        return pd.DataFrame()