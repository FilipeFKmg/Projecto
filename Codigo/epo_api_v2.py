"""
EPO Patent Extraction Tool for siRNA Technology
-------------------------------------------------------------------------
Automated tool for patent extraction from the European Patent Office (EPO).
- Independent Query Architecture: Executes a pure OR search across all terms and codes independently.
- Deep Slicing: Automatically slices queries by Month and Day to bypass the 2000-result limit on broad classifications.
- Advanced CQL Search: Combines CPC/IPC codes with Title/Abstract terms.
- Exclusions: Automatically filters out Aptamers (15/115) and Immunomodulatory properties (15/117).
- Yearly Autosave: Prevents data loss during long extractions.
"""

import time
import base64
import requests
import urllib.parse
import pandas as pd
import numpy as np
import calendar
import re

# =============================================================================
# GLOBAL CACHE CONFIGURATION & CREDENTIALS
# =============================================================================
TOKEN_CACHE = {'token': None, 'timestamp': 0}

# Prioridade de países para selecção do representante de família.
# Menor número = maior prioridade. Países que publicam em inglês primeiro.
COUNTRY_PRIORITY = {
    'US': 1, 'EP': 2, 'WO': 3, 'GB': 4,
    'AU': 5, 'CA': 6, 'NZ': 7, 'IE': 8,
}

# =============================================================================
# 1. SERVER AND QUOTA CHECK
# =============================================================================
def check_epo_quota(consumer_key: str, consumer_secret: str):
    print("="*45)
    print("      EPO OPS API - STATUS DASHBOARD")
    print("="*45)
    
    auth_string = f"{consumer_key}:{consumer_secret}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    weekly_pct = 0.0
    
    try:
        token_res = requests.post(
            "https://ops.epo.org/3.2/auth/accesstoken",
            headers={
                'Authorization': f'Basic {encoded_auth}', 
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={'grant_type': 'client_credentials'}
        )
        token_res.raise_for_status() 
        token = token_res.json()['access_token']
        
        encoded_test_query = urllib.parse.quote("cpc=C12N15/113")
        response = requests.get(
            f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_test_query}",
            headers={'Authorization': f'Bearer {token}', 'X-OPS-Range': '1-1'}
        )
        response.raise_for_status()
        
        h = response.headers
        traffic = h.get('X-Throttling-Control', '')
        
        search_match = re.search(r'search=\w+:(\d+)', traffic)
        search_rpm = int(search_match.group(1)) if search_match else 0
        
        weekly_used_bytes = (
            h.get('X-RegisteredQuotaPerWeek-Used') or 
            h.get('X-IndividualQuotaPerHour-Used') or 
            '0'
        )
        
        try:
            bytes_used = int(weekly_used_bytes)
            weekly_used_mb = bytes_used / (1024 * 1024)
            weekly_pct = (weekly_used_mb / 4000) * 100
            remaining_pct = 100 - weekly_pct
        except ValueError:
            weekly_used_mb = 0.0
            remaining_pct = 100.0
            weekly_pct = 0.0
        
        print(f"\n[DATA CONSUMPTION]")
        print(f"Weekly Volume Used: {weekly_pct:.4f}% ({weekly_used_mb:.2f}MB / 4000MB)")
        print(f"Quota Remaining:    {remaining_pct:.4f}%")
        print(f"Current API Load:   {search_rpm} requests/minute")
        print("\n" + "-"*45)
        
        if weekly_pct >= 100:
            print("[WARNING] Weekly 4GB quota exceeded. Script will try to run anyway.")
        elif "overloaded" in traffic.lower():
            print("[WARNING] Server overloaded. Script will try to run anyway.")
        else:
            print("[OK] Server clear. Safe to proceed with extraction.")
        print("-"*45)

    except Exception as e:
        print(f"\n[QUOTA CHECK ERROR] Ignore this, trying to proceed anyway: {e}")

    return True, weekly_pct

# =============================================================================
# 2. HELPER FUNCTIONS
# =============================================================================
def _get_valid_token(consumer_key: str, consumer_secret: str) -> str:
    current_time = time.time()
    if TOKEN_CACHE['token'] is None or (current_time - TOKEN_CACHE['timestamp']) > 850:
        print("\n[SECURITY] Generating a new EPO Access Token...")
        url = "https://ops.epo.org/3.2/auth/accesstoken"
        auth_string = f"{consumer_key}:{consumer_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        response = requests.post(
            url,
            headers={
                'Authorization': f'Basic {encoded_auth}', 
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={'grant_type': 'client_credentials'}
        )
        response.raise_for_status()
        
        TOKEN_CACHE['token'] = response.json()['access_token']
        TOKEN_CACHE['timestamp'] = time.time()
        
    return TOKEN_CACHE['token']

def _clean_val(node) -> str:
    if isinstance(node, dict): return node.get('$', '')
    return str(node) if node else ""

def clean_text_for_csv(text: str) -> str:
    if not text: return ""
    clean = re.sub(r'<[^>]+>', '', text)
    clean = clean.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    clean = re.sub(r'\[\d{4}\]', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def get_total_results_count(cql_query: str, consumer_key: str, consumer_secret: str) -> int:
    
    encoded_query = urllib.parse.quote(cql_query)
    
    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"
    
    for attempt in range(4):
        try:
            token = _get_valid_token(consumer_key, consumer_secret)
            
            # Requests only the first record (Range: 1-1) to minimize data usage.
            # The goal is not to extract patent details (like family-id or country) here,
            # but simply to trigger the server to send the overarching JSON metadata 
            # so we can identify the total number of patents to be processed.
            response = requests.get(
                url, 
                headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'X-OPS-Range': '1-1'}, 
                timeout=30
            ) 
            
            if response.status_code == 200:
                data = response.json().get('ops:world-patent-data', {}).get('ops:biblio-search', {})

                # Extracts the total number of patents that fulfill the query and converts it to an integer
                total = int(data.get('@total-result-count', 0))
                time.sleep(3) 
                return total
            elif response.status_code == 400:
                print(f"      [SYNTAX ERROR 400] EPO rejected the query syntax. Skipping.")
                return 0
            elif response.status_code == 404:
                time.sleep(2)
                return 0 
            elif response.status_code == 403:
                wait_time = 600
                print(f"      [SECURITY] 403 Forbidden. Server cooling down for {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code in [429, 503]:
                wait_time = 60 * (attempt + 1)
                print(f"      [OVERLOAD] API busy (Status {response.status_code}). Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                time.sleep(15)
        except Exception as e:
            if attempt == 3: print(f"      [ERROR] Connection failed: {e}")
            time.sleep(20)
            
    raise Exception("Critical: EPO Server consistently rejecting count requests.")

def fetch_abstract_fallback(single_id: str, token: str) -> str:
    abs_url = f"https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/{single_id}/abstract"
    try:
        res = requests.get(abs_url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'}, timeout=15)
        if res.status_code == 200:
            data = res.json()
            wpd = data.get('ops:world-patent-data') or {}
            ex_docs = wpd.get('ops:exchange-documents') or {}
            ex_doc = ex_docs.get('ops:exchange-document') or {}
            if isinstance(ex_doc, list): ex_doc = ex_doc[0]
            
            abs_node = ex_doc.get('ops:abstract') or ex_doc.get('abstract') or []
            if isinstance(abs_node, dict): abs_node = [abs_node]
            
            for a in abs_node:
                if isinstance(a, dict) and a.get('@lang', '').lower() == 'en':
                    p_node = a.get('ops:p') or a.get('p')
                    if isinstance(p_node, list):
                        return clean_text_for_csv(" ".join([_clean_val(p) for p in p_node]))
                    return clean_text_for_csv(_clean_val(p_node))
            
            if abs_node:
                first = abs_node[0]
                p_node = first.get('ops:p') or first.get('p') if isinstance(first, dict) else None
                if p_node:
                    text = " ".join([_clean_val(p) for p in p_node]) if isinstance(p_node, list) else _clean_val(p_node)
                    return clean_text_for_csv(text)
                return clean_text_for_csv(_clean_val(first))
    except Exception:
        pass
    return "No abstract available in EPO database"

# =============================================================================
# DEDUPLICAÇÃO POR FAMÍLIA
# =============================================================================
def _deduplicate_by_family(id_records: list) -> list:
    """
    Recebe uma lista de dicts {'id': str, 'family_id': str, 'country': str}
    acumulados de todas as queries independentes.

    Para cada família, mantém apenas o melhor representante segundo
    COUNTRY_PRIORITY (US > EP > WO > GB > ...).
    Patentes sem family_id são mantidas individualmente.

    Devolve uma lista de strings de IDs prontas para a Fase 2.
    """
    family_map = {}   # family_id -> melhor registo até agora
    no_family  = {}   # patent_id -> True (sem family_id, deduplica por ID)

    for rec in id_records:
        fam_id  = rec.get('family_id', '').strip()
        pat_id  = rec.get('id', '')
        country = rec.get('country', '')

        if not fam_id:
            # Sem família — mantém mas evita duplicados de ID puro
            if pat_id:
                no_family[pat_id] = True
            continue

        if fam_id not in family_map:
            family_map[fam_id] = rec
        else:
            # Compara prioridade de país
            existing_score = COUNTRY_PRIORITY.get(family_map[fam_id]['country'], 99)
            new_score      = COUNTRY_PRIORITY.get(country, 99)
            if new_score < existing_score:
                family_map[fam_id] = rec

    result = [rec['id'] for rec in family_map.values()] + list(no_family.keys())
    return result

# =============================================================================
# 3. CORE 1: DOWNLOAD RAW PATENTS
# =============================================================================
def download_raw_patents(consumer_key: str, consumer_secret: str, start_year: int, end_year: int, applicant_filter: str = None) -> pd.DataFrame:
    """Extracts raw data from EPO using fully independent queries with deep day-level slicing."""
    start_time = time.time()
    
    # Define the list for independent queries
    independent_queries = []
    
    # Exclusion string to filter out aptamers and CpG-motifs in every query
    exclusions = ' NOT (cpc="C12N15/115" OR cpc="C12N15/117")'
    
    # 1. Classification codes: Adding exclusions for aptamer and immunomodulatory codes
    for code in ["C12N15/11", "A61K31/7088"]:
        independent_queries.append(f'(cpc=/low "{code}"{exclusions})')
    for code in ["C12N15/113", "A61K31/713", "C12N15/11", "A61K31/7088"]:
        independent_queries.append(f'(ipc="{code}"{exclusions})')
        
    # 2. Text search terms
    search_terms = [
        "siRNA*", "RNAi*", "dsRNA*", "iRNA*", "dsRNAi*", "oligonucleotide*",
        '"si-RNA"', '"ds RNA"',
        '"RNA interference"', '"interfering RNA"',
        '"small interfering RNA"', '"small interfering RNAs"',
        '"short interfering RNAs"', '"short interfering RNA"',
        '"double stranded RNA"', '"double stranded RNAs"', 
        '"RNA duplex"', '"RNA duplexes"',
        '"small interfering ribonucleic acid"', '"small interfering ribonucleic acids"',
        '"short interfering ribonucleic acid"', '"short interfering ribonucleic acids"',
        '"double stranded ribonucleic acid"', '"double stranded ribonucleic acids"',
        '"ribonucleic acid interference"', 
        '"interfering ribonucleic acid"', '"interfering ribonucleic acids"',
    ]
    
    for term in search_terms:
        # Apply the same exclusions to text-based searches
        independent_queries.append(f'(ta={term}{exclusions})')

    #print(independent_queries) ###

    print(f"=== STARTING EPO EXTRACTION ({start_year} - {end_year}) ===")
    print(f"[INFO] Evaluating {len(independent_queries)} independent query conditions.")
    print(f"[INFO] Active Exclusions: C12N15/115 (Aptamers) and C12N15/117 (Immunomodulatory)")

    # =========================================================================
    # FASE 1 — RECOLHA DE IDs + family_id
    # =========================================================================
    def search_patent_ids(cql_query: str) -> list:
        """
        Devolve lista de dicts: [{'id': 'US.123456.A1', 'family_id': '...', 'country': 'US'}, ...]
        """
        encoded_query = urllib.parse.quote(cql_query)
        extracted_records = []
        seen_ids = set()        # evita duplicados dentro da mesma query
        start_index = 1
        
        while start_index <= 2000:
            end_index = start_index + 99 
            success_in_block = False
            res = None 
            
            for attempt in range(4):
                token = _get_valid_token(consumer_key, consumer_secret)
                headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'X-OPS-Range': f'{start_index}-{end_index}'}
                
                try:
                    url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q={encoded_query}"
                    res = requests.get(url, headers=headers, timeout=30)
                    
                    if res.status_code == 200:
                        json_resp = res.json()
                        wpd = json_resp.get('ops:world-patent-data') or json_resp.get('world-patent-data') or {}
                        biblio = wpd.get('ops:biblio-search') or wpd.get('biblio-search') or {}
                        search_res = biblio.get('ops:search-result') or biblio.get('search-result') or {}
                        test_docs = search_res.get('ops:publication-reference') or search_res.get('publication-reference') or []
                        
                        if not test_docs and start_index == 1 and attempt < 2:
                            time.sleep(20) 
                            continue
                        success_in_block = True
                        break 
                    elif res.status_code == 400:
                        print(f"      [SYNTAX ERROR 400 in Fetch] EPO rejected the query syntax. Skipping block.")
                        return extracted_records
                    elif res.status_code == 404:
                        if start_index == 1 and attempt < 2:
                            time.sleep(20) 
                            continue
                        return extracted_records 
                    elif res.status_code == 403: time.sleep(600)
                    elif res.status_code in [429, 503]: time.sleep(60 * (attempt + 1))
                    else: time.sleep(15)
                except Exception:
                    time.sleep(20)
                    
            if not success_in_block or res is None: break 

            if res and res.status_code == 200:
                json_resp = res.json()
                wpd = json_resp.get('ops:world-patent-data') or json_resp.get('world-patent-data') or {}
                biblio = wpd.get('ops:biblio-search') or wpd.get('biblio-search') or {}
                search_res = biblio.get('ops:search-result') or biblio.get('search-result') or {}
                docs = search_res.get('ops:publication-reference') or search_res.get('publication-reference') or []
                
                if not docs: break 
                if isinstance(docs, dict): docs = [docs]

                for doc in docs:
                    # ── OPTIMIZAÇÃO: extrair @family-id do nó pai (ops:publication-reference)
                    # Confirmado empiricamente: o @family-id está directamente aqui,
                    family_id = doc.get('@family-id', '').strip()

                    dids = doc.get('document-id', doc.get('ops:document-id', []))
                    if isinstance(dids, dict): dids = [dids]
                    
                    docdb_node = next((d for d in dids if d.get('@document-id-type') == 'docdb'), None)
                    if docdb_node:
                        country = _clean_val(docdb_node.get('country', ''))
                        number  = _clean_val(docdb_node.get('doc-number', '')).replace(" ", "").replace(".", "")
                        kind    = _clean_val(docdb_node.get('kind', ''))
                        
                        if country and number:
                            pat_id = f"{country}.{number}.{kind}" if kind else f"{country}.{number}"

                            if pat_id not in seen_ids:
                                seen_ids.add(pat_id)
                                extracted_records.append({
                                    'id'       : pat_id,
                                    'family_id': family_id,
                                    'country'  : country,
                                })

            start_index += 100
            time.sleep(15) 
            
        return extracted_records

    def search_with_slicing(base_cql: str, year: int) -> list:
        """
        Deeply slices queries by month and day to avoid losing results over 2000.
        Gives an list[dict] with {'id', 'family_id', 'country'}.
        """
        if applicant_filter:
            year_cql = f'{base_cql} AND pa="{applicant_filter}" AND pd={year}'
        else:
            year_cql = f'{base_cql} AND pd={year}'
            #print(year_cql) ###
            
        total = get_total_results_count(year_cql, consumer_key, consumer_secret)
        
        if total == 0:
            return []
            
        if total <= 2000:
            print(f"    -> Total: {total} results. Fetching yearly...")
            return search_patent_ids(year_cql)
            
        ids = []
        print(f"    -> Total: {total} results (>2000). Slicing by month...")
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]
            m_start = f"{year}{month:02d}01"
            m_end   = f"{year}{month:02d}{last_day}"
            
            if applicant_filter:
                month_cql = f'{base_cql} AND pa="{applicant_filter}" AND pd within "{m_start} {m_end}"'
            else:
                month_cql = f'{base_cql} AND pd within "{m_start} {m_end}"'
                
            month_total = get_total_results_count(month_cql, consumer_key, consumer_secret)
            
            if month_total == 0:
                continue
            elif month_total <= 2000:
                ids.extend(search_patent_ids(month_cql))
            else:
                # Day-level fallback
                print(f"      [SLICING] Month {month} has {month_total} results, slicing by day...")
                for day in range(1, last_day + 1):
                    day_str = f"{year}{month:02d}{day:02d}"
                    if applicant_filter:
                        day_cql = f'{base_cql} AND pa="{applicant_filter}" AND pd within "{day_str} {day_str}"'
                    else:
                        day_cql = f'{base_cql} AND pd within "{day_str} {day_str}"'
                    
                    day_total = get_total_results_count(day_cql, consumer_key, consumer_secret)
                    if day_total > 0:
                        ids.extend(search_patent_ids(day_cql))
                        
                    if day_total > 2000:
                        print(f"        [WARNING] Day {day_str} still has >2000 results ({day_total}). Hardware API cap reached.")
        return ids

    def fetch_details(id_list: list) -> list:
        print(f"  -> Extracting metadata for {len(id_list)} unique IDs...")
        results = []
        for i in range(0, len(id_list), 20):
            batch = id_list[i:i+20]
            url = f"https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/{','.join(batch)}/biblio"
            success = False
            
            for attempt in range(3):
                try:
                    token = _get_valid_token(consumer_key, consumer_secret)
                    res = requests.get(url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'}, timeout=30)
                    
                    if res.status_code == 200:
                        current_len = len(results)
                        _parse_json_metadata(res.json(), results)
                        
                        for idx in range(current_len, len(results)):
                            if results[idx]['Abstract'] == "No abstract available in EPO database":
                                docdb_id = f"{results[idx]['Country']}.{results[idx]['Number']}.{results[idx]['Kind']}".strip(".")
                                results[idx]['Abstract'] = fetch_abstract_fallback(docdb_id, token)
                                time.sleep(5)
                        success = True
                        break
                    elif res.status_code == 404: break 
                    elif res.status_code == 403: time.sleep(600)
                    elif res.status_code in [429, 503]: time.sleep(60 * (attempt + 1))
                    else: time.sleep(15)
                except Exception as e:
                    print(f"  [BATCH ERROR] Attempt {attempt+1} failed for batch starting {batch[0]}: {type(e).__name__}: {e}")
                    time.sleep(20)
            
            if not success:
                for single_id in batch:
                    try:
                        token = _get_valid_token(consumer_key, consumer_secret)
                        s_url = f"https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/{single_id}/biblio"
                        s_res = requests.get(s_url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'}, timeout=20)
                        
                        if s_res and s_res.status_code == 200:
                            current_len = len(results)
                            _parse_json_metadata(s_res.json(), results)
                            if len(results) > current_len:
                                last_entry = results[-1]
                                if last_entry['Abstract'] == "No abstract available in EPO database":
                                    docdb_id = f"{last_entry['Country']}.{last_entry['Number']}.{last_entry['Kind']}".strip(".")
                                    last_entry['Abstract'] = fetch_abstract_fallback(docdb_id, token)
                                    time.sleep(5)
                        time.sleep(5) 
                    except Exception as e:
                        print(f"     [SILENT LOSS] Failed on {single_id}: {e}")
                        pass 
            time.sleep(12) 
        return results

    def _parse_json_metadata(json_data: dict, results_list: list):
        data = json_data.get('ops:world-patent-data') or json_data.get('world-patent-data') or {}
        ex_docs = data.get('ops:exchange-documents') or data.get('exchange-documents') or {}
        docs = ex_docs.get('ops:exchange-document') or ex_docs.get('exchange-document') or []
        
        if isinstance(docs, dict): docs = [docs]

        for doc in docs:
            bib = doc.get('ops:bibliographic-data') or doc.get('bibliographic-data') or {}
            
            t_node = bib.get('ops:invention-title') or bib.get('invention-title') or []
            if isinstance(t_node, dict): t_node = [t_node]
            title_en = next((_clean_val(t) for t in t_node if isinstance(t, dict) and t.get('@lang', '').lower() == 'en'), None)
            title = title_en if title_en else (_clean_val(t_node[0]) if t_node else "No title")

            abs_node = doc.get('ops:abstract') or doc.get('abstract') or []
            if isinstance(abs_node, dict): abs_node = [abs_node]
            
            abstract_text = ""
            for a in abs_node:
                if isinstance(a, dict) and a.get('@lang', '').lower() == 'en':
                    p_node = a.get('ops:p') or a.get('p')
                    if p_node:
                        if isinstance(p_node, list): abstract_text = " ".join([_clean_val(p) for p in p_node])
                        else: abstract_text = _clean_val(p_node)
                    else: abstract_text = _clean_val(a)
                    break
            
            if not abstract_text.strip() and abs_node:
                first_abs = abs_node[0]
                if isinstance(first_abs, dict):
                    p_node = first_abs.get('ops:p') or first_abs.get('p')
                    if p_node:
                        if isinstance(p_node, list): abstract_text = " ".join([_clean_val(p) for p in p_node])
                        else: abstract_text = _clean_val(p_node)
                    else: abstract_text = _clean_val(first_abs)
                else: abstract_text = _clean_val(first_abs)
            
            if not abstract_text.strip(): abstract_text = "No abstract available in EPO database"
            else: abstract_text = clean_text_for_csv(abstract_text)

            parties = bib.get('ops:parties') or bib.get('parties') or {}
            applicants_wrap = parties.get('ops:applicants') or parties.get('applicants') or {}
            app_node = applicants_wrap.get('ops:applicant') or applicants_wrap.get('applicant') or []
            if isinstance(app_node, dict): app_node = [app_node]
            applicants = []
            for app in app_node:
                app_name_wrap = app.get('ops:applicant-name') or app.get('applicant-name') or {}
                names = app_name_wrap.get('ops:name') or app_name_wrap.get('name') or []
                if isinstance(names, dict) or isinstance(names, str): names = [names]
                name = next((_clean_val(n) for n in names if isinstance(n, dict) and n.get('@lang', '').lower() == 'en'), None)
                if not name and names: name = _clean_val(names[0])
                if name: applicants.append(name)
            
            p_ref = bib.get('ops:publication-reference') or bib.get('publication-reference') or {}
            d_ids = p_ref.get('ops:document-id') or p_ref.get('document-id') or []
            if isinstance(d_ids, dict): d_ids = [d_ids]
            pub_date, country, doc_number, kind = "", "", "", ""
            for d in d_ids:
                if d.get('@document-id-type') == 'docdb':
                    pub_date = _clean_val(d.get('date', ''))
                    country = _clean_val(d.get('country', ''))
                    doc_number = _clean_val(d.get('doc-number', '')).replace(".", "")
                    kind = _clean_val(d.get('kind', ''))
            
            pri_date = ""
            pri_claims = bib.get('ops:priority-claims') or bib.get('priority-claims') or {}
            pri_claim_list = pri_claims.get('ops:priority-claim') or pri_claims.get('priority-claim') or []
            if isinstance(pri_claim_list, dict): pri_claim_list = [pri_claim_list]
            dates_found = []
            for claim in pri_claim_list:
                c_d_ids = claim.get('ops:document-id') or claim.get('document-id') or []
                if isinstance(c_d_ids, dict): c_d_ids = [c_d_ids]
                for c_d in c_d_ids:
                    if c_d.get('@document-id-type') == 'docdb' and _clean_val(c_d.get('date')):
                        dates_found.append(_clean_val(c_d.get('date')))
            pri_date = min(dates_found) if dates_found else pub_date

            cpcs_set, ipcs_set = set(), set()
            c_wrap = bib.get('ops:patent-classifications') or bib.get('patent-classifications') or {}
            c_list = c_wrap.get('ops:patent-classification') or c_wrap.get('patent-classification') or []
            if isinstance(c_list, dict): c_list = [c_list]
            
            for c in c_list:
                sec = _clean_val(c.get('section',''))
                cls = _clean_val(c.get('class',''))
                subc = _clean_val(c.get('subclass',''))
                mg = _clean_val(c.get('main-group',''))
                sg = _clean_val(c.get('subgroup',''))
                full_code = f"{sec}{cls}{subc}{mg}/{sg}".strip("/")
                
                if len(full_code) > 2:
                    scheme_node = c.get('ops:classification-scheme') or c.get('classification-scheme') or {}
                    scheme_type = _clean_val(scheme_node.get('@scheme', '')).upper() if isinstance(scheme_node, dict) else (scheme_node.upper() if isinstance(scheme_node, str) else "")
                    
                    if 'CPC' in scheme_type: cpcs_set.add(full_code)
                    elif 'IPC' in scheme_type: ipcs_set.add(full_code)
                    else:
                        cpcs_set.add(full_code)
                        ipcs_set.add(full_code)

            results_list.append({
                'Patent_ID': f"{country}{doc_number}{kind}", 'Country': country, 'Number': doc_number, 'Kind': kind,
                'Family_ID': doc.get('@family-id', ''), 'Priority_Date': pri_date, 'Publication_Date': pub_date,
                'Applicant': " | ".join(applicants), 'Title': title, 'Abstract': abstract_text, 
                'IPCs': ", ".join(sorted(list(ipcs_set))), 'CPCs': ", ".join(sorted(list(cpcs_set)))
            })

    try:
        all_yearly_dfs = [] 
        
        for current_year in range(start_year, end_year + 1):
            print(f"\n[INFO] Processing year {current_year}...")

            # ── FASE 1: recolher registos {id, family_id, country} de todas as queries ──
            all_records_year = []
            for idx, base_query in enumerate(independent_queries):
                print(f"  -> Search {idx+1}/{len(independent_queries)}: {base_query}")
                all_records_year.extend(search_with_slicing(base_query, current_year))

            print(f"  -> Registos brutos acumulados (com duplicados cross-query): {len(all_records_year)}")

            # ── FASE 1.5: deduplicação por família ANTES da Fase 2 ──────────────────────
            # Selecciona o melhor representante de cada família (prioridade de país)
            # usando o @family-id recolhido na Fase 1.
            # Elimina chamadas redundantes ao /biblio antes de as fazer.
            total_ids_year = _deduplicate_by_family(all_records_year)

            print(f"  -> Após deduplicação por família: {len(total_ids_year)} IDs únicos para a Fase 2")
            print(f"  -> Chamadas /biblio poupadas: {len(all_records_year) - len(total_ids_year)}")

            if total_ids_year:
                yearly_data = fetch_details(total_ids_year)
                df_year = pd.DataFrame(yearly_data)
                
                if not df_year.empty:
                    df_year['Family_ID'] = df_year['Family_ID'].replace('', np.nan)
                    truly_empty = df_year['Family_ID'].isna() & df_year['Patent_ID'].eq('')
                    
                    if truly_empty.sum() > 0:
                        print(f"  [WARNING] {truly_empty.sum()} patents with no Family_ID or Patent_ID — assigning placeholders")
                        df_year.loc[truly_empty, 'Family_ID'] = [f"UNKNOWN_FAM_{i}_{current_year}" for i in range(truly_empty.sum())]
                        
                    df_year['Family_ID'] = df_year['Family_ID'].fillna(df_year['Patent_ID'])
                    
                    # Deduplication final: garante que a Fase 2 não trouxe membros
                    # extra da mesma família (pode acontecer em edge cases onde
                    # o family-id da Fase 1 difere do da Fase 2).
                    # Prioriza: bom texto em inglês → data de prioridade mais antiga.
                    has_bad_abstract = df_year['Abstract'].fillna('').str.contains('No abstract available', case=False)
                    has_non_latin = df_year['Title'].fillna('').str.contains(r'[^\x00-\x7F]', regex=True)
                    df_year['Good_Text'] = ~(has_bad_abstract | has_non_latin)
                    
                    df_year = df_year.sort_values(['Good_Text', 'Priority_Date'], ascending=[False, True])
                    df_year = df_year.drop_duplicates('Family_ID', keep='first')
                    df_year = df_year.drop(columns=['Good_Text'])
                    
                    # YEARLY AUTOSAVE (RAW)
                    autosave_name = f"EPO_AutoSave_RAW_{current_year}.csv"
                    df_year.to_csv(autosave_name, index=False, sep=';', encoding='utf-8-sig')
                    print(f"  [AUTOSAVE] Year {current_year} raw data saved to {autosave_name}")
                    
                    all_yearly_dfs.append(df_year)

        if not all_yearly_dfs:
            print("\n[WARNING] No patents found to download in the specified range.")
            return pd.DataFrame()
            
        final_raw_df = pd.concat(all_yearly_dfs, ignore_index=True)
        
        final_raw_df['Family_ID'] = final_raw_df['Family_ID'].replace('', np.nan)
        truly_empty_final = final_raw_df['Family_ID'].isna() & final_raw_df['Patent_ID'].eq('')
        if truly_empty_final.sum() > 0:
            final_raw_df.loc[truly_empty_final, 'Family_ID'] = [f"UNKNOWN_FAM_FINAL_{i}" for i in range(truly_empty_final.sum())]
        final_raw_df['Family_ID'] = final_raw_df['Family_ID'].fillna(final_raw_df['Patent_ID'])

        # Deduplicação Final cross-year
        has_bad_abstract_final = final_raw_df['Abstract'].fillna('').str.contains('No abstract available', case=False)
        has_non_latin_final = final_raw_df['Title'].fillna('').str.contains(r'[^\x00-\x7F]', regex=True)
        final_raw_df['Good_Text'] = ~(has_bad_abstract_final | has_non_latin_final)
        
        final_raw_df = final_raw_df.sort_values(['Good_Text', 'Priority_Date'], ascending=[False, True])
        final_raw_df = final_raw_df.drop_duplicates('Family_ID', keep='first')
        final_raw_df = final_raw_df.drop(columns=['Good_Text'])
        
        raw_filename = f'EPO_siRNA_RAW_{start_year}_{end_year}.csv'
        final_raw_df.to_csv(raw_filename, index=False, sep=';', encoding='utf-8-sig')
        
        elapsed = time.time() - start_time
        print(f"\n[SUCCESS] Downloaded {len(final_raw_df)} raw patents to: {raw_filename}")
        print(f"[TIME] Download execution: {int(elapsed // 60)}m {int(elapsed % 60)}s")
            
        return final_raw_df

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        return pd.DataFrame()
