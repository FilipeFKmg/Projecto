"""Download EP patent full-text XML for an ENTIRE patent family (EPO OPS + EPS).

Strategy (per EPO guidance that family members are NOT copies — divisionals,
continuations and the A vs B versions of one application can carry different
experimental data):

  * For each input patent, fetch its full family from OPS (all members, all
    countries).
  * Non-EP members are logged to not_in_eps.csv — only EP publications can have
    full text on the EPO Publication Server (EPS).
  * Every EP member (any kind code, any number) is tested individually on EPS;
    those with a full-text XML are saved, the rest are logged to not_in_eps.csv.

Nothing is substituted or guessed-as-equivalent: the whole EP family is pulled
and the comparison of which members actually share data is left to a later step.
"""

import pandas as pd
import requests
import os
import time
import base64
import xml.etree.ElementTree as ET
import re

class OPSClient:
    """Handles authentication and queries to the EPO OPS API."""
    
    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.token = None
        self.expiry = 0.0

    def _get_token(self):
        """Fetches or refreshes the OAuth2 token."""
        if self.token and time.time() < (self.expiry - 60):
            return self.token
        
        url = "https://ops.epo.org/3.2/auth/accesstoken"
        auth_str = base64.b64encode(f"{self.key}:{self.secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        try:
            res = requests.post(url, headers=headers, data={"grant_type": "client_credentials"})
            if res.status_code == 200:
                data = res.json()
                self.token = data.get("access_token")
                self.expiry = time.time() + int(data.get("expires_in", 1200))
                return self.token
            else:
                print(f"  [OPS API ERROR] Auth failed! HTTP {res.status_code}: {res.text}")
        except Exception as e:
            print(f"  [OPS API ERROR] Auth request failed: {e}")
        return None

    def _get_with_retry(self, url, accept_header):
        """Performs GET requests with automated retry logic for rate limits/overloads."""
        for attempt in range(3):
            try:
                token = self._get_token()
                if not token: 
                    return None
                    
                headers = {"Authorization": f"Bearer {token}", "Accept": accept_header}
                res = requests.get(url, headers=headers, timeout=90)
                
                # Handle rate limits, server errors, and quota lockouts
                if res.status_code in (429, 502, 503, 504, 403):
                    wait = 10 * (2 ** attempt)
                    print(f"      [WARN] EPO Server Busy (HTTP {res.status_code}). Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                    
                # Strict 8-second delay after every valid request
                time.sleep(8)
                return res
            except Exception as e:
                print(f"      [WARN] Connection issue: {e}. Retrying...")
                time.sleep(10)
                
        return None

    def format_to_epodoc(self, raw_id):
        """Converts a patent ID into strict epodoc format.
        
        As proven by API testing, the equivalents endpoint STRICTLY requires the 
        base publication number WITHOUT the Kind Code (e.g., no 'A1' or 'B1') 
        and WITHOUT dots or spaces.
        """
        raw = str(raw_id).upper().strip()
        raw = raw.replace(".", "").replace(" ", "").replace("/", "").replace("-", "")
        
        # Remove the Kind Code (Regex looks for CC + Numbers + Letters at the end)
        match = re.match(r'^([A-Z]{2,3})(\d+)([A-Z]\d*)$', raw)
        if match:
            cc = match.group(1)   # e.g., EP
            num = match.group(2)  # e.g., 4298220
            return f"{cc}{num}"   # Returns the clean root: EP4298220
            
        return raw

    def _collect_members(self, root, scope_suffixes):
        """Return [(country, number, kind), ...] for every docdb document-id that
        sits under an element whose tag ends with one of `scope_suffixes`.
        ALL countries are returned (not just EP) so non-EP members can be logged."""
        members = []
        for scope in root.iter():
            if not any(scope.tag.endswith(s) for s in scope_suffixes):
                continue
            for elem in scope.iter():
                if not elem.tag.endswith('document-id'):
                    continue
                if elem.attrib.get('document-id-type') != 'docdb':
                    continue
                country = number = kind = ''
                for child in elem:
                    if child.tag.endswith('country'):
                        country = child.text or ''
                    elif child.tag.endswith('doc-number'):
                        number = child.text or ''
                    elif child.tag.endswith('kind'):
                        kind = child.text or ''
                if country and number:
                    members.append((country, number, kind))
        return members

    def get_equivalents(self, patent_id):
        """OPS simple-family equivalents — ALL members, ALL countries."""
        epodoc_id = self.format_to_epodoc(patent_id)
        url = (f"https://ops.epo.org/3.2/rest-services/published-data/"
               f"publication/epodoc/{epodoc_id}/equivalents")
        res = self._get_with_retry(url, "application/ops+xml")

        members = []
        if res and res.status_code == 200:
            try:
                members = self._collect_members(ET.fromstring(res.content),
                                                ('inquiry-result',))
            except Exception as e:
                print(f"  [OPS ERROR] Failed to parse equivalents XML for {epodoc_id}: {e}")
        elif res and res.status_code == 404:
            pass  # No equivalents found
        elif res:
            print(f"  [OPS ERROR] API returned HTTP {res.status_code} "
                  f"for epodoc ID: {epodoc_id}. Reason: {res.reason}")
        return members

    def get_family_members(self, family_id):
        """OPS CQL family search (index 'famn') — ALL members, ALL countries."""
        # Remove '.0' if pandas parsed it as float, then strip ALL non-digit chars.
        clean_fam = re.sub(r"\D", "", str(family_id).split('.')[0])
        if not clean_fam:
            return []

        url = f"https://ops.epo.org/3.2/rest-services/published-data/search?q=famn={clean_fam}"
        res = self._get_with_retry(url, "application/ops+xml")

        members = []
        if res and res.status_code == 200:
            try:
                members = self._collect_members(ET.fromstring(res.content),
                                                ('publication-reference',))
            except Exception as e:
                print(f"  [OPS ERROR] Failed to parse family XML for {clean_fam}: {e}")
        elif res and res.status_code == 404:
            pass  # Family not found
        elif res:
            print(f"  [OPS ERROR] Search API returned HTTP {res.status_code} for family {clean_fam}.")
        return members



# --- EPS XML Downloader Functions ---

def is_full_xml(xml_content):
    """Checks if the downloaded XML contains substantial text content."""
    content_lower = xml_content.lower()
    return b"<description" in content_lower or b"<claims" in content_lower or b"<table" in content_lower

_KIND_RE = re.compile(r'([AB]\d)$')

# Kind codes probed on EPS for every EP publication number. A single number can
# carry both an application (A) and a grant (B); we probe all of these and keep
# every document that exists. Trim this list if you want faster (less thorough)
# runs — e.g. ["A1", "A2", "B1", "B2"] covers the vast majority of cases.
EP_KIND_CODES = ["A1", "A2", "A3", "A4", "B1", "B2", "B3"]


def _requested_kind(raw_id):
    """The specific kind code the caller asked for ('A1', 'B1', ...), or None
    if they gave a bare publication number with no kind code."""
    m = _KIND_RE.search(str(raw_id).upper().replace("NW", ""))
    return m.group(1) if m else None


def try_download_ep(base_id, output_directory, kind_fallback=True, prefer="A"):
    """Attempt to download the full-text XML from the European Publication Server.

    Returns a 4-tuple: (success, downloaded_id, downloaded_kind, exact_kind).
      - downloaded_kind: the kind code actually retrieved ('A1', 'B1', ...).
      - exact_kind: True only when the caller requested a specific kind code AND
        that exact kind code is the one that was downloaded.

    IMPORTANT — why exact_kind matters (per EPO guidance): the A (laid-open
    application) and B (granted) documents of the SAME application are NOT
    interchangeable. Claims, and sometimes the description and its experimental
    data, can be amended during prosecution. So a kind-code fallback — e.g.
    asking for B1 but only A1 being available — yields a DIFFERENT document
    whose tables are not guaranteed to match. Callers must treat
    exact_kind=False as "different document — verify before trusting the data".

    `kind_fallback=False` restricts the search to the exact requested kind code
    (no A/B substitution). `prefer` ('A' or 'B') sets the order in which kind
    codes are tried when guessing.
    """
    if not str(base_id).startswith("EP"):
        return False, None, None, False

    base_url = "https://data.epo.org/publication-server/rest/v1.2/patents/{}/document.xml"

    req_kind = _requested_kind(base_id)
    # Bare publication root, with any NW token and kind code stripped off.
    root = re.sub(r'(NW)?[AB]\d+$', '', str(base_id).upper())

    order = (["A1", "A2", "A3", "A4", "B1", "B2", "B3"] if prefer == "A"
             else ["B1", "B2", "B3", "A1", "A2", "A3", "A4"])

    # Build the ordered list of kind codes to try: the exact requested kind
    # first, then (unless suppressed) the rest as fallbacks.
    kinds = []
    if req_kind:
        kinds.append(req_kind)
    if kind_fallback or not req_kind:
        for k in order:
            if k not in kinds:
                kinds.append(k)

    test_ids = list(dict.fromkeys(f"{root}NW{k}" for k in kinds))

    for test_id in test_ids:
        file_path = os.path.join(output_directory, f"{test_id}.xml")
        dl_kind = _requested_kind(test_id)
        # Already on disk (prior run or shared family member) -> reuse, no request.
        if os.path.exists(file_path):
            exact = bool(req_kind) and (dl_kind == req_kind)
            return True, test_id, dl_kind, exact
        url = base_url.format(test_id)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                if is_full_xml(response.content):
                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                    time.sleep(8)  # STRICT 8-SECOND THROTTLE AFTER SUCCESS
                    exact = bool(req_kind) and (dl_kind == req_kind)
                    return True, test_id, dl_kind, exact
        except requests.exceptions.RequestException:
            pass

        # STRICT 8-SECOND THROTTLE BETWEEN EPS GUESSES
        time.sleep(8)

    return False, None, None, False


# --- Main Engine ---

def download_eps_xmls_with_ops(csv_filename, consumer_key, consumer_secret,
                               output_directory="eps_xmls"):
    """Download the full-text XML of an entire patent family from EPS.

    For each patent in the input CSV:
      1. Fetch its FULL family from OPS (all members, all countries) via both the
         equivalents service and the family-ID search.
      2. Every NON-EP member goes straight to not_in_eps.csv — only EP
         publications can have full text on the EPO Publication Server (EPS).
      3. Every EP member (any kind code A1/B1/..., any publication number) is
         tested individually on EPS. Members whose full-text XML is present
         (description / claims / tables) are saved into `output_directory`; every
         EP member with no full XML is written to not_in_eps.csv.

    This deliberately does NOT pick a single 'best' relative. Family members, and
    even the A vs B versions of one application, can carry different experimental
    data, and the workflow has not pre-identified which differ — so the whole EP
    family is downloaded and the comparison is left to a later step.

    Outputs:
      <output_directory>/*.xml   one file per EP member that had full text
      successful_downloads.csv   what was saved (member id, kind, requested patent)
      not_in_eps.csv             every member with no EPS full text (for analysis)
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    try:
        df = pd.read_csv(csv_filename, sep=';', dtype=str)
    except FileNotFoundError:
        print(f"Error: The file {csv_filename} was not found.")
        return

    ops = OPSClient(consumer_key, consumer_secret)
    print(f"Starting FULL-FAMILY extraction for {len(df)} patents...")
    print("Downloading every EP family member; logging the rest to not_in_eps.csv.")
    print("Enforcing strictly 8+ second delays between all requests.")
    print("-" * 60)

    downloaded = {}    # keyed by downloaded file id -> row (dedupe identical files)
    not_in_eps = {}    # keyed by member patent id  -> row (dedupe identical members)

    for _, row in df.iterrows():
        raw_id = str(row.get('Patent_ID', '')).strip()
        family_id = str(row.get('Family_ID', '')).strip()
        if not raw_id or raw_id == 'nan':
            continue
        print(f"\nProcessing family of: {raw_id}...")

        # 1) Gather the full family (all countries) from both OPS services.
        members = list(ops.get_equivalents(raw_id))
        if family_id and family_id != 'nan':
            members += ops.get_family_members(family_id)

        # Seed with the requested publication itself (if it is EP).
        req_kind = _requested_kind(raw_id)
        if raw_id.startswith("EP"):
            req_num = re.sub(r'^EP', '', re.sub(r'(NW)?[AB]\d+$', '', raw_id.upper()))
            members.append(("EP", req_num, req_kind or ""))

        members = list(dict.fromkeys(members))  # dedupe (country, number, kind)
        if not members:
            print("  [WARN] OPS returned no family members for this patent.")
            not_in_eps.setdefault(raw_id, {
                "Member_Patent_ID": raw_id, "Country": raw_id[:2],
                "Kind": req_kind or "", "Requested_Patent": raw_id,
                "Family_ID": family_id, "Reason": "No family members returned by OPS"})
            continue

        ep_members = [(c, n, k) for (c, n, k) in members if c == "EP"]
        non_ep     = [(c, n, k) for (c, n, k) in members if c != "EP"]
        # Distinct EP publication NUMBERS. We ignore the single kind code OPS
        # reports per member, because OPS frequently lists only one kind (often
        # the application) even when both an application (A) and a grant (B) were
        # published — and those documents can carry different experimental data.
        ep_numbers = list(dict.fromkeys(n for (c, n, k) in ep_members))
        print(f"  Family: {len(members)} members "
              f"({len(ep_numbers)} EP number(s), {len(non_ep)} non-EP)")

        # 2) Non-EP members can never have EPS full text -> log only.
        for (c, n, k) in non_ep:
            mid = f"{c}{n}{k}"
            not_in_eps.setdefault(mid, {
                "Member_Patent_ID": mid, "Country": c, "Kind": k,
                "Requested_Patent": raw_id, "Family_ID": family_id,
                "Reason": "Non-EP member (no EPS full text)"})

        # 3) For each EP number, probe EVERY kind code on EPS and save ALL that
        #    exist (do NOT stop at the first hit) so both the A application and
        #    the B grant of the same number are retrieved when both are present.
        for n in ep_numbers:
            is_requested = (raw_id.startswith("EP")
                            and re.sub(r'(NW)?[AB]\d+$', '', raw_id.upper()) == f"EP{n}")
            rel = "requested patent" if is_requested else "family member"
            found_kinds = []
            for kind in EP_KIND_CODES:
                success, dl_id, dl_kind, _exact = try_download_ep(
                    f"EP{n}{kind}", output_directory, kind_fallback=False)
                if not success:
                    continue
                found_kinds.append(dl_kind)
                if dl_id not in downloaded:
                    print(f"  [SAVED] {dl_id}.xml  ({rel}, kind {dl_kind})")
                downloaded.setdefault(dl_id, {
                    "Downloaded_File": f"{dl_id}.xml", "Member_Patent_ID": dl_id,
                    "Kind": dl_kind, "Relationship": rel,
                    "Requested_Patent": raw_id, "Family_ID": family_id})
            if not found_kinds:
                print(f"  [NO XML] EP{n} -> no full text on EPS for any kind code (logged).")
                not_in_eps.setdefault(f"EP{n}", {
                    "Member_Patent_ID": f"EP{n}", "Country": "EP", "Kind": "",
                    "Requested_Patent": raw_id, "Family_ID": family_id,
                    "Reason": "EP number: no full XML on EPS for any kind code"})

    # --- Final Reporting ---
    print("\n" + "=" * 60)
    dl_df = pd.DataFrame(list(downloaded.values()))
    miss_df = pd.DataFrame(list(not_in_eps.values()))
    if not dl_df.empty:
        dl_df.to_csv("successful_downloads.csv", sep=";", index=False)
    if not miss_df.empty:
        miss_df.to_csv("not_in_eps.csv", sep=";", index=False)

    print("Full-family extraction complete!")
    print(f"  EP XMLs downloaded:        {len(dl_df)}  -> {output_directory}/")
    print(f"  members not on EPS logged: {len(miss_df)}  -> not_in_eps.csv")
    print("=" * 60)
