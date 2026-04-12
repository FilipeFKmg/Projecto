"""
Patent Analysis and Visualization Module
----------------------------------------
Analyzes patent datasets to generate chronological timelines and consolidate 
top applicant data (companies/institutions). 

Includes preprocessing steps (entity resolution, date standardization, deduplication) 
to ensure accurate metrics. Outputs are saved locally as aggregated CSVs and PDFs.

Dependencies: pandas, matplotlib, unicodedata, re, os
"""

import pandas as pd
import matplotlib.pyplot as plt
import unicodedata
import re
import os

def remove_accents(text):
    """
    Normalizes a string by removing diacritics and special characters.
    Crucial for entity resolution to ensure accurate grouping (e.g., 'È' -> 'E').
    
    Args:
        text (str): Input string.
        
    Returns:
        str: ASCII-encoded string without accents, or empty string if NaN.
    """
    if pd.isna(text) or text == 'nan':
        return ""
    
    # Decompose characters (e.g., 'é' into 'e' + accent), drop non-ASCII, and decode
    text = unicodedata.normalize('NFD', str(text))
    text = text.encode('ascii', 'ignore').decode("utf-8")
    return str(text)


def generate_timeline(file_path, start_year=2000, end_year=2026):
    """
    Generates a chronological timeline of unique patent families.
    Outputs a summary CSV and a bar chart PDF to the working directory.

    Args:
        file_path (str): Path to the source CSV.
        start_year (int): Start of analysis window.
        end_year (int): End of analysis window.
    """
    print(f"\n[INFO] Generating timeline for {file_path} ({start_year}-{end_year})...")
    
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return

    # Load as strings to prevent automatic date formatting or dropping leading zeros
    df = pd.read_csv(file_path, sep=';', dtype=str)
    priority_col = 'Priority_Date'
    
    if priority_col not in df.columns or 'Publication_Date' not in df.columns:
        print("[ERROR] Required date columns not found in the CSV.")
        return

    # Count unique inventions (patent families), not individual national filings
    df_unique = df.drop_duplicates(subset=['Family_ID']).copy()
    
    # --- Date Extraction ---
    # Extract the 4-digit year from the priority date
    df_unique['Year'] = df_unique[priority_col].fillna('').astype(str).str[:4]
    
    # Fallback: Use publication date if priority date is missing
    df_unique.loc[df_unique['Year'] == '', 'Year'] = df_unique['Publication_Date'].astype(str).str[:4]
    
    # Filter for strictly 4-digit year strings, then convert to numeric bounds
    df_unique = df_unique[df_unique['Year'].str.match(r'^\d{4}$', na=False)]
    df_unique['Year_Int'] = pd.to_numeric(df_unique['Year'], errors='coerce')
    df_unique = df_unique[(df_unique['Year_Int'] >= start_year) & (df_unique['Year_Int'] <= end_year)]
    
    # --- Aggregation & Export ---
    timeline = df_unique.groupby('Year').size().reset_index(name='Number_of_Patents')
    timeline = timeline.sort_values('Year')
    
    if timeline.empty:
        print(f"[WARNING] No valid data in window ({start_year}-{end_year}).")
        return
        
    csv_filename = f"timeline_years_{file_path}"
    timeline.to_csv(csv_filename, index=False, sep=';', encoding='utf-8-sig')
    
    # --- Plotting ---
    plt.figure(figsize=(12, 6))
    plt.bar(timeline['Year'], timeline['Number_of_Patents'], color='#1f77b4', edgecolor='black', zorder=3)
    
    plt.title(f'Evolution of Patents by Priority Year ({start_year}-{end_year})', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Priority Year', fontsize=12, fontweight='bold')
    plt.ylabel('Number of Patent Families', fontsize=12, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=11, fontweight='bold')
    plt.yticks(fontsize=11, fontweight='bold')
    plt.grid(axis='y', linestyle='--', alpha=0.7, zorder=0)
    plt.tight_layout()
    
    pdf_filename = f"chart_timeline_{file_path.replace('.csv', '.pdf')}"
    plt.savefig(pdf_filename, format='pdf', bbox_inches='tight')
    plt.close()
    
    print(f"[SUCCESS] Timeline generated. CSV: '{csv_filename}' | PDF: '{pdf_filename}'")


def calculate_top_applicants(file_path, top_n=100):
    """
    Cleans, consolidates, and ranks top patent applicants via entity resolution.
    Addresses typos, subsidiaries, and multiple assignees. 
    Outputs a CSV and a horizontal bar chart PDF.

    Args:
        file_path (str): Path to the source CSV.
        top_n (int): Number of top applicants to chart/export.
        
    Returns:
        pd.DataFrame: Aggregated top applicants dataframe (or None on failure).
    """
    print(f"\n[INFO] Calculating Top {top_n} Applicants for {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return None

    df = pd.read_csv(file_path, sep=';', dtype=str)
    priority_col = 'Priority_Date'
    expanded_records = []
    
    # --- Entity Resolution Dictionaries ---
    # Prevents individual scientists from appearing as corporate entities
    inventors_to_ignore = [
        "IWAMOTO NAOKI", "LIU WEI", "LUU KHOA", "MARAPPAN", "VARGEESE", 
        "OWEN ADRIANA", "YANG HSIU", "LAMATTINA", "BYRNE", "SINGH KULDEEP", 
        "SHAH HIMALI", "KAWAMOTO", "GHOSH", "HAEGELE", "HU YANBIN", "HE HUIJUN", 
        "LU JIANYU", "HE HAIYING", "CHEN SHUHUI", "GLEBOCKA", "KLOSSOWSKI", 
        "SHUVAEV", "DESAI JIGAR", "PRAKASHA PRIYANKA", "LONGO KENNETH", "DESAI", 
        "LONGO", "PRAKASHA", "PEI TAO", "BLOKHIN ANDREI", "CHEN JING", 
        "ENDEAN THOMAS", "BENSON JONATHAN", "KANDASAMY", "LU GENLIANG", 
        "KUMARASAMY", "CHATTERJEE"
    ]
    
    # Maps variations, subsidiaries, or branches to a unified parent entity
    unification_dict = {
        "WISCONSIN": "MEDICAL COLLEGE OF WISCONSIN", "MASSACHUSETTS": "UNIVERSITY OF MASSACHUSETTS",
        "BRITISH COLUMBIA": "UNIVERSITY OF BRITISH COLUMBIA", "PEKING": "PEKING UNIVERSITY",
        "BEIJING": "PEKING UNIVERSITY", "ANTWERP": "UNIVERSITY OF ANTWERP",
        "LIEGE": "UNIVERSITY OF LIEGE", "VIRGINIA": "UNIVERSITY OF VIRGINIA",
        "TEXAS": "UNIVERSITY OF TEXAS", "TORONTO": "UNIVERSITY OF TORONTO",
        "CALIFORNIA": "UNIVERSITY OF CALIFORNIA", "FLORIDA": "UNIVERSITY OF FLORIDA",
        "JOHNS HOPKINS": "JOHNS HOPKINS UNIVERSITY", "TOKYO MEDICAL": "TOKYO MEDICAL UNIVERSITY",
        "KOBE": "KOBE UNIVERSITY", "DALIAN MINZU": "DALIAN MINZU UNIVERSITY",
        "XUZHOU": "XUZHOU MEDICAL UNIVERSITY", "DANA FARBER": "DANA FARBER CANCER INSTITUTE",
        "KOREA INST": "KOREA INSTITUTE OF SCIENCE AND TECHNOLOGY", "SUNGKYUNKWAN": "SUNGKYUNKWAN UNIVERSITY",
        "DELAWARE": "DELAWARE STATE UNIVERSITY", "DELFT": "DELFT UNIVERSITY OF TECHNOLOGY",
        "ULSAN": "ULSAN NATIONAL INSTITUTE OF SCIENCE AND TECHNOLOGY", "TOKAI": "TOKAI NATIONAL HIGHER EDUCATION AND RESEARCH SYSTEM",
        "UNIVERSITY NANTONG": "NANTONG UNIVERSITY", "UNIVERSITY NORTHWESTERN POLYTECHNICAL": "NORTHWESTERN POLYTECHNICAL UNIVERSITY",
        "UNIVERSITY XIAMEN": "XIAMEN UNIVERSITY", "UNIVERSITY HEBEI TECHNOLOGY": "HEBEI UNIVERSITY OF TECHNOLOGY",
        "UNIVERSITY EAST CHINA NORMAL": "EAST CHINA NORMAL UNIVERSITY", "UNIVERSITY NANJING AGRICULTURAL": "NANJING AGRICULTURAL UNIVERSITY",
        "UNIVERSITY SOUTH CHINA AGRICULT": "SOUTH CHINA AGRICULTURAL UNIVERSITY", "UNIVERSITY KYOTO": "KYOTO UNIVERSITY",
        "CARDIOLOGIE DE MONTREAL": "MONTREAL HEART INSTITUTE", "NATIONWIDE CHILDREN": "NATIONWIDE CHILDRENS HOSPITAL",
        "CHILDRENS MEDICAL": "CHILDRENS HOSPITAL MEDICAL CENTER", "CHILDRENS HOSPITAL MED": "CHILDRENS HOSPITAL MEDICAL CENTER",
        "GENOME RES": "GENOME RESEARCH", "CMS RES": "CMS RESEARCH & DEVELOPMENT",
        "LILLY": "ELI LILLY", "EXORNA": "EXORNA BIOSCIENCE",
        "RONA": "RONA THERAPEUTICS", "BISIRNA": "BISIRNA THERAPEUTICS",
        "JENKEM": "JENKEM TECHNOLOGY", "QILU": "QILU PHARMACEUTICAL",
        "ANSHUN": "SHANDONG ANSHUN PHARMACEUTICAL", "CHIA TAI": "CHIA TAI TIANQING PHARMACEUTICAL GROUP",
        "ALNYLAM": "ALNYLAM PHARMACEUTICALS", "ARROWHEAD": "ARROWHEAD PHARMACEUTICALS",
        "SUZHOU RIBO": "SUZHOU RIBO LIFE SCIENCE", "RIBO LIFE SCIENCE": "SUZHOU RIBO LIFE SCIENCE",
        "DYNE": "DYNE THERAPEUTICS", "TUOJIE": "TUOJIE BIOTECH SHANGHAI",
        "BEBETTER": "BEBETTER MED", "YINGTE": "YINGTE MED TECH",
        "TAKEDA": "TAKEDA PHARMACEUTICALS", "JANSSEN": "JANSSEN PHARMACEUTICALS",
        "MPEG LA": "MPEG LA", "SRINA": "SIRNA THERAPEUTICS", 
        "SIRNA THERAPEUTICS": "SIRNA THERAPEUTICS", "REGENERON": "REGENERON PHARMACEUTICALS",
        "HANSOH": "HANSOH PHARMACEUTICAL", "NOVO NORDISK": "NOVO NORDISK",
        "OLIX": "OLIX PHARMACEUTICALS", "FRANCAISE CONTRE LES MYOPATHIES": "ASSOCIATION FRANCAISE CONTRE LES MYOPATHIES",
        "HUMANWELL": "HUMANWELL HEALTHCARE GROUP"
    }
    
    # --- Processing ---
    for index, row in df.iterrows():
        family_id = str(row.get('Family_ID', ''))
        priority_date = str(row.get(priority_col, '')) if str(row.get(priority_col, '')) != 'nan' else ""
            
        if pd.isna(row.get('Applicant')) or str(row.get('Applicant')) == 'nan':
            continue
            
        # Split co-assigned applicants
        raw_applicants = str(row['Applicant']).split('|')
        unique_applicants_in_patent = set()
        
        for req in raw_applicants:
            req = req.strip().upper()
            
            # Text Normalization Pipeline
            req = re.sub(r'\[.*?\]', '', req) # Remove bracketed country codes
            req = remove_accents(req)
            req = req.replace(',', '').replace('.', '').replace('(', '').replace(')', '').replace('-', ' ').replace("'", "").replace("&", "AND")
            req = re.sub(r'^THE\s+', '', req)
            req = re.sub(r'\bUNIV\b', 'UNIVERSITY', req)
            
            # Remove corporate suffixes to group regional structures together
            suffixes = r'\b(INC|INCORPORATED|CORP|CORPORATION|L L C|LLC|LTD|LIMITED|CO|COMPANY|GMBH|SA|PLC|NV|A/S|AG|PTY|PTE|AND)\b'
            req = re.sub(suffixes, '', req)
            req = re.sub(r'\s+', ' ', req).strip() 
            
            # Skip invalid entries or known individuals
            if not req or not re.search('[A-Z]', req) or any(inv in req for inv in inventors_to_ignore):
                continue
            
            # Apply unification mapping
            for key, value in unification_dict.items():
                if key in req:
                    req = value
                    break 
            
            unique_applicants_in_patent.add(req)
            
        # Assign separate rows for co-owners to ensure fair patent counting
        for app in unique_applicants_in_patent:
            expanded_records.append({
                'Family_ID': family_id, 
                'Company': app,
                'Earliest_Priority_Date': priority_date
            })
            
    df_exp = pd.DataFrame(expanded_records)
    
    if df_exp.empty:
        print("[WARNING] Could not consolidate applicants. Dataset may be empty.")
        return None

    df_exp['Earliest_Priority_Date'] = df_exp['Earliest_Priority_Date'].replace('', None)
    
    # --- Aggregation & Export ---
    grouped = df_exp.groupby('Company').agg(
        Number_of_Patents=('Family_ID', 'nunique'),
        Earliest_Innovation=('Earliest_Priority_Date', 'min') 
    ).reset_index()
    
    df_top = grouped.sort_values('Number_of_Patents', ascending=False).head(top_n)
    
    if df_top.empty:
        print("[WARNING] No valid data found for Top Applicants.")
        return df_top

    csv_filename = f'top{top_n}_{file_path}'
    df_top.to_csv(csv_filename, index=False, sep=';', encoding='utf-8-sig')
    
    # --- Plotting ---
    df_for_plot = df_top.sort_values('Number_of_Patents', ascending=True)
    dynamic_height = max(8, top_n * 0.25)

    plt.figure(figsize=(10, dynamic_height))
    plt.barh(df_for_plot['Company'], df_for_plot['Number_of_Patents'], color='#ff7f0e', edgecolor='black', zorder=3)
    
    plt.title(f'Top {top_n} Applicants by Patent Families', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Number of Unique Patent Families', fontsize=12, fontweight='bold')
    plt.yticks(fontsize=12, fontweight='bold') 
    plt.xticks(fontsize=11, fontweight='bold')
    plt.grid(axis='x', linestyle='--', alpha=0.7, zorder=0)
    plt.tight_layout()
    
    pdf_filename = f"chart_top_applicants_{file_path.replace('.csv', '.pdf')}"
    plt.savefig(pdf_filename, format='pdf', bbox_inches='tight')
    plt.close() 
    
    print(f"[SUCCESS] Top {top_n} generated. CSV: '{csv_filename}' | PDF: '{pdf_filename}'\n")
    
    return df_top