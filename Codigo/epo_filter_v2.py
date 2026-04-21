import pandas as pd
import numpy as np

def apply_filters(raw_data: pd.DataFrame,
                  output_filename: str = 'EPO_siRNA_FILTERED_FINAL.csv') -> pd.DataFrame:
    """
    Classifica e organiza patents em secções separadas por linhas dummy.
    NUNCA elimina patents — tudo fica no CSV para curação manual.
    
    Secções (por ordem no CSV):
      1. TIER 1  — siRNA confirmado por texto + CPC/IPC  (máxima confiança)
      2. TIER 2  — siRNA confirmado por texto apenas      (alta confiança)
      3. TIER 3  — siRNA confirmado por CPC/IPC apenas    (rever abstract em falta)
      4. TIER 4A — Mixed Tech (siRNA + termo proibido)    (rever manualmente)
      5. TIER 4B — Diagnostic/Biomarker only              (rever manualmente)
      6. TIER 5  — Agri/Vet/Pest com âncora CPC           (rever: pode ser válido)
      7. TIER 6  — Sem sinal siRNA, sem CPC âncora        (provável irrelevante)
      8. TIER 7  — Agri/Vet/Pest sem âncora CPC           (provável irrelevante)
    """
    print("\n=== STARTING LOCAL CLASSIFICATION ===")
    print("[INFO] No patents will be deleted — all go to CSV for manual curation.")

    if raw_data.empty:
        print("[ERROR] Input DataFrame is empty.")
        return pd.DataFrame()

    df = raw_data.copy()
    initial_count = len(df)
    
    # =========================================================================
    # CORREÇÃO BUG #2 — CONSTRUÇÃO DO ESPACENET LINK
    # =========================================================================
    if all(c in df.columns for c in ['Country', 'Number', 'Kind']):
        df['Espacenet_Link'] = (
            "https://worldwide.espacenet.com/publicationDetails/biblio?CC="
            + df['Country'].fillna('').astype(str) + "&NR=" + df['Number'].fillna('').astype(str)
            + "&KC=" + df['Kind'].fillna('').astype(str) + "&FT=D"
        )
    else:
        df['Espacenet_Link'] = ''

    df['Search_Text'] = df['Title'].fillna('') + ' ' + df['Abstract'].fillna('')

    # =========================================================================
    # MELHORIA #2 — FLAG DE REVISÃO MANUAL (Caracteres Não-Latinos + Sem Abstract)
    # =========================================================================
    mask_no_abstract = (
        df['Abstract'].fillna('').str.contains('No abstract available', case=False) | 
        (df['Abstract'].fillna('').str.strip() == '')
    )
    # [^\x00-\x7F] deteta qualquer caractere fora do espetro ASCII normal (ex: Kanji, Cirílico)
    mask_non_latin_title = df['Title'].fillna('').str.contains(r'[^\x00-\x7F]', regex=True)
    
    df['Needs_Espacenet_Review'] = mask_no_abstract & mask_non_latin_title

    # =========================================================================
    # SINAL 1 — ÂNCORA CPC/IPC (C12N15/113 = siRNA por definição taxonómica)
    # =========================================================================
    mask_anchor = (
        df['CPCs'].fillna('').str.contains(r'C12N15/113', regex=False) |
        df['IPCs'].fillna('').str.contains(r'C12N15/113', regex=False)
    )

    # =========================================================================
    # SINAL 2 — TEXTO siRNA (título + abstract)
    # =========================================================================
    sirna_patterns = [
        r'\bsiRNA\b', r'\bsi-RNA\b', r'\bRNAi\b', r'\bdsRNA\b',
        r'\bdsRNAi\b', r'\bds-RNA\b', r'\bRISC\b', r'\bDicer\b',
        r'small\s+interfering\s+R[Nn][Aa]',
        r'short\s+interfering\s+R[Nn][Aa]',
        r'RNA\s+interference',
        r'RNA[\s-]induced\s+silenc',
        r'double[\s-]stranded\s+R[Nn][Aa]',
        r'double[\s-]stranded\s+ribonucleic',
        r'small\s+interfering\s+ribonucleic',
        r'short\s+interfering\s+ribonucleic',
        r'ribonucleic\s+acid\s+interference',
        r'interfering\s+ribonucleic',
        r'\bRNA\s+duplex\b',
        r'RNAi\s+(agent|molecule|therapeuti|oligonucleotide)',
        r'siRNA\s+(agent|molecule|therapeuti|delivery|construct)',
        r'gene\s+silenc\w+',
        r'RNA\s+silenc\w+',
        r'post.transcriptional\s+silenc',
        r'sequence.specific\s+silenc',
    ]
    regex_sirna = '|'.join(sirna_patterns)
    mask_sirna_text = df['Search_Text'].str.contains(
        regex_sirna, case=False, na=False, regex=True
    )

    # =========================================================================
    # SINAL 3 — TERMOS PROIBIDOS (MELHORIA #3: ASO Restrito)
    # =========================================================================
    forbidden_terms = (
        r'\b('
        r'CRISPR|Cas9|Cas12|Cas13|'
        r'antisense|ASO\s*(?:therapy|treatment|oligonucleotide)\b|' # ← ASO corrigido aqui
        r'microRNA|miRNA|miR-\d+|'
        r'circRNA|circular\s+RNA|'
        r'aptamer|'
        r'mRNA\s+vacc|mRNA\s+therap'
        r')\b'
    )
    mask_forbidden = df['Search_Text'].str.contains(
        forbidden_terms, case=False, na=False, regex=True
    )

    # =========================================================================
    # SINAL 4 — DIAGNÓSTICO PURO (MELHORIA #1)
    # =========================================================================
    diagnostic_only = (
        r'\b(biomarker|prognostic\s+marker|diagnostic\s+kit|detection\s+method|'
        r'assay\s+kit|sequencing\s+method|ELISA\s+kit)\b'
    )
    mask_diag = df['Search_Text'].str.contains(diagnostic_only, case=False, na=False, regex=True)
    mask_no_treatment = ~df['Search_Text'].str.contains(
        r'\b(treat|therap|drug|medicine|medicament)\b', case=False, na=False, regex=True
    )
    mask_pure_diag = mask_diag & mask_no_treatment

    # =========================================================================
    # SINAL 5 — AGRI/VET/PEST
    # =========================================================================
    agri_terms = (
        r'\b('
        r'crop|weed|maize|corn(?!ea)|soybean|wheat|barley|tobacco|'
        r'tomato(?!\s+cell)|potato|cotton|canola|rapeseed|sugarcane|'
        r'Arabidopsis|thaliana|'
        r'herbicide|pesticide|insecticide|fungicide|nematicide|'
        r'nematode|aphid|whitefly|thrips|locust|'
        r'livestock|poultry|swine|porcine|ovine|equine|caprine|'
        r'cattle|sheep(?!\s+cell)|goat|turkey|aquaculture|'
        r'Fusarium|Botrytis|Phytophthora|Pythium|powdery\s+mildew|'
        r'Agrobacterium|biopesticide|pest\s+control|biocontrol|'
        r'filamentous\s+fung|yeast\s+strain|fungal\s+strain|'
        r'Aspergillus|Trichoderma|Neurospora|Pichia\s+pastoris|'
        r'Saccharomyces\s+cerevisiae|'
        r'C\.\s*elegans|Caenorhabditis|'
        r'Drosophila\s+melanogaster|'
        r'Xenopus\s+laevis|'
        r'insect\s+cell(?!s?\s+line)|'
        r'Sf9\s+cell|baculovirus\s+express'
        r')\b'
    )

    mask_agri = df['Search_Text'].str.contains(
        agri_terms, case=False, na=False, regex=True
    )

    # =========================================================================
    # CLASSIFICAÇÃO EM TIERS
    # =========================================================================
    conditions = [
        # TIER 1: texto siRNA + âncora CPC/IPC
        (mask_sirna_text & mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),
        # TIER 2: texto siRNA, sem CPC âncora
        (mask_sirna_text & ~mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),
        # TIER 3: só CPC âncora, sem texto
        (~mask_sirna_text & mask_anchor & ~mask_forbidden & ~mask_agri & ~mask_pure_diag),
        # TIER 4A: siRNA confirmado MAS tem termos proibidos
        (mask_sirna_text & mask_forbidden & ~mask_pure_diag),
        # TIER 4B: Diagnóstico puro sem tratamento (NOVO)
        (mask_pure_diag),
        # TIER 5: agri MAS tem âncora CPC
        (mask_agri & mask_anchor & ~mask_pure_diag),
        # TIER 6: sem sinal siRNA, sem âncora
        (~mask_sirna_text & ~mask_anchor & ~mask_agri & ~mask_pure_diag),
        # TIER 7: agri sem âncora
        (mask_agri & ~mask_anchor & ~mask_pure_diag),
    ]
    
    tier_labels = [
        'TIER 1 — siRNA Confirmed (Text + CPC)',
        'TIER 2 — siRNA Confirmed (Text only)',
        'TIER 3 — siRNA by CPC only (Check Abstract)',
        'TIER 4A — Mixed Tech (siRNA + Forbidden Term)',
        'TIER 4B — Diagnostic/Biomarker only (Review)',
        'TIER 5 — Agri/Vet with siRNA CPC (Review)',
        'TIER 6 — No siRNA Signal (Likely Irrelevant)',
        'TIER 7 — Agri/Vet without siRNA (Likely Irrelevant)',
    ]
    
    df['Tier'] = 'UNCLASSIFIED'
    # Aplicar de trás para a frente para que a prioridade mais alta (índice 0) ganhe
    for cond, label in zip(reversed(conditions), reversed(tier_labels)):
        df.loc[cond, 'Tier'] = label

    df = df.drop(columns=['Search_Text'], errors='ignore')

    # =========================================================================
    # CONSTRUÇÃO DO CSV COM DUMMY SEPARATORS
    # =========================================================================
    cols = [
        'Patent_ID', 'Priority_Date', 'Publication_Date', 'Applicant',
        'Title', 'Abstract', 'Espacenet_Link',
        'Tier', 'Needs_Espacenet_Review', 'Warning', # ← Nova Flag de Revisão
        'IPCs', 'CPCs', 'Family_ID'
    ]
    
    cols = [c for c in cols if c in df.columns]

    sections = []
    for label in tier_labels:
        tier_df = df[df['Tier'] == label].copy()
        if tier_df.empty:
            continue
        # Linha dummy de separação
        dummy = {c: '' for c in cols}
        dummy['Patent_ID'] = f'--- {label.upper()} ({len(tier_df)} patents) ---'
        dummy_df = pd.DataFrame([dummy])
        sections.append(dummy_df)
        sections.append(tier_df[cols])

    final_df = pd.concat(sections, ignore_index=True)
    final_df.to_csv(output_filename, index=False, sep=';', encoding='utf-8-sig')

    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print(f"\n{'='*50}")
    print(f"[SUCCESS] Classification complete.")
    print(f"  Total input patents: {initial_count}")
    print()
    for label in tier_labels:
        n = (df['Tier'] == label).sum()
        bar = '█' * (n // 100)
        print(f"  {label[:45]:<45} {n:>5}  {bar}")
    print()
    
    # Aviso de patentes ilegíveis
    needs_review = df['Needs_Espacenet_Review'].sum()
    if needs_review > 0:
        print(f"  [!] {needs_review} patents flagged as Needs_Espacenet_Review (Unreadable text/abstract)")
        print()
        
    print(f"  Saved to: {output_filename}")
    print(f"{'='*50}")

    return final_df