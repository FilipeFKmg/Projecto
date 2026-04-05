# Projecto

Este repositório contém uma pipeline automatizada para a extração, curadoria e harmonização de metadados de patentes provenientes do **European Patent Office (EPO)**. O projeto foca-se no panorama competitivo de terapias baseadas em RNA de interferência (siRNA/RNAi).

## Contexto do Projecto
O objetivo é identificar tendências de inovação e principais intervenientes (Key Opinion Leaders/Companies) no setor farmacêutico de siRNA. A pipeline resolve problemas críticos de mineração de dados em patentes, como a expiração de tokens de API e a inconsistência na nomenclatura de empresas e universidades.
---

## Estrutura do Repositório

Conforme visível na pasta `/Codigo`, o projeto organiza-se da seguinte forma:

* **`codigoEPO.ipynb`**: Notebook principal contendo as duas fases da pipeline:
    1.  *Extração*: Consulta à API OPS (Open Patent Services) via OAuth 2.0.
    2.  *Harmonização*: Script de limpeza e consolidação de nomes (NLP básico).
* **`dataset_siRNA_2026.csv`**: Dados bibliográficos brutos extraídos.
* **`top100_dataset_siRNA_2026.csv`**: Ficheiro pronto para análise estatística e visualização em R.

---

## Funcionalidades Técnicas

### 1. Extração de Dados (API OPS-EPO)
* **Queries Complexas (CQL)**: Pesquisa estruturada em Títulos, Resumos e Reivindicações, integrada com filtros de classificação CPC (C12N, A61K).
* **Gestão de Sessão**: Implementação de renovação proativa de Tokens OAuth 2.0 (evitando interrupções em extrações de larga escala).
* **Paginação e Batching**: Tratamento de limites de resposta da API (100 hits/página) e pedidos detalhados em lotes de 50.

### 2. Curadoria e Harmonização (Data Cleaning)
* **Filtro Lexical Negativo**: Exclusão sistemática de patentes de âmbito veterinário, agrícola ou entomológico através de RegEx (Expressões Regulares).
* **Normalização de Entidades**: 
    * Unificação de nomes institucionais (ex: "PEKING UNIVERSITY" vs "BEIJING UNIV").
    * Remoção de acentos e limpeza de sufixos jurídicos (Inc, Ltd, Gmbh).
    * Exclusão de inventores individuais que constam no campo de Requerente.
* **Deduplicação por Família**: Redução de redundância global mantendo apenas um representante por família de patentes.

---
