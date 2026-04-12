# Projecto

Este repositório contém uma pipeline automatizada para a extração, curadoria e harmonização de metadados de patentes provenientes do **European Patent Office (EPO)**. O projeto foca-se no panorama competitivo de terapias baseadas em RNA de interferência (siRNA/RNAi).

---

## Contexto do Projecto

O objetivo é identificar tendências de inovação e principais intervenientes (Companies) no setor farmacêutico de siRNA. A pipeline resolve problemas críticos de mineração de dados em patentes, como a expiração de tokens de API e a inconsistência na nomenclatura de empresas e universidades.

---

## Estrutura do Repositório

Conforme visível na pasta `/Codigo`, o projeto organiza-se da seguinte forma:

* **`codigoEPO.ipynb`**: Notebook principal contendo as duas fases da pipeline:
    1.  *Extração*: Consulta à API OPS (Open Patent Services) via OAuth 2.0.
    2.  *Harmonização*: Script de limpeza e consolidação de nomes (NLP básico).
* **`dataset_siRNA_2026.csv`**: Dados bibliográficos brutos extraídos.
* **`top100_dataset_siRNA_2026.csv`**: Ficheiro pronto para análise estatística e visualização em R.
---
