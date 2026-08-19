
# 🧱 Atmos Databricks ELT — Processamento e Arquitetura Medalhão

![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta&logoColor=white)

> **Módulo de Transformação (ELT):** Repositório responsável pela leitura dos dados brutos gravados na *Landing Zone* (ADLS Gen2) pelo Azure Data Factory, aplicando limpeza, enriquecimento e modelagem multidimensional via **PySpark** e **Delta Lake**.

---

## 🔗 Conectividade da Arquitetura End-to-End

Este repositório faz parte de um ecossistema *Multi-Cloud*. A etapa de ingestão dos dados originais (REST API e GCP BigQuery) é orquestrada no repositório de infraestrutura:

🔗 **Repositório de Ingestão e Orquestração:** [projeto-atmos-adf](https://github.com/deyvid-martins/projeto-atmos-adf)

---

## 📐 Arquitetura do Processamento (Medallion Architecture)

```text
   ADLS Gen2 (Landing Zone) 
               │
               ▼
┌─────────────────────────────┐
│        CAMADA BRONZE        │ ➔ Ingestão raw e persistência em formato Delta Lake.
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│        CAMADA SILVER        │ ➔ Limpeza, casting de tipos, tratamento de nulos e deduplicação.
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│         CAMADA GOLD         │ ➔ Modelagem Star Schema (Tabelas Fato e Dimensão para Analytics).
└─────────────────────────────┘

```

---

## 🛠️ Tecnologias e Conceitos Aplicados

* **Engine de Processamento:** Azure Databricks (PySpark / Spark SQL)
* **Formatos & Armazenamento:** Delta Lake (Garantia de transações ACID e Time Travel)
* **Modelagem de Dados:** Star Schema / Kimball (Fatos e Dimensões)
* **Fontes de Dados Consumidas:**
* Microdados do INMET (Formato Parquet)
* Previsões e histórico diário da API Visual Crossing (Formato JSON)



---

## 📋 Planejamento de Execução (Roadmap)

* [ ] Configuração de acesso seguro ao ADLS Gen2 via *Service Principal / Key Vault*.
* [ ] Criação do pipeline de carga da **Camada Bronze** (Raw Delta Tables).
* [ ] Modelagem, padronização de tipos e higienização na **Camada Silver**.
* [ ] Construção dos Data Marts de clima na **Camada Gold**.

```

```
