# 🧱 Atmos Databricks ELT — Processamento e Arquitetura Medalhão

![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta&logoColor=white)

> **Módulo de Transformação (ELT):** Repositório responsável pela leitura dos dados brutos gravados na *Landing Zone* (ADLS Gen2) pelo Azure Data Factory, aplicando limpeza, enriquecimento e modelagem multidimensional via **PySpark** e **Delta Lake**.

### 🎯 Objetivo

Construir uma camada analítica confiável para dados meteorológicos, consolidando diferentes fontes em um **modelo climático canônico** e disponibilizando métricas prontas para análise no Databricks SQL.

**Principais características:**

- Arquitetura **Medallion (Bronze → Silver → Gold)**
- Processamento incremental com **Auto Loader**
- Armazenamento em **Delta Lake**
- Normalização de fontes com diferentes formatos e granularidades
- Regras de qualidade e deduplicação
- Orquestração com **Databricks Workflows**
- Camada analítica preparada para **BI e Data Storytelling**

---

## 🔗 Conectividade da Arquitetura End-to-End

Este repositório faz parte de um ecossistema *Multi-Cloud*. A etapa de ingestão dos dados originais (REST API e GCP BigQuery) é orquestrada no repositório de infraestrutura:

🔗 **Repositório de Ingestão e Orquestração:** [projeto-atmos-adf](https://github.com/deyvid-martins/projeto-atmos-adf)

---

## 📋 Sumário

- [Visão Geral da Arquitetura](#-visão-geral-da-arquitetura)
- [Estrutura de Diretórios](#-estrutura-de-diretórios)
- [Descrição das Camadas](#-descrição-das-camadas)
  - [Bronze — Ingestão Bruta](#bronze--ingestão-bruta)
  - [Silver — INMET](#silver--inmet)
  - [Silver — Visual Crossing](#silver--visual-crossing)
  - [Silver — Unificado](#silver--unificado)
  - [Gold — Perfil Sazonal Mensal](#gold--perfil-sazonal-mensal)
- [Dashboard & Inteligência de Negócios (Databricks SQL)](#-dashboard--inteligência-de-negócios-databricks-sql)
- [Esquema Canônico Climático](#-esquema-canônico-climático)
- [Stack Tecnológica](#-stack-tecnológica)
- [Fluxo do Pipeline (Databricks Workflow)](#-fluxo-do-pipeline-databricks-workflow)
- [Como Executar e Fazer Deploy](#-como-executar-e-fazer-deploy)
- [Decisões de Design](#-decisões-de-design)
- [Pré-requisitos](#-pré-requisitos)
- [Referências](#-referências)

### 🚀 Visão rápida

**Fluxo principal:** `Landing Zone → Bronze → Silver → Gold → Dashboard`

**Fontes:** INMET + Visual Crossing
**Processamento:** PySpark / Spark SQL
**Storage:** ADLS Gen2 + Delta Lake
**Orquestração:** Databricks Workflows
**BI:** Databricks SQL

---

## 🏗️ Visão Geral da Arquitetura

O **ATMOS** é um pipeline de dados climáticos de nível produtivo construído sobre **Databricks + Azure Data Lake Storage Gen2**. Ele ingere dados meteorológicos do **INMET** (Instituto Nacional de Meteorologia) e da **Visual Crossing**, processando-os através da **Arquitetura Medallion (Bronze → Silver → Gold)**.

O projeto foi desenhado com foco em idempotência, qualidade de dados e um esquema canônico unificado que permite comparar fontes heterogêneas na mesma estrutura.

```text
╔══════════════════════════════════════════════════════════════════╗
║                        FONTES DE DADOS                           ║
╠══════════════════╦═══════════════════════════════════════════════╣
║  INMET (Parquet) ║          Visual Crossing (JSON)               ║
║  Estações + dados║          Histórico / Previsão                  ║
║  horários        ║          Dados diários por cidade             ║
╚══════════════════╩═══════════════════════════════════════════════╝
          │                              │
          └──────────────┬───────────────┘
                         ▼
╔══════════════════════════════════════════════════════════════════╗
║                    CAMADA BRONZE                                 ║
║              Auto Loader (cloudFiles)                            ║
║  ┌─────────────────────────────────────────────────────────┐     ║
║  │  atmos.bronze.inmet_estacao_raw                         │     ║
║  │  atmos.bronze.inmet_microdados_raw                      │     ║
║  │  atmos.bronze.visual_crossing_raw                       │     ║
║  └─────────────────────────────────────────────────────────┘     ║
║       Delta Lake · Particionado por ingestion_date               ║
╚══════════════════════════════════════════════════════════════════╝
                         │
                         ▼
╔══════════════════════════════════════════════════════════════════╗
║                    CAMADA SILVER                                  ║
║          Normalização · Validação · Esquema Canônico             ║
║  ┌─────────────────────────────────────────────────────────┐     ║
║  │  atmos.silver.climate_inmet                             │     ║
║  │  atmos.silver.climate_visual_crossing                   │     ║
║  │  atmos.silver.climate_unified  ◄── unionByName()        │     ║
║  └─────────────────────────────────────────────────────────┘     ║
╚══════════════════════════════════════════════════════════════════╝
                         │
                         ▼
╔══════════════════════════════════════════════════════════════════╗
║                     CAMADA GOLD                                   ║
║              Agregação em dois estágios (CTEs)                   ║
║  ┌─────────────────────────────────────────────────────────┐     ║
║  │  atmos.gold.perfil_sazonal_mensal                       │     ║
║  │  Diário (AVG/MAX/MIN/SUM) → Mensal (perfil climático)   │     ║
║  └─────────────────────────────────────────────────────────┘     ║
╚══════════════════════════════════════════════════════════════════╝
                         │
                         ▼
╔══════════════════════════════════════════════════════════════════╗
║                      DASHBOARD                                    ║
║        Databricks SQL · perfil_sazonal_mensal                    ║
║        Análise Temporal, Sazonalidade e Métricas de KPIs          ║
╚══════════════════════════════════════════════════════════════════╝

````

## 📁 Estrutura de Diretórios

```
projeto-atmos-databricks-elt/
├── 01 - Create Bronze/
│   └── ingest_bronze.ipynb           # Notebook parametrizado de ingestão via Auto Loader
├── 02 - Create Silver/
│   ├── transform_inmet.ipynb         # Normalização e validação dos dados INMET
│   ├── transform_visual_crossing.ipynb  # Parse do JSON aninhado da Visual Crossing
│   └── transform_unified.ipynb       # União das duas fontes Silver
├── 03 - Create Gold/
│   └── perfil_sazonal_mensal.ipynb   # Agregação diária → mensal com CTEs
├── 04 - Dashboard/
│   └── Dashboard ATMOS.lvdash.json   # Definição do dashboard Databricks SQL (Lakeview)
├── 05 - Pipeline/
│   └── Pipeline_ATMOS.yml            # Definição do Databricks Workflow (YAML)
└── README.md

```

## 🔄 Descrição das Camadas

### 🥉 Bronze — Ingestão Bruta

**Notebook:** `01 - Create Bronze/ingest_bronze.ipynb`

A camada Bronze é o ponto de entrada dos dados. O princípio central é a fidelidade ao dado bruto, armazenado no formato Delta Lake com acréscimo de metadados de rastreabilidade.

- **Mecanismo:** Utiliza o **Auto Loader (`cloudFiles`)** para leitura incremental e contínua do ADLS Gen2, garantindo processamento *exactly-once*.
- **Parâmetros:** Recebe `source_system`, `source_path`, `file_format`, `target_table`, `ingestion_date`, `schema_location` e `checkpoint_location`.
- **Metadados:** Adiciona `ingestion_timestamp`, `source_system`, `file_name` e `ingestion_date` (partição).
- **Escrita:** Append via Spark Structured Streaming com `trigger(availableNow=True)`.
- **Tabelas:**
  - `atmos.bronze.inmet_estacao_raw`
  - `atmos.bronze.inmet_microdados_raw`
  - `atmos.bronze.visual_crossing_raw`

### 🥈 Silver — INMET

**Notebook:** `02 - Create Silver/transform_inmet.ipynb`

Transforma os microdados do INMET no esquema canônico climático.

- **Normalização:** Renomeia e ajusta colunas do formato Parquet para o esquema padronizado.
- **Enriquecimento:** Join com `inmet_estacao_raw` para vincular o nome da cidade ao código da estação.
- **Validação de Qualidade:** Regras de negócio desconsideram ou sinalizam medições fora dos intervalos plausíveis (temperaturas extremas, umidade fora de 0-100%, etc.).
- **Deduplicação:** Filtro por chave composta (`data_observacao`, `hora_observacao`, `id_estacao`).
- **Tabela:** `atmos.silver.climate_inmet`

### 🥈 Silver — Visual Crossing

**Notebook:** `02 - Create Silver/transform_visual_crossing.ipynb`

- **Parse de JSON Aninhado:** Extração do array `days` com `from_json` + `explode`.
- **Rastreabilidade de Cidade:** Nome da cidade derivado do nome do arquivo via Expressão Regular (`atmos_[cidade]_*.json`).
- **Mapeamento Canônico:** Alinhamento dos dados diários. Como a fonte fornece granularidade diária, `hora_observacao` é gravada como `null`.
- **Tabela:** `atmos.silver.climate_visual_crossing`

### 🥈 Silver — Unificado

**Notebook:** `02 - Create Silver/transform_unified.ipynb`

- **Operação:** Consolidação via `unionByName()`, alinhando os esquemas canônicos de ambas as fontes. Campos indisponíveis em uma fonte específica permanecem como `null`.
- **Tabela:** `atmos.silver.climate_unified`

### 🥇 Gold — Perfil Sazonal Mensal

**Notebook:** `03 - Create Gold/perfil_sazonal_mensal.ipynb`

A camada Gold entrega o modelo analítico consolidado usando agregação em dois estágios (CTEs):

1. **CTE Diário:** Agrega os dados horários do INMET para médias, máximas, mínimas e somas diárias.
2. **CTE Mensal:** Consolida os totais e médias diárias de ambas as fontes em perfil mensal por cidade e estação.

| **Coluna**                   | **Descrição**                               |
| ---------------------------- | ------------------------------------------- |
| `temperatura_media_c`        | Temperatura média mensal (°C)               |
| `temperatura_maxima_media_c` | Média das temperaturas máximas diárias (°C) |
| `temperatura_minima_media_c` | Média das temperaturas mínimas diárias (°C) |
| `temperatura_maxima_abs_c`   | Temperatura máxima absoluta no mês (°C)     |
| `temperatura_minima_abs_c`   | Temperatura mínima absoluta no mês (°C)     |
| `precipitacao_total_mm`      | Precipitação acumulada no mês (mm)          |
| `umidade_media_pct`          | Umidade relativa média mensal (%)           |
| `pressao_media_hpa`          | Pressão atmosférica média mensal (hPa)      |
| `amplitude_termica_media_c`  | Amplitude térmica média diária (°C)         |
| `dias_com_dados`             | Total de dias com registros válidos no mês  |
| `timestamp_processamento`    | Data/hora do processamento da carga         |

- **Tabela:** `atmos.gold.perfil_sazonal_mensal`

## 📊 Dashboard & Inteligência de Negócios (Databricks SQL)

**Arquivo:** `04 - Dashboard/Dashboard ATMOS.lvdash.json`

O dashboard nativo do Databricks SQL (Lakeview) foi construído sobre a tabela `atmos.gold.perfil_sazonal_mensal` focado em **Data Storytelling, UI/UX e Inteligência de Negócios**.

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                             DASHBOARD EXEC - ATMOS                            ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  [ KPI 1 ]            [ KPI 2 ]          [ KPI 3 ]          [ KPI 4 ]         ║
║  Temp. Média (21.99°) Temp. Max (36.5°) Temp. Min (5.1°)   Chuva (963.2 mm)  ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  SÉRIE TEMPORAL                                                               ║
║  📈 Evolução Histórica das Temperaturas (Máx / Média / Mín)                   ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  ANÁLISE COMBINADA                       TENDÊNCIA ANUAL                      ║
║  📊 Precipitação vs. Umidade por Mês     📊 Temp. Média por Ano (2020-2026)   ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  EXTREMOS E ESTAÇÕES                                                          ║
║  📊 Amplitude Térmica e Extremos Climáticos por Estação                       ║
╚═══════════════════════════════════════════════════════════════════════════════╝

```

### Análise Executiva e Diagnóstico dos Visuais

- **Cartões de KPI (Topo):** Oferecem síntese imediata sobre os limites e médias registradas (Média: `21.99°C`, Recordes: `36.5°C` / `5.1°C`, Acumulados: `963.2 mm` de chuva e `65.93%` de umidade), fornecendo a linha de base analítica.
- **Evolução Histórica (Linha Temporal):** Exibe a variação temporal e os ciclos climáticos ao longo dos anos com marcadores de contexto visual, identificando a transição entre as estações secas e chuvosas de Brasília.
- **Precipitação vs. Umidade por Mês (Combo):** Gráfico combinado com rótulos de dados arredondados e limpos. Deixa clara a **correlação inversa** entre volume de chuva e umidade nos meses de inverno/seca (Maio a Setembro).
- **Temperatura Média por Ano (Barras 2020–2026):** Gráfico de barras com **rótulos visíveis em todas as colunas** (`21.4°C`, `21.2°C`, `22.0°C`...) e eixo Y ajustado a partir de 15°C, permitindo visualizar com precisão tendências de variação térmica anual.
- **Amplitude Térmica por Estação (Grid Inferior):** Detalha a variação entre temperaturas extremas por estação do ano, permitindo auditoria rápida sem depender de interações passivas (*tooltips*).

## 📊 Esquema Canônico Climático

| **Coluna**            | **Tipo** | **Descrição**                                  |
| --------------------- | -------- | ---------------------------------------------- |
| `data_observacao`     | Date     | Data da observação meteorológica               |
| `hora_observacao`     | String   | Hora da observação (`null` para dados diários) |
| `id_estacao`          | String   | Código identificador da estação                |
| `cidade`              | String   | Nome da cidade associada                       |
| `sistema_origem`      | String   | Fonte do dado (`inmet` ou `visual_crossing`)   |
| `temperatura_c`       | Double   | Temperatura média ou corrente (°C)             |
| `temperatura_max_c`   | Double   | Temperatura máxima (°C)                        |
| `temperatura_min_c`   | Double   | Temperatura mínima (°C)                        |
| `ponto_orvalho_c`     | Double   | Ponto de orvalho (°C)                          |
| `umidade_pct`         | Double   | Umidade relativa do ar (%)                     |
| `umidade_max_pct`     | Double   | Umidade relativa máxima (%)                    |
| `umidade_min_pct`     | Double   | Umidade relativa mínima (%)                    |
| `pressao_hpa`         | Double   | Pressão atmosférica (hPa)                      |
| `precipitacao_mm`     | Double   | Precipitação acumulada (mm)                    |
| `radiacao_solar_wm2`  | Double   | Radiação solar global (W/m²)                   |
| `velocidade_vento_ms` | Double   | Velocidade do vento (m/s)                      |
| `rajada_vento_ms`     | Double   | Velocidade da rajada de vento (m/s)            |
| `direcao_vento_graus` | Double   | Direção do vento em graus                      |
| `indice_uv`           | Double   | Índice UV                                      |
| `data_ingestao`       | Date     | Data de ingestão (chave de partição)           |

## 🛠️ Stack Tecnológica

- **Plataforma:** Azure Databricks (Unity Catalog Habilitado)
- **Armazenamento:** Azure Data Lake Storage Gen2 (`abfss://`)
- **Formato de Tabela:** Delta Lake (Transações ACID, Time Travel, Merge/Overwrite)
- **Ingestão Incremental:** Auto Loader (`cloudFiles`)
- **Processamento:** PySpark & Spark SQL
- **Orquestração:** Databricks Workflows (YAML)
- **Visualização:** Databricks SQL Dashboard

## ⚙️ Fluxo do Pipeline (Databricks Workflow)

**Arquivo de definição:** `05 - Pipeline/Pipeline_ATMOS.yml`

O workflow é orquestrado em 8 tarefas encadeadas por dependências:

```
bronze_inmet_estacoes ──┐
                        ├──► silver_inmet ──────────────┐
bronze_inmet_microdados─┘                               │
                                                        ├──► silver_unified ──► gold_perfil_sazonal_mensal ──► Dashboard_ATMOS
bronze_visualcrossing ──────► silver_visualcrossing ────┘

```

| **Task #** | **Task Name**                | **Dependências** | **Notebook Executado**                  |
| ---------- | ---------------------------- | ---------------- | --------------------------------------- |
| **1**      | `bronze_inmet_estacoes`      | —                | `ingest_bronze.ipynb`                   |
| **2**      | `bronze_inmet_microdados`    | —                | `ingest_bronze.ipynb`                   |
| **3**      | `bronze_visualcrossing`      | —                | `ingest_bronze.ipynb`                   |
| **4**      | `silver_inmet`               | Tasks 1 e 2      | `transform_inmet.ipynb`                 |
| **5**      | `silver_visualcrossing`      | Task 3           | `transform_visual_crossing.ipynb`       |
| **6**      | `silver_unified`             | Tasks 4 e 5      | `transform_unified.ipynb`               |
| **7**      | `gold_perfil_sazonal_mensal` | Task 6           | `perfil_sazonal_mensal.ipynb`           |
| **8**      | `Dashboard_ATMOS`            | Task 7           | Atualização do Dashboard Databricks SQL |

*As tasks 1, 2 e 3 são executadas em paralelo para otimização de tempo de cluster.*

## 🚀 Como Executar e Fazer Deploy

### 1. Pré-requisitos de Configuração

- Workspace Databricks com **Unity Catalog**.
- Storage Account no ADLS Gen2 com o container `atmos` e volume `landing`.
- Catálogo `atmos` criado com os schemas `bronze`, `silver` e `gold`.

### 2. Upload dos Dados Brutos

Carregue os arquivos na Landing Zone do Data Lake:

```
/Volumes/atmos/bronze/landing/inmet/estacoes/      ← Parquets de estações
/Volumes/atmos/bronze/landing/inmet/microdados/    ← Parquets de microdados horários
/Volumes/atmos/bronze/landing/visual_crossing/     ← JSONs (atmos_[cidade]_*.json)

```

### 3. Deploy do Workflow via CLI

```
# Autenticar na Databricks CLI
databricks configure --token

# Deploy do pipeline via Databricks Asset Bundles (DABs)
databricks bundle deploy --target production

# Executar o Workflow
databricks workflows run Pipeline_ATMOS

```

### 4. Importar o Dashboard

1. No Databricks, acesse **SQL** → **Dashboards**.
2. Clique em **Import** e selecione o arquivo `04 - Dashboard/Dashboard ATMOS.lvdash.json`.
3. Vincule ao seu SQL Warehouse ativo.

## 🧠 Decisões de Design

1. **Idempotência Garantida:**
   - **Auto Loader com Checkpoints:** Impede o reprocessamento de arquivos já consumidos.
   - **Deduplicação Nativa:** Limpeza via chaves primárias/compostas na camada Silver.
   - **Replace Where / Overwrite por Partição:** Cargas atualizam apenas as partições afetadas na Gold/Silver.
2. **Evolução de Schema Controlada:**
   - Uso do parâmetro `cloudFiles.schemaEvolutionMode = "addNewColumns"` no Auto Loader para tratar novas colunas da fonte sem quebrar o pipeline.
3. **Paridade Matemática (Agregação de 2 Estágios):**
   - Agrega os dados horários do INMET para a granularidade diária em uma CTE antes de uni-los aos dados diários da Visual Crossing, evitando distorções em médias e acumulados mensais.

## ✅ Pré-requisitos

Para operar ou estender este repositório, recomendam-se conhecimentos em:

- **Databricks Lakehouse Platform & Unity Catalog**
- **Apache Spark (PySpark & Spark SQL)**
- **Delta Lake Storage Engine**
- **Databricks Workflows & Asset Bundles (DABs)**
- **Arquitetura Medallion (Bronze/Silver/Gold)**

## 🔐 Observações de Segurança

O README descreve a arquitetura e o fluxo de execução, mas **não deve conter credenciais, tokens, connection strings, chaves de API ou segredos do ambiente Azure/Databricks**. Esses valores devem ser configurados por mecanismos seguros de autenticação e gerenciamento de secrets.

---

## 📚 Referências

- [Databricks Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [Databricks Workflows CI/CD](https://docs.databricks.com/workflows/index.html)
- [INMET — Instituto Nacional de Meteorologia](https://portal.inmet.gov.br/)
- [Visual Crossing Weather API](https://www.visualcrossing.com/weather-api)
