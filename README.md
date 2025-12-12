# Umane Data Lake — Pipeline Monday → AWS S3

## 🚀 Visão Geral

Este repositório implementa um **pipeline de dados completo e incremental** para ingestão, tratamento e organização de dados da plataforma **Monday.com** em um **Data Lake na AWS (S3)**, estruturado em três camadas clássicas:

- **Bronze** → dados brutos extraídos da API (JSON)
- **Silver** → dados normalizados e tabulares (Parquet)
- **Gold** → datasets analíticos curados, prontos para BI e análises

Arquitetura lógica:

```
Monday API → Bronze (S3) → Silver (S3) → Gold (S3) → Analytics / BI
```

O pipeline suporta **múltiplos boards do Monday**, mantendo rastreabilidade, incrementalidade e chaves estáveis para integração entre tabelas.

---

## 📂 Estrutura do Repositório

```
umane-datalake/
│
├── pyproject.toml              # Configuração de build e dependências
├── README.md
├── LICENSE
│
├── src/
│   └── umane_datalake/
│       ├── __init__.py
│       ├── config.py               # Variáveis de ambiente e configurações globais
│       ├── monday_client.py        # Cliente GraphQL da Monday (extração + paginação)
│       ├── s3_client.py            # Utilitários de leitura/escrita no S3
│       ├── transformacao.py        # Bronze → Silver (flatten, normalização)
│       ├── transformacao_ouro.py   # Silver → Gold (curadoria, chaves estáveis)
│       └── pipeline.py             # Orquestrador principal do pipeline
│
└── venv/                       # Ambiente virtual (não versionado)
```

---

## 🧠 Principais Funcionalidades

### 🔹 1. Extração da API Monday (GraphQL)
- Leitura de múltiplos boards
- Paginação automática (`items_page`)
- Suporte a colunas simples e complexas (mirror, relations, subtasks)
- Salvamento dos dados brutos na camada **Bronze (JSON)**

---

### 🔹 2. Bronze → Silver
Executado via `transformacao.py`:

- Processamento **incremental** (por timestamp)
- Conversão automática de JSON para DataFrame
- Flatten das colunas do Monday
- Prevenção de duplicidade de nomes
- Inclusão da coluna de rastreabilidade `board_origem`
- Salvamento em **Parquet** (otimizado para analytics)

---

### 🔹 3. Silver → Gold
Executado via `transformacao_ouro.py`:

- Normalização de nomes de colunas (snake_case, sem acentos)
- Criação de **chave de negócio do projeto**
- Geração de **`id_projeto` estável (UUID5)** para JOIN entre boards
- Conversão de campos monetários concatenados (`"10 | 20"`)
- Dataset final pronto para BI, SQL e dashboards

---

## 🔧 Como Executar o Pipeline

### 1️⃣ Criar e ativar um ambiente virtual (PowerShell)

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

---

### 2️⃣ Instalar o projeto (modo editable)

> O projeto utiliza `pyproject.toml`

```powershell
pip install -e .
```

---

### 3️⃣ Configurar variáveis de ambiente

```powershell
setx MONDAY_API_TOKEN "seu_token_aqui"
setx AWS_ACCESS_KEY_ID "xxxxx"
setx AWS_SECRET_ACCESS_KEY "xxxxx"
setx AWS_DEFAULT_REGION "xxxxx"
```

---

### 4️⃣ Executar o pipeline

```powershell
python -m umane_datalake.pipeline
```

---

## 🗂️ Boards Suportados

| Nome lógico | Board ID |
|------------|----------|
| funil_originacao | 9718729717 |
| projeto_monday | 18042281125 |

---

## ☁ Estrutura do Data Lake no S3

```
s3://umane-datalake-bronze/
└── monday/{board}/YYYYMM/monday_raw_*.json

s3://umane-datalake-prata/
└── monday/{board}/YYYYMM/monday_items_*.parquet

s3://umane-datalake-ouro/
└── monday/{board}/YYYYMM/monday_gold_*.parquet
```

---

## 🔗 Integração entre Boards (JOIN)

A camada Gold gera a coluna **`id_projeto`**, um identificador estável e determinístico, permitindo JOIN entre diferentes boards e integração com novas fontes no futuro.

---

## ✅ Status do Projeto

- Pipeline incremental funcional
- Multi-board
- Data Lake Bronze / Silver / Gold
- Pronto para BI e Analytics

---

## 📄 Licença

Este projeto está sob a licença MIT.
