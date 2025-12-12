# Umane Data Lake — Pipeline Monday → AWS S3

## 🚀 Visão Geral

Este repositório implementa um pipeline completo para ingestão, tratamento e organização de dados da plataforma **Monday.com** em um **Data Lake AWS** estruturado em três camadas:

- **Bronze** → dados brutos (JSON)
- **Silver** → dados normalizados e flatten (Parquet)
- **Gold** → dataset analítico padronizado, pronto para BI e análises

Arquitetura:

```
Monday API → Bronze (S3) → Silver (S3) → Gold (S3) → Analytics
```

---

## 📂 Estrutura do Repositório

```
umane-datalake/
│
├── src/umane_datalake/
│   ├── config.py               # Carrega variáveis de ambiente e constantes (buckets, tokens, etc.)
│   ├── monday_client.py        # Cliente GraphQL da Monday (extração de itens, colunas, paginação)
│   ├── s3_client.py            # Funções utilitárias para upload/download no AWS S3
│   ├── transformacao.py        # Bronze → Silver (flatten, limpeza, parquet)
│   ├── transformacao_ouro.py   # Silver → Gold (padronização, curadoria, novas chaves)
│   ├── pipeline.py             # Orquestrador principal do pipeline
│
├── requirements.txt
├── README.md
└── LICENSE
```

---

## 🧠 Principais Funcionalidades

### 🔹 1. Extração da API Monday (GraphQL)
- Leitura de boards e itens
- Paginação automática
- Suporte a colunas simples e complexas (mirror, text, numbers, etc.)
- Salvamento dos dados brutos na camada Bronze (JSON)

### 🔹 2. Bronze → Silver
Executado via `transformacao.py`:
- Detecta automaticamente o formato do JSON
- Faz flatten das colunas
- Normaliza tipos
- Concatena múltiplos arquivos
- Salva em formato **Parquet**, otimizado para análises

### 🔹 3. Silver → Gold
Executado via `transformacao_ouro.py`:
- Padronização de nomes de colunas
- Criação de IDs independentes da plataforma Monday
- Somatório e agregações em campos numéricos
- Salvamento da camada ouro em S3

---

## 🔧 Como Executar o Pipeline

### 1️⃣ Criar e ativar um ambiente virtual (PowerShell)
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

### 2️⃣ Instalar dependências
```powershell
pip install -r requirements.txt
```

### 3️⃣ Exportar variáveis de ambiente
```powershell
setx MONDAY_API_KEY "seu_token_aqui"
setx AWS_ACCESS_KEY_ID "xxxxx"
setx AWS_SECRET_ACCESS_KEY "xxxxx"
setx AWS_DEFAULT_REGION "xxxxx"
```

### 4️⃣ Executar o pipeline
```powershell
python -m src.umane_datalake.pipeline
```

Isso irá:
1. Extrair dados da Monday
2. Criar arquivos Bronze → Silver → Gold automaticamente no S3

---

## ☁ Configuração do S3 (Data Lake)

Os buckets esperados são:

```
umane-datalake-bronze/
    monday/{board}/{YYYYMM}/{arquivo.json}

umane-datalake-prata/
    monday/{board}/{YYYYMM}/{arquivo.parquet}

umane-datalake-ouro/
    monday/{board}/{YYYYMM}/{arquivo_gold.parquet}
```

---

## 📄 Licença

Este projeto está sob a licença MIT.
