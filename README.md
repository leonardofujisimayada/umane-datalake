# 🏛️ Umane — DataLake (Python)

Pipeline de dados desenvolvido para ingestão, armazenamento e tratamento de informações provenientes da plataforma **Monday.com**, estruturadas em camadas de Data Lake (Bronze → Prata → Ouro). 
O objetivo é criar um fluxo simples, reprodutível e automatizado para consolidar dados brutos, tratá-los e disponibilizá-los para análises e dashboards.

---

## 📂 Estrutura do Projeto

umane-datalake/
│── codigo/ # Scripts Python do pipeline
│ ├── config.py # Configurações, variáveis ambiente, caminhos
│ ├── funcoes.py # Funções auxiliares (ex: salvar JSON, criar parquet)
│ └── ...
│
│── datalake/
│ ├── bronze/ # Dados brutos (JSON)
│ ├── prata/  # Dados tratados (parquet)
│ └── ouro/   # (opcional - ainda não criado) 
│
│── .gitignore
│── LICENSE

