"""
Módulo de configuração central do Data Lake.

Responsável por carregar automaticamente variáveis de ambiente
a partir do arquivo `.env`, incluindo credenciais da API Monday
e credenciais da AWS para leitura/escrita no S3.
"""

import os
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env automaticamente.
load_dotenv()

# ======================
# Configurações Monday =
# ======================

# Token de autenticação da API Monday.
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

# Endpoint GraphQL da API Monday.
MONDAY_ENDPOINT = "https://api.monday.com/v2"

# ==================
# Configurações S3 =
# ==================

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
