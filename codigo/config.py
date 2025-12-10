# =============
# BIBLIOTECAS =
# =============

import os
from dotenv import load_dotenv

load_dotenv()  # Carrega dados do .env automaticamente

# ============
# MONDAY API =
# ============

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_ENDPOINT = "https://api.monday.com/v2"

# ========
# S3 API =
# ========

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
