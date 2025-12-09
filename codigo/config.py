# =============
# BIBLIOTECAS =
# =============

import os
from dotenv import load_dotenv

load_dotenv()  # Carrega dados do .env automaticamente

# ============
# MONDAY API =
# ============

MONDAY_API_KEY = os.getenv("MONDAY_API_TOKEN")
MONDAY_ENDPOINT = "https://api.monday.com/v2"

