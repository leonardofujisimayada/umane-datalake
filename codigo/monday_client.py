# ---------------------
# IMPORTA BIBLIOTECAS - 
# ---------------------

import requests
from config import MONDAY_API_KEY, MONDAY_ENDPOINT

# --------------------------------------
# FUNCTION PARA BUSCAR DADOS DA MONDAY - 
# --------------------------------------

def busca_dados_monday(query):
    headers = {"Authorization": MONDAY_API_KEY}
    response = requests.post(
        MONDAY_ENDPOINT,
        json={"query": query},
        headers=headers
    )
    response.raise_for_status()
    return response.json()


