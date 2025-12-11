# =============
# BIBLIOTECAS =
# =============

import requests
import json
import os
from umane_datalake.config import MONDAY_API_TOKEN, MONDAY_ENDPOINT

# ==========================
# CONFIGURAÇÕES DO CLIENTE =
# ==========================

if not MONDAY_API_TOKEN:
    raise EnvironmentError("A variável de ambiente MONDAY_API_TOKEN não está definida.")

HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json"
}


# =============================
# QUERY GRAPHQL COM PAGINAÇÃO =
# =============================

QUERY_ITEMS_PAGE = """
query ($board_id: ID!, $limit: Int!, $cursor: String) {
  boards(ids: [$board_id]) {
    items_page(limit: $limit, cursor: $cursor) {
      cursor
      items {
        id
        name
        column_values {
          id
          type
          text
          value
          column {
            title
          }

          # Fragmentos específicos que realmente possuem display_value
          ... on MirrorValue {
            display_value
          }
          ... on BoardRelationValue {
            display_value
          }
          ... on DependencyValue {
            display_value
          }
          ... on SubtasksValue {
            display_value
          }
        }
      }
    }
  }
}
"""


# ==================================================
# FUNÇÃO CENTRAL: BUSCA TODOS OS ITENS DE UM BOARD =
# ==================================================

def busca_dados_monday(board_id: int, limit: int = 500):
    """
    Busca todos os itens de um board Monday usando paginação.
    Retorna uma lista de itens no formato plano (pronto para Bronze).
    """
    
    print(f"➡ Iniciando extração do board {board_id} ...")

    cursor = None
    all_items = []
    page_number = 1

    while True:
        print(f"➡ Buscando página {page_number} (cursor: {cursor}) ...")

        variables = {
            "board_id": board_id,
            "limit": limit,
            "cursor": cursor
        }

        response = requests.post(
            MONDAY_ENDPOINT,
            headers=HEADERS,
            json={"query": QUERY_ITEMS_PAGE, "variables": variables}
        )

        # -----------------------------------------------------
        # TRATA ERRO DE REQUEST HTTP (ex.: DNS, timeout, etc) -
        # -----------------------------------------------------
        try:
            data = response.json()
        except Exception as e:
            raise RuntimeError(f"Erro ao converter resposta para JSON: {e}")

        # --------------------
        # TRATA ERRO GRAPHQL -
        # --------------------
        if "errors" in data:
            raise RuntimeError(
                f"Erro retornado pela API do Monday:\n{json.dumps(data['errors'], indent=2, ensure_ascii=False)}"
            )

        # -------------------------
        # TRATA RESPOSTA INVÁLIDA -
        # -------------------------
        if "data" not in data or not data["data"]["boards"]:
            raise RuntimeError(
                f"Resposta inesperada da API. Conteúdo recebido:\n{json.dumps(data, indent=2, ensure_ascii=False)}"
            )

        page = data["data"]["boards"][0]["items_page"]
        items = page.get("items", [])

        print(f"   ✔ {len(items)} itens encontrados na página {page_number}")

        # Adiciona página ao acumulador
        all_items.extend(items)

        # Atualiza cursor
        cursor = page.get("cursor")

        # Se cursor é None → acabou a paginação
        if cursor is None:
            print("✔ Todas as páginas foram extraídas com sucesso.")
            break

        page_number += 1

    print(f"✔ Total de itens extraídos: {len(all_items)}")
    return all_items


# ==================================
# DEBUG OPCIONAL (execução direta) =
# ==================================

if __name__ == "__main__":
    # Para testes manuais:
    exemplo_board_id = 123456789
    itens = busca_dados_monday(exemplo_board_id)
