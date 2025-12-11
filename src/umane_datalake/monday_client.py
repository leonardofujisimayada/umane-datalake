"""
Cliente oficial da API Monday.com para extração dos dados do board.

Este módulo implementa:
- Autenticação via token
- Execução de queries GraphQL
- Paginação automática via cursor
- Tratamento de erros HTTP e GraphQL
- Extração completa de itens de um board

Função principal:
    busca_dados_monday(board_id, limit)
"""

import requests
import json
import os
from umane_datalake.config import MONDAY_API_TOKEN, MONDAY_ENDPOINT

# Verificação de credenciais
if not MONDAY_API_TOKEN:
    raise EnvironmentError("A variável de ambiente MONDAY_API_TOKEN não está definida.")

HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json"
}

# Query GraphQL com paginação
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
          ... on MirrorValue { display_value }
          ... on BoardRelationValue { display_value }
          ... on DependencyValue { display_value }
          ... on SubtasksValue { display_value }
        }
      }
    }
  }
}
"""


def busca_dados_monday(board_id: int, limit: int = 500):
    """
    Extrai todos os itens de um board Monday utilizando paginação automática.

    Args:
        board_id (int): ID numérico do board Monday.
        limit (int): Quantidade máxima de itens por página da API.

    Returns:
        list: Lista de itens (dict) no formato bruto retornado pela API.

    Raises:
        RuntimeError: Erros de comunicação HTTP, falhas no GraphQL ou estrutura inesperada.
    """

    cursor = None
    all_items = []
    page_number = 1

    print(f"➡ Iniciando extração do board {board_id} ...")

    while True:
        print(f"➡ Buscando página {page_number} (cursor: {cursor}) ...")

        response = requests.post(
            MONDAY_ENDPOINT,
            headers=HEADERS,
            json={"query": QUERY_ITEMS_PAGE, "variables": {"board_id": board_id, "limit": limit, "cursor": cursor}}
        )

        # Tenta converter a resposta em JSON
        try:
            data = response.json()
        except Exception as e:
            raise RuntimeError(f"Erro ao converter resposta para JSON: {e}")

        # Erros retornados pela API GraphQL
        if "errors" in data:
            raise RuntimeError(
                f"Erro retornado pela API do Monday:\n{json.dumps(data['errors'], indent=2, ensure_ascii=False)}"
            )

        # Estrutura inesperada
        if "data" not in data or not data["data"]["boards"]:
            raise RuntimeError(
                f"Resposta inesperada da API. Conteúdo recebido:\n{json.dumps(data, indent=2, ensure_ascii=False)}"
            )

        # Extrai itens
        page = data["data"]["boards"][0]["items_page"]
        items = page.get("items", [])
        print(f"   ✔ {len(items)} itens encontrados na página {page_number}")

        all_items.extend(items)

        # Atualiza cursor
        cursor = page.get("cursor")
        if cursor is None:
            print("✔ Todas as páginas foram extraídas com sucesso.")
            break

        page_number += 1

    print(f"✔ Total de itens extraídos: {len(all_items)}")
    return all_items


if __name__ == "__main__":
    exemplo_board_id = 123456789
    itens = busca_dados_monday(exemplo_board_id)

