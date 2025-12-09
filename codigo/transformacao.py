# ---------------------
# IMPORTA BIBLIOTECAS - 
# ---------------------

import os
import json
import pandas as pd
import re

# ---------------------------------------------------
# FUNCTION PARA CONVERTER ARQUIVO JSON EM DATAFRAME - 
# ---------------------------------------------------

def json_para_dataframe(json_path: str) -> pd.DataFrame:
    """
    Converte JSON exportado da API do Monday em um DataFrame tabular,
    usando o título das colunas em vez do ID.
    """

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    boards = data.get("data", {}).get("boards", [])
    registros = []

    for board in boards:
        items = (
            board.get("items_page", {})
                .get("items", [])
        )

        for item in items:
            linha = {
                "board_id": board.get("id"),
                "item_id": item.get("id"),
                "item_name": item.get("name")
            }

            for col in item.get("column_values", []):
                
                title = (
                    col.get("column", {}).get("title")
                    or col.get("id")  # fallback
                )
                title = re.sub(r"\s+", "_", title.strip())

                original_title = title
                counter = 1
                while title in linha:
                    title = f"{original_title}_{counter}"
                    counter += 1

                linha[title] = col.get("text") or col.get("value")

            registros.append(linha)

    return pd.DataFrame(registros)

# ---------------------------------------------
# FUNCTION PARA SALVAR O DATAFRAME EM PARQUET - 
# ---------------------------------------------

def salvar_parquet(df: pd.DataFrame, destino: str):
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    df.to_parquet(destino, index=False)
    print(f"✔ Arquivo salvo em {destino}")

# ---------------------------------------------------------------------------
# FUNCTION PARA TRANSFORMAR ARQUIVOS NA CAMADA BRONZE E LEVAR PARA A SILVER - 
# ---------------------------------------------------------------------------

def transformar_bronze_para_silver(caminho_bronze: str, caminho_silver: str):
    """
    Pega todos os JSON na camada bronze e gera arquivos parquet na camada silver.
    """

    arquivos = [
        f for f in os.listdir(caminho_bronze)
        if f.endswith(".json")
    ]

    if not arquivos:
        print("⚠ Nenhum arquivo JSON encontrado na camada bronze.")
        return

    for arquivo in arquivos:
        json_path = os.path.join(caminho_bronze, arquivo)
        print(f"➡ Transformando {arquivo}...")

        df = json_para_dataframe(json_path)

        parquet_name = arquivo.replace(".json", ".parquet")
        destino = os.path.join(caminho_silver, parquet_name)

        salvar_parquet(df, destino)

    print("✔ Transformação concluída.")


