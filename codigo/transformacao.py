# =============
# BIBLIOTECAS =
# =============

import os
import json
import pandas as pd
import re
from glob import glob
from datetime import datetime


# =================================================================
# 1. Função principal: transforma todos os JSONs da camada bronze =
# =================================================================

def transformar_bronze_para_silver(path_bronze: str, path_silver: str):

    # Criar pasta silver se não existir
    os.makedirs(path_silver, exist_ok=True)

    # Lista todos os arquivos .json da bronze
    arquivos = glob(os.path.join(path_bronze, "*.json"))

    if not arquivos:
        raise ValueError("Nenhum arquivo JSON encontrado na camada bronze.")

    print(f"➡ {len(arquivos)} arquivos encontrados na camada bronze.")

    # Lista para acumular DataFrames
    dfs = []

    for json_file in arquivos:
        print(f"➡ Processando arquivo: {json_file}")
        df = json_para_dataframe(json_file)
        dfs.append(df)

    # Concatena todos os dataframes em um único silver
    df_final = pd.concat(dfs, ignore_index=True)

    # ----------------------------------------
    # Gera nome com timestamp (igual Bronze) -
    # ----------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_filename = f"monday_items_{timestamp}.parquet"
    parquet_path = os.path.join(path_silver, silver_filename)

    print(f"➡ Salvando camada silver em Parquet: {parquet_path}")
    df_final.to_parquet(parquet_path, index=False)

    print("✔ Transformação bronze → silver concluída.")


# =============================================================
# 2. Detecta o tipo de JSON (lista de itens ou JSON completo) =
# =============================================================

def json_para_dataframe(json_path: str) -> pd.DataFrame:
    """
    Detecta automaticamente se o arquivo JSON é:
    - lista de itens (bronze atual)
    - resposta bruta da API Monday
    E roteia para a função correta.
    """

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Caso 1 → Bronze atual (lista de itens)
    if isinstance(data, list):
        print("✔ Formato detectado: lista de itens (bronze).")
        return json_para_dataframe_lista(data)

    # Caso 2 → JSON bruto da API Monday
    elif isinstance(data, dict) and "data" in data:
        print("✔ Formato detectado: JSON bruto da API Monday.")
        return json_para_dataframe_monday_raw(data)

    else:
        raise ValueError("Formato de JSON não reconhecido.")


# ============================================================
# 3A. Conversão para quando o JSON é lista de itens (bronze) =
# ============================================================

def json_para_dataframe_lista(items: list) -> pd.DataFrame:
    registros = []
    for item in items:
        registros.append(process_item(item))
    return pd.DataFrame(registros)


# ===============================================================
# 3B. Conversão para JSON bruto de API Monday (não usado agora) =
# ===============================================================

def json_para_dataframe_monday_raw(data: dict) -> pd.DataFrame:
    boards = data.get("data", {}).get("boards", [])
    registros = []

    for board in boards:
        items = board.get("items_page", {}).get("items", [])
        for item in items:
            registros.append(process_item(item))

    return pd.DataFrame(registros)


# =================================================================
# 4. Processa um item individual (colunas, mirrors, normalização) =
# =================================================================

def process_item(item: dict) -> dict:
    linha = {
        "item_id": item.get("id"),
        "item_name": item.get("name")
    }

    for col in item.get("column_values", []):
        
        # Nome da coluna
        title = col.get("column", {}).get("title") or col.get("id")
        title = re.sub(r"\s+", "_", title.strip())

        # Evita duplicatas
        base = title
        n = 1
        while title in linha:
            title = f"{base}_{n}"
            n += 1

        # Valores possíveis
        col_type = col.get("type")
        text = col.get("text")
        value = col.get("value")
        display = col.get("display_value")

        # Caso especial: mirror
        if col_type == "mirror" and display:
            valores = [v.strip() for v in display.split(",")]
            linha[title] = " | ".join(valores)
            continue

        # Valor padrão
        linha[title] = text or value

    return linha
