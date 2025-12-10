# =============
# BIBLIOTECAS =
# =============

import os
import json
import pandas as pd
import re
from glob import glob
from datetime import datetime
from io import BytesIO
import boto3


# =================================================================
# 1. Função principal: transforma todos os JSONs da camada bronze =
# =================================================================

def transformar_bronze_para_silver_s3(bucket_bronze: str, prefix_bronze: str):
    """
    Lê todos os arquivos JSON da camada bronze no S3,
    converte para DataFrame e retorna um único DataFrame consolidado.

    Exemplo de prefix:
        monday/funil_originacao
    """

    s3 = boto3.client("s3")

    # ------------------------------
    # 1. LISTAR ARQUIVOS DO BRONZE -
    # ------------------------------

    # Pega o ano e mês atual (pasta corrente)
    ano_mes = datetime.now().strftime("%Y%m")

    prefix_completo = f"{prefix_bronze}/{ano_mes}/"
    print(f"➡ Procurando arquivos em: s3://{bucket_bronze}/{prefix_completo}")

    response = s3.list_objects_v2(
        Bucket=bucket_bronze,
        Prefix=prefix_completo
    )

    if "Contents" not in response:
        raise ValueError("Nenhum arquivo JSON encontrado na camada bronze do S3.")

    # Filtra somente .json
    arquivos = [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".json")
    ]

    if not arquivos:
        raise ValueError("Nenhum arquivo JSON encontrado na pasta bronze correspondente.")

    print(f"➡ {len(arquivos)} arquivos encontrados na camada bronze do S3.")

    # ----------------------------
    # 2. PROCESSAR ARQUIVOS JSON -
    # ----------------------------

    dfs = []

    for key in arquivos:
        print(f"➡ Processando arquivo: s3://{bucket_bronze}/{key}")

        obj = s3.get_object(Bucket=bucket_bronze, Key=key)

        json_bytes = obj["Body"].read()
        json_data = json.loads(json_bytes.decode("utf-8"))

        # Usa sua função atual para transformar JSON → DataFrame
        df = json_para_dataframe(json_data)
        dfs.append(df)

    # -------------------------------
    # 3. CONCATENAR EM UM ÚNICO DF
    # -------------------------------
    df_final = pd.concat(dfs, ignore_index=True)

    print(f"✔ {len(df_final)} linhas totais consolidadas na camada silver.")

    return df_final

# =============================================================
# 2. Detecta o tipo de JSON (lista de itens ou JSON completo) =
# =============================================================

def json_para_dataframe(data) -> pd.DataFrame:
    """
    Detecta automaticamente se 'data' é:
    - lista de itens (JSON já carregado)
    - resposta bruta da API Monday
    """

    # Caso 1 → Bronze (lista)
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
        if col_type == "mirror":
            if display:
                valores = [v.strip() for v in display.split(",")]
                linha[title] = " | ".join(valores)
            elif value and isinstance(value, dict) and "linkedPulseIds" in value:
                ids = [str(v.get("linkedPulseId")) for v in value["linkedPulseIds"]]
                linha[title] = " | ".join(ids)
            else:
                linha[title] = None
            continue

        # Valor padrão
        linha[title] = text or value

    return linha
