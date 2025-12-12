"""
Transformações Bronze → Prata para o Data Lake Monday.

Funcionalidades:
1. Leitura incremental dos arquivos Bronze no S3
2. Identificação automática de novos arquivos a processar
3. Conversão de JSON para DataFrame tabular
4. Tratamento de colunas complexas (mirror, relations, subtasks)
5. Normalização e prevenção de duplicidade de colunas
6. Inclusão da coluna de rastreabilidade `board_origem`
7. Escrita dos arquivos Prata no padrão YYYYMM/monday_items_TIMESTAMP.parquet
"""

import json
import re
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd


# =========================================================
# Função principal: Bronze → Prata incremental no S3
# =========================================================

def transformar_bronze_para_silver_s3(
    bucket_bronze: str,
    prefix_bronze: str,
    bucket_silver: str,
    prefix_silver: str,
    nome_board: str
):
    """
    Processa apenas os arquivos novos da camada Bronze e gera
    equivalentes na camada Prata.

    Incrementalidade baseada no timestamp do nome do arquivo.

    Args:
        bucket_bronze (str): Nome do bucket Bronze
        prefix_bronze (str): Prefixo lógico (ex: monday/funil_originacao)
        bucket_silver (str): Nome do bucket Prata
        prefix_silver (str): Prefixo lógico
        nome_board (str): Nome lógico do board (rastreabilidade)

    Returns:
        pd.DataFrame | None
    """

    s3 = boto3.client("s3")
    ano_mes = datetime.now().strftime("%Y%m")

    bronze_prefix = f"{prefix_bronze}/{ano_mes}/"
    silver_prefix = f"{prefix_silver}/{ano_mes}/"

    print(f"➡ Procurando arquivos bronze em: s3://{bucket_bronze}/{bronze_prefix}")
    print(f"➡ Procurando arquivos prata  em: s3://{bucket_silver}/{silver_prefix}")

    # -----------------------------
    # Listar arquivos Bronze
    # -----------------------------
    bronze_resp = s3.list_objects_v2(
        Bucket=bucket_bronze,
        Prefix=bronze_prefix
    )

    if "Contents" not in bronze_resp:
        print("⚠ Nenhum arquivo bronze encontrado.")
        return None

    bronze_files = [
        obj["Key"] for obj in bronze_resp["Contents"]
        if obj["Key"].endswith(".json")
    ]

    # -----------------------------
    # Listar arquivos Prata
    # -----------------------------
    silver_resp = s3.list_objects_v2(
        Bucket=bucket_silver,
        Prefix=silver_prefix
    )

    silver_files = []
    if "Contents" in silver_resp:
        silver_files = [
            obj["Key"] for obj in silver_resp["Contents"]
            if obj["Key"].endswith(".parquet")
        ]

    # -----------------------------
    # Funções auxiliares de timestamp
    # -----------------------------
    def extrair_stamp_bronze(key):
        nome = key.split("/")[-1]
        if nome.startswith("monday_raw_") and nome.endswith(".json"):
            return nome.replace("monday_raw_", "").replace(".json", "")
        return None

    def extrair_stamp_silver(key):
        nome = key.split("/")[-1]
        if nome.startswith("monday_items_") and nome.endswith(".parquet"):
            return nome.replace("monday_items_", "").replace(".parquet", "")
        return None

    bronze_stamps = {
        extrair_stamp_bronze(k) for k in bronze_files if extrair_stamp_bronze(k)
    }

    silver_stamps = {
        extrair_stamp_silver(k) for k in silver_files if extrair_stamp_silver(k)
    }

    novos = bronze_stamps - silver_stamps

    print(f"➡ {len(novos)} arquivos novos para processar.")

    if not novos:
        print("✔ Nenhum arquivo novo encontrado.")
        return None

    # -----------------------------
    # Processamento incremental
    # -----------------------------
    dfs = []

    for stamp in sorted(novos):
        json_key = f"{bronze_prefix}monday_raw_{stamp}.json"
        print(f"➡ Processando: s3://{bucket_bronze}/{json_key}")

        obj = s3.get_object(
            Bucket=bucket_bronze,
            Key=json_key
        )

        json_data = json.loads(
            obj["Body"].read().decode("utf-8")
        )

        df = json_para_dataframe(json_data)

        # 🔹 Rastreabilidade do board
        df["board_origem"] = nome_board

        dfs.append(df)

        prata_key = f"{silver_prefix}monday_items_{stamp}.parquet"

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket_silver,
            Key=prata_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )

        print(f"✔ Prata salvo: s3://{bucket_silver}/{prata_key}")

    df_final = pd.concat(dfs, ignore_index=True)
    print(f"✔ Total consolidado silver: {len(df_final)} linhas")

    return df_final


# =========================================================
# Conversões JSON → DataFrame
# =========================================================

def json_para_dataframe(data) -> pd.DataFrame:
    """
    Detecta automaticamente o formato do JSON e converte para DataFrame.
    """

    if isinstance(data, list):
        return json_para_dataframe_lista(data)

    if isinstance(data, dict) and "data" in data:
        return json_para_dataframe_monday_raw(data)

    raise ValueError("Formato de JSON não reconhecido.")


def json_para_dataframe_lista(items: list) -> pd.DataFrame:
    registros = [process_item(item) for item in items]
    return pd.DataFrame(registros)


def json_para_dataframe_monday_raw(data: dict) -> pd.DataFrame:
    boards = data.get("data", {}).get("boards", [])
    registros = []

    for board in boards:
        for item in board.get("items_page", {}).get("items", []):
            registros.append(process_item(item))

    return pd.DataFrame(registros)


# =========================================================
# Processamento de cada item Monday
# =========================================================

def process_item(item: dict) -> dict:
    """
    Converte um item do Monday em uma linha tabular.
    """

    linha = {
        "item_id": item.get("id"),
        "item_name": item.get("name")
    }

    for col in item.get("column_values", []):
        title = col.get("column", {}).get("title") or col.get("id")
        title = re.sub(r"\s+", "_", title.strip())

        # Evitar duplicidade de nomes
        base = title
        i = 1
        while title in linha:
            title = f"{base}_{i}"
            i += 1

        col_type = col.get("type")
        text = col.get("text")
        value = col.get("value")
        display = col.get("display_value")

        # Colunas mirror
        if col_type == "mirror":
            if display:
                valores = [v.strip() for v in display.split(",")]
                linha[title] = " | ".join(valores)
            else:
                linha[title] = None
            continue

        linha[title] = text or value

    return linha

