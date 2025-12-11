"""
Transformações Bronze → Prata para o Data Lake Monday.

Este módulo implementa:

1. Leitura incremental dos arquivos Bronze no S3
2. Identificação automática de novos arquivos a processar
3. Conversão de JSON para DataFrame tabular
4. Tratamento de colunas complexas (mirror, subtasks, relations)
5. Normalização de nomes e prevenção de duplicidade de colunas
6. Escrita dos arquivos Prata no padrão YYYYMM/monday_items_TIMESTAMP.parquet

Funções principais:
    transformar_bronze_para_silver_s3
    json_para_dataframe
    process_item
"""

import os
import json
import pandas as pd
import re
from datetime import datetime
from io import BytesIO
import boto3


def transformar_bronze_para_silver_s3(bucket_bronze: str, prefix_bronze: str,
                                      bucket_silver: str, prefix_silver: str):
    """
    Processa apenas os arquivos novos da camada Bronze e gera equivalentes na camada Prata.

    A lógica incremental funciona comparando timestamps existentes:
        - monday_raw_<timestamp>.json   (Bronze)
        - monday_items_<timestamp>.parquet (Prata)

    Args:
        bucket_bronze (str): Nome do bucket Bronze.
        prefix_bronze (str): Subpasta lógica no Bronze.
        bucket_silver (str): Nome do bucket Prata.
        prefix_silver (str): Subpasta lógica no Prata.

    Returns:
        pd.DataFrame | None:
            DataFrame consolidado dos novos arquivos processados,
            ou None caso nenhum novo arquivo seja encontrado.
    """

    s3 = boto3.client("s3")
    ano_mes = datetime.now().strftime("%Y%m")

    bronze_prefix = f"{prefix_bronze}/{ano_mes}/"
    silver_prefix = f"{prefix_silver}/{ano_mes}/"

    print(f"➡ Procurando arquivos bronze em: s3://{bucket_bronze}/{bronze_prefix}")
    print(f"➡ Procurando arquivos prata em:  s3://{bucket_silver}/{silver_prefix}")

    # Listar arquivos Bronze
    bronze_resp = s3.list_objects_v2(Bucket=bucket_bronze, Prefix=bronze_prefix)

    if "Contents" not in bronze_resp:
        print("⚠ Nenhum arquivo na camada bronze.")
        return None

    bronze_files = [
        obj["Key"] for obj in bronze_resp["Contents"]
        if obj["Key"].endswith(".json")
    ]

    # Listar arquivos Prata
    silver_resp = s3.list_objects_v2(Bucket=bucket_silver, Prefix=silver_prefix)
    silver_files = []

    if "Contents" in silver_resp:
        silver_files = [
            obj["Key"] for obj in silver_resp["Contents"]
            if obj["Key"].endswith(".parquet")
        ]

    # Funções internas para extrair timestamp dos nomes
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

    bronze_stamps = {extrair_stamp_bronze(k) for k in bronze_files if extrair_stamp_bronze(k)}
    silver_stamps = {extrair_stamp_silver(k) for k in silver_files if extrair_stamp_silver(k)}

    novos = bronze_stamps - silver_stamps

    print(f"➡ {len(novos)} arquivos novos encontrados.")

    if not novos:
        print("✔ Nenhum arquivo novo para processar.")
        return None

    # Processamento incremental
    dfs = []

    for stamp in sorted(novos):
        json_key = f"{bronze_prefix}monday_raw_{stamp}.json"
        print(f"➡ Processando novo arquivo: s3://{bucket_bronze}/{json_key}")

        # Baixa JSON bruto
        obj = s3.get_object(Bucket=bucket_bronze, Key=json_key)
        json_data = json.loads(obj["Body"].read().decode("utf-8"))

        # Converte JSON → DataFrame tabular
        df = json_para_dataframe(json_data)
        dfs.append(df)

        prata_key = f"{silver_prefix}monday_items_{stamp}.parquet"

        # Escreve DataFrame no S3
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
    print(f"✔ Total consolidado silver: {len(df_final)} linhas.")
    return df_final


def json_para_dataframe(data) -> pd.DataFrame:
    """
    Detecta automaticamente o tipo de estrutura JSON e converte para DataFrame.

    Tipos suportados:
        - Lista de itens (formato bronze)
        - JSON bruto completo retornado pela API Monday

    Args:
        data (list | dict): Estrutura JSON carregada.

    Returns:
        pd.DataFrame: DataFrame tabular normalizado.
    """

    if isinstance(data, list):
        print("✔ Formato detectado: lista de itens (bronze).")
        return json_para_dataframe_lista(data)

    elif isinstance(data, dict) and "data" in data:
        print("✔ Formato detectado: JSON bruto da API Monday.")
        return json_para_dataframe_monday_raw(data)

    else:
        raise ValueError("Formato de JSON não reconhecido.")


def json_para_dataframe_lista(items: list) -> pd.DataFrame:
    """Converte uma lista de itens Monday em DataFrame tabular."""
    registros = [process_item(item) for item in items]
    return pd.DataFrame(registros)


def json_para_dataframe_monday_raw(data: dict) -> pd.DataFrame:
    """Converte a resposta completa da API Monday em DataFrame."""
    boards = data.get("data", {}).get("boards", [])
    registros = []

    for board in boards:
        for item in board.get("items_page", {}).get("items", []):
            registros.append(process_item(item))

    return pd.DataFrame(registros)


def process_item(item: dict) -> dict:
    """
    Converte um item da API Monday em uma linha do DataFrame.

    Essa função trata:
        - Normalização de nomes de colunas
        - Colunas duplicadas (add sufixo _1, _2 ...)
        - Colunas do tipo mirror
        - Conversões de valores textuais

    Returns:
        dict: Dicionário representando uma linha do DataFrame.
    """

    linha = {
        "item_id": item.get("id"),
        "item_name": item.get("name")
    }

    for col in item.get("column_values", []):
        title = col.get("column", {}).get("title") or col.get("id")
        title = re.sub(r"\s+", "_", title.strip())

        # Prevenir duplicidade de nomes
        base = title
        n = 1
        while title in linha:
            title = f"{base}_{n}"
            n += 1

        col_type = col.get("type")
        text = col.get("text")
        value = col.get("value")
        display = col.get("display_value")

        # Caso especial: coluna mirror
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

        linha[title] = text or value

    return linha
