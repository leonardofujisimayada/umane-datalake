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

def transformar_bronze_para_silver_s3(bucket_bronze: str, prefix_bronze: str,
                                      bucket_silver: str, prefix_silver: str):
    """
    Processa apenas arquivos novos da camada bronze no S3 e salva a camada prata.
    """

    s3 = boto3.client("s3")

    # ------------------------------
    # 1. LISTAR ARQUIVOS DO BRONZE -
    # ------------------------------
    ano_mes = datetime.now().strftime("%Y%m")

    bronze_prefix = f"{prefix_bronze}/{ano_mes}/"
    silver_prefix = f"{prefix_silver}/{ano_mes}/"

    print(f"➡ Procurando arquivos bronze em: s3://{bucket_bronze}/{bronze_prefix}")
    print(f"➡ Procurando arquivos prata em:  s3://{bucket_silver}/{silver_prefix}")

    # Bronze
    bronze_resp = s3.list_objects_v2(
        Bucket=bucket_bronze,
        Prefix=bronze_prefix
    )

    if "Contents" not in bronze_resp:
        print("⚠ Nenhum arquivo na camada bronze.")
        return None

    bronze_files = [
        obj["Key"] for obj in bronze_resp["Contents"]
        if obj["Key"].endswith(".json")
    ]

    # Prata
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

    # -----------------------
    # 2. EXTRAIR TIMESTAMPS -
    # -----------------------

    def extrair_stamp_bronze(key):
        nome = key.split("/")[-1]  # pega apenas o nome do arquivo
        if nome.startswith("monday_raw_") and nome.endswith(".json"):
            return nome.replace("monday_raw_", "").replace(".json", "")
        return None

    def extrair_stamp_silver(key):
        nome = key.split("/")[-1]  # pega só o nome do arquivo
        if nome.startswith("monday_items_") and nome.endswith(".parquet"):
            return nome.replace("monday_items_", "").replace(".parquet", "")
        return None

    bronze_stamps = {
        stamp for stamp in (extrair_stamp_bronze(k) for k in bronze_files)
        if stamp is not None
    }

    silver_stamps = {
        stamp for stamp in (extrair_stamp_silver(k) for k in silver_files)
        if stamp is not None
    }

    # -------------------
    # 3. ARQUIVOS NOVOS - 
    # -------------------
    novos = bronze_stamps - silver_stamps

    print(f"➡ {len(novos)} arquivos novos encontrados.")

    if not novos:
        print("✔ Nenhum arquivo novo para processar.")
        return None

    # -----------------------
    # 4. PROCESSAR ARQUIVOS -
    # -----------------------

    dfs = []

    for stamp in sorted(novos):
        json_key = f"{bronze_prefix}monday_raw_{stamp}.json"
        print(f"➡ Processando novo arquivo: s3://{bucket_bronze}/{json_key}")

        obj = s3.get_object(Bucket=bucket_bronze, Key=json_key)
        json_bytes = obj["Body"].read()
        json_data = json.loads(json_bytes.decode("utf-8"))

        df = json_para_dataframe(json_data)
        dfs.append(df)

        # GERAR PRATA DO ARQUIVO
        prata_key = f"{silver_prefix}monday_items_{stamp}.parquet"
        print(f"➡ Salvando arquivo prata: s3://{bucket_silver}/{prata_key}")

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket_silver,
            Key=prata_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )

    # -------------------
    # 5. DF CONSOLIDADO -
    # -------------------
    df_final = pd.concat(dfs, ignore_index=True)
    print(f"✔ Total consolidado da camada silver: {len(df_final)} linhas.")

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
