# =============
# BIBLIOTECAS =
# =============

import os
from datetime import datetime
from s3_client import salvar_json_s3, salvar_parquet_s3
from monday_client import busca_dados_monday
from transformacao import transformar_bronze_para_silver_s3, json_para_dataframe

# ======================================
# CONFIGURAÇÕES DO DATA LAKE NA AWS S3 =
# ======================================

BUCKET_BRONZE = "umane-datalake-bronze"
BUCKET_PRATA  = "umane-datalake-prata"

PREFIX_BRONZE = "monday/funil_originacao"
PREFIX_PRATA  = "monday/funil_originacao"

# ====================
# PIPELINE PRINCIPAL =
# ====================

def run_pipeline():

    print("=================================")
    print("      🚀 INICIANDO PIPELINE      ")
    print("=================================")

    board_id = 9718729717
    print(f"➡ Extraindo dados do board {board_id}...")

    # -------------------------------------------------
    # 1. EXTRAÇÃO — buscando todos os itens da Monday -
    # -------------------------------------------------

    try:
        items = busca_dados_monday(board_id=board_id)
    except Exception as e:
        print("❌ ERRO durante a extração dos dados da Monday:")
        raise e

    print("✔ Dados extraídos com sucesso.")

    # --------------------------------------
    # 1.1 SALVAR BRONZE NO S3 (JSON bruto) -
    # --------------------------------------

    bronze_filename = "monday_raw.json"

    print("➡ Salvando JSON bruto na camada bronze S3...")

    try:
        caminho_bronze = salvar_json_s3(
            data=items,
            bucket=BUCKET_BRONZE,
            prefix=PREFIX_BRONZE,
            filename=bronze_filename
        )
    except Exception as e:
        print("❌ ERRO ao salvar JSON na camada bronze:")
        raise e

    print(f"✔ Arquivo salvo na camada bronze: {caminho_bronze}")

    # ------------------------------------
    # 2. TRANSFORMAÇÃO — Bronze → Silver -
    # ------------------------------------

    print("➡ Iniciando transformação (bronze → silver)...")

    try:
        df_silver = transformar_bronze_para_silver_s3(
            bucket_bronze=BUCKET_BRONZE,
            prefix_bronze=PREFIX_BRONZE
        )
    except Exception as e:
        print("❌ ERRO durante a transformação bronze → silver:")
        raise e

    print("✔ Dados transformados para silver.")

    # -----------------------------------
    # 2.1 SALVAR SILVER NO S3 (PARQUET) -
    # -----------------------------------

    silver_filename = "monday_silver.parquet"

    try:
        caminho_silver = salvar_parquet_s3(
            df=df_silver,
            bucket=BUCKET_PRATA,
            prefix=PREFIX_PRATA,
            filename=silver_filename
        )
    except Exception as e:
        print("❌ ERRO ao salvar parquet na camada prata:")
        raise e

    print(f"✔ Silver salvo com sucesso: {caminho_silver}")

    print("=======================================")
    print("     🎉 PIPELINE EXECUTADO SEM ERROS    ")
    print("=======================================")


# =================
# EXECUÇÃO DIRETA =
# =================

if __name__ == "__main__":
    run_pipeline()
