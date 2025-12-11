# =============
# BIBLIOTECAS =
# =============

import os
from datetime import datetime

from s3_client import salvar_json_s3, salvar_parquet_s3
from monday_client import busca_dados_monday
from transformacao import transformar_bronze_para_silver_s3
from transformacao_ouro import criar_camada_ouro  

# ==========================
# CONFIGURAÇÃO DO DATALAKE =
# ==========================

BUCKET_BRONZE = "umane-datalake-bronze"
BUCKET_PRATA  = "umane-datalake-prata"
BUCKET_OURO   = "umane-datalake-ouro"        

PREFIX_BRONZE = "monday/funil_originacao"
PREFIX_PRATA  = "monday/funil_originacao"
PREFIX_OURO   = "monday/funil_originacao"     


# ====================
# PIPELINE PRINCIPAL =
# ====================

def run_pipeline():

    print("=================================")
    print("      🚀 INICIANDO PIPELINE      ")
    print("=================================")

    # -------------------------------
    # CONFIGURAÇÕES DO BOARD MONDAY -
    # -------------------------------
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

    # ---------------------------------------------
    # 1.1 SALVAR BRONZE NO S3 COM TIMESTAMP ÚNICO -
    # ---------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_filename = f"monday_raw_{timestamp}.json"

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

    # ------------------------------------------------
    # 2. TRANSFORMAÇÃO INCREMENTAL — BRONZE → SILVER -
    # ------------------------------------------------

    print("➡ Iniciando transformação incremental (bronze → silver)...")

    try:
        df_silver_novos = transformar_bronze_para_silver_s3(
            bucket_bronze=BUCKET_BRONZE,
            prefix_bronze=PREFIX_BRONZE,
            bucket_silver=BUCKET_PRATA,
            prefix_silver=PREFIX_PRATA
        )
    except Exception as e:
        print("❌ ERRO durante a transformação bronze → silver:")
        raise e

    if df_silver_novos is None:
        print("✔ Nenhum novo dado silver gerado. Encerrando pipeline.")
        return

    print("✔ Novos dados silver gerados.")

    # ----------------------------------------------
    # 3. TRANSFORMAÇÃO INCREMENTAL — SILVER → GOLD -
    # ----------------------------------------------

    print("➡ Iniciando transformação incremental (silver → gold)...")

    try:
        df_gold = criar_camada_ouro(df_silver_novos)
    except Exception as e:
        print("❌ ERRO na criação da camada ouro:")
        raise e

    print("✔ Dados gold gerados.")

    # -----------------------------------------------
    # 3.1 SALVAR GOLD NO S3 (PARQUET COM TIMESTAMP) -
    # -----------------------------------------------
    gold_filename = f"monday_gold_{timestamp}.parquet"

    try:
        caminho_gold = salvar_parquet_s3(
            df=df_gold,
            bucket=BUCKET_OURO,
            prefix=PREFIX_OURO,
            filename=gold_filename
        )
    except Exception as e:
        print("❌ ERRO ao salvar parquet na camada ouro:")
        raise e

    print(f"✔ Gold salvo com sucesso: {caminho_gold}")

    print("=======================================")
    print("     🎉 PIPELINE EXECUTADO SEM ERROS    ")
    print("=======================================")



# =================
# EXECUÇÃO DIRETA =
# =================

if __name__ == "__main__":
    run_pipeline()
