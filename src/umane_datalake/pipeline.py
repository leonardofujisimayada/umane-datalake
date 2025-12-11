"""
Pipeline principal do Data Lake Monday → AWS S3.

Este módulo orquestra todo o fluxo:
1. Extração dos dados brutos via API Monday
2. Armazenamento em Bronze (JSON)
3. Transformação incremental Bronze → Prata (Parquet)
4. Transformação Prata → Ouro (dataset curado)
5. Armazenamento final em Parquet no S3
"""

import os
from datetime import datetime

from umane_datalake.s3_client import salvar_json_s3, salvar_parquet_s3
from umane_datalake.monday_client import busca_dados_monday
from umane_datalake.transformacao import transformar_bronze_para_silver_s3
from umane_datalake.transformacao_ouro import criar_camada_ouro


# Configurações dos buckets
BUCKET_BRONZE = "umane-datalake-bronze"
BUCKET_PRATA = "umane-datalake-prata"
BUCKET_OURO = "umane-datalake-ouro"

PREFIX_BRONZE = PREFIX_PRATA = PREFIX_OURO = "monday/funil_originacao"


def run_pipeline():
    """
    Executa o pipeline completo Monday → Bronze → Prata → Ouro.

    Etapas:
        1. Extrai dados do board Monday.
        2. Salva JSON bruto na camada Bronze.
        3. Converte incrementos Bronze → Prata.
        4. Gera dataset Ouro a partir dos novos dados Prata.
        5. Salva Parquet final no S3.

    Raises:
        Exception: Propaga erros de qualquer etapa para depuração rápida.
    """

    print("=================================")
    print("      🚀 INICIANDO PIPELINE      ")
    print("=================================")

    board_id = 9718729717
    print(f"➡ Extraindo dados do board {board_id}...")

    # 1. EXTRAÇÃO
    items = busca_dados_monday(board_id=board_id)
    print("✔ Dados extraídos com sucesso.")

    # 2. SALVAR BRONZE
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_filename = f"monday_raw_{timestamp}.json"

    caminho_bronze = salvar_json_s3(
        data=items, bucket=BUCKET_BRONZE, prefix=PREFIX_BRONZE, filename=bronze_filename
    )
    print(f"✔ Arquivo salvo na camada bronze: {caminho_bronze}")

    # 3. BRONZE → PRATA (incremental)
    df_silver_novos = transformar_bronze_para_silver_s3(
        bucket_bronze=BUCKET_BRONZE,
        prefix_bronze=PREFIX_BRONZE,
        bucket_silver=BUCKET_PRATA,
        prefix_silver=PREFIX_PRATA,
    )

    if df_silver_novos is None:
        print("✔ Nenhum novo dado silver gerado. Encerrando pipeline.")
        return

    print("✔ Novos dados silver gerados.")

    # 4. PRATA → OURO
    df_gold = criar_camada_ouro(df_silver_novos)
    print("✔ Dados gold gerados.")

    # 5. Salvar ouro
    gold_filename = f"monday_gold_{timestamp}.parquet"
    caminho_gold = salvar_parquet_s3(
        df=df_gold, bucket=BUCKET_OURO, prefix=PREFIX_OURO, filename=gold_filename
    )

    print(f"✔ Gold salvo com sucesso: {caminho_gold}")
    print("=======================================")
    print("     🎉 PIPELINE EXECUTADO SEM ERROS    ")
    print("=======================================")


if __name__ == "__main__":
    run_pipeline()

