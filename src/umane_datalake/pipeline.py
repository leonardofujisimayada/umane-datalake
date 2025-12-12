from datetime import datetime

from umane_datalake.s3_client import salvar_json_s3, salvar_parquet_s3
from umane_datalake.monday_client import busca_dados_monday
from umane_datalake.transformacao import transformar_bronze_para_silver_s3
from umane_datalake.transformacao_ouro import criar_camada_ouro


BUCKET_BRONZE = "umane-datalake-bronze"
BUCKET_PRATA = "umane-datalake-prata"
BUCKET_OURO = "umane-datalake-ouro"

# Boards suportados
BOARDS = {
    "funil_originacao": 9718729717,
    "projeto_monday": 18042281125
}


def run_pipeline():

    print("=================================")
    print("      🚀 INICIANDO PIPELINE      ")
    print("=================================")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for nome_board, board_id in BOARDS.items():

        print(f"\n➡ Processando board: {nome_board} ({board_id})")
        prefix = f"monday/{nome_board}"

        # 1. EXTRAÇÃO
        items = busca_dados_monday(board_id=board_id)

        # 2. BRONZE
        salvar_json_s3(
            data=items,
            bucket=BUCKET_BRONZE,
            prefix=prefix,
            filename=f"monday_raw_{timestamp}.json"
        )

        # 3. PRATA (incremental)
        df_silver = transformar_bronze_para_silver_s3(
            bucket_bronze=BUCKET_BRONZE,
            prefix_bronze=prefix,
            bucket_silver=BUCKET_PRATA,
            prefix_silver=prefix,
            nome_board=nome_board   # 👈 NOVO
        )

        if df_silver is None:
            continue

        # 4. OURO
        df_gold = criar_camada_ouro(df_silver)

        # 5. SALVAR OURO
        salvar_parquet_s3(
            df=df_gold,
            bucket=BUCKET_OURO,
            prefix=prefix,
            filename=f"monday_gold_{timestamp}.parquet"
        )

    print("\n🎉 PIPELINE FINALIZADO COM SUCESSO")

if __name__ == "__main__":
    run_pipeline()

