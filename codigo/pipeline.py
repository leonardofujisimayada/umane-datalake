# =============
# BIBLIOTECAS =
# =============

import os
from datetime import datetime
from s3_client import salvar_json
from monday_client import busca_dados_monday
from transformacao import transformar_bronze_para_silver
from s3_client import salvar_json

# ==========================================
# CONFIGURAÇÕES DE DIRETÓRIOS DO DATA LAKE =
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Camada BRONZE → JSON cru paginado
PATH_BRONZE = os.path.join(BASE_DIR, "..", "dados", "bronze")

# Camada SILVER → Parquet consolidado
PATH_SILVER = os.path.join(BASE_DIR, "..", "dados", "prata")


# ====================
# PIPELINE PRINCIPAL =
# ====================

def run_pipeline():

    print("=================================")
    print("      🚀 INICIANDO PIPELINE      ")
    print("=================================")

    board_id = 9718729717
    print(f"➡ Extraindo dados do board {board_id}...")

    # 1. EXTRAÇÃO — buscando todos os itens do board
    try:
        items = busca_dados_monday(board_id=board_id)
    except Exception as e:
        print("❌ ERRO durante a extração dos dados da Monday:")
        raise e

    print("✔ Dados extraídos com sucesso.")

    # Criar diretório bronze se não existir
    os.makedirs(PATH_BRONZE, exist_ok=True)

    # Nome do arquivo bronze com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_filename = f"monday_raw_{timestamp}.json"
    bronze_path = os.path.join(PATH_BRONZE, bronze_filename)

    print(f"➡ Salvando JSON bruto na camada bronze: {bronze_path}")
    
    try:
        salvar_json(items, PATH_BRONZE, bronze_filename)
    except Exception as e:
        print("❌ ERRO ao salvar JSON na camada bronze:")
        raise e

    print("✔ Arquivo salvo na camada bronze.")

    # 2. TRANSFORMAÇÃO — Bronze → Silver
    print("➡ Iniciando transformação (bronze → silver)...")

    try:
        transformar_bronze_para_silver(PATH_BRONZE, PATH_SILVER)
    except Exception as e:
        print("❌ ERRO durante a transformação bronze → silver:")
        raise e

    print("✔ Camada silver atualizada com sucesso.")

    print("=======================================")
    print("     🎉 PIPELINE EXECUTADO SEM ERROS    ")
    print("=======================================")


# =================
# EXECUÇÃO DIRETA =
# =================

if __name__ == "__main__":
    run_pipeline()
