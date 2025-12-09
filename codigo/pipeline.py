# ---------------------
# IMPORTA BIBLIOTECAS - 
# ---------------------

import os
from datetime import datetime
from monday_client import busca_dados_monday
from s3_client import salvar_json
from transformacao import transformar_bronze_para_silver

# Caminhos locais simulando o datalake
PATH_BRONZE = "dados/bronze"
PATH_SILVER = "dados/prata"

# ------------------------------------------
# QUERY PARA RETORNAR DADOS DO BOARD FUNIL - 
# ------------------------------------------

query_funil_originacao = ''' 
query GetBoardItems{  
  boards(ids: 9718729717) {  
    items_page {        
      items {  
        id  
        name        
        column_values {  
          id 
          column {
            title
            description            
          }          
          type
          text
          value    
        }  
      }  
    }  
  }  
}
'''

# ------------------------------------------
# FUNCTION PARA SALVAR JSON NA PASTA DADOS - 
# ------------------------------------------

def run_pipeline():

    # ----------------
    # 1. Baixa dados - 
    # ----------------
    print("➡ Obtendo dados do Monday...")
    data = busca_dados_monday(query_funil_originacao)

    # Criar pasta se for inexistente
    os.makedirs(PATH_BRONZE, exist_ok=True)

    # Nomeia o arquivo bronze com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"monday_raw_{timestamp}.json"
    json_path = os.path.join(PATH_BRONZE, json_filename)

    print("➡ Salvando JSON bruto (camada bronze)...")
    salvar_json(data, PATH_BRONZE, json_filename)

    # --------------------------------------------------
    # 2. Transforma dados da camada bronze para silver -
    # --------------------------------------------------
    print("➡ Iniciando tratamento (bronze → silver)...")
    transformar_bronze_para_silver(PATH_BRONZE, PATH_SILVER)

    print("✔ Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    run_pipeline()