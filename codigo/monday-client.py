# ---------------------
# IMPORTA BIBLIOTECAS - 
# ---------------------

import requests
from config import MONDAY_API_KEY, MONDAY_ENDPOINT

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
# --------------------------------------
# FUNCTION PARA BUSCAR DADOS DA MONDAY - 
# --------------------------------------

def busca_dados_monday(query):
    headers = {"Authorization": MONDAY_API_KEY}
    response = requests.post(
        MONDAY_ENDPOINT,
        json={"query": query},
        headers=headers
    )
    response.raise_for_status()
    return response.json()


