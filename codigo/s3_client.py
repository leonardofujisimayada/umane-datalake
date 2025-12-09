import json
import os
from datetime import datetime

def salvar_json(json_data, pasta_destino, nome_arquivo=None):
    """
    Salva um JSON em um arquivo .json dentro da pasta informada.

    Param:
        json_data (dict): JSON a ser salvo.
        pasta_destino (str): caminho da pasta onde o arquivo deve ser salvo.
        nome_arquivo (str): opcional. 

    Retorno:
        str: caminho completo do arquivo salvo.
    """

    # Criar pasta
    os.makedirs(pasta_destino, exist_ok=True)

    # Se o nome estiver ausente, cria automaticamente
    if nome_arquivo is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"arquivo_{timestamp}.json"

    # Garantir que termina com .json
    if not nome_arquivo.endswith(".json"):
        nome_arquivo += ".json"

    # Caminho final do arquivo
    caminho_completo = os.path.join(pasta_destino, nome_arquivo)

    # Salvar o JSON
    with open(caminho_completo, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

    return caminho_completo
