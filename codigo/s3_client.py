# =============
# BIBLIOTECAS =
# =============

import json
import os
from datetime import datetime

# ===================================
# FUNCTION PARA SALVAR ARQUIVO JSON =
# ===================================

def salvar_json(data, base_path, filename):
    """
    Salva um arquivo JSON diretamente na pasta base,
    adicionando a data do dia no próprio nome do arquivo.

    Exemplo final:
        base_path/monday_raw_2025-02-10_153210.json
    """

    # Adiciona data ao nome do arquivo (opcional — deixe assim se quiser)
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    filename_com_data = f"{data_hoje}_{filename}"

    # Garante que a pasta exista
    os.makedirs(base_path, exist_ok=True)

    # Caminho final do arquivo
    full_path = os.path.join(base_path, filename_com_data)

    # Salva o JSON
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✔ JSON salvo em: {full_path}")
    return full_path
