# =============
# BIBLIOTECAS =
# =============

import uuid
import unidecode
import re
import pandas as pd

# ====================================
# FUNCTIONS PARA CRIAR A CAMADA OURO =
# ====================================

def normalizar_nome_coluna(nome):
    nome = unidecode.unidecode(nome)
    nome = nome.lower()
    nome = re.sub(r'[^a-z0-9]+', '_', nome)
    nome = re.sub(r'_+', '_', nome)
    return nome.strip('_')

def somar_coluna_pipe(x):
    if pd.isna(x):
        return 0
    valores = x.split("|")
    resultado = 0
    for v in valores:
        v = v.strip()
        if not v:
            continue
        # remove milhares e troca vírgula decimal
        v = v.replace(".", "").replace(",", ".")
        try:
            resultado += float(v)
        except:
            pass
    return resultado

def gerar_id_estavel(texto):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, texto.lower().strip()))

def criar_camada_ouro(df):
    df = df.copy()
    
    df["id_projeto"] = df["item_name"].apply(gerar_id_estavel)
    
    df.columns = [normalizar_nome_coluna(c) for c in df.columns]
    
    colunas_soma = [
        "valor_total_do_orcamento",
        "valor_total_da_avaliacao",
        "valor_total_coinvestimento"
    ]
    
    for col in colunas_soma:
        if col in df.columns:
            df[col] = df[col].apply(somar_coluna_pipe)

    return df
