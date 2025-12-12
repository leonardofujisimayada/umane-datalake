"""
Criação da camada Ouro do Data Lake Monday.

Características da camada Ouro:
- Colunas normalizadas (snake_case, sem acentos)
- Conversão de colunas monetárias com valores concatenados por pipe
- Geração de ID_PROJETO estável (UUID5) para JOIN entre boards
- Preservação da coluna board_origem
- Dataset pronto para BI, analytics e modelagem estrela
"""

import uuid
import re
import pandas as pd
import unidecode


# =========================================================
# Utilidades
# =========================================================

def normalizar_nome_coluna(nome: str) -> str:
    """
    Normaliza nomes de colunas para padrão data lake:
    - remove acentos
    - lowercase
    - substitui caracteres especiais por "_"
    - remove underscores duplicados
    """
    nome = unidecode.unidecode(nome)
    nome = nome.lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def somar_coluna_pipe(valor):
    """
    Converte strings no formato:
        "10 | 20 | 30"
    em valor numérico somado.

    Retorna 0 para NaN ou strings vazias.
    """
    if pd.isna(valor):
        return 0.0

    total = 0.0
    partes = str(valor).split("|")

    for p in partes:
        p = p.strip()
        if not p:
            continue

        # Remove separador de milhar e ajusta decimal
        p = p.replace(".", "").replace(",", ".")

        try:
            total += float(p)
        except ValueError:
            continue

    return total


def gerar_id_estavel(texto: str) -> str:
    """
    Gera um UUID determinístico (UUID5) a partir de um texto.

    A mesma entrada sempre produzirá o mesmo ID.
    """
    if pd.isna(texto):
        return None

    texto = str(texto).lower().strip()
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, texto))


def obter_chave_projeto(df: pd.DataFrame) -> pd.Series:
    """
    Define a melhor chave de negócio para o projeto.

    Prioridade:
    1. codigo_projeto (se existir)
    2. item_name
    """
    if "codigo_projeto" in df.columns:
        return df["codigo_projeto"].fillna(df["item_name"])

    return df["item_name"]


# =========================================================
# Função principal: Prata → Ouro
# =========================================================

def criar_camada_ouro(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as transformações finais para a camada Ouro.

    Transformações:
    - Criação da chave de projeto
    - Geração do ID_PROJETO estável
    - Normalização de nomes das colunas
    - Conversão de colunas monetárias
    - Preservação de board_origem
    """

    df = df.copy()

    # -----------------------------
    # Chave de negócio do projeto
    # -----------------------------
    df["chave_projeto"] = obter_chave_projeto(df)

    # -----------------------------
    # ID_PROJETO estável (JOIN)
    # -----------------------------
    df["id_projeto"] = df["chave_projeto"].apply(gerar_id_estavel)

    # -----------------------------
    # Normalizar nomes das colunas
    # -----------------------------
    df.columns = [normalizar_nome_coluna(c) for c in df.columns]

    # -----------------------------
    # Colunas monetárias com pipe
    # -----------------------------
    colunas_soma = [
        "valor_total_do_orcamento",
        "valor_total_da_avaliacao",
        "valor_total_coinvestimento",
    ]

    for col in colunas_soma:
        if col in df.columns:
            df[col] = df[col].apply(somar_coluna_pipe)

    return df
