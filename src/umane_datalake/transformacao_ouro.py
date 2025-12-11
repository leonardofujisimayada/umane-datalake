"""
Criação da camada Ouro do Data Lake Monday.

A camada Ouro possui:
- Colunas normalizadas (snake_case, sem acentos)
- Tratamento de valores monetários concatenados por pipe ("10 | 20")
- Geração de ID estável baseado no nome do item
- Dataset pronto para análises e BI

Função principal:
    criar_camada_ouro(df)
"""

import uuid
import unidecode
import re
import pandas as pd


def normalizar_nome_coluna(nome):
    """
    Converte o nome da coluna para formato padrão:
        - remove acentos
        - lowercase
        - substitui caracteres especiais por "_"
        - remove duplicidade de underscore
    """
    nome = unidecode.unidecode(nome)
    nome = nome.lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def somar_coluna_pipe(x):
    """
    Converte colunas do tipo:
        "10 | 20 | 30"
    em valores numéricos somados.

    Args:
        x (str): String contendo números separados por pipe.

    Returns:
        float: Soma dos valores convertidos.
    """
    if pd.isna(x):
        return 0

    valores = x.split("|")
    total = 0

    for v in valores:
        v = v.strip()
        if not v:
            continue

        # Remove separador de milhares e converte vírgula decimal para ponto
        v = v.replace(".", "").replace(",", ".")

        try:
            total += float(v)
        except:
            pass

    return total


def gerar_id_estavel(texto):
    """
    Gera um UUID previsível (UUID5) a partir do nome do item.

    Args:
        texto (str): Nome do item.

    Returns:
        str: UUID estável.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, texto.lower().strip()))


def criar_camada_ouro(df):
    """
    Aplica todas as transformações finais para o dataset Ouro.

    Transformações:
        - Criação de ID estável
        - Normalização de nomes de colunas
        - Conversão e soma de colunas monetárias
        - Retorno do DataFrame pronto para análises

    Args:
        df (pd.DataFrame): DataFrame já transformado na camada Prata.

    Returns:
        pd.DataFrame: DataFrame final da camada Ouro.
    """

    df = df.copy()

    # ID único baseado no nome
    df["id_projeto"] = df["item_name"].apply(gerar_id_estavel)

    # Normalizar nomes das colunas
    df.columns = [normalizar_nome_coluna(c) for c in df.columns]

    # Colunas numéricas representadas como pipe-separated
    colunas_soma = [
        "valor_total_do_orcamento",
        "valor_total_da_avaliacao",
        "valor_total_coinvestimento",
    ]

    for col in colunas_soma:
        if col in df.columns:
            df[col] = df[col].apply(somar_coluna_pipe)

    return df

