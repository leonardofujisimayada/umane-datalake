"""
Módulo utilitário para escrita de arquivos JSON e Parquet na AWS S3.

Funções principais:
    - salvar_json_s3: escreve arquivos brutos (camada Bronze)
    - salvar_parquet_s3: escreve dataframes transformados (camadas Prata e Ouro)

Ambas as funções seguem o padrão de particionamento por ano/mês (YYYYMM) e
não geram timestamp interno, permitindo versionamento controlado pelo pipeline.
"""

import json
from datetime import datetime
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io


def salvar_json_s3(data, bucket, prefix, filename):
    """
    Salva um arquivo JSON no S3 conforme padrão do Data Lake:

        s3://bucket/prefix/YYYYMM/filename

    Args:
        data (dict | list): Dados estruturados a serem salvos em JSON.
        bucket (str): Nome do bucket S3.
        prefix (str): Subpasta lógica (ex.: "monday/funil_originacao").
        filename (str): Nome final do arquivo (já contendo timestamp).

    Returns:
        str: Caminho completo do arquivo no S3.

    Observação:
        O timestamp NÃO é gerado dentro da função.
        Ele deve ser fornecido externamente pelo pipeline.
    """

    s3 = boto3.client("s3")
    ano_mes = datetime.now().strftime("%Y%m")
    s3_key = f"{prefix}/{ano_mes}/{filename}"

    # Serialização JSON com indentação para facilitar leitura manual
    conteudo = json.dumps(data, indent=2, ensure_ascii=False)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=conteudo,
        ContentType="application/json"
    )

    caminho_final = f"s3://{bucket}/{s3_key}"
    print(f"✔ JSON salvo no S3 em:\n{caminho_final}")
    return caminho_final


def salvar_parquet_s3(df, bucket, prefix, filename):
    """
    Salva um DataFrame Pandas como arquivo Parquet no S3.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        bucket (str): Nome do bucket S3.
        prefix (str): Caminho lógico (ex.: "monday/funil_originacao").
        filename (str): Nome do arquivo .parquet.

    Returns:
        str: Caminho completo do Parquet salvo no S3.

    Observação:
        A escrita é feita em memória utilizando BytesIO para eficiência.
    """

    s3 = boto3.client("s3")
    ano_mes = datetime.now().strftime("%Y%m")
    s3_key = f"{prefix}/{ano_mes}/{filename}"

    # Conversão para Parquet em memória (evita escrita local intermediária)
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    caminho = f"s3://{bucket}/{s3_key}"
    print(f"✔ Parquet salvo no S3: {caminho}")
    return caminho
