# =============
# BIBLIOTECAS =
# =============

import json
from datetime import datetime
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io

# =========================================
# FUNCTION PARA SALVAR ARQUIVO JSON NO S3 =
# =========================================

def salvar_json_s3(data, bucket, prefix, filename):
    """
    Salva JSON no S3 no formato:

        s3://bucket/prefix/YYYYMM/filename

    O timestamp deve vir dentro de `filename`, para manter consistência.
    """

    s3 = boto3.client("s3")

    ano_mes = datetime.now().strftime("%Y%m")

    # NÃO gerar timestamp aqui!
    # Usar exatamente o filename enviado pelo pipeline.
    s3_key = f"{prefix}/{ano_mes}/{filename}"

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data, indent=2, ensure_ascii=False),
        ContentType="application/json"
    )

    caminho_final = f"s3://{bucket}/{s3_key}"
    print(f"✔ JSON salvo no S3 em:\n{caminho_final}")

    return caminho_final


# ============================================
# FUNCTION PARA SALVAR ARQUIVO PARQUET NO S3 =
# ============================================

def salvar_parquet_s3(df, bucket, prefix, filename):

    s3 = boto3.client("s3")

    ano_mes = datetime.now().strftime("%Y%m")

    # NÃO adicione timestamp aqui!
    s3_key = f"{prefix}/{ano_mes}/{filename}"

    # Converter para Parquet em memória
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
