
import sys
import json
import boto3

from datetime import datetime, timezone
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Inicializacao do Glue Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Configuracoes
DATABASE = "sro_catalog"
TABLE = "apolice_gold"
BUCKET = "dgo-sro-data-lake"
KEY = "gold/json/sro0001/SRO0001.json"

# Leitura da camada Gold
df = spark.table(f"{DATABASE}.{TABLE}")

# Agregacoes
resumo = df.agg(
    F.sum("quantidade_apolices").alias("quantidade_apolices"),
    F.sum("total_premio").alias("total_premio"),
    F.sum("total_limite_garantia").alias("total_limite_garantia")
).first()

def numero(valor):
    return float(valor) if valor is not None else 0.0

# Estrutura JSON simplificada
documento = {
    "arquivo": "SRO0001",
    "codigo_seguradora": 5185,
    "moeda": "BRL",
    "resumo": {
        "quantidade_apolices": int(
            resumo["quantidade_apolices"] or 0
        ),
        "total_premio": numero(resumo["total_premio"]),
        "total_limite_garantia": numero(
            resumo["total_limite_garantia"]
        )
    },
    "data_processamento": datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")
}

# Gravacao no Amazon S3
s3 = boto3.client("s3")

s3.put_object(
    Bucket=BUCKET,
    Key=KEY,
    Body=json.dumps(
        documento,
        ensure_ascii=False,
        indent=2
    ).encode("utf-8"),
    ContentType="application/json"
)

print(f"Arquivo gerado: s3://{BUCKET}/{KEY}")
print(json.dumps(documento, indent=2))

job.commit()
