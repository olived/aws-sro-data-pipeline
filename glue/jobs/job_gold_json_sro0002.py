
import sys
import json
import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ============================================================
# 1. Inicialização
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ============================================================
# 2. Configurações
# ============================================================

DATABASE = "sro_catalog"
TABLE = "endosso_gold"

BUCKET = "dgo-sro-data-lake"
OUTPUT_KEY = "gold/json/sro0002/SRO0002.json"

# ============================================================
# 3. Leitura da camada Gold
# ============================================================

df = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=TABLE
).toDF()

print("Schema da Gold:")
df.printSchema()

# ============================================================
# 4. Montagem do JSON SRO0002
# ============================================================

registros = df.collect()

endossos = []

for row in registros:

    endosso = {
        "codigo_seguradora": row["codigo_seguradora"],
        "total_premio": row["total_premio"],
        "quantidade_endossos": row["count_alteracao_codigo_#0"],
        "total_iof": row["sum_premio_iof_#1"]
    }

    endossos.append(endosso)

resultado = {
    "sro": "SRO0002",
    "endossos": endossos
}

# ============================================================
# 5. Gravação no Amazon S3
# ============================================================

s3 = boto3.client("s3")

s3.put_object(
    Bucket=BUCKET,
    Key=OUTPUT_KEY,
    Body=json.dumps(
        resultado,
        ensure_ascii=False,
        indent=4,
        default=str
    ).encode("utf-8"),
    ContentType="application/json"
)

print(f"JSON gerado: s3://{BUCKET}/{OUTPUT_KEY}")
print(f"Quantidade de registros Gold: {len(endossos)}")

# ============================================================
# 6. Finalização
# ============================================================

job.commit()