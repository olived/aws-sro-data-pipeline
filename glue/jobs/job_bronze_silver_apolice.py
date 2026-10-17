import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Bronze Apolice
BronzeApolice_node1786885176412 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://dgo-sro-data-lake/bronze/apolice/"], "recurse": True}, transformation_ctx="BronzeApolice_node1786885176412")

# Script generated for node Change Schema
ChangeSchema_node1786885701164 = ApplyMapping.apply(frame=BronzeApolice_node1786885176412, mappings=[("codigo_seguradora", "long", "codigo_seguradora", "long"), ("apolice_codigo", "string", "apolice_codigo", "string"), ("certificado_codigo", "string", "certificado_codigo", "string"), ("tipo_documento_emitido", "long", "tipo_documento_emitido", "long"), ("natureza_documento", "long", "natureza_documento", "long"), ("data_emissao", "string", "data_emissao", "timestamp"), ("data_inicio", "string", "data_inicio", "timestamp"), ("data_termino", "string", "data_termino", "timestamp"), ("moeda_apolice", "string", "moeda_apolice", "string"), ("limite_maximo_garantia_real", "long", "limite_maximo_garantia_real", "long"), ("percentual_retido", "double", "percentual_retido", "double"), ("possui_beneficiario", "boolean", "possui_beneficiario", "boolean"), ("possui_beneficiario_final", "boolean", "possui_beneficiario_final", "boolean"), ("possui_intermediario", "boolean", "possui_intermediario", "boolean"), ("retificacao_registro", "boolean", "retificacao_registro", "boolean"), ("premio_valor_total_real", "long", "premio_valor_total_real", "long"), ("premio_iof", "double", "premio_iof", "double"), ("numero_parcelas", "long", "numero_parcelas", "long")], transformation_ctx="ChangeSchema_node1786885701164")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1786885701164, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786884978635", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786887477187 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1786885701164, connection_type="s3", format="glueparquet", connection_options={"path": "s3://dgo-sro-data-lake/silver/apolice/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1786887477187")

job.commit()