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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1784994619149 = glueContext.create_dynamic_frame.from_catalog(database="sro_catalog", table_name="apolice", transformation_ctx="AWSGlueDataCatalog_node1784994619149")

# Script generated for node Change Schema
ChangeSchema_node1784994971318 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1784994619149, mappings=[("codigo_seguradora", "long", "codigo_seguradora", "long"), ("apolice_codigo", "string", "apolice_codigo", "string"), ("certificado_codigo", "string", "certificado_codigo", "string"), ("tipo_documento_emitido", "long", "tipo_documento_emitido", "long"), ("natureza_documento", "long", "natureza_documento", "long"), ("data_emissao", "string", "data_emissao", "string"), ("data_inicio", "string", "data_inicio", "string"), ("data_termino", "string", "data_termino", "string"), ("moeda_apolice", "string", "moeda_apolice", "string"), ("limite_maximo_garantia_real", "long", "limite_maximo_garantia_real", "long"), ("percentual_retido", "double", "percentual_retido", "double"), ("possui_beneficiario", "boolean", "possui_beneficiario", "boolean"), ("possui_beneficiario_final", "boolean", "possui_beneficiario_final", "boolean"), ("possui_intermediario", "boolean", "possui_intermediario", "boolean"), ("retificacao_registro", "boolean", "retificacao_registro", "boolean"), ("premio_valor_total_real", "long", "premio_valor_total_real", "long"), ("premio_iof", "double", "premio_iof", "double"), ("numero_parcelas", "long", "numero_parcelas", "long")], transformation_ctx="ChangeSchema_node1784994971318")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1784994971318, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1784994041037", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1784995046314 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1784994971318, connection_type="s3", format="glueparquet", connection_options={"path": "s3://dgo-sro-data-lake/bronze/apolice/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1784995046314")

job.commit()