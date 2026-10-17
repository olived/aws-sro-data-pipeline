import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

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
AWSGlueDataCatalog_node1791552415897 = glueContext.create_dynamic_frame.from_catalog(database="sro_catalog", table_name="apolice_silver", transformation_ctx="AWSGlueDataCatalog_node1791552415897")

# Script generated for node Aggregate
Aggregate_node1791552619781 = sparkAggregate(glueContext, parentFrame = AWSGlueDataCatalog_node1791552415897, groups = ["codigo_seguradora", "moeda_apolice"], aggs = [["premio_valor_total_real", "sum"], ["limite_maximo_garantia_real", "sum"], ["apolice_codigo", "count"]], transformation_ctx = "Aggregate_node1791552619781")

# Script generated for node Change Schema
ChangeSchema_node1791553987823 = ApplyMapping.apply(frame=Aggregate_node1791552619781, mappings=[("codigo_seguradora", "long", "codigo_seguradora", "long"), ("moeda_apolice", "string", "moeda_apolice", "string"), ("`sum(premio_valor_total_real)`", "long", "total_premio", "long"), ("`sum(limite_maximo_garantia_real)`", "long", "total_limite_garantia", "long"), ("`count(apolice_codigo)`", "long", "quantidade_apolices", "long")], transformation_ctx="ChangeSchema_node1791553987823")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1791553987823, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791552246558", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791554179574 = glueContext.getSink(path="s3://dgo-sro-data-lake/gold/apolice/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791554179574")
AmazonS3_node1791554179574.setCatalogInfo(catalogDatabase="sro_catalog",catalogTableName="apolice_gold")
AmazonS3_node1791554179574.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791554179574.writeFrame(ChangeSchema_node1791553987823)
job.commit()