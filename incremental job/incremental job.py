import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
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

# Script generated for node fetch_details table
fetch_detailstable_node1760794395144 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "fetch_details",
        "connectionName": "Postgresql connection",
    },
    transformation_ctx = "fetch_detailstable_node1760794395144"
)

# Script generated for node orders table
orderstable_node1760794399635 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "orders",
        "connectionName": "Postgresql connection",
    },
    transformation_ctx = "orderstable_node1760794399635"
)

# Script generated for node SQL Query
SqlQuery17 = '''
select * from orders where order_last_updated >
(select max(last_fetched) from orders_fd
where tablename = 'orders')
'''
SQLQuery_node1760794543321 = sparkSqlQuery(glueContext, query = SqlQuery17, mapping = {"orders_fd":fetch_detailstable_node1760794395144, "orders":orderstable_node1760794399635}, transformation_ctx = "SQLQuery_node1760794543321")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1760794543321, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760793028063", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760794692710 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1760794543321, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-fs/landing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1760794692710")

job.commit()