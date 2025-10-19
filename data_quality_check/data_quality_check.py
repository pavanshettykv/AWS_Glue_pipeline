import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import concurrent.futures
import re
import boto3 

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

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

# Script generated for node s3-landing
s3landing_node1760868098054 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://retail-fs/landing/"], "recurse": True}, transformation_ctx="s3landing_node1760868098054")

# Script generated for node valid_order_status_lk
valid_order_status_lk_node1760868422249 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "valid_order_status_lk",
        "connectionName": "Postgresql connection",
    },
    transformation_ctx = "valid_order_status_lk_node1760868422249"
)

# Script generated for node Join
s3landing_node1760868098054DF = s3landing_node1760868098054.toDF()
valid_order_status_lk_node1760868422249DF = valid_order_status_lk_node1760868422249.toDF()
Join_node1760868555235 = DynamicFrame.fromDF(s3landing_node1760868098054DF.join(valid_order_status_lk_node1760868422249DF, (s3landing_node1760868098054DF['order_status'] == valid_order_status_lk_node1760868422249DF['status_name']), "left"), glueContext, "Join_node1760868555235")

# Script generated for node SQL Query
SqlQuery0 = '''
select order_id,order_last_updated,customer_id,order_status,
coalesce(status_name,'INVALID') status_name
from myDataSource
'''
SQLQuery_node1760868985895 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1760868555235}, transformation_ctx = "SQLQuery_node1760868985895")

# Script generated for node Conditional Router
ConditionalRouter_node1760869235025 = threadedRoute(glueContext,
  source_DyF = SQLQuery_node1760868985895,
  group_filters = [GroupFilter(name = "Invalid_records", filters = lambda row: (bool(re.match("INVALID", row["status_name"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("INVALID", row["status_name"])))))])

# Script generated for node default_group
default_group_node1760869235667 = SelectFromCollection.apply(dfc=ConditionalRouter_node1760869235025, key="default_group", transformation_ctx="default_group_node1760869235667")

# Script generated for node Invalid_records
Invalid_records_node1760869235761 = SelectFromCollection.apply(dfc=ConditionalRouter_node1760869235025, key="Invalid_records", transformation_ctx="Invalid_records_node1760869235761")

# Script generated for node invalid_records
valid_records_node1760869191037 = ApplyMapping.apply(frame=default_group_node1760869235667, mappings=[("order_last_updated", "timestamp", "order_last_updated", "timestamp"), ("customer_id", "int", "customer_id", "int"), ("order_id", "int", "order_id", "int"), ("order_status", "string", "order_status", "string")], transformation_ctx="valid_records_node1760869191037")

# Script generated for node valid_records
invalid_records_node1760869386366 = ApplyMapping.apply(frame=Invalid_records_node1760869235761, mappings=[("order_id", "int", "order_id", "int"), ("order_last_updated", "timestamp", "order_last_updated", "timestamp"), ("customer_id", "int", "customer_id", "int"), ("order_status", "string", "order_status", "string")], transformation_ctx="invalid_records_node1760869386366")

# Script generated for node s3-archive
EvaluateDataQuality().process_rows(frame=s3landing_node1760868098054, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760866357497", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
s3archive_node1760869482629 = glueContext.write_dynamic_frame.from_options(frame=s3landing_node1760868098054, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-fs/archive/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="s3archive_node1760869482629")

# Script generated for node s3-discard
EvaluateDataQuality().process_rows(frame=invalid_records_node1760869386366, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760866357497", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
s3discard_node1760869425139 = glueContext.write_dynamic_frame.from_options(frame=invalid_records_node1760869386366, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-fs/discard/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="s3discard_node1760869425139")

# Script generated for node s3-staging
EvaluateDataQuality().process_rows(frame=valid_records_node1760869191037, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760866357497", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
s3staging_node1760869430023 = glueContext.write_dynamic_frame.from_options(frame=valid_records_node1760869191037, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-fs/staging/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="s3staging_node1760869430023")


bucket_name ='retail-fs'
landing_folder='landing/'

print("Deleting files from landing folder")
s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=landing_folder)
# Check if there are any files in the source folder
if 'Contents' in response:
    # Iterate over each object and move it to the destination and archive 
    for obj in response['Contents']:
        source_key = obj['Key']
        # Avoid processing the directory itself (only process files)
        if not source_key.endswith('/'):
            s3_client.delete_object(Bucket=bucket_name, Key=source_key)

job.commit()