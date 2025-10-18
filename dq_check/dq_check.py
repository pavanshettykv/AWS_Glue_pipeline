import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import *

s3_client = boto3.client('s3')

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

# Script generated for node Amazon S3
AmazonS3_node1760800806304 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://retail-fs/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1760800806304")

# Script generated for node SQL Query
SqlQuery0 = '''
select count(*) as invalid_count from orders_dq
where order_status not in (
'ON_HOLD','PAYMENT_REVIEW','PROCESSING','CLOSED','SUSPECTED_FRAUD','COMPLETE','PENDING','CANCELED','PENDING_PAYMENT')
'''
SQLQuery_node1760800811849 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"orders_dq":AmazonS3_node1760800806304}, transformation_ctx = "SQLQuery_node1760800811849")

invalid_count = SQLQuery_node1760800811849.toDF().collect()[0][0]
print(f"invalid count = {invalid_count}")

bucket_name ='retail-fs'
landing_folder='landing/'
discarded_folder='discard/'
archive_folder='archive/'
staging_folder='staging/'

# Define a function to move files from one S3 location to another 
def move_files(src_folder, dest_folder, archive_folder):
# List objects in the source folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=src_folder)
# Check if there are any files in the source folder
    if 'Contents' in response:
    # Iterate over each object and move it to the destination and archive 
        for obj in response['Contents']:
            source_key = obj['Key']
            # Avoid processing the directory itself (only process files)
            if not source_key.endswith('/'):
                destination_key = source_key.replace(src_folder, dest_folder) 
                archive_key = source_key.replace(src_folder, archive_folder)
                
                # Copy file to the destination folder
                s3_client.copy_object(CopySource={'Bucket': bucket_name, 'Key': source_key}, Bucket=bucket_name, Key=destination_key)

                # Copy file to the archive folder
                s3_client.copy_object(CopySource={'Bucket': bucket_name, 'Key': source_key}, Bucket=bucket_name,Key=archive_key)

                # Delete the original file to complete the move operation 
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                
                
                
if invalid_count > 0: 
    print(f"Found {invalid_count} invalid records. Moving files to 'discarded' folder")
    move_files(landing_folder, discarded_folder, archive_folder)

else:
    print("No invalid records found. Moving files to 'staging' folder.") 
    move_files(landing_folder, staging_folder, archive_folder)
    print("File movement complete.")


job.commit()
