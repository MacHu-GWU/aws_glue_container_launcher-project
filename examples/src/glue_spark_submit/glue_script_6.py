# -*- coding: utf-8 -*-

"""
This Glue ETL Script perform snapshot query from a Hudi table on your
local laptop using glue container. It covers the following topics:

- how to import custom code from your custom glue python library
- how to import third party python library
- how to set job name, job run id, and job arguments
- how to read data from Hudi table
"""

# standard libraries
import os
import sys

# third party libraries
import boto3

# pyspark / glue stuff
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# --- detect if we are in a glue container
IS_GLUE_CONTAINER: bool = os.environ.get("IS_GLUE_CONTAINER", "false") == "true"

# add workspace dir to system path so it can import your project glue python library
if IS_GLUE_CONTAINER:
    sys.path.append("/home/glue_user/workspace")

# --- create spark session
conf = (
    SparkConf()
    .setAppName("myapp")
    .setAll(
        [
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            ("spark.sql.hive.convertMetastoreParquet", "true"),
        ]
    )
)
spark_ses = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark_ctx = spark_ses.sparkContext
if IS_GLUE_CONTAINER:
    spark_ctx.setLogLevel("ERROR")
glue_ctx = GlueContext(spark_ctx)

# --- resolve args
print(f"sys.argv = {sys.argv}")
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
    ],
)
print("--- args ---")
for k, v in args.items():
    print(f"{k} = {v}")

# --- create glue job
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# --- get account and region
boto_ses = boto3.session.Session()
aws_account_id = boto_ses.client("sts").get_caller_identity()["Account"]
aws_region = boto_ses.region_name

print(f"aws_account_id = {aws_account_id}")
print(f"aws_region = {aws_region}")

# --- read data
database = "mydatabase"
table = "glue_spark_submit_test_5_mytable"
bucket = f"{aws_account_id}-{aws_region}-data"
prefix = (
    f"projects/aws_glue_container_launcher/glue_spark_submit/test5/{database}/{table}/"
)

delimiter = ","
partition_paths_str = delimiter.join([
    f"s3://{bucket}/{prefix}year=2000/month=01/day=01",
    f"s3://{bucket}/{prefix}year=2000/month=01/day=02",
    f"s3://{bucket}/{prefix}year=2000/month=01/day=03",
])
query_options = {
    "hoodie.datasource.query.type": "snapshot",
}
df = (
    spark_ses.read.format("org.apache.hudi")
    .option("hoodie.datasource.read.paths", partition_paths_str)
    .options(**query_options).load()
)
df.show()

# --- transform data

# --- write data
console_url = f"https://{aws_region}.console.aws.amazon.com/athena/home?region={aws_region}#/query-editor/"
print(f"preview at athena: {console_url}")

# --- commit job
job.commit()
