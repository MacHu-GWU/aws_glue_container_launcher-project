# -*- coding: utf-8 -*-

"""
This Glue ETL Script perform upsert to a Hudi table (can automatically create
and update) on your local laptop using glue container. It covers the following topics:

- how to import custom code from your custom glue python library
- how to import third party python library
- how to set job name, job run id, and job arguments
- how to write data to Hudi table
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
df = spark_ses.createDataFrame(
    [
        # you can run this script two times
        # first time, insert two rows
        ("id-1", "2000", "01", "01", "2000-01-01 00:00:00", 1),
        ("id-2", "2000", "01", "02", "2000-01-01 00:00:00", 2),
        # second time, update id-2 and insert id-3
        # ("id-2", "2000", "01", "02", "2000-01-01 00:00:00", 222),
        # ("id-3", "2000", "01", "03", "2000-01-01 00:00:00", 3),
    ],
    ("id", "year", "month", "day", "ts", "value"),
)
df.show()

# --- transform data

# --- write data
database = "mydatabase"
table = "glue_spark_submit_test_5_mytable"
bucket = f"{aws_account_id}-{aws_region}-data"
prefix = (
    f"projects/aws_glue_container_launcher/glue_spark_submit/test5/{database}/{table}/"
)

additional_options = {
    "hoodie.table.name": table,
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "year,month,day",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": database,
    "hoodie.datasource.hive_sync.table": table,
    "hoodie.datasource.hive_sync.partition_fields": "year,month,day",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": f"s3://{bucket}/{prefix}",
}
(
    df.write.format("hudi")
    .options(**additional_options)
    # first time you can use overwrite mode to create the table
    .mode("overwrite")
    # second time you can use append mode to perform upsert
    # if you keep using overwrite, then it will delete existing data before writing
    # .mode("append")
    .save()
)

console_url = (
    f"https://{aws_region}.console.aws.amazon.com/s3/buckets"
    f"/{bucket}?prefix={prefix}&region={aws_region}"
)
print(f"preview at s3: {console_url}")

console_url = f"https://{aws_region}.console.aws.amazon.com/athena/home?region={aws_region}#/query-editor/"
print(f"preview at athena: {console_url}")

# --- commit job
job.commit()
