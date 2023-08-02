# -*- coding: utf-8 -*-

"""
This Glue ETL Script readfrom an existing Glue catalog table with partitions,
on your local laptop using glue container. It covers the following topics:

- how to import custom code from your custom glue python library
- how to import third party python library
- how to set job name, job run id, and job arguments
- how to read data from glue catalog
- how to use push_down_predicate or catalogPartitionPredicate to filter partitions
"""

# standard libraries
import os
import sys

# third party libraries
import boto3
import s3pathlib

# pyspark / glue stuff
from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# --- detect if we are in a glue container
IS_GLUE_CONTAINER: bool = (os.environ.get("IS_GLUE_CONTAINER", "false") == "true")

# add workspace dir to system path so it can import your project glue python library
if IS_GLUE_CONTAINER:
    sys.path.append("/home/glue_user/workspace")

# the current project glue python library
import aws_glue_container_launcher

# --- verify we can import extra python libraries
print(f"s3pathlib = {s3pathlib.__version__}")
print(f"aws_glue_container_launcher = {aws_glue_container_launcher.__version__}")


# --- create spark session
spark_ctx = SparkContext.getOrCreate()
if IS_GLUE_CONTAINER:
    spark_ctx.setLogLevel("ERROR")
glue_ctx = GlueContext(spark_ctx)
spark_ses = glue_ctx.spark_session

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
table = "glue_spark_submit_test_3_mytable"

df = glue_ctx.create_dynamic_frame.from_catalog(
    database=database,
    table_name=table,
    # you can use push_down_predicate to scan less data using partition
    # however, this is basically loading all partition into memory
    # then filter partitions based on push_down_predicate
    # if you have millions of partitions, this will be slow
    # you can use catalogPartitionPredicate to filter partitions more efficiently
    # ref: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-partitions.html#aws-glue-programming-etl-partitions-pushdowns
    push_down_predicate="(year == '2000' and month == '01' and day <= '02')",
    # you can use catalogPartitionPredicate to filter partitions more efficiently
    # using partition index
    # ref: https://docs.aws.amazon.com/glue/latest/dg/partition-indexes.html#partition-index-creating-table
    additional_options=dict(
        catalogPartitionPredicate="(year = '2000' and month = '01' and day >= '02')"
    ),
    transformation_ctx="datasource",
).toDF()
df.show()

# --- transform data

# --- write data

# --- commit job
job.commit()
