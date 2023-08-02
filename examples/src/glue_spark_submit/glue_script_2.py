# -*- coding: utf-8 -*-

"""
This Glue ETL Script show you how to run a regular glue job in a glue container
on your local laptop. It covers the following topics:

- how to import custom code from your custom glue python library
- how to import third party python library
- how to set job name, job run id, and job arguments
- how to read data from s3
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
bucket = f"{aws_account_id}-{aws_region}-data"
prefix = (
    "projects/aws_glue_container_launcher/glue_spark_submit/test1/mydatabase/mytable/"
)
# ref: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader-from_options
df = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options=dict(
        paths=[f"s3://{bucket}/{prefix}"],
        recurse=True,
    ),
    format="parquet",
    transformation_ctx="datasource",
)
df.toDF().show()

# --- transform data

# --- write data

# --- commit job
job.commit()
