# -*- coding: utf-8 -*-

"""
This script demonstrates how to use ``aws_glue_container_launcher`` library
to create a shell script that can be used to run a Glue ETL related tasks
in container.
"""

import typing as T

import boto3
import sys
import subprocess
from pathlib import Path

from s3pathlib import S3Path, context

from aws_glue_container_launcher.docker_command import (
    build_spark_submit_args,
    build_jupyter_lab_args,
)

boto_ses = boto3.session.Session(profile_name="awshsh_app_dev_us_east_1")
aws_account_id = boto_ses.client("sts").get_caller_identity()["Account"]
aws_region = boto_ses.region_name
context.attach_boto_session(boto_ses)

dir_home = Path.home()
dir_examples = Path(__file__).absolute().parent
dir_workspace = dir_examples.parent
dir_site_packages = dir_workspace.joinpath(
    ".venv",
    "lib",
    f"python{sys.version_info.major}.{sys.version_info.minor}",
    "site-packages",
)
dir_src = dir_examples.joinpath("src")
dir_glue_spark_submit = dir_src.joinpath("glue_spark_submit")


def preview_args(args: T.List[str]):
    print("\n\t".join(args))


def spark_submit_test_1():
    # define variables
    bucket = f"{aws_account_id}-{aws_region}-data"
    prefix = "projects/aws_glue_container_launcher/glue_spark_submit/test1/mydatabase/mytable/"
    # clean up the table s3 folder
    s3dir = S3Path(f"s3://{bucket}/{prefix}")
    s3dir.delete()
    # run glue job
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_1.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        job_name="glue_script_1",
    )
    # preview_args(args)
    subprocess.run(args, check=True)


def spark_submit_test_2():
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_2.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        job_name="glue_script_2",
    )
    # preview_args(args)
    subprocess.run(args, check=True)


def spark_submit_test_3():
    """
    note: you need pandas and awswrangler to run this. because we need
    awswrangler to create the initial glue catalog table for us.
    """
    import pandas as pd
    import awswrangler as wr

    def create_catalog_table():
        # define variables
        database = "mydatabase"
        table = "glue_spark_submit_test_3_mytable"
        bucket = f"{aws_account_id}-{aws_region}-data"
        prefix = f"projects/aws_glue_container_launcher/glue_spark_submit/test3/{database}/{table}/"

        # clean up the table s3 folder
        s3dir = S3Path(f"s3://{bucket}/{prefix}")
        s3dir.delete()

        # use dummy data to create the initial glue table
        df = pd.DataFrame(
            [
                ("id-1", "2000", "01", "01", "2000-01-01 00:00:00", 1),
            ],
            columns=("id", "year", "month", "day", "ts", "value"),
        )
        res = wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/{prefix}",
            dataset=True,
            database=database,
            table=table,
            mode="overwrite",
            boto3_session=boto_ses,
            partition_cols=["year", "month", "day"],
        )
        # clean up the table s3 folder
        s3dir.delete()

    # create_catalog_table()

    # run glue job
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_3.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        job_name="glue_script_3",
    )
    # preview_args(args)
    subprocess.run(args, check=True)


def spark_submit_test_4():
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_4.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        job_name="glue_script_4",
    )
    # preview_args(args)
    subprocess.run(args, check=True)


def spark_submit_test_5():
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_5.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        enable_hudi=True,
        job_name="glue_script_5",
    )
    # preview_args(args)
    res = subprocess.run(args, check=True)


def spark_submit_test_6():
    args = build_spark_submit_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        path_script=dir_glue_spark_submit.joinpath("glue_script_6.py"),
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        enable_hudi=True,
        job_name="glue_script_6",
    )
    # preview_args(args)
    res = subprocess.run(args, check=True)


def run_jupyter_lab():
    args = build_jupyter_lab_args(
        dir_home=dir_home,
        dir_workspace=dir_workspace,
        boto_session=boto_ses,
        dir_site_packages=dir_site_packages,
        enable_hudi=True,
    )
    preview_args(args)
    res = subprocess.run(args, check=True)


if __name__ == "__main__":
    # spark_submit_test_1()
    # spark_submit_test_2()
    # spark_submit_test_3()
    # spark_submit_test_4()

    # spark_submit_test_5()
    # spark_submit_test_6()

    # run_jupyter_lab()
    pass
