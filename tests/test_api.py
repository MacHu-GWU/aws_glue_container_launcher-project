# -*- coding: utf-8 -*-

from aws_glue_container_launcher import api


def test():
    _ = api
    _ = api.GlueVersionEnum
    _ = api.glue_version_to_python_version_mapper
    _ = api.build_spark_submit_args
    _ = api.build_pytest_args
    _ = api.build_jupyter_lab_args


if __name__ == "__main__":
    from aws_glue_container_launcher.tests import run_cov_test

    run_cov_test(__file__, "aws_glue_container_launcher.api", preview=False)
