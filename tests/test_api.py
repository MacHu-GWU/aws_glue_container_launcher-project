# -*- coding: utf-8 -*-

from aws_glue_container_launcher import api


def test():
    _ = api


if __name__ == "__main__":
    from aws_glue_container_launcher.tests import run_cov_test

    run_cov_test(__file__, "aws_glue_container_launcher.api", preview=False)
