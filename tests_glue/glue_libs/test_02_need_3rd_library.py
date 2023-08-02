# -*- coding: utf-8 -*-

"""
This example only depends on one 3rd party Python library.
"""

from pathlib_mate import Path


def test():
    assert Path.dir_here(__file__).basename == "glue_libs"


if __name__ == "__main__":
    from aws_glue_container_launcher.tests.glue import run_unit_test

    run_unit_test(__file__)
