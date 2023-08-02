# -*- coding: utf-8 -*-

import sys
import subprocess
from pathlib import Path

import boto3

from ..paths import dir_project_root, dir_venv, dir_home
from ..api import GlueVersionEnum, build_pytest_args

boto_ses = boto3.session.Session(profile_name="awshsh_app_dev_us_east_1")


def run_unit_test(
    script: str,
    is_folder: bool = False,
    glue_version: str = GlueVersionEnum.GLUE_4_0.value,
):
    """
    Run a unit test in a Glue container.

    :param script: The current pytest Python script, which is ``__file__``.
    :param is_folder: if False, then you want to run unit test for this script,
        if True, then you want to run all unit tests in this folder.
    :param glue_version: the Glue version you want to use, we need this value
        to determine which container image to use. default is 4.0.
    """
    path_test_scope = Path(script).absolute()
    if is_folder:
        path_test_scope = path_test_scope.parent
    dir_site_packages = dir_venv.joinpath(
        "lib",
        f"python{sys.version_info.major}.{sys.version_info.minor}",
        "site-packages",
    )
    args = build_pytest_args(
        dir_home=dir_home,
        dir_workspace=dir_project_root,
        path_script_or_folder=path_test_scope,
        glue_version=glue_version,
        dir_site_packages=dir_site_packages,
        boto_session=boto_ses,
        enable_hudi=True,
    )
    # print("\n\r".join(args))
    subprocess.run(args, check=True)
