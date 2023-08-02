# -*- coding: utf-8 -*-

import typing as T
import uuid
import platform
from pathlib import Path, PurePosixPath

import boto3

from .enumeration import (
    GlueVersionEnum,
    glue_version_to_python_version_mapper,
    processor_to_image_tag_suffix_mapper,
    ActionEnum,
)


DIR_HOME_GLUE_USER = PurePosixPath("/home/glue_user")
DIR_HOME_GLUE_USER_WORKSPACE = DIR_HOME_GLUE_USER.joinpath("workspace")
DIR_JUPYTER_WORKSPACE = DIR_HOME_GLUE_USER_WORKSPACE.joinpath("jupyter_workspace")
DIR_EXTRA_PYTHON_PATH = DIR_HOME_GLUE_USER_WORKSPACE.joinpath("extra_python_path")
PATH_JUPYTER_START_SH = DIR_HOME_GLUE_USER.joinpath("jupyter", "jupyter_start.sh")

def get_docker_run_args(
    auto_remove_container: bool = True,
) -> T.List[str]:
    args = ["docker", "run"]
    if auto_remove_container:
        args.append("--rm")
    args.extend(["-e", "DISABLE_SSL=true"])
    return args


def get_container_name_args(
    name: str,
) -> T.List[str]:
    return ["--name", name]


def get_spark_port_args(
    spark_ui_port: int = 4040,
    spark_history_server_port: int = 18080,
) -> T.List[str]:
    return [
        "-p",
        f"{spark_ui_port}:4040",
        "-p",
        f"{spark_history_server_port}:18080",
    ]


def get_jupyter_lab_port_args(
    livy_server_port: int = 8998,
    jupyter_notebook_port: int = 8888,
) -> T.List[str]:
    return [
        "-p",
        f"{livy_server_port}:8998",
        "-p",
        f"{jupyter_notebook_port}:8888",
    ]


def get_env_vars_args(
    env_vars: T.Dict[str, str],
) -> T.List[str]:
    args = list()
    for key, value in env_vars.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def get_mount_aws_dir_args(
    dir_home: Path,
) -> T.List[str]:
    return ["-v", f"{dir_home}/.aws:{DIR_HOME_GLUE_USER}/.aws"]


def get_mount_workspace_args(
    dir_workspace: Path,
) -> T.List[str]:
    return [
        "-v",
        f"{dir_workspace}/:{DIR_HOME_GLUE_USER_WORKSPACE}/",
    ]


def get_extra_python_path_args(
    dir_site_packages: Path,
) -> T.List[str]:
    return [
        "-v",
        f"{dir_site_packages}/:{DIR_EXTRA_PYTHON_PATH}/",
        "-e",
        f"PYTHONPATH=$PYTHONPATH:{DIR_EXTRA_PYTHON_PATH}/",
    ]


def get_mount_jupyter_workspace_args(
    dir_jupyter_workspace: Path,
) -> T.List[str]:
    return ["-v", f"{dir_jupyter_workspace}/:{DIR_JUPYTER_WORKSPACE}/"]


def get_aws_credential_args(
    boto_ses: boto3.session.Session,
) -> T.List[str]:
    args = list()
    cred = boto_ses.get_credentials()
    aws_profile = boto_ses.profile_name
    aws_region = boto_ses.region_name
    aws_access_key_id = cred.access_key
    aws_secret_access_key = cred.secret_key
    aws_session_token = cred.token
    if aws_profile is not None:
        args.extend(["-e", f"AWS_PROFILE={aws_profile}"])
    if aws_region is not None:
        args.extend(["-e", f"AWS_REGION={aws_region}"])
    if aws_access_key_id is not None:
        args.extend(["-e", f"AWS_ACCESS_KEY_ID={aws_access_key_id}"])
    if aws_secret_access_key is not None:
        args.extend(["-e", f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}"])
    if aws_session_token is not None:
        args.extend(["-e", f"AWS_SESSION_TOKEN={aws_session_token}"])
    return args


def get_enable_datalake_libraries_args(
    enable_hudi: bool = False,
    enable_delta_lake: bool = False,
    enable_iceberg: bool = False,
) -> T.List[str]:
    formats = list()
    if enable_hudi:
        formats.append("hudi")
    if enable_delta_lake:
        formats.append("delta")
    if enable_iceberg:
        formats.append("iceberg")
    if len(formats):
        return ["-e", "DATALAKE_FORMATS={}".format(",".join(formats))]
    else:
        return []


def get_image_uri(
    glue_version: str,
) -> str:
    """
    Reference:

    - Amazon ECR Public Gallery - glue/aws-glue-libs: https://gallery.ecr.aws/glue/aws-glue-libs
    """
    return "amazon/aws-glue-libs:glue_libs_{}.0_image_01{}".format(
        glue_version, processor_to_image_tag_suffix_mapper.get(platform.processor(), "")
    )


def get_job_args(
    kwargs: T.Dict[str, str],
) -> T.List[str]:
    args = list()
    for key, value in kwargs.items():
        args.extend([f"--{key}", value])
    return args


def build_spark_submit_args(
    dir_home: Path,
    dir_workspace: Path,
    path_script: Path,
    job_name: T.Optional[str] = None,
    job_run_id: T.Optional[str] = None,
    container_name: str = "glue_spark_submit",
    auto_remove_container: bool = True,
    glue_version: str = GlueVersionEnum.GLUE_4_0.value,
    dir_site_packages: T.Optional[Path] = None,
    boto_session: T.Optional[boto3.session.Session] = None,
    spark_ui_port: int = 4040,
    spark_history_server_port: int = 18080,
    enable_hudi: bool = False,
    enable_delta_lake: bool = False,
    enable_iceberg: bool = False,
    additional_docker_run_args: T.Optional[T.List[str]] = None,
    additional_job_args: T.Optional[T.Dict[str, str]] = None,
    additional_cli_args: T.Optional[T.List[str]] = None,
):
    """
    :param dir_home:
    :param dir_workspace:
    :param path_script:
    :param job_name:
    :param job_run_id:
    :param container_name:
    :param auto_remove_container:
    :param glue_version:
    :param dir_site_packages:
    :param boto_session:
    :param spark_ui_port:
    :param spark_history_server_port:
    :param enable_hudi:
    :param enable_delta_lake:
    :param enable_iceberg:
    :param additional_docker_run_args:
    :param additional_job_args:
    :param additional_cli_args:
    """
    args = get_docker_run_args(
        auto_remove_container=auto_remove_container,
    )
    args.extend(get_container_name_args(name=container_name))
    args.extend(
        get_spark_port_args(
            spark_ui_port=spark_ui_port,
            spark_history_server_port=spark_history_server_port,
        )
    )
    args.extend(get_mount_aws_dir_args(dir_home=dir_home))
    args.extend(get_mount_workspace_args(dir_workspace=dir_workspace))
    if dir_site_packages is not None:
        args.extend(get_extra_python_path_args(dir_site_packages=dir_site_packages))
    if boto_session is not None:
        args.extend(get_aws_credential_args(boto_ses=boto_session))
    args.extend(
        get_enable_datalake_libraries_args(
            enable_hudi=enable_hudi,
            enable_delta_lake=enable_delta_lake,
            enable_iceberg=enable_iceberg,
        )
    )
    args.extend((get_env_vars_args({"IS_GLUE_CONTAINER": "true"})))
    if additional_docker_run_args is not None:
        args.extend(additional_docker_run_args)
    args.extend(
        [
            get_image_uri(glue_version=glue_version),
            "spark-submit",
            str(
                DIR_HOME_GLUE_USER_WORKSPACE.joinpath(
                    path_script.relative_to(dir_workspace)
                )
            ),
        ]
    )
    if job_name is not None:
        args.extend(["--JOB_NAME", job_name])
    if job_run_id is None:
        job_run_id = str(uuid.uuid4())
    args.extend(["--JOB_RUN_ID", job_run_id])
    if additional_job_args is not None:
        args.extend(get_job_args(kwargs=additional_job_args))
    if additional_cli_args is not None:
        args.extend(additional_cli_args)
    return args


def build_pytest_args(
    dir_home: Path,
    dir_workspace: Path,
    path_script_or_folder: Path,
    container_name: str = "glue_pytest",
    auto_remove_container: bool = True,
    glue_version: str = GlueVersionEnum.GLUE_4_0.value,
    dir_site_packages: T.Optional[Path] = None,
    boto_session: T.Optional[boto3.session.Session] = None,
    spark_ui_port: int = 4040,
    spark_history_server_port: int = 18080,
    enable_hudi: bool = False,
    enable_delta_lake: bool = False,
    enable_iceberg: bool = False,
    additional_docker_run_args: T.Optional[T.List[str]] = None,
):
    """
    :param dir_home:
    :param dir_workspace:
    :param path_script_or_folder:
    :param container_name:
    :param auto_remove_container:
    :param glue_version:
    :param dir_site_packages:
    :param boto_session:
    :param spark_ui_port:
    :param spark_history_server_port:
    :param enable_hudi:
    :param enable_delta_lake:
    :param enable_iceberg:
    :param additional_docker_run_args:
    """
    args = get_docker_run_args(
        auto_remove_container=auto_remove_container,
    )
    args.extend(get_container_name_args(name=container_name))
    args.extend(
        get_spark_port_args(
            spark_ui_port=spark_ui_port,
            spark_history_server_port=spark_history_server_port,
        )
    )
    args.extend(get_mount_aws_dir_args(dir_home=dir_home))
    args.extend(get_mount_workspace_args(dir_workspace=dir_workspace))
    if dir_site_packages is not None:
        args.extend(get_extra_python_path_args(dir_site_packages=dir_site_packages))
    if boto_session is not None:
        args.extend(get_aws_credential_args(boto_ses=boto_session))
    args.extend(
        get_enable_datalake_libraries_args(
            enable_hudi=enable_hudi,
            enable_delta_lake=enable_delta_lake,
            enable_iceberg=enable_iceberg,
        )
    )
    args.extend((get_env_vars_args({"IS_GLUE_CONTAINER": "true"})))
    if additional_docker_run_args is not None:
        args.extend(additional_docker_run_args)
    args.extend(
        [
            get_image_uri(glue_version=glue_version),
            "-c",
            f"python3 -m pytest {path_script_or_folder.relative_to(dir_workspace)} -s --disable-warnings",
        ]
    )
    return args


def build_jupyter_lab_args(
    dir_home: Path,
    dir_workspace: Path,
    container_name: str = "glue_jupyter_lab",
    auto_remove_container: bool = True,
    glue_version: str = GlueVersionEnum.GLUE_4_0.value,
    dir_site_packages: T.Optional[Path] = None,
    boto_session: T.Optional[boto3.session.Session] = None,
    spark_ui_port: int = 4040,
    spark_history_server_port: int = 18080,
    livy_server_port: int = 8998,
    jupyter_notebook_port: int = 8888,
    enable_hudi: bool = False,
    enable_delta_lake: bool = False,
    enable_iceberg: bool = False,
    additional_docker_run_args: T.Optional[T.List[str]] = None,
):
    """
    :param dir_home:
    :param dir_workspace:
    :param container_name:
    :param auto_remove_container:
    :param glue_version:
    :param dir_site_packages:
    :param boto_session:
    :param spark_ui_port:
    :param spark_history_server_port:
    :param livy_server_port:
    :param jupyter_notebook_port:
    :param enable_hudi:
    :param enable_delta_lake:
    :param enable_iceberg:
    :param additional_docker_run_args:
    """
    args = get_docker_run_args(
        auto_remove_container=auto_remove_container,
    )
    args.extend(get_container_name_args(name=container_name))
    args.extend(
        get_spark_port_args(
            spark_ui_port=spark_ui_port,
            spark_history_server_port=spark_history_server_port,
        )
    )
    args.extend(
        get_jupyter_lab_port_args(
            livy_server_port=livy_server_port,
            jupyter_notebook_port=jupyter_notebook_port,
        )
    )
    args.extend(get_mount_aws_dir_args(dir_home=dir_home))
    args.extend(get_mount_jupyter_workspace_args(dir_jupyter_workspace=dir_workspace))
    if dir_site_packages is not None:
        args.extend(get_extra_python_path_args(dir_site_packages=dir_site_packages))
    if boto_session is not None:
        args.extend(get_aws_credential_args(boto_ses=boto_session))
    args.extend(
        get_enable_datalake_libraries_args(
            enable_hudi=enable_hudi,
            enable_delta_lake=enable_delta_lake,
            enable_iceberg=enable_iceberg,
        )
    )
    args.extend((get_env_vars_args({"IS_GLUE_CONTAINER": "true"})))
    if additional_docker_run_args is not None:
        args.extend(additional_docker_run_args)
    args.extend(
        [
            get_image_uri(glue_version=glue_version),
            str(PATH_JUPYTER_START_SH),
        ]
    )
    return args
