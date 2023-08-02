# -*- coding: utf-8 -*-

import subprocess
from pathlib import Path
from boto_session_manager import BotoSesManager

# ------------------------------------------------------------------------------
# Enter your AWS profile name here
aws_profile = "edf_dev_eu_west_1_mfa"
glue_version = "4.0"
# ------------------------------------------------------------------------------

glue_version_to_python_version = {
    "4.0": "3.10",
    "3.0": "3.7",
    "2.0": "3.7",
    "1.0": "3.6",
    "0.9": "2.7",
}

image = f"amazon/aws-glue-libs:glue_libs_{glue_version}.0_image_01-arm64"

dir_project_root = Path(__file__).absolute().parent.parent
dir_venv = dir_project_root.joinpath(".venv")

bsm = BotoSesManager(profile_name=aws_profile)

aws_region = bsm.aws_region
credentials = bsm.boto_ses.get_credentials()
aws_access_key_id = credentials.access_key
aws_secret_access_key = credentials.secret_key
aws_session_token = credentials.token

path_jupyter_start_sh = Path("/home/glue_user/jupyter/jupyter_start.sh")

args = ["docker", "run", "--rm"]
args.extend(["--name", "glue_jupyter_lab"])

# mount project root folder to container
args.extend(["-v", f"{dir_project_root}/:/home/glue_user/workspace/jupyter_workspace/"])

# mount dependencies in virtual environment to container
python_version = glue_version_to_python_version[glue_version]
args.extend(
    [
        "-v",
        f"{dir_venv}/lib/python{python_version}/site-packages/:/home/glue_user/workspace/extra_python_path/",
    ]
)
# inject the Python dependency discovery path
args.extend(
    ["-e", "PYTHONPATH=$PYTHONPATH:/home/glue_user/workspace/extra_python_path/"]
)

# mount .aws folder to container so that the container has AWS permission
dir_home = Path.home()
args.extend(["-v", f"{dir_home}/.aws:/home/glue_user/.aws"])

# inject environment variables so that the code in container knows that
# it is running in the container.
args.extend(["-e", "IS_GLUE_CONTAINER=yes"])

# inject AWS credentials to grant the container AWS permission
args.extend(["-e", f"AWS_PROFILE={aws_profile}"])
args.extend(["-e", f"AWS_REGION={aws_region}"])
args.extend(["-e", f"AWS_ACCESS_KEY_ID={aws_access_key_id}"])
args.extend(["-e", f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}"])
args.extend(["-e", f"AWS_SESSION_TOKEN={aws_session_token}"])
args.extend(["-e", "DATALAKE_FORMATS=hudi"])
args.extend(["-e", "CONF=\"spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false\""])
args.extend(["-e", "DISABLE_SSL=true"])
args.extend(["-p", "4040:4040"])
args.extend(["-p", "18080:18080"])
args.extend(["-p", "8998:8998"])
args.extend(["-p", "8888:8888"])

args.extend([image, str(path_jupyter_start_sh)])
subprocess.run(args)
