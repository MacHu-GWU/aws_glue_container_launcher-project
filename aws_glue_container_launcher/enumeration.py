# -*- coding: utf-8 -*-

import enum


class GlueVersionEnum(str, enum.Enum):
    GLUE_4_0 = "4.0"
    GLUE_3_0 = "3.0"
    GLUE_2_0 = "2.0"
    GLUE_1_0 = "1.0"
    GLUE_0_9 = "0.9"


glue_version_to_python_version_mapper = {
    GlueVersionEnum.GLUE_4_0.value: "3.10",
    GlueVersionEnum.GLUE_3_0.value: "3.7",
    GlueVersionEnum.GLUE_2_0.value: "3.7",
    GlueVersionEnum.GLUE_1_0.value: "3.6",
    GlueVersionEnum.GLUE_0_9.value: "2.7",
}


processor_to_image_tag_suffix_mapper = {
    "x86_64": "-amd64",
    "arm": "-arm64",
}


class ActionEnum(str, enum.Enum):
    spark_submit = "spark_submit"
    repl_shell = "repl_shell"
    pytest = "pytest"
    jupyter_lab = "jupyter_lab"
    vs_code = "vs_code"
