
.. .. image:: https://readthedocs.org/projects/aws-glue-container-launcher/badge/?version=latest
    :target: https://aws-glue-container-launcher.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/aws_glue_container_launcher-project/workflows/CI/badge.svg
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/aws_glue_container_launcher-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/aws_glue_container_launcher-project

.. image:: https://img.shields.io/pypi/v/aws-glue-container-launcher.svg
    :target: https://pypi.python.org/pypi/aws-glue-container-launcher

.. image:: https://img.shields.io/pypi/l/aws-glue-container-launcher.svg
    :target: https://pypi.python.org/pypi/aws-glue-container-launcher

.. image:: https://img.shields.io/pypi/pyversions/aws-glue-container-launcher.svg
    :target: https://pypi.python.org/pypi/aws-glue-container-launcher

.. image:: https://img.shields.io/badge/Release_History!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project

------

.. .. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://aws-glue-container-launcher.readthedocs.io/en/latest/

.. .. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://aws-glue-container-launcher.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/aws_glue_container_launcher-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/aws-glue-container-launcher#files


Welcome to ``aws_glue_container_launcher`` Documentation
==============================================================================
`AWS Big Data Blog - Develop and test AWS Glue version 3.0 and 4.0 jobs locally using a Docker container <https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/>`_ introduced a method using AWS maintained glue container to perform local development, unit testing, and interactive jupyter notebook on local laptop or in CI environment. I personally use this to accelerate AWS Glue ETL development, improve development experience and ETL logic visibility, and bring the quality of ETL code to the next level by adding lots of unit test and integration test.

``aws_glue_container_launcher`` is a zero-dependency, pure python library that can easily create shell script to perform tasks introduced in the AWS blog. This project is the core building block in my personal AWS Glue ETL project CI/CD best practice that is used to delivery high-quality ETL pipeline for my clients.


Dependencies
------------------------------------------------------------------------------
- You need to have docker installed on your local laptop or CI environment. You can test it by running ``docker --version`` in your terminal.
- Python3.6 + to install this library and run the shell script.
- MacOS / Linux / Windows OS to run the shell script, but I only tested on MacOS and Linux.


Usage
------------------------------------------------------------------------------
- `run glue job locally in container, see example at <https://github.com/MacHu-GWU/aws_glue_container_launcher-project/blob/main/examples/run_container.py>`_
- `run pytest locally in container <https://github.com/MacHu-GWU/aws_glue_container_launcher-project/tree/main/tests_glue/glue_libs>`_
- `run jupyter lab locally in container <https://github.com/MacHu-GWU/aws_glue_container_launcher-project/blob/main/examples/run_container.py#L173>`_
- REPL SHELL (TO DO)
- Visual Studio Code (TODO)


.. _install:

Install
------------------------------------------------------------------------------

``aws_glue_container_launcher`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install aws-glue-container-launcher

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade aws-glue-container-launcher
