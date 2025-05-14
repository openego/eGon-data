========
Overview
========

.. start-badges

|commits-since| |tests| |docs| |requires|

|coveralls| |codecov| |scrutinizer| |codacy| |codeclimate|

.. commented
    * - tests
      - |appveyor|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|

.. |docs| image:: https://readthedocs.org/projects/egon-data/badge/?version=latest
    :target: https://egon-data.readthedocs.io
    :alt: Documentation Status

.. |tests| image:: https://github.com/openego/eGon-data/workflows/Tests,%20code%20style%20&%20coverage/badge.svg
    :alt: GitHub actions tests status
    :target: https://github.com/openego/eGon-data/actions?query=workflow%3A%22Tests%2C+code+style+%26+coverage%22

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/openego/eGon-data?branch=dev&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/openego/eGon-data

.. |requires| image:: https://requires.io/github/openego/eGon-data/requirements.svg?branch=dev
    :alt: Requirements Status
    :target: https://requires.io/github/openego/eGon-data/requirements/?branch=dev

.. |coveralls| image:: https://coveralls.io/repos/openego/eGon-data/badge.svg?branch=dev&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/openego/eGon-data

.. |codecov| image:: https://codecov.io/gh/openego/eGon-data/branch/dev/graphs/badge.svg?branch=dev
    :alt: Coverage Status
    :target: https://codecov.io/github/openego/eGon-data

.. |codacy| image:: https://img.shields.io/codacy/grade/d639ac4296a04edb8da5c882ea36e98b.svg
    :target: https://www.codacy.com/app/openego/eGon-data
    :alt: Codacy Code Quality Status

.. |codeclimate| image:: https://codeclimate.com/github/openego/eGon-data/badges/gpa.svg
   :target: https://codeclimate.com/github/openego/eGon-data
   :alt: CodeClimate Quality Status

.. |version| image:: https://img.shields.io/pypi/v/egon.data.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/egon.data

.. |wheel| image:: https://img.shields.io/pypi/wheel/egon.data.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/egon.data

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/egon.data.svg
    :alt: Supported versions
    :target: https://pypi.org/project/egon.data

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/egon.data.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/egon.data

.. |commits-since| image:: https://img.shields.io/badge/dynamic/json.svg?label=v0.0.0&url=https%3A%2F%2Fapi.github.com%2Frepos%2Fopenego%2FeGon-data%2Fcompare%2Fv0.0.0...dev&query=%24.total_commits&colorB=blue&prefix=%2b&suffix=%20commits
    :alt: Latest release and commits since then
    :target: https://github.com/openego/eGon-data/compare/v0.0.0...dev


.. |scrutinizer| image:: https://img.shields.io/scrutinizer/quality/g/openego/eGon-data/dev.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/openego/eGon-data/


.. end-badges

The data used in the eGo^N project along with the code importing, generating and processing it.

* Free software: GNU Affero General Public License v3 or later (AGPLv3+)

.. begin-getting-started-information

Pre-requisites
==============

In addition to the installation of Python packages, some non-Python
packages are required too. Right now these are:

* `Docker <https://docs.docker.com/get-started/>`_: Docker is used to provide
  a PostgreSQL database (in the default case).

  Docker provides extensive installation instruction. Best you consult `their
  docs <https://docs.docker.com/get-docker/>`_ and choose the appropriate
  install method for your OS.

  Docker is not required if you use a local PostreSQL installation.

* The `psql` executable. On Ubuntu, this is provided by the
  `postgresql-client-common` package.

* Header files for the :code:`libpq5` PostgreSQL library. These are necessary
  to build the :code:`psycopg2` package from source and are provided by the
  :code:`libpq-dev` package on Ubuntu.

* `osm2pgsql <https://osm2pgsql.org/>`_
  On recent Ubuntu version you can install it via
  :code:`sudo apt install osm2pgsql`.

* `postgis <https://postgis.net/>`_
  On recent Ubuntu version you can install it via
  :code:`sudo apt install postgis`.

* osmTGmod resp. osmosis needs `java <https://www.java.com/>`_.
  On recent Ubuntu version you can install it via
  :code:`sudo apt install default-jre` and
  :code:`sudo apt install default-jdk`.

* conda is needed for the subprocess of running pypsa-eur-sec.
  For the installation of miniconda, check out the
  `conda installation guide
  <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_.

* pypsa-eur-sec resp. Fiona needs the additional library :code:`libtbb2`.
  On recent Ubuntu version you can install it via
  :code:`sudo apt install libtbb2`

* `gdal <https://gdal.org/>`_
  On recent Ubuntu version you can install it via
  :code:`sudo apt install gdal-bin`.

* curl is required.
  You can install it via :code:`sudo apt install curl`.

* To download ERA5 weather data you need to register at the CDS
  registration page and install the CDS API key as described
  `here <https://cds.climate.copernicus.eu/api-how-to>`_
  You also have to agree on the `terms of use
  <https://cds.climate.copernicus.eu/cdsapp/#!/terms/licence-to-use-copernicus-products>`_

* Make sure you have enough free disk space (~350 GB) in your working
  directory.

Installation
============

Since no release is available on PyPI and installations are probably
used for development, cloning it via

.. code-block:: bash

   git clone git@github.com:openego/eGon-data.git

and installing it in editable mode via

.. code-block:: bash

   pip install -e eGon-data/

are recommended.

In order to keep the package installation isolated, we recommend
installing the package in a dedicated virtual environment with
Python 3.8, as eGon-data works currently only with that Python version.
There's both, an `external tool`_ and a `builtin module`_ which help in
doing so. I also highly recommend spending the time to set up
`virtualenvwrapper`_ to manage your virtual environments if you start
having to keep multiple ones around.

If you run into any problems during the installation of ``egon.data``,
try looking into the list of `known installation problems`_ we have
collected. Maybe we already know of your problem and also of a solution
to it.

.. _external tool: https://virtualenv.pypa.io/en/latest/
.. _builtin module: https://docs.python.org/3/tutorial/venv.html#virtual-environments-and-packages
.. _virtualenvwrapper: https://virtualenvwrapper.readthedocs.io/en/latest/index.html
.. _known installation problems: https://eGon-data.readthedocs.io/en/latest/troubleshooting.html#installation-errors


Run the workflow
================

The :code:`egon.data` package installs a command line application
called :code:`egon-data` with which you can control the workflow so once
the installation is successful, you can explore the command line
interface starting with :code:`egon-data --help`.

The most useful subcommand is probably :code:`egon-data serve`. After
running this command, you can open your browser and point it to
`localhost:8080`, after which you will see the web interface of `Apache
Airflow`_ with which you can control the :math:`eGo^n` data processing
pipeline.

If running :code:`egon-data` results in an error, we also have collected
a list of `known runtime errors`_, which can consult in search of a
solution.

To run the workflow from the CLI without using :code:`egon-data serve` you can use

.. code-block:: bash

   egon-data airflow scheduler
   egon-data airflow dags trigger egon-data-processing-pipeline

For further details how to use the CLI see `Apache Airflow CLI Reference`_.

.. _Apache Airflow: https://airflow.apache.org/docs/apache-airflow/stable/ui.html#ui-screenshots
.. _known runtime errors: https://eGon-data.readthedocs.io/en/latest/troubleshooting.html#runtime-errors
.. _Apache Airflow CLI Reference: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html

.. warning::

   A complete run of the workflow might require much computing power and
   can't be run on laptop. Use the `test mode <#test-mode>`_ for
   experimenting.

.. warning::

   A complete run of the workflow needs loads of free disk space (~350 GB) to
   store (temporary) files.

Test mode
---------

The workflow can be tested on a smaller subset of data on example of the
federal state of Schleswig-Holstein.
Data is reduced during execution of the workflow to represent only this area.

.. warning::

   Right now, the test mode is set in `egon.data/airflow/pipeline.py`.


.. end-getting-started-information

Further Reading
===============

You can find more in-depth documentation at https://eGon-data.readthedocs.io.
