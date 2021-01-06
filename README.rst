========
Overview
========

.. start-badges

|commits-since| |travis| |docs| |requires|

|coveralls| |codecov| |scrutinizer| |codacy| |codeclimate|

.. commented
    * - tests
      - |appveyor|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|

.. |docs| image:: https://readthedocs.org/projects/egon-data/badge/?version=latest
    :target: https://egon-data.readthedocs.io
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/openego/eGon-data.svg?branch=dev
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/openego/eGon-data

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

* `osm2pgsql <https://osm2pgsql.org/>`_
  On recent Ubuntu version you can install it via
  :code:`sudo apt install osm2pgsql`.


Installation
============

Since no release is available on PyPI and installations are probably
used for development, cloning

.. code-block:: bash

   git clone git@github.com:openego/eGon-data.git

and installing in editable mode recommended.

.. code-block:: bash

   pip install -e eGon-data


Run the workflow
================

The :py:mod:`egon.data` package installs a command line application
called :code:`egon-data` with which you can control the workflow so once
the installation is successful, you can explore the command line
interface starting with :code:`egon-data --help`.

The most useful subcommand is probably :code:`egon-data serve`. After
running this command, you can open your browser and point it to
`localhost:8080`, after which you will see the web interface of `Apache
Airflow`_ with which you can control the :math:`eGo^n` data processing
pipeline.

.. _Apache Airflow: https://airflow.apache.org/docs/apache-airflow/stable/ui.html#ui-screenshots

.. warning::

   A complete run of the workflow might require much computing power and
   can't be run on laptop. Use the :ref:`test mode <Test mode>` for
   experimenting.


Test mode
---------

The workflow can be tested on a smaller subset of data on example of the
federal state of Bremen.

.. warning::

   Right now, only OSM data for Bremen get's imported. This is hard-wired in
   `egon.data/data_sets.yml`.


Troubleshooting
===============

Having trouble to install `eGon-data`? Here's a list of recurring issues with
the installation including a solution.

Insufficient permissions for executing docker?
----------------------------------------------

To verify, please execute :code:`docker-compose up -d --build` and you should see
something like

.. code-block:: none

    ERROR: Couldn't connect to Docker daemon at http+docker://localunixsocket - is it running?

    If it's at a non-standard location, specify the URL with the DOCKER_HOST environment variable.

If this is the case, your :code:`$USER` is not member of the group `docker`.
Read `in docker docs <https://docs.docker.com/engine/install/linux-postinstall/
#manage-docker-as-a-non-root-user>`_
how to add :code:`$USER` to the group `docker`. Read the `initial discussion
<https://github.com/openego/eGon-data/issues/33>`_ for more context.

importlib_metadata.PackageNotFoundError
---------------------------------------

It might happen that you have installed `importlib-metadata=3.1.0` for some
reason which will lead to this error. Make sure you have
`importlib-metadata>=3.1.1` installed. For more information read
`here <https://github.com/openego/eGon-data/issues/60>`_.

Import errors or incompatible package version errors
----------------------------------------------------

If you get an :py:class:`ImportError` when trying to run ``egon-data``,
or the installation complains with something like

.. code-block:: none

  first-package a.b.c requires second-package>=q.r.r, but you'll have
  second-package x.y.z which is incompatible.

you might have run into a problem of earlier ``pip`` versions. Either
upgrade to a ``pip`` version >=20.3 and reinstall ``egon.data``, or
reinstall the package via ``pip install -U --use-feature=2020-resolver``.
The ``-U`` flag is important to actually force a reinstall. For more
information read the discussions in issues :issue:`36` and :issue:`37`.

.. end-getting-started-information

Further Reading
===============

You can find more in depth documentation at https://eGon-data.readthedocs.io/.
