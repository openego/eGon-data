***************
Getting Started
***************

Pre-requisites
==============

In addition to the installation of Python packages, some non-Python
packages are required too. Right now these are:

* `Docker <https://docs.docker.com/get-started/>`_: Docker is used to provide
  a PostgreSQL database (in the default case).

  Docker provides extensive installation instruction. Best you consult `their
  docs <https://docs.docker.com/get-docker/>`_ and choose the appropriate
  install method for your OS.

  Docker is not required if you use a local postresql installation.

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
the installation is successfull, you can explore the command line
interface starting with :code:`egon-data --help`.

The most useful subcommand is probably :code:`egon-data serve`. After
running this command, you can open your browser and point it to
`localhost:8080`, after which you will see the webinterface of `Apache
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

.. code-block::

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
