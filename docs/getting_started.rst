***************
Getting Started
***************

Installation
============

Since no release is available on PyPI and installations are probably
used for development, cloning

.. code-block:: bash

   git clone git@github.com:openego/eGon-data.git

and installing in editable mode recommended.

.. code-block:: bash

   pip install -e eGon-data


Pre-requisites
==============

In addition to the installation of Python packages, some non-Python
packages are required too. Right now these are:

* `osm2pgsql <https://osm2pgsql.org/>`_
  On recent Ubuntu version you can install it via
  :code:`sudo apt install osm2pgsql`.


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
