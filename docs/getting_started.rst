***************
Getting Started
***************

Installation
============

Since no release is available on PyPi and installations are probably
used for development, cloning

.. code-block:: bash

   git clone git@github.com:openego/eGon-data.git

and installing in editable mode recommended.

.. code-block:: bash

   pip install -e eGon-data


Pre-requisites
==============

In addition to the installation of python packages, OS-level packages are
required

* `osm2pgsql <https://osm2pgsql.org/>`_: Install with :code:`sudo apt install
  osm2pgsql`

Run the workflow
================

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
