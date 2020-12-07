***************
Getting Started
***************

Pre-requisites
==============

In addition to the installation of python packages, OS-level packages are
required

* `Docker <https://docs.docker.com/get-started/>`_: Docker is used to provide
  a PostgreSQL database (in the default case).

  Docker provides extensive installation instruction. Best you consult `their
  docs <https://docs.docker.com/get-docker/>`_ and choose the appropriate
  install method for your OS.

  Docker is not required if you use a local postresql installation.
* `osm2pgsql <https://osm2pgsql.org/>`_: Install with :code:`sudo apt install
  osm2pgsql`

Installation
============

Since no release is available on PyPi and installations are probably used for development, cloning

.. code-block:: bash

   git clone git@github.com:openego/eGon-data.git

and installing in editable mode recommended.

.. code-block:: bash

   pip install -e eGon-data

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It might happen that you have installed `importlib-metadata=3.1.0` for some
reason which will lead to this error. Make sure you have
`importlib-metadata>=3.1.1` installed. For more information read
`here <https://github.com/openego/eGon-data/issues/60>`_.

Run the workflow
================

.. warning::

   A complete run of the workflow might require much computing power and can't be run on laptop.
   Use the :ref:`test mode <Test mode>` for experimenting.


Test mode
---------

The workflow can be tested on a smaller subset of data on example of the federal state of Bremen.

.. warning::

   Right now, only OSM data for Bremen get's imported. This is hard-wired in
   `egon.data/data_sets.yml`.
