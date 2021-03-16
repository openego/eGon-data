***************
Troubleshooting
***************

Having trouble installing or running ``eGon-data``? Here's a list of
known issues including a solution.


Installation Errors
===================

These are some errors you might encounter while trying to install
:py:mod:`egon.data`.

``importlib_metadata.PackageNotFoundError: No package metadata ...``
--------------------------------------------------------------------

It might happen that you have installed `importlib-metadata=3.1.0` for some
reason which will lead to this error. Make sure you have
`importlib-metadata>=3.1.1` installed. For more information read the
discussion in :issue:`60`.


Runtime Errors
==============

These are some of the errors you might encounter while trying to run
:code:`egon-data`.

``ERROR: Couldn't connect to Docker daemon ...``
------------------------------------------------

To verify, please execute :code:`docker-compose -f <(echo {"service":
{"image": "hellow-world"}}) ps` and you should see something like


.. code-block:: none

    ERROR: Couldn't connect to Docker daemon at http+docker://localunixsocket - is it running?

    If it's at a non-standard location, specify the URL with the DOCKER_HOST environment
    variable.

This can have at least two possible reasons. First, the docker daemon
might not be running. On Linux Systems, you can check for this by
running :code:`ps -e | grep dockerd`. If this generates no output, you
have to start the docker daemon, which you can do via :code:`sudo
systemctl start docker.service` on recent Ubuntu systems.

Second, your current user might not be a member of the `docker` group. On
Linux, you can check this by running :code:`groups $(whoami)`. If the
output does not contain the word `docker`, you have to add your current
user to the `docker` group. You can find more information on how to do
this in the `docker documentation`_. Read the :issue:`initial discussion
<33>` for more context.

.. _docker documentation: https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user


``[ERROR] Connection in use ...``
---------------------------------

This error might arise when running :code:`egon-data serve` making it
shut down early with :code:`ERROR - Shutting down webserver`. The reason
for this is that the local webserver from a previous :code:`egon-data
serve` run didn't shut down properly and is still running. This can be
fixed by running :code:`ps -eo pid,command  | grep "gunicorn: master" |
grep -v grep` which should lead to output like :code:`NUMBER gunicorn:
master [airflow-webserver]` where :code:`NUMBER` is a varying number.
Once you got this, run :code:`kill -s INT NUMBER`, substituting
:code:`NUMBER` with what you got previously. After this,
:code:`egon-data serve` should run without errors again.


Other runtime errors
====================

If you trigger the DAG run in the webinterface you may encounter the error
``ERROR - [0 / 0] Some workers seem to have died ...``. This was observed in
connection with the scheduler message
``sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) database is locked``.

If this error persists, it may help to build the database from scratch by
deleting the docker container using

.. code-block:: bash

   docker stop egon-data-local-database
   docker rm egon-data-local-database

and re-trigger the DAG.

Other import or incompatible package version errors
===================================================

If you get an :py:class:`ImportError` when trying to run ``egon-data``,
or the installation complains with something like

.. code-block:: none

  first-package a.b.c requires second-package>=q.r.r, but you'll have
  second-package x.y.z which is incompatible.

you might have run into a problem of earlier ``pip`` versions. Either
upgrade to a ``pip`` version >=20.3 and reinstall ``egon.data``, or
reinstall the package via ``pip install -U --use-feature=2020-resolver``.
The ``-U`` flag is important to actually force a reinstall. For more
information read the discussions in issues :issue:`#36 <36>` and
:issue:`#37 <37>`.
