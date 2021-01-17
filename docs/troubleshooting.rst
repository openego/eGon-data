***************
Troubleshooting
***************

Having trouble installing or running ``eGon-data``? Here's a list of
known issues including a solution.

Insufficient permissions for executing docker?
----------------------------------------------

To verify, please execute :code:`docker-compose up -d --build` and you should see
something like

.. code-block:: none

    ERROR: Couldn't connect to Docker daemon at http+docker://localunixsocket - is it running?

    If it's at a non-standard location, specify the URL with the DOCKER_HOST environment
    variable.

If this is the case, your :code:`$USER` is not member of the group `docker`.
Read `in docker docs <https://docs.docker.com/engine/install/linux-postinstall/
#manage-docker-as-a-non-root-user>`_
how to add :code:`$USER` to the group `docker`. Read the :issue:`initial
discussion <33>` for more context.

importlib_metadata.PackageNotFoundError
---------------------------------------

It might happen that you have installed `importlib-metadata=3.1.0` for some
reason which will lead to this error. Make sure you have
`importlib-metadata>=3.1.1` installed. For more information read the
discussion in :issue:`60`.

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
information read the discussions in issues :issue:`#36 <36>` and
:issue:`#37 <37>`.
