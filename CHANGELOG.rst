=========
Changelog
=========

Unreleased
==========

Added
-----

* There's now a wrapper around `subprocess.run` in
  `egon.data.subprocess.run`. This wrapper catches errors better and
  displays better error messages that Python's built-in function. Use
  this wrapper wenn calling other programs in Airflow tasks.

* You can now override the default database configuration by putting a
  "local-database.yaml" into the current working directory. Values read
  from this file will override the default values. You can also generate
  this file by specifying command line switches to ``egon-data``. Look
  for the switches starting with ``--database`` in ``egon-data --help``.

* Docker will not be used if there is already a service listening on the
  HOST:PORT combination configured for the database.

* OSM data import as done in open_ego
  `#1 <https://github.com/openego/eGon-data/issues/1>`_
* Verwaltungsgebiete data import (vg250) more or less done as in open_ego
  `#3 <https://github.com/openego/eGon-data/issues/3>`_
* Zensus population data import
  `#2 <https://github.com/openego/eGon-data/issues/2>`_

Changed
-------

* Adapt structure of the documentation to project specific requirements
  `#20 <https://github.com/openego/eGon-data/issues/20>`_

Bug fixes
---------
