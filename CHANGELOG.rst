=========
Changelog
=========

Unreleased
==========

Added
-----


* Include description of the egon-data workflow in our documentation
  `#23 <https://github.com/openego/eGon-data/issues/23>`_
* There's now a wrapper around `subprocess.run` in
  `egon.data.subprocess.run`. This wrapper catches errors better and
  displays better error messages than Python's built-in function. Use
  this wrapper wenn calling other programs in Airflow tasks.

* You can now override the default database configuration using command
  line arguments. Look for the switches starting with ``--database`` in
  ``egon-data --help``.
  Note that this is currently only useful if you are using your own
  database and want ``egon-data`` to use that one, because the
  "Docker"ed database's configuration is not affected by these options.

* Docker will not be used if there is already a service listening on the
  HOST:PORT combination configured for the database.

* You can now supply values for the command line arguments for
  ``egon-data`` using a configuration file. If the configuration file
  doesn't exist, it will be created by ``egon-data`` on it's first run.
  Note that the configuration file is read from and writte to the
  directtory in which ``egon-data`` is started, so it's probably best to
  run ``egon-data`` in a dedicated directory.

* OSM data import as done in open_ego
  `#1 <https://github.com/openego/eGon-data/issues/1>`_
* Verwaltungsgebiete data import (vg250) more or less done as in open_ego
  `#3 <https://github.com/openego/eGon-data/issues/3>`_
* Zensus population data import
  `#2 <https://github.com/openego/eGon-data/issues/2>`_
* Zensus data import for households, apartments and buildings
  `#91 <https://github.com/openego/eGon-data/issues/91>`_
* DemandRegio data import for annual electricity demands
  `#5 <https://github.com/openego/eGon-data/issues/5>`_
* Download cleaned open-MaStR data from Zenodo
  `#14 <https://github.com/openego/eGon-data/issues/14>`_
* NEP 2021 input data import
  `#45 <https://github.com/openego/eGon-data/issues/45>`_
* Option for running workflow in test mode
  `#112 <https://github.com/openego/eGon-data/issues/112>`_

Changed
-------

* Adapt structure of the documentation to project specific requirements
  `#20 <https://github.com/openego/eGon-data/issues/20>`_
* Switch from Travis to GitHub actions for CI jobs
  `#92 <https://github.com/openego/eGon-data/issues/92>`_

Bug fixes
---------
