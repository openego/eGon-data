=========
Changelog
=========

Unreleased
==========

Added
-----

* You can no override the default database configuration by putting a
  "local-database.yaml" into the current working directory. Values read
  from this file will override the default values. You can also generate
  this file by specifying command line switches to ``egon-data``. Look
  for the switches starting with ``--database`` in ``egon-data --help``.

* OSM data import as done in open_ego
  `#1 <https://github.com/openego/eGon-data/issues/1>`_
* Verwaltungsgebiete data import (vg250) more or less done as in open_ego
  `#3 <https://github.com/openego/eGon-data/issues/3>`_

Changed
-------

* Adapt structure of the documentation to project specific requirements
  `#20 <https://github.com/openego/eGon-data/issues/20>`_

Bug fixes
---------
