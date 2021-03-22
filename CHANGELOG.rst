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
* Abstraction of hvmv and ehv substations
  `#9 <https://github.com/openego/eGon-data/issues/9>`_
* Filter zensus being inside Germany and assign population to municipalities
  `#7 <https://github.com/openego/eGon-data/issues/7>`_
* RE potential areas data import
  `#124 <https://github.com/openego/eGon-data/issues/124>`_
* Heat demand data import
  `#101 <https://github.com/openego/eGon-data/issues/101>`_
* Demographic change integration
  `#47 <https://github.com/openego/eGon-data/issues/47>`_

Changed
-------

* Adapt structure of the documentation to project specific requirements
  `#20 <https://github.com/openego/eGon-data/issues/20>`_
* Switch from Travis to GitHub actions for CI jobs
  `#92 <https://github.com/openego/eGon-data/issues/92>`_
* Rename columns to id and zensus_population_id in zensus tables
  `#140 <https://github.com/openego/eGon-data/issues/140>`_
* Revise docs CONTRIBUTING section and in particular PR guidelines
  `#88 <https://github.com/openego/eGon-data/issues/88>`_ and
  `#145 <https://github.com/openego/eGon-data/issues/145>`_
* Drop support for Python3.6
  `#148 <https://github.com/openego/eGon-data/issues/148>`_
* Improve selection of zensus data in test mode
  `#151 <https://github.com/openego/eGon-data/issues/151>`_
* Adjust residential heat demand in unpopulated zenus cells
  `#167 <https://github.com/openego/eGon-data/issues/167>`_

Bug fixes
---------
* Heat demand data import
  `#157 <https://github.com/openego/eGon-data/issues/157>`_
