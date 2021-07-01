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
  ``egon-data --help``. See `PR #159`_ for more details.

* Docker will not be used if there is already a service listening on the
  HOST:PORT combination configured for the database.

* You can now supply values for the command line arguments for
  ``egon-data`` using a configuration file. If the configuration file
  doesn't exist, it will be created by ``egon-data`` on it's first run.
  Note that the configuration file is read from and written to the
  directtory in which ``egon-data`` is started, so it's probably best to
  run ``egon-data`` in a dedicated directory.
  There's also the new function `egon.data.config.settings` which
  returns the current configuration settings. See `PR #159`_ for more
  details.

* OSM data import as done in open_ego
  `#1 <https://github.com/openego/eGon-data/issues/1>`_
  which was updated to the latest long-term data set of the 2021-01-01 in
  #223 <https://github.com/openego/eGon-data/issues/223>`_
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
* Creation of voronoi polygons for hvmv and ehv substations
  `#9 <https://github.com/openego/eGon-data/issues/9>`_
* Add hydro and biomass power plants eGon2035
  `#127 <https://github.com/openego/eGon-data/issues/127>`_
* Creation of the ehv/hv grid model with osmTGmod, see
  `issue #4 <https://github.com/openego/eGon-data/issues/4>`_ and
  `PR #164 <https://github.com/openego/eGon-data/pull/164>`_
* Identification of medium-voltage grid districts
  `#10 <https://github.com/openego/eGon-data/pull/10>`_
* Distribute electrical demands of households to zensus cells
  `#181 <https://github.com/openego/eGon-data/issues/181>`_
* Distribute electrical demands of cts to zensus cells
  `#210 <https://github.com/openego/eGon-data/issues/210>`_
* Include industrial sites' download, import and merge
  `#117 <https://github.com/openego/eGon-data/issues/117>`_
* Integrate scenario table with parameters for each sector
  `#177 <https://github.com/openego/eGon-data/issues/177>`_
* The volume of the docker container for the PostgreSQL database
  is saved in the project directory under `docker/database-data`.
  The current user (`$USER`) is owner of the volume.
  Containers created prior to this change will fail when using the
  changed code. The container needs to be re-created.
  `#228 <https://github.com/openego/eGon-data/issues/228>`_
* Extract landuse areas from OSM
  `#214 <https://github.com/openego/eGon-data/issues/214>`_
* Integrate weather data and renewable feedin timeseries
  `#19 <https://github.com/openego/eGon-data/issues/19>`_
* Create and import district heating areas
  `#162 <https://github.com/openego/eGon-data/issues/162>`_
* Integrate electrical load time series for cts sector
  `#109 <https://github.com/openego/eGon-data/issues/109>`_
* Assign voltage level and bus_id to power plants
  `#15 <https://github.com/openego/eGon-data/issues/15>`_
* Integrate solar rooftop for etrago tables
  `#255 <https://github.com/openego/eGon-data/issues/255>`_
* Integrate gas bus and link tables
  `#198 <https://github.com/openego/eGon-data/issues/198>`_
* Integrate data bundle
  `#272 <https://github.com/openego/eGon-data/issues/272>`_
* Integrate distribution of wind onshore and pv ground mounted generation
  `#146 <https://github.com/openego/eGon-data/issues/146>`_
* Integrate dynamic line rating potentials
  `#72 <https://github.com/openego/eGon-data/issues/72>`_
* Integrate gas voronoi polygons
  `#308 <https://github.com/openego/eGon-data/issues/308>`_


.. _PR #159: https://github.com/openego/eGon-data/pull/159


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
* Delete tables before re-creation and data insertation
  `#166 <https://github.com/openego/eGon-data/issues/166>`_
* Adjust residential heat demand in unpopulated zenus cells
  `#167 <https://github.com/openego/eGon-data/issues/167>`_
* Introduce mapping between VG250 municipalities and census cells
  `#165 <https://github.com/openego/eGon-data/issues/165>`_
* Delete tables if they exist before re-creation and data insertation
  `#166 <https://github.com/openego/eGon-data/issues/166>`_
* Add gdal to pre-requisites
  `#185 <https://github.com/openego/eGon-data/issues/185>`_
* Update task zensus-inside-germany
  `#196 <https://github.com/openego/eGon-data/issues/196>`_
* Update installation of demandregio's disaggregator
  `#202 <https://github.com/openego/eGon-data/issues/202>`_
* Update etrago tables
  `#243 <https://github.com/openego/eGon-data/issues/243>`_and
  `#285 <https://github.com/openego/eGon-data/issues/285>`_
* Migrate VG250 to datasets
  `#283 <https://github.com/openego/eGon-data/issues/283>`_

Bug fixes
---------
* Heat demand data import
  `#157 <https://github.com/openego/eGon-data/issues/157>`_
* Substation sequence
  `#171 <https://github.com/openego/eGon-data/issues/171>`_
* Adjust names of demandregios nuts3 regions according to nuts version 2016
  `#201 <https://github.com/openego/eGon-data/issues/201>`_
* Delete zensus buildings, apartments and households in unpopulated cells
  `#202 <https://github.com/openego/eGon-data/issues/202>`_
* Fix input table of electrical-demands-zensus
  `#217 <https://github.com/openego/eGon-data/issues/217>`_
* Import heat demand raster files successively to fix import for dataset==Everything
  `#204 <https://github.com/openego/eGon-data/issues/204>`_
* Replace wrong table name in SQL function used in substation extraction
  `#236 <https://github.com/openego/eGon-data/issues/236>`_
* Fix osmtgmod for osm data from 2021 by updating substation in Garenfeld and set srid
  `#241 <https://github.com/openego/eGon-data/issues/241>`_
  `#258 <https://github.com/openego/eGon-data/issues/258>`_
* Adjust format of voltage levels in hvmv substation
  `#248 <https://github.com/openego/eGon-data/issues/248>`_
* Change order of osmtgmod tasks
  `#253 <https://github.com/openego/eGon-data/issues/253>`_
* Fix missing municipalities
  `#279 <https://github.com/openego/eGon-data/issues/279>`_
