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
* You can now use tasks which are not part of a ``Dataset``, i.e. which are
  unversioned, as dependencies of a dataset. See `PR #318`_ for more
  details.
* You can now force the tasks of a ``Dataset`` to be always executed by
  giving the version of the ``Dataset`` a ``".dev"`` suffix. See `PR
  #318`_ for more details.
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
* Add household electricity demand time series, mapping of
  demand profiles to census cells and aggregated household
  electricity demand time series at MV grid district level
  `#256 <https://github.com/openego/eGon-data/issues/256>`_
* Integrate power-to-gas installation potential links
  `#293 <https://github.com/openego/eGon-data/issues/293>`_
* Integrate distribution of wind onshore and pv ground mounted generation
  `#146 <https://github.com/openego/eGon-data/issues/146>`_
* Integrate dynamic line rating potentials
  `#72 <https://github.com/openego/eGon-data/issues/72>`_
* Integrate gas voronoi polygons
  `#308 <https://github.com/openego/eGon-data/issues/308>`_
* Integrate supply strategies for individual and district heating
  `#232 <https://github.com/openego/eGon-data/issues/232>`_
* Integrate gas production
  `#321 <https://github.com/openego/eGon-data/issues/321>`_
* Integrate industrial time series creation
  `#237 <https://github.com/openego/eGon-data/issues/237>`_
* Merge electrical loads per bus and export to etrago tables
  `#328 <https://github.com/openego/eGon-data/issues/328>`_
* Insert industial gas demand
  `#321 <https://github.com/openego/eGon-data/issues/358>`_
* Integrate existing CHP and extdended CHP > 10MW_el
  `#266 <https://github.com/openego/eGon-data/issues/266>`_
* Add random seed to CLI parameters
  `#351 <https://github.com/openego/eGon-data/issues/351>`_
* Extend zensus by a combined table with all cells where
  there's either building, apartment or population data
  `#359 <https://github.com/openego/eGon-data/issues/359>`_
* Include allocation of pumped hydro units
  `#332 <https://github.com/openego/eGon-data/issues/332>`_
* Add example metadata for OSM, VG250 and Zensus VG250.
  Add metadata templates for licences, context and some helper
  functions. Extend docs on how to create metadata for tables.
  `#139 <https://github.com/openego/eGon-data/issues/139>`_
* Integrate DSM potentials for CTS and industry
  `#259 <https://github.com/openego/eGon-data/issues/259>`_
* Assign weather cell id to weather dependant power plants
  `#330 <https://github.com/openego/eGon-data/issues/330>`_
* Distribute wind offshore capacities
  `#329 <https://github.com/openego/eGon-data/issues/329>`_
* Add CH4 storages
  `#405 <https://github.com/openego/eGon-data/issues/405>`_
* Include allocation of conventional (non CHP) power plants
  `#392 <https://github.com/openego/eGon-data/issues/392>`_
* Fill egon-etrago-generators table
  `#485 <https://github.com/openego/eGon-data/issues/485>`_
* Include time-dependent coefficient of performance for heat pumps
  `#532 <https://github.com/openego/eGon-data/issues/532>`_
* Limit number of parallel processes per task
  `#265 <https://github.com/openego/eGon-data/issues/265>`_
* Include biomass CHP plants to eTraGo tables
  `#498 <https://github.com/openego/eGon-data/issues/498>`_
* Include Pypsa default values in table creation
  `#544 <https://github.com/openego/eGon-data/issues/544>`_
* Include PHS in eTraGo tables
  `#333 <https://github.com/openego/eGon-data/issues/333>`_
* Include feedin time series for wind offshore
  `#531 <https://github.com/openego/eGon-data/issues/531>`_
* Include carrier names in eTraGo table
  `#551 <https://github.com/openego/eGon-data/issues/551>`_
* Include hydrogen infrastructure for eGon2035 scenario
  `#474 <https://github.com/openego/eGon-data/issues/474>`_
* Include downloaded pypsa-eur-sec results
  `#138 <https://github.com/openego/eGon-data/issues/138>`_
* Create heat buses for eGon100RE scenario
  `#582 <https://github.com/openego/eGon-data/issues/582>`_
* Filter for DE in gas infrastructure deletion at beginning of respective tasks
  `#567 <https://github.com/openego/eGon-data/issues/567>`_
* Insert open cycle gas turbines into eTraGo tables
  `#548 <https://github.com/openego/eGon-data/issues/548>`_
* Preprocess buildings and amenities for LV grids
  `#262 <https://github.com/openego/eGon-data/issues/262>`_
* Assign household profiles to OSM buildings
  `#435 <https://github.com/openego/eGon-data/issues/435>`_
* Add link to meta creator to docs
  `#599 <https://github.com/openego/eGon-data/issues/599>`_
* Add extendable batteries and heat stores
  `#566 <https://github.com/openego/eGon-data/issues/566>`_
* Add efficiency, capital_cost and marginal_cost to gas related data in
  etrago tables `#596 <https://github.com/openego/eGon-data/issues/596>`_
* Add wind onshore farms for the eGon100RE scenario
  `#690 <https://github.com/openego/eGon-data/issues/690>`_
* The shared memory under `"/dev/shm"` is now shared between host and
  container. This was done because Docker has a rather tiny default for
  the size of `"/dev/shm"` which caused random problems. Guessing what
  size is correct is also not a good idea, so sharing between host and
  container seems like the best option. This restricts using `egon-data`
  with docker to Linux and MacOS, if the latter has `"/dev/shm"` but
  seems like the best course of action for now. Done via `PR #703`_ and
  hopefully prevents issues `#702`_ and `#267`_ from ever occurring
  again.
* Provide wrapper to catch DB unique violation
  `#514 <https://github.com/openego/eGon-data/issues/514>`_
* Add electric scenario parameters for eGon100RE
  `#699 <https://github.com/openego/eGon-data/issues/699>`_
* Introduce Sanity checks for eGon2035
  `#382 <https://github.com/openego/eGon-data/issues/382>`_
* Add motorized individual travel
  `#553 <https://github.com/openego/eGon-data/issues/553>`_
* Add mapping zensus - weather cells
  `#845 <https://github.com/openego/eGon-data/issues/845>`_
* Add pv rooftop plants per mv grid for eGon100RE
  `#861 <https://github.com/openego/eGon-data/issues/861>`_
* Integrated heavy duty transport FCEV
  `#552 <https://github.com/openego/eGon-data/issues/552>`_
* Assign CTS demands to buildings
  `#671 <https://github.com/openego/eGon-data/issues/671>`_
* Add sanity checks for residential electricity loads
  `#902 <https://github.com/openego/eGon-data/issues/902>`_
* Add sanity checks for cts loads
  `#919 <https://github.com/openego/eGon-data/issues/919>`_
* Add distribution of CHP plants for eGon100RE
  `#851 <https://github.com/openego/eGon-data/issues/851>`_
* Add charging infrastructure for e-mobility
  `#937 <https://github.com/openego/eGon-data/issues/937>`_
* Add zipfile check
  `#969 <https://github.com/openego/eGon-data/issues/969>`_
* Add marginal costs for generators abroad and for carriers nuclear and coal
  `#907 <https://github.com/openego/eGon-data/issues/907>`_
* Add wind off shore power plants for eGon100RE
  `#868 <https://github.com/openego/eGon-data/issues/868>`_
* Write simBEV metadata to DB table
  `PR #978 <https://github.com/openego/eGon-data/pull/978>`_

.. _PR #159: https://github.com/openego/eGon-data/pull/159
.. _PR #703: https://github.com/openego/eGon-data/pull/703
.. _#702: https://github.com/openego/eGon-data/issues/702
.. _#267: https://github.com/openego/eGon-data/issues/267


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
* Allow configuring the airflow port
  `#281 <https://github.com/openego/eGon-data/issues/281>`_
* Migrate mastr, mv_grid_districts and re_potential_areas to datasets
  `#297 <https://github.com/openego/eGon-data/issues/297>`_
* Migrate industrial sites to datasets
  `#237 <https://github.com/openego/eGon-data/issues/237>`_
* Rename etrago tables from e.g. egon_pf_hv_bus to egon_etrago bus etc.
  `#334 <https://github.com/openego/eGon-data/issues/334>`_
* Move functions used by multiple datasets
  `#323 <https://github.com/openego/eGon-data/issues/323>`_
* Migrate scenario tables to datasets
  `#309 <https://github.com/openego/eGon-data/issues/309>`_
* Migrate weather data and power plants to datasets
  `#314 <https://github.com/openego/eGon-data/issues/314>`_
* Create and fill table for CTS electricity demand per bus
  `#326 <https://github.com/openego/eGon-data/issues/326>`_
* Migrate osmTGmod to datasets
  `#305 <https://github.com/openego/eGon-data/issues/305>`_
* Filter osm landuse areas, rename industrial sites tables and update load curve function
  `#378 <https://github.com/openego/eGon-data/issues/378>`_
* Remove version columns from eTraGo tables and related code
  `#384 <https://github.com/openego/eGon-data/issues/384>`_
* Remove country column from scenario capacities table
  `#391 <https://github.com/openego/eGon-data/issues/391>`_
* Update version of zenodo download
  `#397 <https://github.com/openego/eGon-data/issues/397>`_
* Rename columns gid to id
  `#169 <https://github.com/openego/eGon-data/issues/169>`_
* Remove upper version limit of pandas
  `#383 <https://github.com/openego/eGon-data/issues/383>`_
* Use random seed from CLI parameters for CHP and society prognosis functions
  `#351 <https://github.com/openego/eGon-data/issues/351>`_
* Changed demand.egon_schmidt_industrial_sites - table and merged table (industrial_sites)
  `#423 <https://github.com/openego/eGon-data/issues/423>`_
* Replace 'gas' carrier with 'CH4' and 'H2' carriers
  `#436 <https://github.com/openego/eGon-data/issues/436>`_
* Adjust file path for industrial sites import
  `#397 <https://github.com/openego/eGon-data/issues/418>`_
* Rename columns subst_id to bus_id
  `#335 <https://github.com/openego/eGon-data/issues/335>`_
* Apply black and isort for all python scripts
  `#463 <https://github.com/openego/eGon-data/issues/463>`_
* Update deposit id for zenodo download
  `#397 <https://github.com/openego/eGon-data/issues/498>`_
* Add to etrago.setug.py the busmap table
  `#484 <https://github.com/openego/eGon-data/issues/484>`_
* Migrate dlr script to datasets
  `#508 <https://github.com/openego/eGon-data/issues/508>`_
* Migrate loadarea scripts to datasets
  `#525 <https://github.com/openego/eGon-data/issues/525>`_
* Migrate plot.py to dataset of district heating areas
  `#527 <https://github.com/openego/eGon-data/issues/527>`_
* Migrate substation scripts to datasets
  `#304 <https://github.com/openego/eGon-data/issues/304>`_
* Update deposit_id for zenodo download
  `#540 <https://github.com/openego/eGon-data/issues/540>`_
* Add household demand profiles to etrago table
  `#381 <https://github.com/openego/eGon-data/issues/381>`_
* Migrate zensus scripts to datasets
  `#422 <https://github.com/openego/eGon-data/issues/422>`_
* Add information on plz, city and federal state to data on mastr without chp
  `#425 <https://github.com/openego/eGon-data/issues/425>`_
* Assign residential heat demands to osm buildings
  `#557 <https://github.com/openego/eGon-data/issues/557>`_
* Add foreign gas buses and adjust cross bording pipelines
  `#545 <https://github.com/openego/eGon-data/issues/545>`_
* Integrate fuel and CO2 costs for eGon2035 to scenario parameters
  `#549 <https://github.com/openego/eGon-data/issues/549>`_
*  Aggregate generators and stores for CH4
  `#629 <https://github.com/openego/eGon-data/issues/629>`_
* Fill missing household data for populated cells
  `#431 <https://github.com/openego/eGon-data/issues/431>`_
* Fix RE potential areas outside of Germany by updating
  the dataset. Import files from data bundle.
  `#592 <https://github.com/openego/eGon-data/issues/592>`_
  `#595 <https://github.com/openego/eGon-data/issues/595>`_
* Add DC lines from Germany to Sweden and Denmark
  `#611 <https://github.com/openego/eGon-data/issues/611>`_
* H2 demand is met from the H2_grid buses. In Addtion, it can be met from the
  H2_saltcavern buses if a proximity criterion is fulfilled
  `#620 <https://github.com/openego/eGon-data/issues/620>`_
* Create H2 pipeline infrastructure for eGon100RE
  `#638 <https://github.com/openego/eGon-data/issues/638>`_
* Change refinement method for households types
  `#651 <https://github.com/openego/eGon-data/issues/#651>`_
* H2 feed in links are changed to non extendable
  `#653 <https://github.com/openego/eGon-data/issues/653>`_
* Remove the '_fixed' suffix
  `#628 <https://github.com/openego/eGon-data/issues/628>`_
* Fill table demand.egon_demandregio_zensus_electricity after profile allocation
  `#620 <https://github.com/openego/eGon-data/issues/586>`_
* Change method of building assignment
  `#663 <https://github.com/openego/eGon-data/issues/663>`_
* Create new OSM residential building table
  `#587 <https://github.com/openego/eGon-data/issues/587>`_
* Move python-operators out of pipeline
  `#644 <https://github.com/openego/eGon-data/issues/644>`_
* Add annualized investment costs to eTraGo tables
  `#672 <https://github.com/openego/eGon-data/issues/672>`_
* Improve modelling of NG and biomethane production
  `#678 <https://github.com/openego/eGon-data/issues/678>`_
* Unify carrier names for both scenarios
  `#575 <https://github.com/openego/eGon-data/issues/575>`_
* Add automatic filtering of gas data: Pipelines of length zero and gas buses
  isolated of the grid are deleted.
  `#590 <https://github.com/openego/eGon-data/issues/590>`_
* Add gas data in neighbouring countries
  `#727 <https://github.com/openego/eGon-data/issues/727>`_
* Aggregate DSM components per substation
  `#661 <https://github.com/openego/eGon-data/issues/661>`_
* Aggregate NUTS3 industrial loads for CH4 and H2
  `#452 <https://github.com/openego/eGon-data/issues/452>`_
* Update OSM dataset from 2021-02-02 to 2022-01-01
  `#486 <https://github.com/openego/eGon-data/issues/486>`_
* Update deposit id to access v0.6 of the zenodo repository
  `#627 <https://github.com/openego/eGon-data/issues/627>`_
* Include electricity storages for eGon100RE scenario
  `#581 <https://github.com/openego/eGon-data/issues/581>`_
* Update deposit id to access v0.7 of the zenodo repository
  `#736 <https://github.com/openego/eGon-data/issues/736>`_
* Include simplified restrictions for H2 feed-in into CH4 grid
  `#790 <https://github.com/openego/eGon-data/issues/790>`_
*  Update hh electricity profiles
  `#735 <https://github.com/openego/eGon-data/issues/735>`_
* Improve CH4 stores and productions aggregation by removing dedicated task
  `#775 <https://github.com/openego/eGon-data/pull/775>`_
* Add CH4 stores in Germany for eGon100RE
  `#779 <https://github.com/openego/eGon-data/issues/779>`_
* Assigment of H2 and CH4 capacitites for pipelines in eGon100RE
  `#686 <https://github.com/openego/eGon-data/issues/686>`_
* Update deposit id to access v0.8 of the zenodo repository
  `#760 <https://github.com/openego/eGon-data/issues/760>`_
* Add primary key to table openstreetmap.osm_ways_with_segments
  `#787 <https://github.com/openego/eGon-data/issues/787>`_
* Update pypsa-eur-sec fork and store national demand time series
  `#402 <https://github.com/openego/eGon-data/issues/402>`_
* Move and merge the two assign_gas_bus_id functions to a central place
  `#797 <https://github.com/openego/eGon-data/issues/797>`_
* Add coordinates to non AC buses abroad in eGon100RE
  `#803 <https://github.com/openego/eGon-data/issues/803>`_
* Integrate additional industrial electricity demands for eGon100RE
  `#817 <https://github.com/openego/eGon-data/issues/817>`_
* Set non extendable gas components from p-e-s as so for eGon100RE
  `#877 <https://github.com/openego/eGon-data/issues/877>`_
* Integrate new data bundle using zenodo sandbox
  `#866 <https://github.com/openego/eGon-data/issues/866>`_
* Add noflex scenario for motorized individual travel
  `#821 <https://github.com/openego/eGon-data/issues/821>`_
* Add sanity checks for motorized individual travel
  `#820 <https://github.com/openego/eGon-data/issues/820>`_
* Parallelize sanity checks
  `#882 <https://github.com/openego/eGon-data/issues/882>`_
* Rename noflex to lowflex scenario for motorized individual travel
  `#921 <https://github.com/openego/eGon-data/issues/921>`_
* Update creation of heat demand timeseries
  `#857 <https://github.com/openego/eGon-data/issues/857>`_
  `#856 <https://github.com/openego/eGon-data/issues/856>`_
* Introduce carrier name 'others'
  `#819 <https://github.com/openego/eGon-data/issues/819>`_

Bug Fixes
---------

* Some dependencies have their upper versions restricted now. This is
  mostly due to us not yet supporting Airflow 2.0 which means that it
  will no longer work with certain packages, but we also won't get and
  upper version limit for those from Airflow because version 1.X is
  unlikely to to get an update. So we had to make some implicit
  dependencies explicit in order to give them them upper version limits.
  Done via `PR #692`_ in order to fix issues `#343`_, `#556`_, `#641`_
  and `#669`_.
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
* Fix import of hydro power plants
  `#270 <https://github.com/openego/eGon-data/issues/270>`_
* Fix path to osm-file for osmtgmod_osm_import
  `#258 <https://github.com/openego/eGon-data/issues/258>`_
* Fix conflicting docker containers by setting a project name
  `#289 <https://github.com/openego/eGon-data/issues/289>`_
* Update task insert-nep-data for pandas version 1.3.0
  `#322 <https://github.com/openego/eGon-data/issues/322>`_
* Fix versioning conflict with mv_grid_districts
  `#340 <https://github.com/openego/eGon-data/issues/340>`_
* Set current working directory as java's temp dir when executing osmosis
  `#344 <https://github.com/openego/eGon-data/issues/344>`_
* Fix border gas voronoi polygons which had no bus_id
  `#362 <https://github.com/openego/eGon-data/issues/362>`_
* Add dependency from WeatherData to Vg250
  `#387 <https://github.com/openego/eGon-data/issues/387>`_
* Fix unnecessary columns in normal mode for inserting the gas production
  `#387 <https://github.com/openego/eGon-data/issues/390>`_
* Add xlrd and openpyxl to installation setup
  `#400 <https://github.com/openego/eGon-data/issues/400>`_
* Store files of OSM, zensus and VG250 in working dir
  `#341 <https://github.com/openego/eGon-data/issues/341>`_
* Remove hard-coded slashes in file paths to ensure Windows compatibility
  `#398 <https://github.com/openego/eGon-data/issues/398>`_
* Add missing dependency in pipeline.py
  `#412 <https://github.com/openego/eGon-data/issues/412>`_
* Add prefix egon to MV grid district tables
  `#349 <https://github.com/openego/eGon-data/issues/349>`_
* Bump MV grid district version no
  `#432 <https://github.com/openego/eGon-data/issues/432>`_
* Add curl to prerequisites in the docs
  `#440 <https://github.com/openego/eGon-data/issues/440>`_
* Replace NAN by 0 to avoid empty p_set column in DB
  `#414 <https://github.com/openego/eGon-data/issues/414>`_
* Exchange bus 0 and bus 1 in Power-to-H2 links
  `#458 <https://github.com/openego/eGon-data/issues/458>`_
* Fix missing cts demands for eGon2035
  `#511 <https://github.com/openego/eGon-data/issues/511>`_
* Add `data_bundle` to `industrial_sites` task dependencies
  `#468 <https://github.com/openego/eGon-data/issues/468>`_
* Lift `geopandas` minimum requirement to `0.10.0`
  `#504 <https://github.com/openego/eGon-data/issues/504>`_
* Use inbuilt `datetime` package instead of `pandas.datetime`
  `#516 <https://github.com/openego/eGon-data/issues/516>`_
* Add missing 'sign' for CH4 and H2 loads
  `#516 <https://github.com/openego/eGon-data/issues/538>`_
* Delete only AC loads for eTraGo in electricity_demand_etrago
  `#535 <https://github.com/openego/eGon-data/issues/535>`_
* Filter target values by scenario name
  `#570 <https://github.com/openego/eGon-data/issues/570>`_
* Reduce number of timesteps of hh electricity demand profiles to 8760
  `#593 <https://github.com/openego/eGon-data/issues/593>`_
* Fix assignemnt of heat demand profiles at German borders
  `#585 <https://github.com/openego/eGon-data/issues/585>`_
* Change source for H2 steel tank storage to Danish Energy Agency
  `#605 <https://github.com/openego/eGon-data/issues/605>`_
* Change carrier name from 'pv' to 'solar' in eTraGo_generators
  `#617 <https://github.com/openego/eGon-data/issues/617>`_
* Assign "carrier" to transmission lines with no value in grid.egon_etrago_line
  `#625 <https://github.com/openego/eGon-data/issues/625>`_
* Fix deleting from eTraGo tables
  `#613 <https://github.com/openego/eGon-data/issues/613>`_
* Fix positions of the foreign gas buses
  `#618 <https://github.com/openego/eGon-data/issues/618>`_
* Create and fill transfer_busses table in substation-dataset
  `#610 <https://github.com/openego/eGon-data/issues/610>`_
* H2 steel tanks are removed again from saltcavern storage
  `#621 <https://github.com/openego/eGon-data/issues/621>`_
* Timeseries not deleted from grid.etrago_generator_timeseries
  `#645 <https://github.com/openego/eGon-data/issues/645>`_
* Fix function to get scaled hh profiles
  `#674 <https://github.com/openego/eGon-data/issues/674>`_
* Change order of pypsa-eur-sec and scenario-capacities
  `#589 <https://github.com/openego/eGon-data/issues/589>`_
* Fix gas storages capacities
  `#676 <https://github.com/openego/eGon-data/issues/676>`_
* Distribute rural heat supply to residetntial and service demands
  `#679 <https://github.com/openego/eGon-data/issues/679>`_
* Fix time series creation for pv rooftop
  `#688 <https://github.com/openego/eGon-data/issues/688>`_
* Fix extraction of buildings without amenities
  `#693 <https://github.com/openego/eGon-data/issues/693>`_
* Assign DLR capacities to every transmission line
  `#683 <https://github.com/openego/eGon-data/issues/683>`_
* Fix solar ground mounted total installed capacity
  `#695 <https://github.com/openego/eGon-data/issues/695>`_
* Fix twisted number error residential demand
  `#704 <https://github.com/openego/eGon-data/issues/704>`_
* Fix industrial H2 and CH4 demand for eGon100RE scenario
  `#687 <https://github.com/openego/eGon-data/issues/687>`_
* Clean up `"pipeline.py"`
  `#562 <https://github.com/openego/eGon-data/issues/562>`_
* Assign timeseries data to crossborder generators ego2035
  `#724 <https://github.com/openego/eGon-data/issues/724>`_
* Add missing dataset dependencies in "pipeline.py"
  `#725 <https://github.com/openego/eGon-data/issues/725>`_
* Fix assignemnt of impedances (x) to etrago tables
  `#710 <https://github.com/openego/eGon-data/issues/710>`_
* Fix country_code attribution of two gas buses
  `#744 <https://github.com/openego/eGon-data/issues/744>`_
* Fix voronoi assignemnt for enclaves
  `#734 <https://github.com/openego/eGon-data/issues/734>`_
* Set lengths of non-pipeline links to 0
  `#741 <https://github.com/openego/eGon-data/issues/741>`_
* Change table name from :code:`boundaries.saltstructures_inspee` to
  :code:`boundaries.inspee_saltstructures`
  `#746 <https://github.com/openego/eGon-data/issues/746>`_
* Add missing marginal costs for conventional generators in Germany
  `#722 <https://github.com/openego/eGon-data/issues/722>`_
* Fix carrier name for solar ground mounted in scenario parameters
  `#752 <https://github.com/openego/eGon-data/issues/752>`_
* Create rural_heat buses only for mv grid districts with heat load
  `#708 <https://github.com/openego/eGon-data/issues/708>`_
* Solve problem while creating generators series data egon2035
  `#758 <https://github.com/openego/eGon-data/issues/758>`_
* Correct wrong carrier name when assigning marginal costs
  `#766 <https://github.com/openego/eGon-data/issues/766>`_
* Use db.next_etrago_id in dsm and pv_rooftop dataset
  `#748 <https://github.com/openego/eGon-data/issues/748>`_
* Add missing dependency to heat_etrago
  `#771 <https://github.com/openego/eGon-data/issues/771>`_
* Fix country code of gas pipeline DE-AT
  `#813 <https://github.com/openego/eGon-data/issues/813>`_
* Fix distribution of resistive heaters in district heating grids
  `#783 <https://github.com/openego/eGon-data/issues/783>`_
* Fix missing reservoir and run_of_river power plants in eTraGo tables,
  Modify fill_etrago_gen to also group generators from eGon100RE,
  Use db.next_etrago_id in fill_etrago_gen
  `#798 <https://github.com/openego/eGon-data/issues/798>`_
  `#776 <https://github.com/openego/eGon-data/issues/776>`_
* Fix model load timeseries in motorized individual travel
  `#830 <https://github.com/openego/eGon-data/issues/830>`_
* Fix gas costs
  `#830 <https://github.com/openego/eGon-data/issues/847>`_
* Add imports that have been wrongly deleted
  `#849 <https://github.com/openego/eGon-data/issues/849>`_
* Fix final demand of heat demand timeseries
  `#781 <https://github.com/openego/eGon-data/issues/781>`_
* Add extendable batteries only to buses at substations
  `#852 <https://github.com/openego/eGon-data/issues/852>`_
* Move class definition for grid.egon_gas_voronoi out of etrago_setup
  `#888 <https://github.com/openego/eGon-data/issues/888>`_
* Temporarily set upper version limit for pandas
  `#829 <https://github.com/openego/eGon-data/issues/829>`_
* Change industrial gas load modelling
  `#871 <https://github.com/openego/eGon-data/issues/871>`_
* Delete eMob MIT data from eTraGo tables on init
  `#878 <https://github.com/openego/eGon-data/issues/878>`_
* Fix model id issues in DSM potentials for CTS and industry
  `#901 <https://github.com/openego/eGon-data/issues/901>`_
* Drop isolated buses and tranformers in eHV grid
  `#874 <https://github.com/openego/eGon-data/issues/874>`_
* Model gas turbines always as links
  `#914 <https://github.com/openego/eGon-data/issues/914>`_
* Drop era5 weather cell table using cascade
  `#909 <https://github.com/openego/eGon-data/issues/909>`_
* Delete gas bus with wrong country code
  `#958 <https://github.com/openego/eGon-data/issues/958>`_
* Overwrite capacities for conventional power plants with data from nep list
  `#403 <https://github.com/openego/eGon-data/issues/403>`_

.. _PR #692: https://github.com/openego/eGon-data/pull/692
.. _#343: https://github.com/openego/eGon-data/issues/343
.. _#556: https://github.com/openego/eGon-data/issues/556
.. _#641: https://github.com/openego/eGon-data/issues/641
.. _#669: https://github.com/openego/eGon-data/issues/669
