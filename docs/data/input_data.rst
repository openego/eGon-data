.. _data-bundle-ref:

Data bundle
-----------

The data bundle is published on
`zenodo <https://zenodo.org/records/4896526>`_. It contains several data
sets, which serve as a basis for egon-data:

* Climate zones in Germany
* Data on eMobility individual trips of electric vehicles
* Spatial distribution of deep geothermal potentials in Germany
* Annual profiles in hourly resolution of electricity demand of private households
* Sample heat time series including hot water and space heating for single- and multi-familiy houses
* Hydrogen storage potentials in salt structures
* Information about industrial sites with DSM-potential in Germany
* Data extracted from the German grid development plan - power
* Parameters for the classification of gas pipelines
* Preliminary results from scenario generator pypsa-eur-sec
* German regions suitable to model dynamic line rating
* Eligible areas for wind turbines and ground-mounted PV systems
* Definitions of industrial and commercial branches
* Zensus data on households
* Geocoding of all unique combinations of ZIP code and municipality within the Marktstammdatenregister

For further description of the data including licenses and references please refer to the Zenodo repository.

.. _mastr-ref:

Marktstammdatenregister
-----------------------

The `Marktstammdatenregister <https://www.marktstammdatenregister.de/MaStR>`_ (MaStR)
is the register for the German electricity and gas
market holding, among others, data on electricity and gas generation plants. In eGon-data
it is used for status quo data on PV plants, wind turbines, biomass, hydro power plants,
combustion power plants, nuclear power plants, geo- and solarthermal power plants, and storage units.
The data are obtained from zenodo, where raw MaStR data, downloaded with the tool
`open-MaStR <https://github.com/OpenEnergyPlatform/open-MaStR>`_ using the MaStR webservice,
is provided. It contains all data from the MaStR, including possible duplicates.
Currently, two versions are used:

* `2021-04-30 <https://zenodo.org/records/10480930>`_
* `2022-11-17 <https://zenodo.org/records/10480958>`_

The download is implemented in :class:`MastrData<egon.data.datasets.mastr.MastrData>`.

.. _osm-ref:

OpenStreetMap
-------------

`OpenStreetMap <https://www.openstreetmap.org/>`_ (OSM) is a free, editable map of the whole
world that is being built by volunteers and released with an open-content license.
In eGon-data it is, among others, used to obtain information on land use as well as
locations of buildings and amenities to spatially dissolve energy demand.
The OSM data is downloaded from the `Geofabrik <https://www.geofabrik.de/>`_ download
server, which holds extracts from the OpenStreetMap. Afterwards, they are imported
to the database using osm2pgsql with a custom style file. The implementation of this
can be found in :class:`OpenStreetMap<egon.data.datasets.osm.OpenStreetMap>`.

In the :class:`OpenStreetMap<egon.data.datasets.osm_buildings_streets.OsmBuildingsStreets>`
dataset, the OSM data is filtered, processed and enriched with other data. This is
described in the following subsections.

Amenity data
++++++++++++++

The data on amenities is used to disaggregate CTS demand data. It is filtered from the
raw OSM data using tags listed in script `osm_amenities_shops_preprocessing.sql`, e.g.
shops and restaurants. The filtered data is written to database table
`openstreetmap.osm_amenities_shops_filtered`.

.. _building-data-ref:

Building data
++++++++++++++

The data on buildings is required by several tasks in the
pipeline, such as the disaggregation of household demand profiles or PV home
systems to buildings, as well as the DIstribution Network Generat0r `ding0
<https://github.com/openego/ding0>`_ (see also :ref:`ding0-grids`).

The data processing steps are:

* Extract buildings and filter using relevant tags, e.g. residential and
  commercial, see script `osm_buildings_filter.sql` for the full list of tags.
  Resulting tables:

  * All buildings: `openstreetmap.osm_buildings`
  * Filtered buildings: `openstreetmap.osm_buildings_filtered`
  * Residential buildings: `openstreetmap.osm_buildings_residential`

* Create a mapping table for building's OSM IDs to the Zensus cells the
  building's centroid is located in.
  Resulting tables:

  * `boundaries.egon_map_zensus_buildings_filtered` (filtered)
  * `boundaries.egon_map_zensus_buildings_residential` (residential only)

* Enrich each building by number of apartments from Zensus table
  `society.egon_destatis_zensus_apartment_building_population_per_ha`
  by splitting up the cell's sum equally to the buildings. In some cases, a
  Zensus cell does not contain buildings but there is a building nearby which
  the no. of apartments is to be allocated to. To make sure apartments are
  allocated to at least one building, a radius of 77m is used to catch building
  geometries.
* Split filtered buildings into 3 datasets using the amenities' locations:
  temporary tables are created in script `osm_buildings_temp_tables.sql`, the
  final tables in `osm_buildings_amentities_results.sql`.
  Resulting tables:

  * Buildings w/ amenities: `openstreetmap.osm_buildings_with_amenities`
  * Buildings w/o amenities: `openstreetmap.osm_buildings_without_amenities`
  * Amenities not allocated to buildings:
    `openstreetmap.osm_amenities_not_in_buildings`

As there are discrepancies between the Census data [Census]_ and OSM building data when both
datasets are used to generate electricity demand profiles of households, synthetic buildings
are added in Census cells with households but without buildings. This is done as part
of the :class:`Demand_Building_Assignment<egon.data.datasets.electricity_demand_timeseries.hh_buildings.Demand_Building_Assignment>`
dataset in function :func:`generate_synthetic_buildings<egon.data.datasets.electricity_demand_timeseries.hh_buildings.generate_synthetic_buildings>`.
The synthetic building data are written to table `openstreetmap.osm_buildings_synthetic`.
The same is done in case of CTS electricity demand profiles. Here, electricity demand is
disaggregated to Census cells according to heat demand information from the
Pan European Thermal Atlas [Peta]_. In case there are Census cells with electricity demand
assigned but no building or amenity data, synthetic buildings are added.
This is done as part
of the :class:`CtsDemandBuildings<egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsDemandBuildings>`
dataset in function :func:`create_synthetic_buildings<egon.data.datasets.electricity_demand_timeseries.cts_buildings.create_synthetic_buildings>`.
The synthetic building data are again written to table `openstreetmap.osm_buildings_synthetic`.

Street data
++++++++++++++

The data on streets is used in the DIstribution Network Generat0r `ding0
<https://github.com/openego/ding0>`_, e.g. for the routing of the grid.
It is filtered from the
raw OSM data using tags listed in script `osm_ways_preprocessing.sql`, e.g.
highway=secondary. Additionally, each way is split into its line segments and their
lengths is retained. The filtered streets data is written to database table
`openstreetmap.osm_ways_preprocessed` and the filtered streets with segments
to table `openstreetmap.osm_ways_with_segments`.
