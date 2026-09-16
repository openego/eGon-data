.. _mobility-demand-mit-ref:

Motorized individual travel
++++++++++++++++++++++++++++

The electricity demand data of motorized individual travel (MIT) is set up for all
configured scenarios in the
:py:class:`MotorizedIndividualTravel<egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`
dataset.
The workflow is visualised in figure :ref:`mit-model` and is analogous for each
scenario. Scenarios differ in the assumed number of EVs and in whether flexible
(smart) charging is modelled: status quo scenarios are modelled with dumb charging
only, while projection scenarios additionally get a charging link, a battery store
and a lowflex counterpart
(cf. :py:func:`is_flexible<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.is_flexible>`).
In a first step, pre-generated SimBEV trip data, including information on driving, parking and
(user-oriented) charging times is downloaded.
In the second step, the number of EVs in each MV grid district in the future scenarios is determined.
Last, based on the trip data and the EV numbers, charging time series as well as
time series to model the flexibility of EVs are set up.
In the following, these steps are explained in more detail.

.. figure:: /images/eGon_emob_MIT_model.png
  :name: mit-model
  :width: 800

  Workflow to set up charging demand data for MIT


The trip data are generated using a modified version of
`SimBEV v0.1.3 <https://github.com/rl-institut/simbev/tree/1f87c716d14ccc4a658b8d2b01fd12b88a4334d5>`_.
SimBEV generates driving and parking profiles for battery electric vehicles (BEVs) and
plug-in hybrid electric vehicles (PHEVs) based on MID survey data [MiD2017]_ per
RegioStaR7 region type [RegioStaR7_2020]_.
The data contain information on energy consumption during the drive, as well as on
the availability of charging points at the parking
location and in case of an available charging point the corresponding charging demand,
charging power and charging point use case
(home charging point, workplace charging point, public charging point and fast charging
point).
Different vehicle classes are taken
into account whose assumed technical data is given in table :ref:`ev-types-data`.
Moreover, charging probabilities for multiple types of charging
infrastructure are presumed based on [NOW2020]_ and [Helfenbein2021]_.
Given these assumptions, trip data for a pool of 33.000 EV-types is pre-generated and provided through the data bundle
(see :ref:`data-bundle-ref`). The data is as well written to database tables
:py:class:`EgonEvTrip<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvTrip>`,
containing information on the driving and parking times of each EV,
and :py:class:`EgonEvPool<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvPool>`,
containing information on the type of EV and RegioStaR7 region the trip data corresponds to.
The complete technical data and assumptions of the SimBEV run can be found in the
metadata_simbev_run.json file, that is provided along with the trip data through the data bundle.
The metadata is as well written to the database table
:py:class:`EgonEvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMetadata>`.

.. csv-table:: Differentiated EV types and corresponding technical data
    :header: "Technology", "Size", "Max. slow charging capacity in kW", "Max. fast charging capacity in kW", "Battery capacity in kWh", "Energy consumption in kWh/km"
    :widths: 10, 10, 30, 30, 25, 10
    :name: ev-types-data

    "BEV", "mini", 11, 120, 60, 0.1397
    "BEV", "medium", 22, 350, 90, 0.1746
    "BEV", "luxury", 50, 350, 110, 0.2096
    "PHEV", "mini", 3.7, 40, 14, 0.1425
    "PHEV", "medium", 11, 40, 20, 0.1782
    "PHEV", "luxury", 11, 120, 30, 0.2138

The assumed total number of EVs in Germany is 2.62 million in the status2024 scenario
(BEV and PHEV stock as of 01.01.2025 according to [KBA2025]_), 34.1 million in the
reGon2037 scenario and 40.6 million in the reGon2045 scenario (both according to the
network development plan [NEP2025]_, Scenario C). The numbers per scenario are defined
in :py:func:`mobility<egon.data.datasets.scenario_parameters.parameters.mobility>`.

.. note::

   No dedicated SimBEV runs exist for the reGon scenarios yet. They reuse the
   pre-generated trip data of the earlier scenarios by horizon (reGon2037 reuses the
   eGon2035 run, reGon2045 the eGon100RE run). As EV profiles are drawn from the pool
   with replacement, the larger fleets simply resample the pool more often, but the
   vehicle and charging power assumptions are those of the reused run rather than of
   the target year.
To spatially disaggregate the charging demand, the total number of EVs per EV type
is first allocated to MV grid districts based on vehicle registration [KBA]_ and population [Census]_ data
(see function :py:func:`allocate_evs_numbers<egon.data.datasets.emobility.motorized_individual_travel.ev_allocation.allocate_evs_numbers>`).
The resulting number of EVs per EV type in each MV grid district in each scenario is written to the database table
:py:class:`EgonEvCountMvGridDistrict<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvCountMvGridDistrict>`.
Each MV grid district is then assigned a random pool of EV profiles from the pre-generated
trip data based on the RegioStaR7 region [RegioStaR7_2020]_ the grid district is assigned to and the counts
per EV type
(see function :py:func:`allocate_evs_to_grid_districts<egon.data.datasets.emobility.motorized_individual_travel.ev_allocation.allocate_evs_to_grid_districts>`).
The results are written to table
:py:class:`EgonEvMvGridDistrict<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMvGridDistrict>`.

On the basis of the assigned EVs per MV grid district and the trip data, charging demand
time series in each MV grid district can be determined. For inflexible charging
(see lower right in figure :ref:`mit-model`) it is
assumed that the EVs are charged with full power as soon as they arrive at a charging
station until they are fully charged. The respective charging power and demand is obtained
from the trip data. The individual charging demand time series per EV are summed up
to obtain the charging time series per MV grid district.
The generation of time series to model flexible charging of EVs (upper right in figure
:ref:`mit-model`) is described in section :ref:`flexible-charging-ref`.

For grid analyses of the MV and LV level, the charging demand needs to be further disaggregated
within the MV grid districts. To this end, potential charging sites are determined.
These potential charging sites are then used to allocate the charging demand of the
EVs in each MV grid district to specific charging points. This allocation is not done in
eGon-data but in the `eDisGo <https://github.com/openego/eDisGo>`_ tool and further
described in the
`eDisGo documentation <https://edisgo.readthedocs.io/en/dev/features_in_detail.html#allocation-of-charging-demand>`_.

The determination of potential charging sites is conducted in class
:py:class:`MITChargingInfrastructure<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.MITChargingInfrastructure>`.
The results are written to database table
:py:class:`EgonEmobChargingInfrastructure<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes.EgonEmobChargingInfrastructure>`.
The approach used to determine potential charging sites is based on the method implemented in
`TracBEV <https://github.com/rl-institut/tracbev>`_.
Four use cases for charging points are differentiated - home, work, public and high-power charging (hpc).
The potential charging sites are determined based on geographical data. Each
possible charging site is assigned an attractivity that represents the likelihood that a
charging point is installed at that site.
The used approach is for each use case shortly described in the following:

* Home charging: The allocation of home charging stations is based on the number of apartments in each
  100 x 100 m grid given by the Census 2011 [Census]_. The cell with the highest
  number of apartments receives the highest attractivity.
* Work charging: The allocation of work charging stations is based on the area classification obtained from
  OpenStreetMap [OSM]_ using the landuse key. Work charging stations are allocated to areas
  tagged with commercial, retail or industrial. The attractivity of each area
  depends on the size of the area as well as the classification.
  Commercial areas receive the highest attractivity, followed
  by retail areas. Industrial areas are ranked lowest.
* Public charging (slow): The basis for the allocation
  of public charging stations are points of interest (POI) from
  OSM [OSM]_. POI can be schools, shopping malls, supermarkets,
  etc. The attractivity of each POI is determined by empirical
  studies conducted in previous projects.
* High-power charging: The basis for the allocation of fast
  charging stations are the locations of existing petrol stations
  obtained from OSM [OSM]_. The locations are ranked randomly at the moment.

The necessary input data is downloaded from `zenodo <https://zenodo.org/records/6466480>`_.


.. _mobility-demand-hdt-ref:

Heavy-duty transport
+++++++++++++++++++++

In the context of the eGon project, it is assumed that all e-trucks will be
completely hydrogen-powered. The hydrogen demand data of all e-trucks is set up
in the :py:class:`HeavyDutyTransport<egon.data.datasets.emobility.heavy_duty_transport.HeavyDutyTransport>`
dataset for both the eGon2035 and eGon100RE scenario.

In both scenarios the hydrogen consumption is
assumed to be 6.68 kgH2 per 100 km with an additional supply chain leakage rate of 0.5 %
(see `here <https://www.energy.gov/eere/fuelcells/doe-technical-targets-hydrogen-delivery>`_).

For the eGon2035 scenario the ramp-up figures are taken from the
network development plan [NEP2021]_
(Scenario C 2035). According to this, 100,000 e-trucks are
expected in Germany in 2035, each covering an average of 100,000 km per year.
In total this means 10 Billion km.

For the eGon100RE scenario it is assumed that the heavy-duty transport is
completely hydrogen-powered. The total freight traffic with 40 Billion km is
taken from the
`BMWK Langfristszenarien <https://www.langfristszenarien.de/enertile-explorer-wAssets/docs/LFS3_Langbericht_Verkehr_final.pdf#page=17>`_
for heavy-duty vehicles larger 12 t allowed total weight (SNF > 12 t zGG).

The total hydrogen demand is spatially distributed on the basis of traffic volume data from [BASt]_.
For this purpose, first a voronoi partition of Germany using the traffic measuring points is created.
Afterwards, the spatial shares of the Voronoi regions in each NUTS3 area are used to allocate
hydrogen demand to the NUTS3 regions and are then aggregated per NUTS3 region.
The refuelling is assumed to take place at a constant rate.
Finally, to
determine the hydrogen bus where the hydrogen demand is allocated to, the centroid
of each NUTS3 region is used to determine the respective hydrogen Voronoi cell (see
:py:class:`GasAreaseGon2035<egon.data.datasets.gas_areas.GasAreaseGon2035>` and
:py:class:`GasAreaseGon100RE<egon.data.datasets.gas_areas.GasAreaseGon100RE>`) it is
located in.

.. _mobility-demand-rail-ref:

Rail and public transport
+++++++++++++++++++++++++

The electricity demand of electrified rail and urban public transport is set up
in the
:py:class:`RailTransitDemand<egon.data.datasets.rail_transport_demand.RailTransitDemand>`
dataset. It covers three traction systems, each written as its own eTraGo
carrier, because they draw from the public grid at different places and at
different voltage levels:

.. list-table::
   :header-rows: 1
   :widths: 30 25 20 25

   * - System
     - Carrier
     - Coupling point
     - Grid level
   * - 16.7 Hz main-line traction
     - ``rail_traction``
     - converter stations
     - EHV/HV
   * - S-Bahn Berlin and Hamburg (DC)
     - ``rail_sbahn_dc``
     - rectifier substations
     - MV
   * - Tram, U-Bahn, Stadtbahn (DC)
     - ``rail_transit_dc``
     - rectifier substations
     - MV

The 16.7-Hz network is an island: it is fed from the public grid only through a
small number of converter stations, so the whole main-line traction demand of
Germany enters the model at 19 points. The DC systems, by contrast, are fed by
many rectifier substations spread across each city.

Input data
----------

The data bundle carries only what eGon cannot derive itself
(``data_bundle_egon_data/rail_transport_demand/``):

* ``converter_load_points.csv`` -- the 16.7-Hz converter stations with
  coordinates, base energy and grid level. These are curated, because OSM
  under-tags converter stations and they are too few and too important to
  reconstruct heuristically.
* ``dc_city_energy.csv`` -- annual energy per city and traction system, with
  the city centroid.
* ``load_profiles.csv`` -- normalized hourly shapes per system, derived from
  measured load data.

Everything else is computed from eGon's own tables.

Processing steps
----------------

* **Classify DC rectifier substations from OSM.** Substations are read from
  eGon's OSM tables and classified by their ``frequency`` and ``voltage`` tags:
  a station that declares 16.7 Hz *and nothing else* belongs to the traction
  island and can never be a rectifier, while a station that declares both 50 Hz
  and 0 Hz, or carries a DC output voltage, is one. The run logs how many of
  the OSM substations were classified.
* **Distribute city energy over its rectifiers.** For each city and system, all
  DC rectifiers within 25 km of the city centroid receive an equal share of
  that city's annual energy. Where no rectifier is mapped, the full energy is
  placed at the city centroid instead; the run logs how many rows fall back and
  how much energy they carry.
* **Assign a bus per coupling level.** Points at EHV/HV level are joined into
  the EHV substation voronoi cells, points at MV level into the MV grid
  districts. Points that fall outside every polygon -- along the coastline, for
  instance -- are attached to the nearest one, and the run logs how often that
  fallback bites.
* **Re-index the load profiles onto weather year 2011.** The shapes are
  measured on a recent year whose weekday sequence differs from 2011, so each
  2011 hour takes the shape of the hour with the same ISO week, weekday and
  hour of day, falling back to the (weekday, hour) mean. Each column is
  renormalized to sum to 1 over the 8760 hours, which makes
  ``p_set[h] = energy_mwh_a * profile[h]`` an average power in MW.
* **Scale per scenario and write.** The scenario factor is the ratio of the
  gross rail consumption stored in the scenario parameters,
  ``total(scn) / total(status2024)``. Only the level is scaled -- the hourly
  shape is identical in every scenario.

Results are written to ``grid.egon_etrago_load`` and
``grid.egon_etrago_load_timeseries``.

Alongside them, every load point is persisted to
``grid.egon_rail_transport_load_points`` with the geometry it was placed at and
how it got there. One row per load row, joinable on ``(scn_name, load_id)``:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Column
     - Meaning
   * - ``method``
     - ``converter`` for the 16.7-Hz stations, ``dc_rectifier`` where a city's
       energy was split over mapped rectifiers, ``dc_centroid`` where no
       rectifier was mapped and the energy stayed at the city centroid
   * - ``place``
     - the city or site the point belongs to, where the input names one
   * - ``bus_method``
     - ``within`` if the point fell inside a polygon, ``nearest`` if it was
       attached to the closest one
   * - ``energy_mwh_a``
     - the scaled annual energy of that point in this scenario
   * - ``geom``
     - the point itself, EPSG:3035 -- the table loads in QGIS as it is

This is what makes a surprising result traceable: a load that looks misplaced
can be asked why it sits where it does, rather than only counted.

.. note::
   The dataset writes loads only for scenarios that the run actually builds
   (see
   :py:func:`configured_scenarios<egon.data.datasets.rail_transport_demand.configured_scenarios>`).
   Writing them for a scenario left out by ``--scenarios`` would attach them to
   buses that ``grid.egon_etrago_bus`` has no rows for, and no eTraGo export
   could resolve them.

Known limitations
-----------------

These are ordered by how much they could move a result, not by how easy they
are to state.

.. warning::
   **Where a city has no mapped rectifier, its entire DC energy sits on a
   single point.** OSM maps tram and U-Bahn rectifier substations only
   sparsely, and a city/system row without one falls back to the city
   centroid. This is the largest placement uncertainty in the dataset, and it
   is not marginal: in the source data more than half of the city/system rows
   had no mapped rectifier, and the single largest load in the whole dataset --
   the Munich U-Bahn at roughly 198 GWh a year -- is one of them, attached to
   one MV bus at the city centre. The run logs how many rows fall back and how
   much energy they carry; read that figure before using the result at city
   resolution.

   **The energy of the DC systems is the weakest anchor.** Tram and U-Bahn
   consumption is not measured per city; it is calibrated. The 16.7-Hz traction
   anchor and the two S-Bahn networks rest on reported figures, the tram and
   U-Bahn figure does not.

   **The 25 km radius and the equal split are settings, not derived values.**
   A rectifier within the radius receives the same share as any other,
   regardless of how much network it actually feeds. A weighting by network
   length or population density would be defensible and is not implemented.

   **Rectifiers are not assigned per traction system.** Every city/system row
   draws on all DC rectifiers within its radius, so in cities that run both an
   S-Bahn and a tram network -- Berlin and Hamburg -- the two systems share the
   same set. On the full Germany run, every bus carrying S-Bahn load also
   carried tram load. Energy per city and system is preserved; the placement
   within the city is smeared between the two.

   **The hourly shape is assumed constant over time.** Future scenarios differ
   from the status quo by a scalar factor only. Changes in service frequency or
   operating hours are not represented.

   **The rectifier classification itself is not persisted.** The load points
   are (see above), so a city that sits at its centroid says so. What is not
   stored is the full set of OSM substations the classifier looked at and
   rejected: a rectifier that was classified but had no city within 25 km
   leaves no row. Judging the classifier's recall therefore still means
   re-running the query against OSM.

.. note::
   **Several loads may share one bus, and that is not a problem.** A city's
   rectifiers often fall into the same MV grid district, so the number of load
   rows exceeds the number of buses -- on the full Germany run, 464 DC loads on
   138 buses. This is valid in PyPSA, where loads on a bus sum, and the model
   result is the same as for one aggregated load per bus and carrier. The
   annual energy is split across those rows, not duplicated.
