.. _mobility-demand-mit-ref:

Motorized individual travel and light commercial vehicles
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The electricity demand of motorized individual travel (MIT, vehicle class M1)
and light commercial vehicles (LGV, vehicle class N1) is set up for all
configured scenarios in the
:py:class:`MotorizedIndividualTravel<egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`
dataset, together with the charging locations of the new methodology. The
charging sites of the legacy methodology are set up in
:py:class:`MITChargingInfrastructure<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.MITChargingInfrastructure>`.
The workflow is visualised in figure :ref:`mit-model` and is analogous for each
scenario. Scenarios differ in the assumed number of vehicles and in whether
flexible (smart) charging is modelled: status quo scenarios are modelled with
dumb charging only, while projection scenarios additionally get a charging
link, a battery store and a lowflex counterpart
(cf. :py:func:`is_flexible<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.is_flexible>`).

.. figure:: /images/eGon_emob_MIT_model.png
  :name: mit-model
  :width: 800

  Workflow to set up charging demand data for MIT

Two methodologies live side by side, dispatched per scenario by
:py:func:`is_legacy_scenario<egon.data.datasets.emobility.mit_lgv_input_data.is_legacy_scenario>`.
The scenarios ``status2024``, ``reGon2037`` and ``reGon2045`` use the **new**
methodology described below. ``eGon2035`` is kept as a long-term stable
scenario on the **legacy** methodology and its original input data; it is not
part of the default scenario list and has to be requested explicitly with
``--scenarios eGon2035``. Both write the same tables, discriminated by
``scenario``.

.. note::

   Because :py:func:`create_tables<egon.data.datasets.emobility.motorized_individual_travel.create_tables>`
   drops and recreates all tables, ``eGon2035`` and the new scenarios have to
   be built in the **same** run. Running ``eGon2035`` afterwards destroys the
   rows of the other scenarios.

Input data (new methodology)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The vehicles, their driving and parking events and the charging sites are
generated together by the data providers, so events, vehicles and charging
points are mutually consistent. One bundle per scenario is published on Zenodo
as a single zip archive and downloaded by the pipeline itself
(:py:func:`download_and_extract<egon.data.datasets.emobility.motorized_individual_travel.mit_import.download_and_extract>`);
it is not part of the egon-data data bundle. The archives are extracted into
``emobility/input_data/<scenario>/`` in the run's working directory.

.. csv-table:: Delivered input data per scenario
    :header: "File", "Content", "Imported into the database"
    :widths: 30, 45, 25
    :name: mit-input-data

    "``ev_pool``", "the pool of pre-generated vehicle profiles", ":py:class:`EgonEvMitLgvPool<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvPool>`"
    "``ev_event``", "driving and parking events of all pool vehicles", ":py:class:`EgonEvMitLgvTrip<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvTrip>`, charging events only"
    "``ev_count_municipality``", "vehicles per type and municipality", ":py:class:`EgonEvMitLgvCountMunicipality<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountMunicipality>`"
    "``ev_mapping_ev_municipality``", "one row per vehicle, giving its profile and municipality", "counted per profile into ``n_vehicles`` of the pool"
    "``ev_charging_location``", "charging locations", ":py:class:`EgonEvMitLgvChargingLocation<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvChargingLocation>`"
    "``ev_mapping_event_location``", "one row per placement: a vehicle's charging event at a charging location", ":py:class:`EgonEvMitLgvLocationEvents<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvLocationEvents>`, folded per location"
    "``metadata_simbev_run.json``, ``metadata_geolis_run.json``", "run configurations and technical data per vehicle type", ":py:class:`EgonEvMitLgvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`"

``ev_mapping_event_location`` holds the **placements**: one row per vehicle and
charging event, naming the charging location the vehicle charges at — about
5·10\ :sup:`8` rows for ``status2024`` and 5·10\ :sup:`9` for each reGon
scenario. One row each would take several hundred GB in PostgreSQL, so the
import folds them into one array of event ids per charging location
(:py:func:`import_location_events<egon.data.datasets.emobility.motorized_individual_travel.mit_import.import_location_events>`).
The placements are the basis of the eTraGo model, cf.
:ref:`mit-charging-time-series-ref`.

Before the import, every extracted delivery is checked for completeness and
for every column the importers read, and the delivery's identity (the GeoLIS
consolidation time and result counts) is recorded, so a left-over extraction
of an earlier delivery is not reused by mistake.

The driving and parking profiles are generated with
`SimBEV <https://github.com/rl-institut/simbev>`_ from MID survey data
[MiD2017]_ per RegioStaR7 region type [RegioStaR7_2020]_. They contain the
energy consumption of each drive as well as, for each parking event, the
availability of a charging point and — where one is available — the charging
demand, the charging power and the charging use case. Charging probabilities
for the different types of charging infrastructure are presumed based on
[NOW2020]_ and [Helfenbein2021]_. The charging sites are then placed with
GeoLIS, using the same run, which is what makes vehicles, events and charging
points mutually consistent. The complete technical data and assumptions of both
runs are stored in
:py:class:`EgonEvMitLgvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`.

Because the pool is national while the fleet is not, each profile is
instantiated many times across Germany: ``n_vehicles`` of the pool counts the
vehicles of each profile, and each of its charging events is placed once per
vehicle.

Vehicle types
~~~~~~~~~~~~~

Nine vehicle types are distinguished. Six of them are privately registered
passenger cars, two are commercially registered passenger cars (still M1) and
one is a light commercial vehicle below 3.5 t (N1). N1 is battery-electric
only; there is no plug-in hybrid light commercial vehicle. The technical data
of each type — battery capacity, energy consumption and the charging power
distribution — is delivered with the run configuration in
``metadata_simbev_run.json`` and stored verbatim in
:py:class:`EgonEvMitLgvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`.
The import asserts that every type occurring in the pool has an entry there, so
a missing one fails immediately rather than deep inside the timeseries
generation.

.. csv-table:: Vehicle types of the new methodology
    :header: "Type", "Class", "Vehicle group"
    :widths: 40, 20, 40
    :name: mit-vehicle-types

    "``bev_mini``, ``bev_medium``, ``bev_luxury``", "M1", "``private``"
    "``phev_mini``, ``phev_medium``, ``phev_luxury``", "M1", "``private``"
    "``bev_commercial``, ``phev_commercial``", "M1", "``pkw_commercial``"
    "``bev_light_duty_vehicle``", "N1", "``light_duty_vehicle``"

Charging use cases and locations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every event carries a ``location`` and, if it charges, a ``use_case``. The
eight charging use cases form one vocabulary for all vehicle groups; four of
them are treated as eligible for flexible charging, the other four are always
charged dumb.

.. csv-table:: Charging use cases
    :header: "Use case", "Flexible"
    :widths: 60, 40
    :name: mit-use-cases

    "``depot``", "yes"
    "``home_detached``", "yes"
    "``home_apartment``", "yes"
    "``work``", "yes"
    "``street``", "no"
    "``retail``", "no"
    "``urban_fast``", "no"
    "``highway_fast``", "no"

``use_case`` is NULL for driving events and for parking events that carry a
location but no charging power. Every event with a charging demand greater than
zero has a use case.

``location`` uses **two parallel vocabularies**, selected by the vehicle's
group: English values (``home``, ``private``, ``leisure``, ``work``,
``shopping``, ``business``, ``school``, ``hpc``, ``driving``) for private
vehicles and German ones (``nach_hause``, ``arbeitsplatz``, ``einkauf``,
``freizeit``, ``sonstige_privat``, ``sonstige_dienstlich``, ``personen``,
``dienstleistung``, ``gueter``, ``rueckfahrt_betrieb``, ``hpc``, ``driving``)
for commercial passenger cars and light commercial vehicles. Nothing in the
model keys on ``location`` — the flexibility mask uses ``use_case`` — but
downstream consumers have to handle both. Note that ``work`` as a *use case*
occurs only for private vehicles: commercial workplace charging is classified
as ``depot`` at ``arbeitsplatz``.

Charging locations and the grid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The new methodology does not allocate vehicles to grid districts: vehicles
charge at locations spread over several districts, so the model is built
from the charging locations instead
(:py:mod:`charging_locations<egon.data.datasets.emobility.motorized_individual_travel.charging_locations>`).

* Locations at exactly identical coordinates form one **grid connection**.
  Its summed installed capacity (charging points × average charging capacity)
  gives its **voltage level**, with the thresholds used for HGV charging and
  public bus depots (> 120 MW: 1, > 20 MW: 3, > 5.5 MW: 4, > 0.2 MW: 5,
  > 0.1 MW: 6, else 7).
* Every grid connection gets the MV grid district it lies in; a connection
  outside every district, e.g. on the coast, gets the nearest one, so that no
  charging energy is lost.
* Voltage levels 4–7 belong to the **district model** of their MV grid
  district, on the district's HV/MV substation bus. Levels 1–3 are
  **transmission charging locations** with a model of their own per grid
  connection, on the district's bus (level 3) or on the bus of the EHV
  substation whose Voronoi cell they lie in (levels 1–2).

For the legacy methodology the total number of vehicles per type is
allocated to MV grid districts from vehicle registration [KBA]_ and population
[Census]_ data
(:py:func:`allocate_evs_numbers<egon.data.datasets.emobility.motorized_individual_travel.ev_allocation.allocate_evs_numbers>`),
and each grid district is then assigned a random sample of profiles from the
pool based on its RegioStaR7 region [RegioStaR7_2020]_
(:py:func:`allocate_evs_to_grid_districts<egon.data.datasets.emobility.motorized_individual_travel.ev_allocation.allocate_evs_to_grid_districts>`).

The assumed total number of vehicles in Germany is 2.62 million in the
status2024 scenario (BEV and PHEV stock as of 01.01.2025 according to
[KBA2025]_), 34.1 million in the reGon2037 scenario and 40.6 million in the
reGon2045 scenario (both according to the network development plan [NEP2025]_,
Scenario C). The numbers per scenario are defined in
:py:func:`mobility<egon.data.datasets.scenario_parameters.parameters.mobility>`.
For the new methodology these figures are a **cross-check only**: the delivered
vehicles are authoritative, and the import logs the difference between the
two.

.. _mit-charging-time-series-ref:

Charging time series
~~~~~~~~~~~~~~~~~~~~

The unit of the new methodology's model is the **placement**: one vehicle's
charging event at one charging location
(:py:mod:`placement_model<egon.data.datasets.emobility.motorized_individual_travel.placement_model>`).
A placement arrives with its **deficit**, its battery-side charging demand,
and is back to full when it leaves; nothing of the vehicle is carried between
placements. A model therefore needs only the placements of its own charging
locations.

* The dumb charging load assumes that vehicles charge at full power as soon as
  they arrive until the deficit is recharged; it is written for status quo
  scenarios and for the lowflex counterparts.
* In flexible scenarios the deficit is taken from the store as driving load at
  arrival, and the store band lets flexible placements recharge it anywhere
  within their parking window, cf. :ref:`flexible-charging-ref`.

For the legacy methodology the time series are determined from the vehicles
assigned to each MV grid district and their event data
(:py:mod:`model_timeseries<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries>`).

.. _mit-reference-points-ref:

Reference points of the eTraGo load
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``land_transport_EV`` load of the eTraGo model does **not** always mean the
same quantity:

* in dumb charging scenarios and in the lowflex counterparts it is the
  **grid-side charging energy**,
* in flexible scenarios it is the **battery-side driving energy**, because the
  charging itself is then a decision of the optimisation and is carried by the
  ``BEV_charger`` link.

The two differ by the charging point efficiency ``eta_cp``, so over a year

.. math::

   \sum_t \mathrm{driving\_load}(t)
       = \eta_{cp} \cdot \sum_t \mathrm{charging\_load\_grid}(t)

is an **identity, not an inconsistency**. For the new methodology it holds
exactly — the driving load *is* the deficit of the placements — up to dumb
charging past the year end, which is cropped.

To make comparisons across scenarios and between the flexible and the lowflex
model well defined without knowing which model shape a scenario got, the
pipeline writes both sides for every scenario on the new methodology, per AC
bus, model type (district or transmission) and hour, to
:py:class:`EgonEvMitLgvFlexTimeseries<egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvFlexTimeseries>`:

* ``charging_load_grid`` — the grid-side load of the dumb charging schedule,
* ``charging_load_grid_flex`` — the share of it that occurs at a use case
  eligible for flexible charging,
* ``driving_load`` — the battery-side driving load.

``eta_cp`` is recorded per scenario in
:py:class:`EgonEvMitLgvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`
(``simbev_config -> config -> basic -> eta_cp``) and must be read from there,
never assumed: it has changed between deliveries.

Two further tables carry the same information at different resolutions:
:py:class:`EgonEvMitLgvChargingProfileUseCase<egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvChargingProfileUseCase>`
splits ``charging_load_grid`` by charging use case, and
:py:class:`EgonEvMitLgvEnergyBalance<egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvEnergyBalance>`
gives the annual energy balance per AC bus, model type, charging use case and
vehicle type, carrying both the battery-side and the grid-side charging energy
and the number of placements.

Post-processing an eTraGo result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the dumb charging reference available, the flexibility a scenario actually
used follows from the optimisation result. Let :math:`p(t)` be the dispatch of
the ``BEV_charger`` link of a model, i.e. its grid-side charging power.
Then

.. math::

   \Delta(t) = p(t) - \mathrm{charging\_load\_grid}(t)

is the load shift against the dumb reference — positive where the optimisation
charged earlier or more, negative where it deferred charging — and

.. math::

   E_{shift} = \tfrac{1}{2} \sum_t \left| \Delta(t) \right|

is the energy shifted over the year, counted once rather than twice. Both are
well defined for every scenario, including the dumb charging ones, where
:math:`p(t)` is the load itself and :math:`\Delta(t)` is zero by construction.

.. warning::

   ``charging_load_grid_flex`` and the ``flexible`` flag of the energy balance
   are a **potential, not a realised flexibility**. They are populated for dumb
   charging scenarios as well, which get no bus, link or store at all. Read as
   "share of charging that occurs at a use case eligible for flexible charging"
   they are meaningful and comparable across all scenarios of the new
   methodology; read as "flexibility the model used" they are wrong for dumb
   charging scenarios and incomplete for the others, where the realised usage
   is :math:`\Delta(t)`.

   Two further effects bound the potential in opposite directions and do not
   cancel. The hourly resample takes the minimum of the lower and the maximum
   of the upper state-of-charge bound over the four quarter-hours, so the
   hourly band is a superset of the true 15-minute band and the model is
   slightly *more* flexible than the events allow. Conversely, only charging
   events contribute flexibility — time spent plugged in without charging is
   discarded — which understates the potential.

Charging sites
~~~~~~~~~~~~~~

For grid analyses of the MV and LV level, the charging demand needs to be
further disaggregated within the MV grid districts, which is done in the
`eDisGo <https://github.com/openego/eDisGo>`_ tool.

For the new methodology the charging locations are part of the delivered input
data and every charging event is already placed at one of them. They are stored
in
:py:class:`EgonEvMitLgvChargingLocation<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvChargingLocation>`
with their number of charging points, average charging capacity, charging use
case and grid assignment, and their placements in
:py:class:`EgonEvMitLgvLocationEvents<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvLocationEvents>`,
so a consumer can take one charging park per location with its charging events
as they are. A charging point is occupied only while a vehicle charges, not
while it stays parked. The flag ``is_synthetic_location`` marks fallback
centroids generated where a municipality has no real candidate site for a use
case — consumers placing charging infrastructure need to be able to tell them
from real sites.

For the legacy methodology potential charging sites are determined, to which
eDisGo allocates the charging demand of the vehicles in each MV grid district
(see the
`eDisGo documentation <https://edisgo.readthedocs.io/en/dev/features_in_detail.html#allocation-of-charging-demand>`_).
They are determined in
:py:class:`MITChargingInfrastructure<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.MITChargingInfrastructure>`
and written to
:py:class:`EgonEmobChargingInfrastructure<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes.EgonEmobChargingInfrastructure>`.
The approach used is based on the method implemented in
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

Test mode
~~~~~~~~~

With ``--dataset-boundary`` set to a federal state, the selection starts from
the charging locations inside the boundary and cascades to their placements,
the charging events those reference and the profiles of those events. The
result is exactly what a Germany-wide run gives for the grid districts inside
the boundary, including the charging of vehicles from outside.


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
:py:class:`GasAreas<egon.data.datasets.gas_areas.GasAreas>` it is
located in.

.. note::
   As of this restoration, only the eGon2035 scenario is active for
   heavy-duty transport (see ``datasets.yml``'s ``mobility_hgv.original_data.
   constants.scenarios`` and ``emobility/heavy_duty_transport/__init__.py``)
   -- eGon100RE is not currently registered as an ``EgonScenario`` row in
   this fork and would fail with a foreign-key error if enabled without
   first registering it.

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

.. _mobility-demand-public-bus-ref:

Public buses
++++++++++++

Electricity demand of public buses (vehicle class M3) is set up for the
``status2024``, ``reGon2037`` and ``reGon2045`` scenarios by
:py:class:`PublicBusCharging<egon.data.datasets.emobility.public_bus_charging.PublicBusCharging>`.
The ``eGon2035`` and ``eGon100RE`` scenarios deliberately carry no public bus
demand.

Unlike motorized individual travel and heavy-duty transport, the charging
demand is not generated within egon-data. Depot locations and a static hourly
charging series per depot and scenario are produced externally and ship in the
data bundle under ``data_bundle_egon_data/bus_charging/``. No flexibility is
modelled: the series are taken as given, so no charging link or storage
component is written and no lowflex scenario variant is created.

Each depot is written per scenario to
:py:class:`demand.egon_ev_bus_charging_depot<egon.data.datasets.emobility.public_bus_charging.db_classes.EgonEvBusChargingDepot>`
with its geometry, grid connection voltage level and full 8760-step time
series. This per-location detail is required by eDisGo, which cannot
reconstruct a predefined series from an aggregate.

The voltage level is derived per depot **and per scenario** from that
scenario's own peak load, using the same thresholds as
:py:func:`identify_voltage_level<egon.data.datasets.industry.temporal.identify_voltage_level>`.
Because bus electrification grows steeply between the scenarios, the majority
of depots cross a threshold: only 493 of the 1053 depots present in all three
scenarios keep the same voltage level, and 560 are assigned a higher one.
Consistent with the treatment of other growing loads in egon-data, connection
upgrades between scenarios are not modelled as events.

The grid connection point is found by a spatial join: depots at voltage levels
3 to 7 against ``grid.egon_mv_grid_district``, and those at levels 1 or 2
against ``grid.egon_ehv_substation_voronoi``. Since an MV grid district is the
Voronoi cell of a single HV/MV substation, all depots within one district share
that district's eTraGo bus. Depots that fall into no district are dropped
rather than matched to the nearest one.

For eTraGo, the depot series are summed per bus and written to
``grid.egon_etrago_load`` and ``grid.egon_etrago_load_timeseries`` under the
carrier ``land_transport_bus``.

.. note::
   The ``reGon2037`` and ``reGon2045`` input series each contain 1821 missing
   values across 34 depots, all falling in the first three hours of a Sunday.
   These are currently replaced with zero so that the pipeline can run, which
   understates the affected depots' demand. The substitution is logged with the
   affected depot ids on every run and should be removed once the input data is
   corrected.
