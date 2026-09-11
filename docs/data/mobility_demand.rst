.. _mobility-demand-mit-ref:

Motorized individual travel and light commercial vehicles
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The electricity demand of motorized individual travel (MIT, vehicle class M1)
and light commercial vehicles (LGV, vehicle class N1) is set up for all
configured scenarios in the
:py:class:`MotorizedIndividualTravel<egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`
dataset, the corresponding charging sites in
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
    "``ev_event``", "driving and parking events of all pool vehicles", ":py:class:`EgonEvMitLgvTrip<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvTrip>`"
    "``ev_count_municipality``", "vehicles per type and municipality", ":py:class:`EgonEvMitLgvCountMunicipality<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountMunicipality>`"
    "``ev_mapping_ev_municipality``", "one row per vehicle, giving its municipality", ":py:class:`EgonEvMitLgvMappingEvMunicipality<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMappingEvMunicipality>`"
    "``ev_charging_location``", "charging sites", ":py:class:`EgonEvMitLgvChargingLocation<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes.EgonEvMitLgvChargingLocation>`"
    "``ev_mapping_event_location``", "charging events mapped to charging sites", "**no**, see below"
    "``metadata_simbev_run.json``, ``metadata_geolis_run.json``", "run configurations and technical data per vehicle type", ":py:class:`EgonEvMitLgvMetadata<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`"

``ev_mapping_event_location`` is the one file that is **not** imported. It
carries one row per (charging event × drawn vehicle), which is about
5·10\ :sup:`8` rows for ``status2024`` and an estimated 8·10\ :sup:`9` for
``reGon2045`` — several hundred GB in PostgreSQL, against a budget of about
350 GB for the entire pipeline. Nothing in the database needs it, because the
charging locations carry their own ``use_case``. Consumers that do need it read
``ev_mapping_event_location.parquet`` directly from the extracted scenario
directory.

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
instantiated many times across Germany; the delivered
``ev_mapping_ev_municipality`` records every instance separately.

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

Spatial allocation
~~~~~~~~~~~~~~~~~~

The allocation of vehicles to municipalities is delivered as input data, with
one row per vehicle. egon-data only distributes those vehicles over the MV grid
districts that intersect the municipality
(:py:func:`allocate_ev_instances_to_grid_districts<egon.data.datasets.emobility.motorized_individual_travel.mit_import.allocate_ev_instances_to_grid_districts>`).
Per municipality and vehicle type the count is split by population share using
largest-remainder rounding and the individual vehicles are then handed out in
``id`` order. No random number generator is involved, municipal totals are
preserved exactly, and for the roughly 92 % of municipalities that lie inside a
single grid district the split degenerates to a plain copy. The result is
written to
:py:class:`EgonEvMitLgvMvGridDistrict<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMvGridDistrict>`,
which records the municipality each vehicle came from, and aggregated into
:py:class:`EgonEvMitLgvCountMvGridDistrict<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountMvGridDistrict>`.

Vehicles in municipalities that the VG250 dataset [Census]_ does not know
cannot be placed — the split joins on the municipality key — and are dropped.
The number of affected municipalities, the number of vehicles lost per type and
their share of the delivered fleet are written to the task log so the loss is
quantified rather than silent.

For the legacy methodology the total number of vehicles per type is instead
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
``ev_count_municipality`` is authoritative, and the import logs the difference
between the two.

Charging time series
~~~~~~~~~~~~~~~~~~~~

On the basis of the vehicles assigned to each MV grid district and their event
data, charging demand time series are determined. For inflexible charging (see
lower right in figure :ref:`mit-model`) it is assumed that vehicles charge at
full power as soon as they arrive at a charging station until they are fully
charged. The respective charging power and demand is obtained from the event
data. The individual charging demand time series are summed up to obtain the
charging time series per MV grid district. The generation of time series to
model flexible charging is described in section :ref:`flexible-charging-ref`.

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

is an **identity, not an inconsistency**. It holds up to two terms that never
cancel exactly:

1. the driving load is only filled where the state of charge actually
   decreases, so plug-in hybrid kilometres driven on fuel do not enter it. The
   gap grows with the share of commercial plug-in hybrids;
2. the annual battery state-of-charge drift of the fleet. The store is not
   cyclic and the delivered events carry the same drift, which is in the order
   of 0.1 % of the annual charging energy.

To make comparisons across scenarios and between the flexible and the lowflex
model well defined without knowing which model shape a scenario got, the
pipeline writes both sides for every scenario on the new methodology, per MV
grid district and hour, to
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
gives the annual energy balance per grid district, charging use case and
vehicle type, carrying both the battery-side and the grid-side charging energy.

Post-processing an eTraGo result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the dumb charging reference available, the flexibility a scenario actually
used follows from the optimisation result. Let :math:`p(t)` be the dispatch of
the ``BEV_charger`` link of a grid district, i.e. its grid-side charging power.
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
further disaggregated within the MV grid districts. To this end, potential
charging sites are determined. These potential charging sites are then used to
allocate the charging demand of the vehicles in each MV grid district to
specific charging points. This allocation is not done in eGon-data but in the
`eDisGo <https://github.com/openego/eDisGo>`_ tool and further described in the
`eDisGo documentation <https://edisgo.readthedocs.io/en/dev/features_in_detail.html#allocation-of-charging-demand>`_.

For the new methodology the charging sites are part of the delivered input data
and are imported into
:py:class:`EgonEvMitLgvChargingLocation<egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes.EgonEvMitLgvChargingLocation>`
as delivered, with their number of charging points, their average charging
capacity and their charging use case. Each site additionally gets the MV grid
district it lies in from a point-in-polygon join; sites outside every grid
district keep a NULL grid district and are logged rather than dropped. The flag
``is_synthetic_location`` marks fallback centroids generated where a
municipality has no real candidate site for a use case — consumers placing
high-power charging infrastructure need to be able to tell them from real
sites.

For the legacy methodology the potential charging sites are determined in
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

With ``--dataset-boundary`` set to a federal state, the delivered municipality
mapping is filtered to the municipalities inside the boundary and the selection
cascades to the vehicle pool and the events; the charging locations are
filtered spatially. Expect the event table to shrink far less than the region
share suggests: the vehicle profile pool is national and each profile is
instantiated many times across Germany, so more than half of all profiles are
used somewhere even in a single federal state. This is not a defect and cannot
be reduced without breaking referential integrity.


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
