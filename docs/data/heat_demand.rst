.. _heat_demand:
Heat demands comprise space heating and drinking hot water demands from
residential and commercial trade and service (CTS) buildings.
Process heat demands from the industry are, depending on the required temperature
level, modelled as electricity, hydrogen or methane demand.

The spatial distribution of annual heat demands is taken from the Pan-European
Thermal Atlas version 5.0.1 [Peta]_.
This source provides data on annual European residential and CTS heat demands
per census cell for the year 2015.
In order to model future demands, the demand distribution extracted by Peta is
then scaled to meet a national annual demand from external sources.
The following national demands are taken for the selected scenarios:

.. list-table:: Heat demands per sector and scenario
   :widths: 25 25 25 25
   :header-rows: 1

   * -
     - Residential sector
     - CTS sector
     - Sources

   * - eGon2035
     - 379 TWh
     - 122 TWh
     - [Energiereferenzprognose]_

   * - eGon100RE
     - 284 TWh
     - 89 TWh
     - [Energiereferenzprognose]_


The resulting annual heat demand data per census cell is stored in the database table
:py:class:`demand.egon_peta_heat <egon.data.datasets.heat_demand.EgonPetaHeat>`.
The implementation of these data processing steps can be found in :py:class:`HeatDemandImport <egon.data.datasets.heat_demand.HeatDemandImport>`.

Figure :ref:`residential-heat-demand-annual` shows the census cell distribution of residential heat demands for scenario ``eGon2035``,
categorized for different levels of annual demands.

.. figure:: /images/residential_heat_demand.png
  :name: residential-heat-demand-annual
  :width: 400

  Spatial distribution of residential heat demand per census cell in scenario ``eGon2035``

In a next step, the annual demand per census cell is further disaggregated to buildings.
In case of residential buildings the demand is equally distributed to all residential
buildings within the census cell. The annual demand per residential building is not
saved in any database table but needs to be calculated from the annual demand in the census
cell in table :py:class:`demand.egon_peta_heat <egon.data.datasets.heat_demand.EgonPetaHeat>`
and the number of residential buildings in table
:py:class:`demand.egon_heat_timeseries_selected_profiles <egon.data.datasets.heat_demand_timeseries.idp_pool.EgonHeatTimeseries>`
(see also query below).
The disaggregation of the annual CTS heat demand per census cell to buildings is
done analogous to the disaggregation of the electricity demand, which is in detail
described in section :ref:`disagg-cts-elec-ref`.
The share of each CTS building of the corresponding HV-MV substation's heat profile is
for both the eGon2035 and eGon100RE scenario written to the database table
:py:class:`EgonCtsHeatDemandBuildingShare<egon.data.datasets.electricity_demand_timeseries.cts_buildings.EgonCtsHeatDemandBuildingShare>`.
The peak heat demand per building, including residential and CTS demand, in the two
scenarios eGon2035 and eGon100RE is calculated in the datasets
:py:class:`HeatPumps2035 <egon.data.datasets.heat_supply.HeatPumps2035>` and
:py:class:`HeatPumpsPypsaEurSec <egon.data.datasets.heat_supply.HeatPumpsPypsaEurSec>`,
respectively, and written to table
:py:class:`demand.egon_building_heat_peak_loads <egon.data.datasets.heat_supply.BuildingHeatPeakLoads>`.

The hourly heat demand profiles are for both sectors created in the Dataset
:py:class:`HeatTimeSeries <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>`.
For residential heat demand profiles a pool of synthetically created bottom-up demand
profiles is used. Depending on the mean temperature per day, these profiles are
randomly assigned to each residential building. The methodology is described in
detail in [Buettner2022]_.
Data on residential heat demand profiles is stored in the database within the tables
:py:class:`demand.egon_heat_timeseries_selected_profiles <egon.data.datasets.heat_demand_timeseries.idp_pool.EgonHeatTimeseries>`,
:py:class:`demand.egon_daily_heat_demand_per_climate_zone <egon.data.datasets.heat_demand_timeseries.daily.EgonDailyHeatDemandPerClimateZone>`,
:py:class:`boundaries.egon_map_zensus_climate_zones <egon.data.datasets.heat_demand_timeseries.daily.EgonMapZensusClimateZones>`.
To create the profiles for a selected building, these tables
have to be combined, e.g. like this:

.. code-block:: none

   SELECT (b.demand/f.count * UNNEST(e.idp) * d.daily_demand_share)*1000 AS demand_profile
   FROM	(SELECT * FROM demand.egon_heat_timeseries_selected_profiles,
   UNNEST(selected_idp_profiles) WITH ORDINALITY as selected_idp) a
   JOIN demand.egon_peta_heat b
   ON b.zensus_population_id = a.zensus_population_id
   JOIN boundaries.egon_map_zensus_climate_zones c
   ON c.zensus_population_id = a.zensus_population_id
   JOIN demand.egon_daily_heat_demand_per_climate_zone d
   ON (c.climate_zone = d.climate_zone AND d.day_of_year = ordinality)
   JOIN demand.egon_heat_idp_pool e
   ON selected_idp = e.index
   JOIN (SELECT zensus_population_id, COUNT(building_id)
   FROM demand.egon_heat_timeseries_selected_profiles
   GROUP BY zensus_population_id
   ) f
   ON f.zensus_population_id = a.zensus_population_id
   WHERE a.building_id = SELECTED_BUILDING_ID
   AND b.scenario = 'eGon2035'
   AND b.sector = 'residential';


Exemplary resulting residential heat demand time series for a selected day in winter and
summer considering different aggregation levels are visualized in figures :ref:`residential-heat-demand-timeseries-winter` and :ref:`residential-heat-demand-timeseries-summer`.

.. figure:: /images/residential_heat_demand_profile_winter.png
  :name: residential-heat-demand-timeseries-winter
  :width: 400

  Temporal distribution of residential heat demand for a selected day in winter

.. figure:: /images/residential_heat_demand_profile_summer.png
  :name: residential-heat-demand-timeseries-summer
  :width: 400

  Temporal distribution of residential heat demand for a selected day in summer

The temporal disaggregation of CTS heat demand is done using Standard Load Profiles Gas
from ``demandregio`` [demandregio]_ considering different profiles per CTS branch.
