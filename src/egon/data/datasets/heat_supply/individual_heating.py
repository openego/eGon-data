"""The central module containing all code dealing with individual heat supply.

The following main things are done in this module:

* ..
* Desaggregation of heat pump capacities to individual buildings
* Determination of minimum required heat pump capacity for pypsa-eur-sec

The determination of the minimum required heat pump capacity for pypsa-eur-sec takes
place in the dataset 'HeatPumpsPypsaEurSec'. The goal is to ensure that the heat pump
capacities determined in pypsa-eur-sec are large enough to serve the heat demand of
individual buildings after the desaggregation from a few nodes in pypsa-eur-sec to the
individual buildings.
To determine minimum required heat pump capacity per building the buildings heat peak
load in the eGon100RE scenario is used (as pypsa-eur-sec serves as the scenario
generator for the eGon100RE scenario; see
:func:`determine_minimum_hp_capacity_per_building` for information on how minimum
required heat pump capacity is determined). As the heat peak load is not previously
determined, it is as well done in the course of this task.
Further, as determining heat peak load requires heat load
profiles of the buildings to be set up, this task is also utilised to set up
heat load profiles of all buildings with heat pumps within a grid in the eGon100RE
scenario used in eTraGo.
The resulting data is stored in separate tables respectively a csv file:

* `input-pypsa-eur-sec/minimum_hp_capacity_mv_grid_100RE.csv`:
    This csv file contains minimum required heat pump capacity per MV grid in MW as
    input for pypsa-eur-sec. It is created within :func:`export_min_cap_to_csv`.
* `demand.egon_etrago_timeseries_individual_heating`:
    This table contains aggregated heat load profiles of all buildings with heat pumps
    within an MV grid in the eGon100RE scenario used in eTraGo. It is created within
    :func:`individual_heating_per_mv_grid_tables`.
* `demand.egon_building_heat_peak_loads`:
    Mapping of peak heat demand and buildings including cell_id,
    building, area and peak load. This table is created in
    :func:`delete_heat_peak_loads_100RE`.

The desaggregation of heat pump capcacities to individual buildings takes place in two
separate datasets: 'HeatPumps2035' for eGon2035 scenario and 'HeatPumps2050' for
eGon100RE.
It is done separately because for one reason in case of the eGon100RE scenario the
minimum required heat pump capacity per building can directly be determined using the
heat peak load per building determined in the dataset 'HeatPumpsPypsaEurSec', whereas
heat peak load data does not yet exist for the eGon2035 scenario. Another reason is,
that in case of the eGon100RE scenario all buildings with individual heating have a
heat pump whereas in the eGon2035 scenario buildings are randomly selected until the
installed heat pump capacity per MV grid is met. All other buildings with individual
heating but no heat pump are assigned a gas boiler.

In the 'HeatPumps2035' dataset the following things are done.
First, the building's heat peak load in the eGon2035 scenario is determined for sizing
the heat pumps. To this end, heat load profiles per building are set up.
Using the heat peak load per building the minimum required heat pump capacity per
building is determined (see :func:`determine_minimum_hp_capacity_per_building`).
Afterwards, the total heat pump capacity per MV grid is desaggregated to individual
buildings in the MV grid, wherefore buildings are randomly chosen until the MV grid's total
heat pump capacity is reached (see :func:`determine_buildings_with_hp_in_mv_grid`).
Buildings with PV rooftop plants are more likely to be assigned a heat pump. In case
the minimum heat pump capacity of all chosen buildings is smaller than the total
heat pump capacity of the MV grid but adding another building would exceed the total
heat pump capacity of the MV grid, the remaining capacity is distributed to all
buildings with heat pumps proportionally to the size of their respective minimum
heat pump capacity. Therefore, the heat pump capacity of a building can be larger
than the minimum required heat pump capacity.
The generated heat load profiles per building are in a last step utilised to set up
heat load profiles of all buildings with heat pumps within a grid as well as for all
buildings with a gas boiler (i.e. all buildings with decentral heating system minus
buildings with heat pump) needed in eTraGo.
The resulting data is stored in the following tables:

* `demand.egon_hp_capacity_buildings`:
    This table contains the heat pump capacity of all buildings with a heat pump.
    It is created within :func:`delete_hp_capacity_2035`.
* `demand.egon_etrago_timeseries_individual_heating`:
    This table contains aggregated heat load profiles of all buildings with heat pumps
    within an MV grid as well as of all buildings with gas boilers within an MV grid in
    the eGon100RE scenario used in eTraGo. It is created within
    :func:`individual_heating_per_mv_grid_tables`.
* `demand.egon_building_heat_peak_loads`:
    Mapping of heat demand time series and buildings including cell_id,
    building, area and peak load. This table is created in
    :func:`delete_heat_peak_loads_2035`.

In the 'HeatPumps2050' dataset the total heat pump capacity in each MV grid can be
directly desaggregated to individual buildings, as the building's heat peak load was
already determined in the 'HeatPumpsPypsaEurSec' dataset. Also in contrast to the
'HeatPumps2035' dataset, all buildings with decentral heating system are assigned a
heat pump, wherefore no random sampling of buildings needs to be conducted.
The resulting data is stored in the following table:

* `demand.egon_hp_capacity_buildings`:
    This table contains the heat pump capacity of all buildings with a heat pump.
    It is created within :func:`delete_hp_capacity_2035`.

**The following datasets from the database are mainly used for creation:**

* `boundaries.egon_map_zensus_grid_districts`:


* `boundaries.egon_map_zensus_district_heating_areas`:


* `demand.egon_peta_heat`:
    Table of annual heat load demand for residential and cts at census cell
    level from peta5.
* `demand.egon_heat_timeseries_selected_profiles`:


* `demand.egon_heat_idp_pool`:


* `demand.egon_daily_heat_demand_per_climate_zone`:


* `boundaries.egon_map_zensus_mvgd_buildings`:
    A final mapping table including all buildings used for residential and
    cts, heat and electricity timeseries. Including census cells, mvgd bus_id,
    building type (osm or synthetic)

* `supply.egon_individual_heating`:


* `demand.egon_cts_heat_demand_building_share`:
    Table including the mv substation heat profile share of all selected
    cts buildings for scenario eGon2035 and eGon100RE. This table is created
    within :func:`cts_heat()`


**What is the goal?**

The goal is threefold. Primarily, heat pump capacity of individual buildings is
determined as it is necessary for distribution grid analysis. Secondly, as heat
demand profiles need to be set up during the process, the heat demand profiles of all
buildings with individual heat pumps respectively gas boilers per MV grid are set up
to be used in eTraGo. Thirdly, minimum heat pump capacity is determined as input for
pypsa-eur-sec to avoid that heat pump capacity per building is too little to meet
the heat demand after desaggregation to individual buildings.

**What is the challenge?**

The main challenge lies in the set up of heat demand profiles per building in
:func:`aggregate_residential_and_cts_profiles()` as it takes alot of time and
in grids with a high number of buildings requires alot of RAM. Both runtime and
RAM usage needed to be improved several times. To speed up the process, tasks are set
up to run in parallel. This currently leads to alot of connections being opened and
at a certain point to a runtime error due to too many open connections.

**What are central assumptions during the data processing?**

Central assumption for determining minimum heat pump capacity and desaggregating
heat pump capacity to individual buildings is that the required heat pump capacity
is determined using an approach from the
`network development plan <https://www.netzentwicklungsplan.de/sites/default/files/paragraphs-files/Szenariorahmenentwurf_NEP2035_2021_1.pdf>`_
(pp.46-47) (see :func:`determine_minimum_hp_capacity_per_building()`). There, the heat
pump capacity is determined by multiplying the heat peak
demand of the building by a minimum assumed COP of 1.7 and a flexibility factor of
24/18, taking into account that power supply of heat pumps can be interrupted for up
to six hours by the local distribution grid operator.
Another central assumption is, that buildings with PV rooftop plants are more likely
to have a heat pump than other buildings (see
:func:`determine_buildings_with_hp_in_mv_grid()` for details)

**Drawbacks and limitations of the data**

In the eGon2035 scenario buildings with heat pumps are selected randomly with a higher
probability for a heat pump for buildings with PV rooftop (see
:func:`determine_buildings_with_hp_in_mv_grid()` for details).
Another limitation may be the sizing of the heat pumps, as in the eGon2035 scenario
their size rigidly depends on the heat peak load and a fixed flexibility factor. During
the coldest days of the year, heat pump flexibility strongly depends on this
assumption and cannot be dynamically enlarged to provide more flexibility (or only
slightly through larger heat storage units).

Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""

from pathlib import Path
import os
import random

from airflow.operators.python import PythonOperator
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import ARRAY, REAL, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd
import saio

from egon.data import config, db, logger
from egon.data.datasets import Dataset
from egon.data.datasets.district_heating_areas import (
    MapZensusDistrictHeatingAreas,
)
from egon.data.datasets.electricity_demand_timeseries.cts_buildings import (
    calc_cts_building_profiles,
)
from egon.data.datasets.electricity_demand_timeseries.mapping import (
    EgonMapZensusMvgdBuildings,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    write_table_to_postgres,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    reduce_mem_usage,
)
from egon.data.datasets.heat_demand import EgonPetaHeat
from egon.data.datasets.heat_demand_timeseries.daily import (
    EgonDailyHeatDemandPerClimateZone,
    EgonMapZensusClimateZones,
)
from egon.data.datasets.heat_demand_timeseries.idp_pool import (
    EgonHeatTimeseries,
)

# get zensus cells with district heating
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts

engine = db.engine()
Base = declarative_base()

scenarios = config.settings()["egon-data"]["--scenarios"]


class EgonEtragoTimeseriesIndividualHeating(Base):
    __tablename__ = "egon_etrago_timeseries_individual_heating"
    __table_args__ = {"schema": "demand"}
    bus_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    carrier = Column(String, primary_key=True)
    dist_aggregated_mw = Column(ARRAY(REAL))


class EgonHpCapacityBuildings(Base):
    __tablename__ = "egon_hp_capacity_buildings"
    __table_args__ = {"schema": "demand"}
    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    hp_capacity = Column(REAL)


class HeatPumpsPypsaEurSec(Dataset):
    def __init__(self, dependencies):
        def dyn_parallel_tasks_pypsa_eur_sec():
            """Dynamically generate tasks
            The goal is to speed up tasks by parallelising bulks of mvgds.

            The number of parallel tasks is defined via parameter
            `parallel_tasks` in the dataset config `datasets.yml`.

            Returns
            -------
            set of airflow.PythonOperators
                The tasks. Each element is of
                :func:`egon.data.datasets.heat_supply.individual_heating.
                determine_hp_cap_peak_load_mvgd_ts_pypsa_eur_sec`
            """
            parallel_tasks = config.datasets()["demand_timeseries_mvgd"].get(
                "parallel_tasks", 1
            )

            tasks = set()
            for i in range(parallel_tasks):
                tasks.add(
                    PythonOperator(
                        task_id=(
                            f"individual_heating."
                            f"determine-hp-capacity-pypsa-eur-sec-"
                            f"mvgd-bulk{i}"
                        ),
                        python_callable=split_mvgds_into_bulks,
                        op_kwargs={
                            "n": i,
                            "max_n": parallel_tasks,
                            "func": determine_hp_cap_peak_load_mvgd_ts_pypsa_eur_sec,  # noqa: E501
                        },
                    )
                )
            return tasks

        tasks_HeatPumpsPypsaEur = set()

        if "eGon100RE" in scenarios:
            tasks_HeatPumpsPypsaEur = (
                delete_pypsa_eur_sec_csv_file,
                delete_mvgd_ts_100RE,
                delete_heat_peak_loads_100RE,
                {*dyn_parallel_tasks_pypsa_eur_sec()},
            )
        else:
            tasks_HeatPumpsPypsaEur = (
                PythonOperator(
                    task_id="HeatPumpsPypsaEur_skipped",
                    python_callable=skip_task,
                    op_kwargs={
                        "scn": "eGon100RE",
                        "task": "HeatPumpsPypsaEur",
                    },
                ),
            )

        super().__init__(
            name="HeatPumpsPypsaEurSec",
            version="0.0.3",
            dependencies=dependencies,
            tasks=tasks_HeatPumpsPypsaEur,
        )


class HeatPumps2019(Dataset):
    def __init__(self, dependencies):
        def dyn_parallel_tasks_2019():
            """Dynamically generate tasks

            The goal is to speed up tasks by parallelising bulks of mvgds.

            The number of parallel tasks is defined via parameter
            `parallel_tasks` in the dataset config `datasets.yml`.

            Returns
            -------
            set of airflow.PythonOperators
                The tasks. Each element is of
                :func:`egon.data.datasets.heat_supply.individual_heating.
                determine_hp_cap_peak_load_mvgd_ts_2019`
            """
            parallel_tasks = config.datasets()["demand_timeseries_mvgd"].get(
                "parallel_tasks", 1
            )
            tasks = set()
            for i in range(parallel_tasks):
                tasks.add(
                    PythonOperator(
                        task_id=(
                            "individual_heating."
                            f"determine-hp-capacity-2019-"
                            f"mvgd-bulk{i}"
                        ),
                        python_callable=split_mvgds_into_bulks,
                        op_kwargs={
                            "n": i,
                            "max_n": parallel_tasks,
                            "func": determine_hp_cap_peak_load_mvgd_ts_2019,
                        },
                    )
                )
            return tasks

        tasks_HeatPumps2019 = set()

        if "status2019" in scenarios:
            tasks_HeatPumps2019 = (
                delete_heat_peak_loads_2019,
                delete_hp_capacity_2019,
                delete_mvgd_ts_2019,
                {*dyn_parallel_tasks_2019()},
            )
        else:
            tasks_HeatPumps2019 = (
                PythonOperator(
                    task_id="HeatPumps2019_skipped",
                    python_callable=skip_task,
                    op_kwargs={"scn": "status2019", "task": "HeatPumps2019"},
                ),
            )

        super().__init__(
            name="HeatPumps2019",
            version="0.0.3",
            dependencies=dependencies,
            tasks=tasks_HeatPumps2019,
        )


class HeatPumps2035(Dataset):
    def __init__(self, dependencies):
        def dyn_parallel_tasks_2035():
            """Dynamically generate tasks

            The goal is to speed up tasks by parallelising bulks of mvgds.

            The number of parallel tasks is defined via parameter
            `parallel_tasks` in the dataset config `datasets.yml`.

            Returns
            -------
            set of airflow.PythonOperators
                The tasks. Each element is of
                :func:`egon.data.datasets.heat_supply.individual_heating.
                determine_hp_cap_peak_load_mvgd_ts_2035`
            """
            parallel_tasks = config.datasets()["demand_timeseries_mvgd"].get(
                "parallel_tasks", 1
            )
            tasks = set()
            for i in range(parallel_tasks):
                tasks.add(
                    PythonOperator(
                        task_id=(
                            "individual_heating."
                            f"determine-hp-capacity-2035-"
                            f"mvgd-bulk{i}"
                        ),
                        python_callable=split_mvgds_into_bulks,
                        op_kwargs={
                            "n": i,
                            "max_n": parallel_tasks,
                            "func": determine_hp_cap_peak_load_mvgd_ts_2035,
                        },
                    )
                )
            return tasks

        tasks_HeatPumps2035 = set()

        if "eGon2035" in scenarios:
            tasks_HeatPumps2035 = (
                delete_heat_peak_loads_2035,
                delete_hp_capacity_2035,
                delete_mvgd_ts_2035,
                {*dyn_parallel_tasks_2035()},
            )
        else:
            tasks_HeatPumps2035 = (
                PythonOperator(
                    task_id="HeatPumps2035_skipped",
                    python_callable=skip_task,
                    op_kwargs={"scn": "eGon2035", "task": "HeatPumps2035"},
                ),
            )

        super().__init__(
            name="HeatPumps2035",
            version="0.0.3",
            dependencies=dependencies,
            tasks=tasks_HeatPumps2035,
        )


class HeatPumps2050(Dataset):
    def __init__(self, dependencies):
        tasks_HeatPumps2050 = set()

        if "eGon100RE" in scenarios:
            tasks_HeatPumps2050 = (
                delete_hp_capacity_100RE,
                determine_hp_cap_buildings_eGon100RE,
            )
        else:
            tasks_HeatPumps2050 = (
                PythonOperator(
                    task_id="HeatPumps2050_skipped",
                    python_callable=skip_task,
                    op_kwargs={"scn": "eGon100RE", "task": "HeatPumps2050"},
                ),
            )

        super().__init__(
            name="HeatPumps2050",
            version="0.0.3",
            dependencies=dependencies,
            tasks=tasks_HeatPumps2050,
        )


class BuildingHeatPeakLoads(Base):
    __tablename__ = "egon_building_heat_peak_loads"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    sector = Column(String, primary_key=True)
    peak_load_in_w = Column(REAL)


def skip_task(scn=str, task=str):
    def not_executed():
        logger.info(
            f"{scn} is not in the list of scenarios. {task} dataset is skipped."
        )

    return not_executed


def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def cascade_per_technology(
    heat_per_mv,
    technologies,
    scenario,
    distribution_level,
    max_size_individual_chp=0.05,
):
    """Add plants for individual heat.
    Currently only on mv grid district level.

    Parameters
    ----------
    mv_grid_districts : geopandas.geodataframe.GeoDataFrame
        MV grid districts including the heat demand
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    scenario : str
        Name of the scenario
    max_size_individual_chp : float
        Maximum capacity of an individual chp in MW
    Returns
    -------
    mv_grid_districts : geopandas.geodataframe.GeoDataFrame
        MV grid district which need additional individual heat supply
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    append_df : pandas.DataFrame
        List of plants per mv grid for the selected technology

    """
    sources = config.datasets()["heat_supply"]["sources"]

    tech = technologies[technologies.priority == technologies.priority.max()]

    # Distribute heat pumps linear to remaining demand.
    if tech.index == "heat_pump":
        if distribution_level == "federal_state":
            # Select target values per federal state
            target = db.select_dataframe(
                f"""
                    SELECT DISTINCT ON (gen) gen as state, capacity
                    FROM {sources['scenario_capacities']['schema']}.
                    {sources['scenario_capacities']['table']} a
                    JOIN {sources['federal_states']['schema']}.
                    {sources['federal_states']['table']} b
                    ON a.nuts = b.nuts
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'residential_rural_heat_pump'
                    """,
                index_col="state",
            )

            heat_per_mv["share"] = heat_per_mv.groupby(
                "state"
            ).remaining_demand.apply(lambda grp: grp / grp.sum())

            append_df = (
                heat_per_mv["share"]
                .mul(target.capacity[heat_per_mv["state"]].values)
                .reset_index()
            )
        else:
            # Select target value for Germany
            target = db.select_dataframe(
                f"""
                    SELECT SUM(capacity) AS capacity
                    FROM {sources['scenario_capacities']['schema']}.
                    {sources['scenario_capacities']['table']} a
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'residential_rural_heat_pump'
                    """
            )

            heat_per_mv["share"] = (
                heat_per_mv.remaining_demand
                / heat_per_mv.remaining_demand.sum()
            )

            append_df = (
                heat_per_mv["share"].mul(target.capacity[0]).reset_index()
            )

        append_df.rename(
            {"bus_id": "mv_grid_id", "share": "capacity"}, axis=1, inplace=True
        )

    elif tech.index == "gas_boiler":
        append_df = pd.DataFrame(
            data={
                "capacity": heat_per_mv.remaining_demand.div(
                    tech.estimated_flh.values[0]
                ),
                "carrier": "residential_rural_gas_boiler",
                "mv_grid_id": heat_per_mv.index,
                "scenario": scenario,
            }
        )

    if append_df.size > 0:
        append_df["carrier"] = tech.index[0]
        heat_per_mv.loc[
            append_df.mv_grid_id, "remaining_demand"
        ] -= append_df.set_index("mv_grid_id").capacity.mul(
            tech.estimated_flh.values[0]
        )

    heat_per_mv = heat_per_mv[heat_per_mv.remaining_demand >= 0]

    technologies = technologies.drop(tech.index)

    return heat_per_mv, technologies, append_df


def cascade_heat_supply_indiv(scenario, distribution_level, plotting=True):
    """Assigns supply strategy for individual heating in four steps.

    1.) all small scale CHP are connected.
    2.) If this is not suitable, the mv grid is also supplied by heat pumps.
    3.) The last option are individual gas boilers.

    Parameters
    ----------
    scenario : str
        Name of scenario
    plotting : bool, optional
        Choose if individual heating supply is plotted. The default is True.

    Returns
    -------
    resulting_capacities : pandas.DataFrame
        List of plants per mv grid

    """

    sources = config.datasets()["heat_supply"]["sources"]

    # Select residential heat demand per mv grid district and federal state
    heat_per_mv = db.select_geodataframe(
        f"""
        SELECT d.bus_id as bus_id, SUM(demand) as demand,
        c.vg250_lan as state, d.geom
        FROM {sources['heat_demand']['schema']}.
        {sources['heat_demand']['table']} a
        JOIN {sources['map_zensus_grid']['schema']}.
        {sources['map_zensus_grid']['table']} b
        ON a.zensus_population_id = b.zensus_population_id
        JOIN {sources['map_vg250_grid']['schema']}.
        {sources['map_vg250_grid']['table']} c
        ON b.bus_id = c.bus_id
        JOIN {sources['mv_grids']['schema']}.
        {sources['mv_grids']['table']} d
        ON d.bus_id = c.bus_id
        WHERE scenario = '{scenario}'
        AND a.zensus_population_id NOT IN (
            SELECT zensus_population_id
            FROM {sources['map_dh']['schema']}.{sources['map_dh']['table']}
            WHERE scenario = '{scenario}')
        GROUP BY d.bus_id, vg250_lan, geom
        """,
        index_col="bus_id",
    )

    # Store geometry of mv grid
    geom_mv = heat_per_mv.geom.centroid.copy()

    # Initalize Dataframe for results
    resulting_capacities = pd.DataFrame(
        columns=["mv_grid_id", "carrier", "capacity"]
    )

    # Set technology data according to
    # http://www.wbzu.de/seminare/infopool/infopool-bhkw
    if scenario == "eGon2035":
        technologies = pd.DataFrame(
            index=["heat_pump", "gas_boiler"],
            columns=["estimated_flh", "priority"],
            data={"estimated_flh": [4000, 8000], "priority": [2, 1]},
        )
    elif scenario == "eGon100RE":
        technologies = pd.DataFrame(
            index=["heat_pump"],
            columns=["estimated_flh", "priority"],
            data={"estimated_flh": [4000], "priority": [1]},
        )
    elif scenario == "status2019":
        technologies = pd.DataFrame(
            index=["heat_pump"],
            columns=["estimated_flh", "priority"],
            data={"estimated_flh": [4000], "priority": [1]},
        )

    # In the beginning, the remaining demand equals demand
    heat_per_mv["remaining_demand"] = heat_per_mv["demand"]

    # Connect new technologies, if there is still heat demand left
    while (len(technologies) > 0) and (len(heat_per_mv) > 0):
        # Attach new supply technology
        heat_per_mv, technologies, append_df = cascade_per_technology(
            heat_per_mv, technologies, scenario, distribution_level
        )
        # Collect resulting capacities
        resulting_capacities = pd.concat(
            [resulting_capacities, append_df], ignore_index=True
        )

    if plotting:
        plot_heat_supply(resulting_capacities)

    return gpd.GeoDataFrame(
        resulting_capacities,
        geometry=geom_mv[resulting_capacities.mv_grid_id].values,
    )


def get_peta_demand(mvgd, scenario):
    """
    Retrieve annual peta heat demand for residential buildings for either
    eGon2035 or eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE

    Returns
    -------
    df_peta_demand : pd.DataFrame
        Annual residential heat demand per building and scenario. Columns of
        the dataframe are zensus_population_id and demand.

    """

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonPetaHeat.demand,
            )
            .filter(MapZensusGridDistricts.bus_id == mvgd)
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .filter(
                EgonPetaHeat.sector == "residential",
                EgonPetaHeat.scenario == scenario,
            )
        )

        df_peta_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

    return df_peta_demand


def get_residential_heat_profile_ids(mvgd):
    """
    Retrieve 365 daily heat profiles ids per residential building and selected
    mvgd.

    Parameters
    ----------
    mvgd : int
        ID of MVGD

    Returns
    -------
    df_profiles_ids : pd.DataFrame
        Residential daily heat profile ID's per building. Columns of the
        dataframe are zensus_population_id, building_id,
        selected_idp_profiles, buildings and day_of_year.

    """
    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonHeatTimeseries.building_id,
                EgonHeatTimeseries.selected_idp_profiles,
            )
            .filter(MapZensusGridDistricts.bus_id == mvgd)
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonHeatTimeseries.zensus_population_id
            )
        )

        df_profiles_ids = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
    # Add building count per cell
    df_profiles_ids = pd.merge(
        left=df_profiles_ids,
        right=df_profiles_ids.groupby("zensus_population_id")["building_id"]
        .count()
        .rename("buildings"),
        left_on="zensus_population_id",
        right_index=True,
    )

    # unnest array of ids per building
    df_profiles_ids = df_profiles_ids.explode("selected_idp_profiles")
    # add day of year column by order of list
    df_profiles_ids["day_of_year"] = (
        df_profiles_ids.groupby("building_id").cumcount() + 1
    )
    return df_profiles_ids


def get_daily_profiles(profile_ids):
    """
    Parameters
    ----------
    profile_ids : list(int)
        daily heat profile ID's

    Returns
    -------
    df_profiles : pd.DataFrame
        Residential daily heat profiles. Columns of the dataframe are idp,
        house, temperature_class and hour.

    """

    saio.register_schema("demand", db.engine())
    from saio.demand import egon_heat_idp_pool

    with db.session_scope() as session:
        query = session.query(egon_heat_idp_pool).filter(
            egon_heat_idp_pool.index.in_(profile_ids)
        )

        df_profiles = pd.read_sql(
            query.statement, query.session.bind, index_col="index"
        )

    # unnest array of profile values per id
    df_profiles = df_profiles.explode("idp")
    # Add column for hour of day
    df_profiles["hour"] = df_profiles.groupby(axis=0, level=0).cumcount() + 1

    return df_profiles


def get_daily_demand_share(mvgd):
    """per census cell
    Parameters
    ----------
    mvgd : int
        MVGD id

    Returns
    -------
    df_daily_demand_share : pd.DataFrame
        Daily annual demand share per cencus cell. Columns of the dataframe
        are zensus_population_id, day_of_year and daily_demand_share.

    """

    with db.session_scope() as session:
        query = session.query(
            MapZensusGridDistricts.zensus_population_id,
            EgonDailyHeatDemandPerClimateZone.day_of_year,
            EgonDailyHeatDemandPerClimateZone.daily_demand_share,
        ).filter(
            EgonMapZensusClimateZones.climate_zone
            == EgonDailyHeatDemandPerClimateZone.climate_zone,
            MapZensusGridDistricts.zensus_population_id
            == EgonMapZensusClimateZones.zensus_population_id,
            MapZensusGridDistricts.bus_id == mvgd,
        )

        df_daily_demand_share = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
    return df_daily_demand_share


def calc_residential_heat_profiles_per_mvgd(mvgd, scenario):
    """
    Gets residential heat profiles per building in MV grid for either eGon2035
    or eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE.

    Returns
    --------
    pd.DataFrame
        Heat demand profiles of buildings. Columns are:
            * zensus_population_id : int
                Zensus cell ID building is in.
            * building_id : int
                ID of building.
            * day_of_year : int
                Day of the year (1 - 365).
            * hour : int
                Hour of the day (1 - 24).
            * demand_ts : float
                Building's residential heat demand in MW, for specified hour
                of the year (specified through columns `day_of_year` and
                `hour`).
    """

    columns = [
        "zensus_population_id",
        "building_id",
        "day_of_year",
        "hour",
        "demand_ts",
    ]

    df_peta_demand = get_peta_demand(mvgd, scenario)
    df_peta_demand = reduce_mem_usage(df_peta_demand)

    # TODO maybe return empty dataframe
    if df_peta_demand.empty:
        logger.info(f"No demand for MVGD: {mvgd}")
        return pd.DataFrame(columns=columns)

    df_profiles_ids = get_residential_heat_profile_ids(mvgd)

    if df_profiles_ids.empty:
        logger.info(f"No profiles for MVGD: {mvgd}")
        return pd.DataFrame(columns=columns)

    df_profiles = get_daily_profiles(
        df_profiles_ids["selected_idp_profiles"].unique()
    )

    df_daily_demand_share = get_daily_demand_share(mvgd)

    # Merge profile ids to peta demand by zensus_population_id
    df_profile_merge = pd.merge(
        left=df_peta_demand, right=df_profiles_ids, on="zensus_population_id"
    )

    df_profile_merge.demand = df_profile_merge.demand.div(
        df_profile_merge.buildings
    )
    df_profile_merge.drop("buildings", axis="columns", inplace=True)

    # Merge daily demand to daily profile ids by zensus_population_id and day
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_daily_demand_share,
        on=["zensus_population_id", "day_of_year"],
    )
    df_profile_merge.demand = df_profile_merge.demand.mul(
        df_profile_merge.daily_demand_share
    )
    df_profile_merge.drop("daily_demand_share", axis="columns", inplace=True)
    df_profile_merge = reduce_mem_usage(df_profile_merge)

    # Merge daily profiles by profile id
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_profiles[["idp", "hour"]],
        left_on="selected_idp_profiles",
        right_index=True,
    )
    df_profile_merge = reduce_mem_usage(df_profile_merge)

    df_profile_merge.demand = df_profile_merge.demand.mul(
        df_profile_merge.idp.astype(float)
    )
    df_profile_merge.drop("idp", axis="columns", inplace=True)

    df_profile_merge.rename(
        {"demand": "demand_ts"}, axis="columns", inplace=True
    )

    df_profile_merge = reduce_mem_usage(df_profile_merge)

    return df_profile_merge.loc[:, columns]


def plot_heat_supply(resulting_capacities):
    from matplotlib import pyplot as plt

    mv_grids = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_mv_grid_district
        """,
        index_col="bus_id",
    )

    for c in ["CHP", "heat_pump"]:
        mv_grids[c] = (
            resulting_capacities[resulting_capacities.carrier == c]
            .set_index("mv_grid_id")
            .capacity
        )

        fig, ax = plt.subplots(1, 1)
        mv_grids.boundary.plot(linewidth=0.2, ax=ax, color="black")
        mv_grids.plot(
            ax=ax,
            column=c,
            cmap="magma_r",
            legend=True,
            legend_kwds={
                "label": f"Installed {c} in MW",
                "orientation": "vertical",
            },
        )
        plt.savefig(f"plots/individual_heat_supply_{c}.png", dpi=300)


def get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id
):
    """
    Returns zensus cell IDs with decentral heating systems in given MV grid.

    As cells with district heating differ between scenarios, this is also
    depending on the scenario.

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    pd.Index(int)
        Zensus cell IDs (as int) of buildings with decentral heating systems in
        given MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """

    # get zensus cells in grid
    zensus_population_ids = db.select_dataframe(
        f"""
        SELECT zensus_population_id
        FROM boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
        """,
        index_col=None,
    ).zensus_population_id.values

    # maybe use adapter
    # convert to pd.Index (otherwise type is np.int64, which will for some
    # reason throw an error when used in a query)
    zensus_population_ids = pd.Index(zensus_population_ids)

    # get zensus cells with district heating
    with db.session_scope() as session:
        query = session.query(
            MapZensusDistrictHeatingAreas.zensus_population_id,
        ).filter(
            MapZensusDistrictHeatingAreas.scenario == scenario,
            MapZensusDistrictHeatingAreas.zensus_population_id.in_(
                zensus_population_ids
            ),
        )

        cells_with_dh = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        ).zensus_population_id.values

    # remove zensus cells with district heating
    zensus_population_ids = zensus_population_ids.drop(
        cells_with_dh, errors="ignore"
    )
    return pd.Index(zensus_population_ids)


def get_residential_buildings_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id
):
    """
    Returns building IDs of buildings with decentral residential heat demand in
    given MV grid.

    As cells with district heating differ between scenarios, this is also
    depending on the scenario.

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in
        given MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """
    # get zensus cells with decentral heating
    zensus_population_ids = (
        get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
            scenario, mv_grid_id
        )
    )

    # get buildings with decentral heat demand
    saio.register_schema("demand", engine)
    from saio.demand import egon_heat_timeseries_selected_profiles

    with db.session_scope() as session:
        query = session.query(
            egon_heat_timeseries_selected_profiles.building_id,
        ).filter(
            egon_heat_timeseries_selected_profiles.zensus_population_id.in_(
                zensus_population_ids
            )
        )

        buildings_with_heat_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        ).building_id.values

    return pd.Index(buildings_with_heat_demand)


def get_cts_buildings_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id
):
    """
    Returns building IDs of buildings with decentral CTS heat demand in
    given MV grid.

    As cells with district heating differ between scenarios, this is also
    depending on the scenario.

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in
        given MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """

    # get zensus cells with decentral heating
    zensus_population_ids = (
        get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
            scenario, mv_grid_id
        )
    )

    # get buildings with decentral heat demand
    with db.session_scope() as session:
        query = session.query(EgonMapZensusMvgdBuildings.building_id).filter(
            EgonMapZensusMvgdBuildings.sector == "cts",
            EgonMapZensusMvgdBuildings.zensus_population_id.in_(
                zensus_population_ids
            ),
        )

        buildings_with_heat_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        ).building_id.values

    return pd.Index(buildings_with_heat_demand)


def get_buildings_with_decentral_heat_demand_in_mv_grid(mvgd, scenario):
    """
    Returns building IDs of buildings with decentral heat demand in
    given MV grid.

    As cells with district heating differ between scenarios, this is also
    depending on the scenario. CTS and residential have to be retrieved
    seperatly as some residential buildings only have electricity but no
    heat demand. This does not occure in CTS.

    Parameters
    -----------
    mvgd : int
        ID of MV grid.
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".

    Returns
    --------
    pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in
        given MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """
    # get residential buildings with decentral heating systems
    buildings_decentral_heating_res = (
        get_residential_buildings_with_decentral_heat_demand_in_mv_grid(
            scenario, mvgd
        )
    )

    # get CTS buildings with decentral heating systems
    buildings_decentral_heating_cts = (
        get_cts_buildings_with_decentral_heat_demand_in_mv_grid(scenario, mvgd)
    )

    # merge residential and CTS buildings
    buildings_decentral_heating = buildings_decentral_heating_res.union(
        buildings_decentral_heating_cts
    ).unique()

    return buildings_decentral_heating


def get_total_heat_pump_capacity_of_mv_grid(scenario, mv_grid_id):
    """
    Returns total heat pump capacity per grid that was previously defined
    (by NEP or pypsa-eur-sec).

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    float
        Total heat pump capacity in MW in given MV grid.

    """
    from egon.data.datasets.heat_supply import EgonIndividualHeatingSupply

    with db.session_scope() as session:
        query = (
            session.query(
                EgonIndividualHeatingSupply.mv_grid_id,
                EgonIndividualHeatingSupply.capacity,
            )
            .filter(EgonIndividualHeatingSupply.scenario == scenario)
            .filter(EgonIndividualHeatingSupply.carrier == "heat_pump")
            .filter(EgonIndividualHeatingSupply.mv_grid_id == mv_grid_id)
        )

        hp_cap_mv_grid = pd.read_sql(
            query.statement, query.session.bind, index_col="mv_grid_id"
        )
    if hp_cap_mv_grid.empty:
        return 0.0
    else:
        return hp_cap_mv_grid.capacity.values[0]


def get_heat_peak_demand_per_building(scenario, building_ids):
    """"""

    with db.session_scope() as session:
        query = (
            session.query(
                BuildingHeatPeakLoads.building_id,
                BuildingHeatPeakLoads.peak_load_in_w,
            )
            .filter(BuildingHeatPeakLoads.scenario == scenario)
            .filter(BuildingHeatPeakLoads.building_id.in_(building_ids))
        )

        df_heat_peak_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

    # TODO remove check
    if df_heat_peak_demand.duplicated("building_id").any():
        raise ValueError("Duplicate building_id")

    # convert to series and from W to MW
    df_heat_peak_demand = (
        df_heat_peak_demand.set_index("building_id").loc[:, "peak_load_in_w"]
        * 1e6
    )
    return df_heat_peak_demand


def determine_minimum_hp_capacity_per_building(
    peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
):
    """
    Determines minimum required heat pump capacity.

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    flexibility_factor : float
        Factor to overdimension the heat pump to allow for some flexible
        dispatch in times of high heat demand. Per default, a factor of 24/18
        is used, to take into account

    Returns
    -------
    pd.Series
        Pandas series with minimum required heat pump capacity per building in
        MW.

    """
    return peak_heat_demand * flexibility_factor / cop


def determine_buildings_with_hp_in_mv_grid(
    hp_cap_mv_grid, min_hp_cap_per_building
):
    """
    Distributes given total heat pump capacity to buildings based on their peak
    heat demand.

    Parameters
    -----------
    hp_cap_mv_grid : float
        Total heat pump capacity in MW in given MV grid.
    min_hp_cap_per_building : pd.Series
        Pandas series with minimum required heat pump capacity per building
         in MW.

    Returns
    -------
    pd.Index(int)
        Building IDs (as int) of buildings to get heat demand time series for.

    """
    building_ids = min_hp_cap_per_building.index

    # get buildings with PV to give them a higher priority when selecting
    # buildings a heat pump will be allocated to
    saio.register_schema("supply", engine)
    from saio.supply import egon_power_plants_pv_roof_building

    with db.session_scope() as session:
        query = session.query(
            egon_power_plants_pv_roof_building.building_id
        ).filter(
            egon_power_plants_pv_roof_building.building_id.in_(building_ids),
            egon_power_plants_pv_roof_building.scenario == "eGon2035",
        )

        buildings_with_pv = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        ).building_id.values
    # set different weights for buildings with PV and without PV
    weight_with_pv = 1.5
    weight_without_pv = 1.0
    weights = pd.concat(
        [
            pd.DataFrame(
                {"weight": weight_without_pv},
                index=building_ids.drop(buildings_with_pv, errors="ignore"),
            ),
            pd.DataFrame({"weight": weight_with_pv}, index=buildings_with_pv),
        ]
    )
    # normalise weights (probability needs to add up to 1)
    weights.weight = weights.weight / weights.weight.sum()

    # get random order at which buildings are chosen
    np.random.seed(db.credentials()["--random-seed"])
    buildings_with_hp_order = np.random.choice(
        weights.index,
        size=len(weights),
        replace=False,
        p=weights.weight.values,
    )

    # select buildings until HP capacity in MV grid is reached (some rest
    # capacity will remain)
    hp_cumsum = min_hp_cap_per_building.loc[buildings_with_hp_order].cumsum()
    buildings_with_hp = hp_cumsum[hp_cumsum <= hp_cap_mv_grid].index

    # choose random heat pumps until remaining heat pumps are larger than
    # remaining heat pump capacity
    remaining_hp_cap = (
        hp_cap_mv_grid - min_hp_cap_per_building.loc[buildings_with_hp].sum()
    )
    min_cap_buildings_wo_hp = min_hp_cap_per_building.loc[
        building_ids.drop(buildings_with_hp)
    ]
    possible_buildings = min_cap_buildings_wo_hp[
        min_cap_buildings_wo_hp <= remaining_hp_cap
    ].index
    while len(possible_buildings) > 0:
        random.seed(db.credentials()["--random-seed"])
        new_hp_building = random.choice(possible_buildings)
        # add new building to building with HP
        buildings_with_hp = buildings_with_hp.union(
            pd.Index([new_hp_building])
        )
        # determine if there are still possible buildings
        remaining_hp_cap = (
            hp_cap_mv_grid
            - min_hp_cap_per_building.loc[buildings_with_hp].sum()
        )
        min_cap_buildings_wo_hp = min_hp_cap_per_building.loc[
            building_ids.drop(buildings_with_hp)
        ]
        possible_buildings = min_cap_buildings_wo_hp[
            min_cap_buildings_wo_hp <= remaining_hp_cap
        ].index

    return buildings_with_hp


def desaggregate_hp_capacity(min_hp_cap_per_building, hp_cap_mv_grid):
    """
    Desaggregates the required total heat pump capacity to buildings.

    All buildings are previously assigned a minimum required heat pump
    capacity. If the total heat pump capacity exceeds this, larger heat pumps
    are assigned.

    Parameters
    ------------
    min_hp_cap_per_building : pd.Series
        Pandas series with minimum required heat pump capacity per building
         in MW.
    hp_cap_mv_grid : float
        Total heat pump capacity in MW in given MV grid.

    Returns
    --------
    pd.Series
        Pandas series with heat pump capacity per building in MW.

    """
    # distribute remaining capacity to all buildings with HP depending on
    # installed HP capacity

    allocated_cap = min_hp_cap_per_building.sum()
    remaining_cap = hp_cap_mv_grid - allocated_cap

    fac = remaining_cap / allocated_cap
    hp_cap_per_building = (
        min_hp_cap_per_building * fac + min_hp_cap_per_building
    )
    hp_cap_per_building.index.name = "building_id"

    return hp_cap_per_building


def determine_min_hp_cap_buildings_pypsa_eur_sec(
    peak_heat_demand, building_ids
):
    """
    Determines minimum required HP capacity in MV grid in MW as input for
    pypsa-eur-sec.

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    building_ids : pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in
        given MV grid.

    Returns
    --------
    float
        Minimum required HP capacity in MV grid in MW.

    """
    if len(building_ids) > 0:
        peak_heat_demand = peak_heat_demand.loc[building_ids]
        # determine minimum required heat pump capacity per building
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            peak_heat_demand
        )
        return min_hp_cap_buildings.sum()
    else:
        return 0.0


def determine_hp_cap_buildings_pvbased_per_mvgd(
    scenario, mv_grid_id, peak_heat_demand, building_ids
):
    """
    Determines which buildings in the MV grid will have a HP (buildings with PV
    rooftop are more likely to be assigned) in the eGon2035 scenario, as well
    as their respective HP capacity in MW.

    Parameters
    -----------
    mv_grid_id : int
        ID of MV grid.
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    building_ids : pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in
        given MV grid.

    """

    hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(scenario, mv_grid_id)

    if len(building_ids) > 0 and hp_cap_grid > 0.0:
        peak_heat_demand = peak_heat_demand.loc[building_ids]

        # determine minimum required heat pump capacity per building
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            peak_heat_demand
        )

        # select buildings that will have a heat pump
        buildings_with_hp = determine_buildings_with_hp_in_mv_grid(
            hp_cap_grid, min_hp_cap_buildings
        )

        # distribute total heat pump capacity to all buildings with HP
        hp_cap_per_building = desaggregate_hp_capacity(
            min_hp_cap_buildings.loc[buildings_with_hp], hp_cap_grid
        )

        return hp_cap_per_building.rename("hp_capacity")

    else:
        return pd.Series(dtype="float64").rename("hp_capacity")


def determine_hp_cap_buildings_eGon100RE_per_mvgd(mv_grid_id):
    """
    Determines HP capacity per building in eGon100RE scenario.

    In eGon100RE scenario all buildings without district heating get a heat
    pump.

    Returns
    --------
    pd.Series
        Pandas series with heat pump capacity per building in MW.

    """

    hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(
        "eGon100RE", mv_grid_id
    )

    if hp_cap_grid > 0.0:
        # get buildings with decentral heating systems
        building_ids = get_buildings_with_decentral_heat_demand_in_mv_grid(
            mv_grid_id, scenario="eGon100RE"
        )

        logger.info(f"MVGD={mv_grid_id} | Get peak loads from DB")
        df_peak_heat_demand = get_heat_peak_demand_per_building(
            "eGon100RE", building_ids
        )

        logger.info(f"MVGD={mv_grid_id} | Determine HP capacities.")
        # determine minimum required heat pump capacity per building
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            df_peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
        )

        logger.info(f"MVGD={mv_grid_id} | Desaggregate HP capacities.")
        # distribute total heat pump capacity to all buildings with HP
        hp_cap_per_building = desaggregate_hp_capacity(
            min_hp_cap_buildings, hp_cap_grid
        )

        return hp_cap_per_building.rename("hp_capacity")
    else:
        return pd.Series(dtype="float64").rename("hp_capacity")


def determine_hp_cap_buildings_eGon100RE():
    """
    Main function to determine HP capacity per building in eGon100RE scenario.

    """

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.bus_id,
            )
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .distinct(MapZensusGridDistricts.bus_id)
        )
        mvgd_ids = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
    mvgd_ids = mvgd_ids.sort_values("bus_id")
    mvgd_ids = mvgd_ids["bus_id"].values

    df_hp_cap_per_building_100RE_db = pd.DataFrame(
        columns=["building_id", "hp_capacity"]
    )

    for mvgd_id in mvgd_ids:
        logger.info(f"MVGD={mvgd_id} | Start")

        hp_cap_per_building_100RE = (
            determine_hp_cap_buildings_eGon100RE_per_mvgd(mvgd_id)
        )

        if not hp_cap_per_building_100RE.empty:
            df_hp_cap_per_building_100RE_db = pd.concat(
                [
                    df_hp_cap_per_building_100RE_db,
                    hp_cap_per_building_100RE.reset_index(),
                ],
                axis=0,
            )

    logger.info(f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write data to db.")
    df_hp_cap_per_building_100RE_db["scenario"] = "eGon100RE"

    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)

    write_table_to_postgres(
        df_hp_cap_per_building_100RE_db,
        EgonHpCapacityBuildings,
        drop=False,
    )


def aggregate_residential_and_cts_profiles(mvgd, scenario):
    """
    Gets residential and CTS heat demand profiles per building and aggregates
    them.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE.

    Returns
    --------
    pd.DataFrame
        Table of demand profile per building. Column names are building IDs and
        index is hour of the year as int (0-8759).

    """
    # ############### get residential heat demand profiles ###############
    df_heat_ts = calc_residential_heat_profiles_per_mvgd(
        mvgd=mvgd, scenario=scenario
    )

    # pivot to allow aggregation with CTS profiles
    df_heat_ts = df_heat_ts.pivot(
        index=["day_of_year", "hour"],
        columns="building_id",
        values="demand_ts",
    )
    df_heat_ts = df_heat_ts.sort_index().reset_index(drop=True)

    # ############### get CTS heat demand profiles ###############
    heat_demand_cts_ts = calc_cts_building_profiles(
        bus_ids=[mvgd],
        scenario=scenario,
        sector="heat",
    )

    # ############# aggregate residential and CTS demand profiles #############
    df_heat_ts = pd.concat([df_heat_ts, heat_demand_cts_ts], axis=1)

    df_heat_ts = df_heat_ts.groupby(axis=1, level=0).sum()

    return df_heat_ts


def export_to_db(df_peak_loads_db, df_heat_mvgd_ts_db, drop=False):
    """
    Function to export the collected results of all MVGDs per bulk to DB.

        Parameters
    ----------
    df_peak_loads_db : pd.DataFrame
        Table of building peak loads of all MVGDs per bulk
    df_heat_mvgd_ts_db : pd.DataFrame
        Table of all aggregated MVGD profiles per bulk
    drop : boolean
        Drop and recreate table if True

    """

    df_peak_loads_db = df_peak_loads_db.melt(
        id_vars="building_id",
        var_name="scenario",
        value_name="peak_load_in_w",
    )
    df_peak_loads_db["building_id"] = df_peak_loads_db["building_id"].astype(
        int
    )
    df_peak_loads_db["sector"] = "residential+cts"
    # From MW to W
    df_peak_loads_db["peak_load_in_w"] = (
        df_peak_loads_db["peak_load_in_w"] * 1e6
    )
    write_table_to_postgres(df_peak_loads_db, BuildingHeatPeakLoads, drop=drop)

    dtypes = {
        column.key: column.type
        for column in EgonEtragoTimeseriesIndividualHeating.__table__.columns
    }
    df_heat_mvgd_ts_db = df_heat_mvgd_ts_db.loc[:, dtypes.keys()]

    if drop:
        logger.info(
            f"Drop and recreate table "
            f"{EgonEtragoTimeseriesIndividualHeating.__table__.name}."
        )
        EgonEtragoTimeseriesIndividualHeating.__table__.drop(
            bind=engine, checkfirst=True
        )
        EgonEtragoTimeseriesIndividualHeating.__table__.create(
            bind=engine, checkfirst=True
        )

    with db.session_scope() as session:
        df_heat_mvgd_ts_db.to_sql(
            name=EgonEtragoTimeseriesIndividualHeating.__table__.name,
            schema=EgonEtragoTimeseriesIndividualHeating.__table__.schema,
            con=session.connection(),
            if_exists="append",
            method="multi",
            index=False,
            dtype=dtypes,
        )


def export_min_cap_to_csv(df_hp_min_cap_mv_grid_pypsa_eur_sec):
    """Export minimum capacity of heat pumps for pypsa eur sec to csv"""

    df_hp_min_cap_mv_grid_pypsa_eur_sec.index.name = "mvgd_id"
    df_hp_min_cap_mv_grid_pypsa_eur_sec = (
        df_hp_min_cap_mv_grid_pypsa_eur_sec.to_frame(
            name="min_hp_capacity"
        ).reset_index()
    )

    folder = Path(".") / "input-pypsa-eur-sec"
    file = folder / "minimum_hp_capacity_mv_grid_100RE.csv"
    # Create the folder, if it does not exist already
    if not os.path.exists(folder):
        os.mkdir(folder)
    if not file.is_file():
        logger.info(f"Create {file}")
        df_hp_min_cap_mv_grid_pypsa_eur_sec.to_csv(file, mode="w", header=True)
    else:
        df_hp_min_cap_mv_grid_pypsa_eur_sec.to_csv(
            file, mode="a", header=False
        )


def delete_pypsa_eur_sec_csv_file():
    """Delete pypsa eur sec minimum heat pump capacity csv before new run"""

    folder = Path(".") / "input-pypsa-eur-sec"
    file = folder / "minimum_hp_capacity_mv_grid_100RE.csv"
    if file.is_file():
        logger.info(f"Delete {file}")
        os.remove(file)


def catch_missing_buidings(buildings_decentral_heating, peak_load):
    """
    Check for missing buildings and reduce the list of buildings with
    decentral heating if no peak loads available. This should only happen
    in case of cutout SH

    Parameters
    -----------
    buildings_decentral_heating : list(int)
        Array or list of buildings with decentral heating

    peak_load : pd.Series
        Peak loads of all building within the mvgd

    """
    # Catch missing buildings key error
    # should only happen within cutout SH
    if (
        not all(buildings_decentral_heating.isin(peak_load.index))
        and config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        diff = buildings_decentral_heating.difference(peak_load.index)
        logger.warning(
            f"Dropped {len(diff)} building ids due to missing peak "
            f"loads. {len(buildings_decentral_heating)} left."
        )
        logger.info(f"Dropped buildings: {diff.values}")
        buildings_decentral_heating = buildings_decentral_heating.drop(diff)

    return buildings_decentral_heating


def determine_hp_cap_peak_load_mvgd_ts_2035(mvgd_ids):
    """
    Main function to determine HP capacity per building in eGon2035 scenario.
    Further, creates heat demand time series for all buildings with heat pumps
    in MV grid, as well as for all buildings with gas boilers, used in eTraGo.

    Parameters
    -----------
    mvgd_ids : list(int)
        List of MV grid IDs to determine data for.

    """

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    df_peak_loads_db = pd.DataFrame()
    df_hp_cap_per_building_2035_db = pd.DataFrame()
    df_heat_mvgd_ts_db = pd.DataFrame()

    for mvgd in mvgd_ids:
        logger.info(f"MVGD={mvgd} | Start")

        # ############# aggregate residential and CTS demand profiles #####

        df_heat_ts = aggregate_residential_and_cts_profiles(
            mvgd, scenario="eGon2035"
        )

        # ##################### determine peak loads ###################
        logger.info(f"MVGD={mvgd} | Determine peak loads.")

        peak_load_2035 = df_heat_ts.max().rename("eGon2035")

        # ######## determine HP capacity per building #########
        logger.info(f"MVGD={mvgd} | Determine HP capacities.")

        buildings_decentral_heating = (
            get_buildings_with_decentral_heat_demand_in_mv_grid(
                mvgd, scenario="eGon2035"
            )
        )

        # Reduce list of decentral heating if no Peak load available
        # TODO maybe remove after succesfull DE run
        # Might be fixed in #990
        buildings_decentral_heating = catch_missing_buidings(
            buildings_decentral_heating, peak_load_2035
        )

        hp_cap_per_building_2035 = determine_hp_cap_buildings_pvbased_per_mvgd(
            "eGon2035",
            mvgd,
            peak_load_2035,
            buildings_decentral_heating,
        )
        buildings_gas_2035 = pd.Index(buildings_decentral_heating).drop(
            hp_cap_per_building_2035.index
        )

        # ################ aggregated heat profiles ###################
        logger.info(f"MVGD={mvgd} | Aggregate heat profiles.")

        df_mvgd_ts_2035_hp = df_heat_ts.loc[
            :,
            hp_cap_per_building_2035.index,
        ].sum(axis=1)

        # heat demand time series for buildings with gas boiler
        df_mvgd_ts_2035_gas = df_heat_ts.loc[:, buildings_gas_2035].sum(axis=1)

        df_heat_mvgd_ts = pd.DataFrame(
            data={
                "carrier": ["heat_pump", "CH4"],
                "bus_id": mvgd,
                "scenario": ["eGon2035", "eGon2035"],
                "dist_aggregated_mw": [
                    df_mvgd_ts_2035_hp.to_list(),
                    df_mvgd_ts_2035_gas.to_list(),
                ],
            }
        )

        # ################ collect results ##################
        logger.info(f"MVGD={mvgd} | Collect results.")

        df_peak_loads_db = pd.concat(
            [df_peak_loads_db, peak_load_2035.reset_index()],
            axis=0,
            ignore_index=True,
        )

        df_heat_mvgd_ts_db = pd.concat(
            [df_heat_mvgd_ts_db, df_heat_mvgd_ts], axis=0, ignore_index=True
        )

        df_hp_cap_per_building_2035_db = pd.concat(
            [
                df_hp_cap_per_building_2035_db,
                hp_cap_per_building_2035.reset_index(),
            ],
            axis=0,
        )

    # ################ export to db #######################
    logger.info(f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write data to db.")

    export_to_db(df_peak_loads_db, df_heat_mvgd_ts_db, drop=False)

    df_hp_cap_per_building_2035_db["scenario"] = "eGon2035"

    # TODO debug duplicated building_ids
    duplicates = df_hp_cap_per_building_2035_db.loc[
        df_hp_cap_per_building_2035_db.duplicated("building_id", keep=False)
    ]

    if not duplicates.empty:
        logger.info(
            f"Dropped duplicated buildings: "
            f"{duplicates.loc[:,['building_id', 'hp_capacity']]}"
        )

    df_hp_cap_per_building_2035_db.drop_duplicates("building_id", inplace=True)

    df_hp_cap_per_building_2035_db.building_id = (
        df_hp_cap_per_building_2035_db.building_id.astype(int)
    )

    write_table_to_postgres(
        df_hp_cap_per_building_2035_db,
        EgonHpCapacityBuildings,
        drop=False,
    )


def determine_hp_cap_peak_load_mvgd_ts_2019(mvgd_ids):
    """
    Main function to determine HP capacity per building in status2019 scenario.
    Further, creates heat demand time series for all buildings with heat pumps
    in MV grid, as well as for all buildings with gas boilers, used in eTraGo.

    Parameters
    -----------
    mvgd_ids : list(int)
        List of MV grid IDs to determine data for.

    """

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    df_peak_loads_db = pd.DataFrame()
    df_hp_cap_per_building_2019_db = pd.DataFrame()
    df_heat_mvgd_ts_db = pd.DataFrame()

    for mvgd in mvgd_ids:
        logger.info(f"MVGD={mvgd} | Start")

        # ############# aggregate residential and CTS demand profiles #####

        df_heat_ts = aggregate_residential_and_cts_profiles(
            mvgd, scenario="status2019"
        )

        # ##################### determine peak loads ###################
        logger.info(f"MVGD={mvgd} | Determine peak loads.")

        peak_load_2019 = df_heat_ts.max().rename("status2019")

        # ######## determine HP capacity per building #########
        logger.info(f"MVGD={mvgd} | Determine HP capacities.")

        buildings_decentral_heating = (
            get_buildings_with_decentral_heat_demand_in_mv_grid(
                mvgd, scenario="status2019"
            )
        )

        # Reduce list of decentral heating if no Peak load available
        # TODO maybe remove after succesfull DE run
        # Might be fixed in #990
        buildings_decentral_heating = catch_missing_buidings(
            buildings_decentral_heating, peak_load_2019
        )

        hp_cap_per_building_2019 = determine_hp_cap_buildings_pvbased_per_mvgd(
            "status2019",
            mvgd,
            peak_load_2019,
            buildings_decentral_heating,
        )
        buildings_gas_2019 = pd.Index(buildings_decentral_heating).drop(
            hp_cap_per_building_2019.index
        )

        # ################ aggregated heat profiles ###################
        logger.info(f"MVGD={mvgd} | Aggregate heat profiles.")

        df_mvgd_ts_2019_hp = df_heat_ts.loc[
            :,
            hp_cap_per_building_2019.index,
        ].sum(axis=1)

        df_heat_mvgd_ts = pd.DataFrame(
            data={
                "carrier": "heat_pump",
                "bus_id": mvgd,
                "scenario": "status2019",
                "dist_aggregated_mw": [df_mvgd_ts_2019_hp.to_list()],
            }
        )

        # ################ collect results ##################
        logger.info(f"MVGD={mvgd} | Collect results.")

        df_peak_loads_db = pd.concat(
            [df_peak_loads_db, peak_load_2019.reset_index()],
            axis=0,
            ignore_index=True,
        )

        df_heat_mvgd_ts_db = pd.concat(
            [df_heat_mvgd_ts_db, df_heat_mvgd_ts], axis=0, ignore_index=True
        )

        df_hp_cap_per_building_2019_db = pd.concat(
            [
                df_hp_cap_per_building_2019_db,
                hp_cap_per_building_2019.reset_index(),
            ],
            axis=0,
        )

    # ################ export to db #######################
    logger.info(f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write data to db.")

    export_to_db(df_peak_loads_db, df_heat_mvgd_ts_db, drop=False)

    df_hp_cap_per_building_2019_db["scenario"] = "status2019"

    # TODO debug duplicated building_ids
    duplicates = df_hp_cap_per_building_2019_db.loc[
        df_hp_cap_per_building_2019_db.duplicated("building_id", keep=False)
    ]

    if not duplicates.empty:
        logger.info(
            f"Dropped duplicated buildings: "
            f"{duplicates.loc[:,['building_id', 'hp_capacity']]}"
        )

    df_hp_cap_per_building_2019_db.drop_duplicates("building_id", inplace=True)

    df_hp_cap_per_building_2019_db.building_id = (
        df_hp_cap_per_building_2019_db.building_id.astype(int)
    )

    write_table_to_postgres(
        df_hp_cap_per_building_2019_db,
        EgonHpCapacityBuildings,
        drop=False,
    )


def determine_hp_cap_peak_load_mvgd_ts_pypsa_eur_sec(mvgd_ids):
    """
    Main function to determine minimum required HP capacity in MV for
    pypsa-eur-sec. Further, creates heat demand time series for all buildings
    with heat pumps in MV grid in eGon100RE scenario, used in eTraGo.

    Parameters
    -----------
    mvgd_ids : list(int)
        List of MV grid IDs to determine data for.

    """

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    df_peak_loads_db = pd.DataFrame()
    df_heat_mvgd_ts_db = pd.DataFrame()
    df_hp_min_cap_mv_grid_pypsa_eur_sec = pd.Series(dtype="float64")

    for mvgd in mvgd_ids:
        logger.info(f"MVGD={mvgd} | Start")

        # ############# aggregate residential and CTS demand profiles #####

        df_heat_ts = aggregate_residential_and_cts_profiles(
            mvgd, scenario="eGon100RE"
        )

        # ##################### determine peak loads ###################
        logger.info(f"MVGD={mvgd} | Determine peak loads.")

        peak_load_100RE = df_heat_ts.max().rename("eGon100RE")

        # ######## determine minimum HP capacity pypsa-eur-sec ###########
        logger.info(f"MVGD={mvgd} | Determine minimum HP capacity.")

        buildings_decentral_heating = (
            get_buildings_with_decentral_heat_demand_in_mv_grid(
                mvgd, scenario="eGon100RE"
            )
        )

        # Reduce list of decentral heating if no Peak load available
        # TODO maybe remove after succesfull DE run
        buildings_decentral_heating = catch_missing_buidings(
            buildings_decentral_heating, peak_load_100RE
        )

        hp_min_cap_mv_grid_pypsa_eur_sec = (
            determine_min_hp_cap_buildings_pypsa_eur_sec(
                peak_load_100RE,
                buildings_decentral_heating,
            )
        )

        # ################ aggregated heat profiles ###################
        logger.info(f"MVGD={mvgd} | Aggregate heat profiles.")

        df_mvgd_ts_hp = df_heat_ts.loc[
            :,
            buildings_decentral_heating,
        ].sum(axis=1)

        df_heat_mvgd_ts = pd.DataFrame(
            data={
                "carrier": "heat_pump",
                "bus_id": mvgd,
                "scenario": "eGon100RE",
                "dist_aggregated_mw": [df_mvgd_ts_hp.to_list()],
            }
        )

        # ################ collect results ##################
        logger.info(f"MVGD={mvgd} | Collect results.")

        df_peak_loads_db = pd.concat(
            [df_peak_loads_db, peak_load_100RE.reset_index()],
            axis=0,
            ignore_index=True,
        )

        df_heat_mvgd_ts_db = pd.concat(
            [df_heat_mvgd_ts_db, df_heat_mvgd_ts], axis=0, ignore_index=True
        )

        df_hp_min_cap_mv_grid_pypsa_eur_sec.loc[mvgd] = (
            hp_min_cap_mv_grid_pypsa_eur_sec
        )

    # ################ export to db and csv ######################
    logger.info(f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write data to db.")

    export_to_db(df_peak_loads_db, df_heat_mvgd_ts_db, drop=False)

    logger.info(
        f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write "
        f"pypsa-eur-sec min "
        f"HP capacities to csv."
    )
    export_min_cap_to_csv(df_hp_min_cap_mv_grid_pypsa_eur_sec)


def split_mvgds_into_bulks(n, max_n, func):
    """
    Generic function to split task into multiple parallel tasks,
    dividing the number of MVGDs into even bulks.

    Parameters
    -----------
    n : int
        Number of bulk
    max_n: int
        Maximum number of bulks
    func : function
        The funnction which is then called with the list of MVGD as
        parameter.
    """

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.bus_id,
            )
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .distinct(MapZensusGridDistricts.bus_id)
        )
        mvgd_ids = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

    mvgd_ids = mvgd_ids.sort_values("bus_id").reset_index(drop=True)

    mvgd_ids = np.array_split(mvgd_ids["bus_id"].values, max_n)
    # Only take split n
    mvgd_ids = mvgd_ids[n]

    logger.info(f"Bulk takes care of MVGD: {min(mvgd_ids)} : {max(mvgd_ids)}")
    func(mvgd_ids)


def delete_hp_capacity(scenario):
    """Remove all hp capacities for the selected scenario

    Parameters
    -----------
    scenario : string
        Either eGon2035 or eGon100RE

    """

    with db.session_scope() as session:
        # Buses
        session.query(EgonHpCapacityBuildings).filter(
            EgonHpCapacityBuildings.scenario == scenario
        ).delete(synchronize_session=False)


def delete_mvgd_ts(scenario):
    """Remove all hp capacities for the selected scenario

    Parameters
    -----------
    scenario : string
        Either eGon2035 or eGon100RE

    """

    with db.session_scope() as session:
        # Buses
        session.query(EgonEtragoTimeseriesIndividualHeating).filter(
            EgonEtragoTimeseriesIndividualHeating.scenario == scenario
        ).delete(synchronize_session=False)


def delete_hp_capacity_100RE():
    """Remove all hp capacities for the selected eGon100RE"""
    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)
    delete_hp_capacity(scenario="eGon100RE")


def delete_hp_capacity_2019():
    """Remove all hp capacities for the selected status2019"""
    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)
    delete_hp_capacity(scenario="status2019")


def delete_hp_capacity_2035():
    """Remove all hp capacities for the selected eGon2035"""
    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)
    delete_hp_capacity(scenario="eGon2035")


def delete_mvgd_ts_2019():
    """Remove all mvgd ts for the selected status2019"""
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    delete_mvgd_ts(scenario="status2019")


def delete_mvgd_ts_2035():
    """Remove all mvgd ts for the selected eGon2035"""
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    delete_mvgd_ts(scenario="eGon2035")


def delete_mvgd_ts_100RE():
    """Remove all mvgd ts for the selected eGon100RE"""
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    delete_mvgd_ts(scenario="eGon100RE")


def delete_heat_peak_loads_2019():
    """Remove all heat peak loads for status2019."""
    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)
    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.scenario == "status2019"
        ).delete(synchronize_session=False)


def delete_heat_peak_loads_2035():
    """Remove all heat peak loads for eGon2035."""
    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)
    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.scenario == "eGon2035"
        ).delete(synchronize_session=False)


def delete_heat_peak_loads_100RE():
    """Remove all heat peak loads for eGon100RE."""
    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)
    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.scenario == "eGon100RE"
        ).delete(synchronize_session=False)
