"""The central module containing all code dealing with individual heat supply.

The following main things are done in this module:

* ??
* Desaggregation of heat pump capacities to individual buildings

"""

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
from egon.data.datasets import (
    Dataset,
    load_sources_and_targets,
    wrapped_partial,
)
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
from egon.data.datasets.scenario_parameters import (
    demand_source_scenario,
    pv_source_scenario,
)

# get zensus cells with district heating
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts

engine = db.engine()
Base = declarative_base()

#: Absolute tolerance in MW when comparing heat pump capacities. Capacities are
#: derived from peak heat demands through several float operations, so exact
#: equality is not meaningful. 1e-9 MW is one microwatt, far below any
#: physically relevant difference.
FLOOR_TOLERANCE = 1e-9


class EgonEtragoTimeseriesIndividualHeating(Base):
    """
    Class definition of table demand.egon_etrago_timeseries_individual_heating.

    This table contains aggregated heat load profiles of all buildings with heat pumps
    within an MV grid as well as of all buildings with gas boilers within an MV grid for
    the different scenarios. The data is used in eTraGo.

    """

    __tablename__ = "egon_etrago_timeseries_individual_heating"
    __table_args__ = {"schema": "demand"}
    bus_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    carrier = Column(String, primary_key=True)
    dist_aggregated_mw = Column(ARRAY(REAL))


class EgonHpCapacityBuildings(Base):
    """
    Class definition of table demand.egon_hp_capacity_buildings.

    This table contains the heat pump capacity of all buildings with a heat pump.

    """

    __tablename__ = "egon_hp_capacity_buildings"
    __table_args__ = {"schema": "demand"}
    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    hp_capacity = Column(REAL)


def order_scenarios_by_hp_floor_chain(scenarios):
    """
    Orders scenarios so that the heat pump floor chain runs in its declared
    direction.

    Scenarios on the chain (see :func:`get_hp_floor_chain`) come first, in
    chain order, because each inherits its predecessor's result and therefore
    cannot be distributed before it. Scenarios off the chain keep their
    relative order from the input and follow, as they neither inherit a floor
    nor pass one on.

    Parameters
    -----------
    scenarios : list of str
        Scenario names, e.g. as selected in the pipeline config.

    Returns
    --------
    list of str
        The same scenarios, reordered.

    """
    chain = get_hp_floor_chain()

    on_chain = [s for s in chain if s in scenarios]
    off_chain = [s for s in scenarios if s not in chain]

    return on_chain + off_chain


class HeatPumpsStatusQuo(Dataset):
    def __init__(self, dependencies):
        def dyn_parallel_tasks_status_quo(scenario):
            """Dynamically generate tasks

            The goal is to speed up tasks by parallelising bulks of mvgds.

            The number of parallel tasks is defined via parameter
            `parallel_tasks` in the dataset config `datasets.yml`.

            Returns
            -------
            set of airflow.PythonOperators
                The tasks. Each element is of
                :func:`egon.data.datasets.heat_supply.individual_heating.
                determine_hp_cap_peak_load_mvgd_ts_status_quo`
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
                            f"determine-hp-capacity-{scenario}-"
                            f"mvgd-bulk{i}"
                        ),
                        python_callable=split_mvgds_into_bulks,
                        op_kwargs={
                            "n": i,
                            "max_n": parallel_tasks,
                            "scenario": scenario,
                            "func": determine_hp_cap_peak_load_mvgd_ts_status_quo,
                        },
                    )
                )
            return tasks

        if any(
            "status" in scenario
            for scenario in config.settings()["egon-data"]["--scenarios"]
        ):
            tasks = ()

            for scenario in config.settings()["egon-data"]["--scenarios"]:
                if "status" in scenario:
                    postfix = f"_{scenario[-4:]}"

                    tasks += (
                        wrapped_partial(
                            delete_heat_peak_loads_status_quo,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                        wrapped_partial(
                            delete_hp_capacity_status_quo,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                        wrapped_partial(
                            delete_mvgd_ts_status_quo,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                    )

                    tasks += ({*dyn_parallel_tasks_status_quo(scenario)},)
        else:
            tasks = (
                PythonOperator(
                    task_id="HeatPumpsSQ_skipped",
                    python_callable=skip_task,
                    op_kwargs={"scn": "sq", "task": "HeatPumpsStatusQuo"},
                ),
            )

        super().__init__(
            name="HeatPumpsStatusQuo",
            version="0.0.6",
            dependencies=dependencies,
            tasks=tasks,
        )


class HeatPumpsCascade(Dataset):
    """
    Class for desaggregation of heat pump capcacities per MV grid district to individual
    buildings for the eGon2035, reGon2037 and reGon2045 scenarios.

    The heat pump capacity per MV grid district is disaggregated to buildings
    with individual heating based on the buildings heat peak demand. The buildings are
    chosen randomly until the target capacity per MV grid district is reached. Buildings
    with PV rooftop have a higher probability to be assigned a heat pump.

    For scenarios on the heat pump floor chain declared in `datasets.yml`
    (`status2024` -> `reGon2037` -> `reGon2045`), the assignment is *floored*: a
    building that had a heat pump in the previous link keeps exactly its inherited
    capacity, and only the remaining capacity of the MV grid district is distributed
    over the other buildings. This makes the heat pump stock monotonically
    non-decreasing along the scenario timeline. `eGon2035` is deliberately not on the
    chain and keeps its independent distribution. Because each link inherits its
    predecessor's result, the scenarios on the chain are processed in chain order and
    re-running one link invalidates everything downstream of it.

    As the building's heat peak load is not previously determined, it is as well done
    in this dataset. Further, as determining heat peak load requires heat load
    profiles of the buildings to be set up, this task is also utilised to set up
    aggregated heat load profiles of all buildings with heat pumps within a grid as
    well as for all buildings with a gas boiler (i.e. all buildings with decentral
    heating system minus buildings with heat pump) needed in eTraGo.

    For more information see data documentation on :ref:`dec-heat-pumps-ref`.

    *Dependencies*
      * :py:class:`CtsDemandBuildings
        <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsDemandBuildings>`
      * :py:class:`DistrictHeatingAreas
        <egon.data.datasets.district_heating_areas.DistrictHeatingAreas>`
      * :py:class:`HeatSupply <egon.data.datasets.heat_supply.HeatSupply>`
      * :py:class:`HeatTimeSeries
        <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>`
      * :py:func:`pv_rooftop_to_buildings
        <egon.data.datasets.power_plants.pv_rooftop_buildings.pv_rooftop_to_buildings>`

    *Resulting tables*
      * :py:class:`demand.egon_hp_capacity_buildings
        <egon.data.datasets.heat_supply.individual_heating.EgonHpCapacityBuildings>`
        is created (if it doesn't yet exist) and filled
      * :py:class:`demand.egon_etrago_timeseries_individual_heating
        <egon.data.datasets.heat_supply.individual_heating.EgonEtragoTimeseriesIndividualHeating>`
        is created (if it doesn't yet exist) and filled
      * :py:class:`demand.egon_building_heat_peak_loads
        <egon.data.datasets.heat_supply.individual_heating.BuildingHeatPeakLoads>`
        is created (if it doesn't yet exist) and filled

    **What is the challenge?**

    The main challenge lies in the set up of heat demand profiles per building in
    :func:`aggregate_residential_and_cts_profiles()` as it takes alot of time and
    in grids with a high number of buildings requires alot of RAM. Both runtime and RAM
    usage needed to be improved several times. To speed up the process, tasks are set
    up to run in parallel. This currently leads to alot of connections being opened and
    at a certain point to a runtime error due to too many open connections.

    **What are central assumptions during the data processing?**

    Central assumption for desaggregating the heat pump capacity to individual buildings
    is that heat pumps can be dimensioned using an approach from the network development
    plan that uses the building's peak heat demand and a fixed COP (see
    data documentation on :ref:`dec-heat-pumps-ref`).
    Another central assumption is, that buildings with PV rooftop plants are more likely
    to have a heat pump than other buildings (see
    :func:`determine_buildings_with_hp_in_mv_grid()` for details).

    **Drawbacks and limitations of the data**

    The heat demand profiles used here to determine the heat peak load have very few
    very high peaks that lead to large heat pump capacities. This should be solved
    somehow. Cutting off the peak is not possible, as the time series of each building
    is not saved but generated on the fly. Also, just using smaller heat pumps would
    lead to infeasibilities in eDisGo.

    """

    #:
    name: str = "HeatPumpsCascade"
    #:
    version: str = "0.0.8"

    def __init__(self, dependencies):
        def dyn_parallel_tasks_2035(scenario):
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
                            f"determine-hp-capacity-{scenario}-"
                            f"mvgd-bulk{i}"
                        ),
                        python_callable=split_mvgds_into_bulks,
                        op_kwargs={
                            "n": i,
                            "max_n": parallel_tasks,
                            "scenario": scenario,
                            "func": determine_hp_cap_peak_load_mvgd_ts_2035,
                        },
                    )
                )
            return tasks

        if any(
            "status" not in scenario
            for scenario in config.settings()["egon-data"]["--scenarios"]
        ):
            tasks_HeatPumpsCascade = ()

            # The floor chain is inherently sequential: reGon2045 must not be
            # distributed before reGon2037 has been written, or it would floor
            # against no rows and reGon2037 would then floor against its own
            # successor - silently inverting the chain. Ordering the scenarios
            # by the chain declared in datasets.yml is what encodes the
            # direction; the Dataset task-graph API turns this flat tuple into
            # the required ordering, as all of one scenario's parallel bulks
            # complete before any task of the next begins.
            for scenario in order_scenarios_by_hp_floor_chain(
                config.settings()["egon-data"]["--scenarios"]
            ):
                if "status" not in scenario:
                    postfix = f"_{scenario}"

                    tasks_HeatPumpsCascade += (
                        wrapped_partial(
                            delete_heat_peak_loads_2035,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                        wrapped_partial(
                            delete_hp_capacity_2035,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                        wrapped_partial(
                            delete_mvgd_ts_2035,
                            scenario=scenario,
                            postfix=postfix,
                        ),
                    )

                    tasks_HeatPumpsCascade += (
                        {*dyn_parallel_tasks_2035(scenario)},
                    )
        else:
            tasks_HeatPumpsCascade = (
                PythonOperator(
                    task_id="HeatPumpsCascade_skipped",
                    python_callable=skip_task,
                    op_kwargs={
                        "scn": "eGon2035/reGon2037/reGon2045",
                        "task": "HeatPumpsCascade",
                    },
                ),
            )

        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks_HeatPumpsCascade,
        )


class BuildingHeatPeakLoads(Base):
    """
    Class definition of table demand.egon_building_heat_peak_loads.

    Table with peak heat demand of residential and CTS heat demand combined for
    each building.

    """

    __tablename__ = "egon_building_heat_peak_loads"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    sector = Column(String, primary_key=True)
    peak_load_in_w = Column(REAL)


def skip_task(scn=str, task=str):
    logger.info(
        f"{scn} is not in the list of scenarios. {task} dataset is skipped."
    )

    return


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
    sources, targets = load_sources_and_targets("HeatSupply")

    tech = technologies[technologies.priority == technologies.priority.max()]

    # Distribute heat pumps linear to remaining demand.
    if tech.index == "heat_pump":
        if distribution_level == "federal_states":
            # Select target values per federal state
            target = db.select_dataframe(
                f"""
                    SELECT DISTINCT ON (gen) gen as state, capacity
                    FROM {sources.tables['scenario_capacities']} a
                    JOIN {sources.tables['federal_states']} b
                    ON a.nuts = b.nuts
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'residential_rural_heat_pump'
                    """,
                index_col="state",
            )

            heat_per_mv["share"] = heat_per_mv.groupby(
                "state",
                group_keys=False,
            ).remaining_demand.apply(lambda grp: grp / grp.sum())

            append_df = (
                heat_per_mv["share"]
                .mul(target.capacity[heat_per_mv["state"]].values)
                .reset_index()
            )
        else:
            # Select target value for Germany
            target = db.select_dataframe(f"""
                    SELECT SUM(capacity) AS capacity
                    FROM {sources.tables['scenario_capacities']} a
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'rural_heat_pump'
                    """)

            if not target.capacity[0]:
                target.capacity[0] = 0

            # No testmode scaling here: the 'rural_heat_pump' target is already
            # reduced to the dataset boundary by population_share() when it is
            # inserted in scenario_capacities.insert_capacities_status_quo().
            # Dividing by 16 again applied the boundary reduction twice and made
            # the distributed capacity ~16x too low.

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

    elif (tech.index == "gas_boiler") & ("status" not in scenario):
        append_df = pd.DataFrame(
            data={
                "capacity": heat_per_mv.remaining_demand.div(
                    tech.estimated_flh.values[0]
                ),
                "carrier": f"residential_rural_{tech.index}",
                "mv_grid_id": heat_per_mv.index,
                "scenario": scenario,
            }
        )

    else:
        append_df = pd.DataFrame(
            data={
                "capacity": heat_per_mv.remaining_demand.div(
                    tech.estimated_flh.values[0]
                ),
                "carrier": f"residential_rural_{tech.index}",
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
    1. all small scale CHP are connected.
    2. If the supply can not  meet the heat demand, solar thermal collectors
       are attached. This is not implemented yet, since individual
       solar thermal plants are not considered in eGon2035 scenario.
    3. If this is not suitable, the mv grid is also supplied by heat pumps.
    4. The last option are individual gas boilers.

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

    sources, targets = load_sources_and_targets("HeatSupply")

    demand_scenario = demand_source_scenario(scenario)

    # Select residential heat demand per mv grid district and federal state
    heat_per_mv = db.select_geodataframe(
        f"""
        SELECT d.bus_id as bus_id, SUM(demand) as demand,
        c.vg250_lan as state, d.geom
        FROM {sources.tables['heat_demand']} a
        JOIN {sources.tables['map_zensus_grid']} b
        ON a.zensus_population_id = b.zensus_population_id
        JOIN {sources.tables['map_vg250_grid']} c
        ON b.bus_id = c.bus_id
        JOIN {sources.tables['mv_grids']} d
        ON d.bus_id = c.bus_id
        WHERE scenario = '{demand_scenario}'
        AND a.zensus_population_id NOT IN (
            SELECT zensus_population_id
            FROM {sources.tables['map_dh']}
            WHERE scenario = '{demand_scenario}')
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
    if "status" in scenario:
        technologies = pd.DataFrame(
            index=["heat_pump"],
            columns=["estimated_flh", "priority"],
            data={"estimated_flh": [4000], "priority": [1]},
        )
    else:
        technologies = pd.DataFrame(
            index=["heat_pump", "gas_boiler"],
            columns=["estimated_flh", "priority"],
            data={"estimated_flh": [4000, 8000], "priority": [2, 1]},
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
    the configured scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Name of the scenario.

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
    Gets residential heat profiles per building in MV grid for the given
    scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Name of the scenario.

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

    df_peta_demand = get_peta_demand(mvgd, demand_source_scenario(scenario))
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
        Name of scenario.
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
            MapZensusDistrictHeatingAreas.scenario
            == demand_source_scenario(scenario),
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
        Name of scenario.
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
        Name of scenario.
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
        Name of scenario.

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
    by the NEP-based cascade.

    Parameters
    -----------
    scenario : str
        Name of scenario.
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
    hp_cap_mv_grid, min_hp_cap_per_building, scenario
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
    scenario : str
        Name of the scenario. Determines which scenario's PV rooftop data is
        used to weight buildings (via
        :func:`~.scenario_parameters.pv_source_scenario`).

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
            egon_power_plants_pv_roof_building.scenario
            == pv_source_scenario(scenario),
        )

        buildings_with_pv = (
            pd.read_sql(query.statement, query.session.bind, index_col=None)
            .building_id.drop_duplicates()
            .sort_values()
            .values
        )
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

    # Guard against duplicated building ids reaching the caller. A duplicate
    # would be allocated capacity twice and then silently dropped by
    # drop_duplicates() in the bulk export, losing that capacity from the
    # grid's budget without any rescaling.
    return buildings_with_hp.drop_duplicates()


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


def get_hp_floor_chain():
    """
    Returns the ordered scenario chain along which the fixed heat pump floor
    propagates.

    The chain is declared in the dataset config `datasets.yml` rather than
    hardcoded here, because which scenarios floor against which is a modelling
    choice. Scenarios not on the chain (notably `eGon2035`) neither inherit a
    floor nor pass one on.

    Returns
    --------
    list of str
        Scenario names in chain order, e.g.
        ``["status2024", "reGon2037", "reGon2045"]``. Empty if no chain is
        configured.

    """
    return list(
        config.datasets()["demand_timeseries_mvgd"].get("hp_floor_chain", [])
    )


def get_hp_floor_predecessors(scenario):
    """
    Returns the predecessors of a scenario on the floor chain, nearest first.

    Parameters
    -----------
    scenario : str
        Name of the scenario.

    Returns
    --------
    list of str
        Scenario names preceding `scenario` on the chain, ordered from nearest
        to furthest. Empty if `scenario` is not on the chain or is its first
        link.

    """
    chain = get_hp_floor_chain()

    if scenario not in chain:
        return []

    return chain[: chain.index(scenario)][::-1]


def get_inherited_hp_capacity(scenario, building_ids):
    """
    Returns the fixed heat pump capacity a scenario inherits from the floor
    chain.

    The floor a scenario inherits is its predecessor's *result*. Because
    scenarios are selected independently, a run can omit a middle link of the
    chain (e.g. `[status2024, reGon2045]`). In that case the floor walks back
    along the chain to the nearest predecessor that actually has rows, which is
    a weaker but still valid constraint in the monotonic direction the chain
    assumes. If walking back exhausts the chain and no predecessor has any
    rows, the scenario is distributed unfloored and a warning is logged, since
    running a single future scenario standalone is a legitimate way to test
    the distribution itself.

    Parameters
    -----------
    scenario : str
        Name of the scenario.
    building_ids : pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in the
        given MV grid.

    Returns
    --------
    pd.Series
        Inherited heat pump capacity in MW per building, indexed by building
        ID. Contains only buildings that are both in `building_ids` and had a
        heat pump in the inherited scenario. Empty if the scenario is
        unfloored.

    """
    empty = pd.Series(dtype="float64", name="hp_capacity")
    empty.index.name = "building_id"

    predecessors = get_hp_floor_predecessors(scenario)

    if not predecessors:
        return empty

    for predecessor in predecessors:
        with db.session_scope() as session:
            query = session.query(
                EgonHpCapacityBuildings.building_id,
                EgonHpCapacityBuildings.hp_capacity,
            ).filter(
                EgonHpCapacityBuildings.scenario == predecessor,
                EgonHpCapacityBuildings.building_id.in_(
                    building_ids.tolist()
                ),
            )

            inherited = pd.read_sql(
                query.statement, query.session.bind, index_col="building_id"
            ).hp_capacity

        # An empty result for one MV grid does not prove the predecessor was
        # never written, so fall back to the next link only if the predecessor
        # scenario holds no rows at all.
        if not inherited.empty:
            if predecessor != predecessors[0]:
                logger.info(
                    f"Scenario {scenario} inherits its heat pump floor from "
                    f"{predecessor}, as the nearer link(s) "
                    f"{predecessors[: predecessors.index(predecessor)]} hold "
                    f"no data in this run."
                )
            return inherited.rename("hp_capacity")

        if scenario_has_hp_capacity(predecessor):
            # Predecessor exists but has no heat pumps among these buildings -
            # a valid empty floor, not a missing link.
            return empty

    logger.warning(
        f"No predecessor of scenario {scenario} on the heat pump floor chain "
        f"{get_hp_floor_chain()} holds any data (tried {predecessors}). "
        f"{scenario} is distributed UNFLOORED: its heat pump assignment is "
        f"drawn independently and buildings may lose heat pumps relative to "
        f"earlier scenarios. Include an earlier link of the chain in "
        f"'--scenarios' to floor it."
    )

    return empty


def scenario_has_hp_capacity(scenario):
    """
    Returns whether a scenario has any heat pump capacity rows at all.

    Used to tell a predecessor that was never run (walk further back along the
    floor chain) from one that was run but has no heat pumps in the MV grid at
    hand (a valid empty floor).

    Parameters
    -----------
    scenario : str
        Name of the scenario.

    Returns
    --------
    bool
        True if the scenario has at least one row in
        :py:class:`EgonHpCapacityBuildings`.

    """
    with db.session_scope() as session:
        return (
            session.query(EgonHpCapacityBuildings.building_id)
            .filter(EgonHpCapacityBuildings.scenario == scenario)
            .first()
            is not None
        )


def determine_hp_cap_buildings_pvbased_per_mvgd(
    scenario, mv_grid_id, peak_heat_demand, building_ids
):
    """
    Determines which buildings in the MV grid will have a HP (buildings with PV
    rooftop are more likely to be assigned), as well
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

    Notes
    -----
    For scenarios on the heat pump floor chain (see
    :func:`get_hp_floor_chain`), buildings that had a heat pump in the inherited
    scenario keep **at least** their inherited capacity: the floor is a lower
    bound, not a fixed value. Where the building's own peak heat demand has
    grown since the inherited scenario, its capacity is raised to the minimum
    the sizing rule requires for the current scenario
    (:func:`determine_minimum_hp_capacity_per_building`); a heat pump kept at
    its inherited size would otherwise be unable to cover the building's heat
    demand.

    Floored buildings are then exempt from the proportional scaling in
    :func:`desaggregate_hp_capacity`, and only the remaining budget
    (`hp_cap_grid` minus the floored capacity in this grid, after any
    up-scaling) is distributed over the not-yet-equipped buildings using the
    PV-weighted selection unchanged. Up-scaling therefore reduces the budget
    available for new heat pumps rather than adding to the grid total: both
    invariants stay exact -- every floored building satisfies the sizing rule,
    and the distributed total still equals `hp_cap_grid`.

    """

    hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(scenario, mv_grid_id)

    if len(building_ids) > 0 and hp_cap_grid > 0.0:
        peak_heat_demand = peak_heat_demand.loc[building_ids]

        # determine minimum required heat pump capacity per building
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            peak_heat_demand
        )

        # Capacity inherited from the previous link of the floor chain is a
        # lower bound, not a fixed value: raise it to the minimum this
        # scenario's sizing rule requires where the building's peak heat demand
        # has grown, so an inherited heat pump still covers its building.
        inherited_hp_cap = get_inherited_hp_capacity(scenario, building_ids)

        if not inherited_hp_cap.empty:
            required = min_hp_cap_buildings.reindex(inherited_hp_cap.index)
            floored_hp_cap = inherited_hp_cap.combine(
                required.fillna(0.0), max
            )

            n_scaled_up = int(
                (floored_hp_cap > inherited_hp_cap + FLOOR_TOLERANCE).sum()
            )
            if n_scaled_up:
                logger.info(
                    f"MVGD={mv_grid_id} | Scenario {scenario}: raised "
                    f"{n_scaled_up} of {len(inherited_hp_cap)} inherited heat "
                    f"pumps to the minimum size required by their current peak "
                    f"heat demand (+"
                    f"{(floored_hp_cap.sum() - inherited_hp_cap.sum()):.4f} "
                    f"MW)."
                )
        else:
            floored_hp_cap = inherited_hp_cap

        floored_cap = floored_hp_cap.sum()

        # honouring the floor and hitting the capacity target are mutually
        # exclusive if the floored stock exceeds the grid's own target
        if floored_cap > hp_cap_grid:
            raise ValueError(
                f"Heat pump capacity required by the floor "
                f"({floored_cap:.4f} MW, of which "
                f"{inherited_hp_cap.sum():.4f} MW inherited) in MV grid "
                f"{mv_grid_id} exceeds the capacity target of scenario "
                f"{scenario} ({hp_cap_grid:.4f} MW). The floor cannot be "
                f"honoured while meeting the target."
            )

        remaining_cap_grid = hp_cap_grid - floored_cap
        min_hp_cap_remaining = min_hp_cap_buildings.drop(
            floored_hp_cap.index, errors="ignore"
        )

        hp_cap_per_building = pd.Series(dtype="float64")
        hp_cap_per_building.index.name = "building_id"

        if remaining_cap_grid > 0.0 and not min_hp_cap_remaining.empty:
            # select additional buildings that will have a heat pump
            buildings_with_hp = determine_buildings_with_hp_in_mv_grid(
                remaining_cap_grid, min_hp_cap_remaining, scenario
            )

            # the remaining budget can be too small for any further
            # building, in which case the grid's capacity stays entirely
            # with the floored buildings
            if len(buildings_with_hp) > 0:
                # distribute the remaining capacity to the selected buildings
                hp_cap_per_building = desaggregate_hp_capacity(
                    min_hp_cap_remaining.loc[buildings_with_hp],
                    remaining_cap_grid,
                )

        hp_cap_per_building = pd.concat(
            [floored_hp_cap, hp_cap_per_building]
        )
        hp_cap_per_building.index.name = "building_id"

        return hp_cap_per_building.rename("hp_capacity")

    else:
        return pd.Series(dtype="float64").rename("hp_capacity")


def aggregate_residential_and_cts_profiles(mvgd, scenario):
    """
    Gets residential and CTS heat demand profiles per building and aggregates
    them.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Name of the scenario.

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
    if not all(buildings_decentral_heating.isin(peak_load.index)):
        diff = buildings_decentral_heating.difference(peak_load.index)
        logger.warning(
            f"Dropped {len(diff)} building ids due to missing peak "
            f"loads. {len(buildings_decentral_heating)} left."
        )
        logger.info(f"Dropped buildings: {diff.values}")
        buildings_decentral_heating = buildings_decentral_heating.drop(diff)

    return buildings_decentral_heating


def determine_hp_cap_peak_load_mvgd_ts_2035(mvgd_ids, scenario):
    """
    Main function to determine HP capacity per building in the eGon2035,
    reGon2037 or reGon2045 scenario.
    Further, creates heat demand time series for all buildings with heat pumps
    in MV grid, as well as for all buildings with gas boilers, used in eTraGo.

    Parameters
    -----------
    mvgd_ids : list(int)
        List of MV grid IDs to determine data for.
    scenario : str
        Name of the scenario.

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
            mvgd, scenario=scenario
        )

        # ##################### determine peak loads ###################
        logger.info(f"MVGD={mvgd} | Determine peak loads.")

        peak_load_2035 = df_heat_ts.max().rename(scenario)
        # If df_heat_ts has no columns (mvgd has no decentral heating
        # buildings), the index name is lost and reset_index() below
        # would create a stray "index" column instead of "building_id",
        # which export_to_db's melt() then turns into bogus scenario
        # rows with NULL peak loads.
        peak_load_2035.index.name = "building_id"

        # ######## determine HP capacity per building #########
        logger.info(f"MVGD={mvgd} | Determine HP capacities.")

        buildings_decentral_heating = (
            get_buildings_with_decentral_heat_demand_in_mv_grid(
                mvgd, scenario=scenario
            )
        )

        # Reduce list of decentral heating if no Peak load available
        # TODO maybe remove after succesfull DE run
        # Might be fixed in #990
        buildings_decentral_heating = catch_missing_buidings(
            buildings_decentral_heating, peak_load_2035
        )

        hp_cap_per_building_2035 = determine_hp_cap_buildings_pvbased_per_mvgd(
            scenario,
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
                "scenario": [scenario, scenario],
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

    df_hp_cap_per_building_2035_db["scenario"] = scenario

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


def determine_hp_cap_peak_load_mvgd_ts_status_quo(mvgd_ids, scenario):
    """
    Main function to determine HP capacity per building in status quo scenario.
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
    df_hp_cap_per_building_status_quo_db = pd.DataFrame()
    df_heat_mvgd_ts_db = pd.DataFrame()

    for mvgd in mvgd_ids:
        logger.info(f"MVGD={mvgd} | Start")

        # ############# aggregate residential and CTS demand profiles #####

        df_heat_ts = aggregate_residential_and_cts_profiles(
            mvgd, scenario=scenario
        )

        # ##################### determine peak loads ###################
        logger.info(f"MVGD={mvgd} | Determine peak loads.")

        peak_load_status_quo = df_heat_ts.max().rename(scenario)
        # If df_heat_ts has no columns (mvgd has no decentral heating
        # buildings), the index name is lost and reset_index() below
        # would create a stray "index" column instead of "building_id",
        # which export_to_db's melt() then turns into bogus scenario
        # rows with NULL peak loads.
        peak_load_status_quo.index.name = "building_id"

        # ######## determine HP capacity per building #########
        logger.info(f"MVGD={mvgd} | Determine HP capacities.")

        buildings_decentral_heating = (
            get_buildings_with_decentral_heat_demand_in_mv_grid(
                mvgd, scenario=scenario
            )
        )

        # Reduce list of decentral heating if no Peak load available
        # TODO maybe remove after succesfull DE run
        # Might be fixed in #990
        buildings_decentral_heating = catch_missing_buidings(
            buildings_decentral_heating, peak_load_status_quo
        )

        hp_cap_per_building_status_quo = (
            determine_hp_cap_buildings_pvbased_per_mvgd(
                scenario,
                mvgd,
                peak_load_status_quo,
                buildings_decentral_heating,
            )
        )

        # ################ aggregated heat profiles ###################
        logger.info(f"MVGD={mvgd} | Aggregate heat profiles.")

        df_mvgd_ts_status_quo_hp = df_heat_ts.loc[
            :,
            hp_cap_per_building_status_quo.index,
        ].sum(axis=1)

        df_heat_mvgd_ts = pd.DataFrame(
            data={
                "carrier": "heat_pump",
                "bus_id": mvgd,
                "scenario": scenario,
                "dist_aggregated_mw": [df_mvgd_ts_status_quo_hp.to_list()],
            }
        )

        # ################ collect results ##################
        logger.info(f"MVGD={mvgd} | Collect results.")

        df_peak_loads_db = pd.concat(
            [df_peak_loads_db, peak_load_status_quo.reset_index()],
            axis=0,
            ignore_index=True,
        )

        df_heat_mvgd_ts_db = pd.concat(
            [df_heat_mvgd_ts_db, df_heat_mvgd_ts], axis=0, ignore_index=True
        )

        df_hp_cap_per_building_status_quo_db = pd.concat(
            [
                df_hp_cap_per_building_status_quo_db,
                hp_cap_per_building_status_quo.reset_index(),
            ],
            axis=0,
        )

    # ################ export to db #######################
    logger.info(f"MVGD={min(mvgd_ids)} : {max(mvgd_ids)} | Write data to db.")

    export_to_db(df_peak_loads_db, df_heat_mvgd_ts_db, drop=False)

    df_hp_cap_per_building_status_quo_db["scenario"] = scenario

    # TODO debug duplicated building_ids
    duplicates = df_hp_cap_per_building_status_quo_db.loc[
        df_hp_cap_per_building_status_quo_db.duplicated(
            "building_id", keep=False
        )
    ]

    if not duplicates.empty:
        logger.info(
            f"Dropped duplicated buildings: "
            f"{duplicates.loc[:,['building_id', 'hp_capacity']]}"
        )

    df_hp_cap_per_building_status_quo_db.drop_duplicates(
        "building_id", inplace=True
    )

    df_hp_cap_per_building_status_quo_db.building_id = (
        df_hp_cap_per_building_status_quo_db.building_id.astype(int)
    )

    write_table_to_postgres(
        df_hp_cap_per_building_status_quo_db,
        EgonHpCapacityBuildings,
        drop=False,
    )


def split_mvgds_into_bulks(n, max_n, func, scenario=None):
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

    if scenario is not None:
        func(mvgd_ids, scenario=scenario)
    else:
        func(mvgd_ids)


def delete_hp_capacity(scenario):
    """Remove all hp capacities for the selected scenario

    Parameters
    -----------
    scenario : string
        Name of the scenario.

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
        Name of the scenario.

    """

    with db.session_scope() as session:
        # Buses
        session.query(EgonEtragoTimeseriesIndividualHeating).filter(
            EgonEtragoTimeseriesIndividualHeating.scenario == scenario
        ).delete(synchronize_session=False)


def delete_hp_capacity_status_quo(scenario):
    """Remove all hp capacities for the selected status quo"""
    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)
    delete_hp_capacity(scenario=scenario)


def delete_hp_capacity_2035(scenario):
    """Remove all hp capacities for the selected scenario"""
    EgonHpCapacityBuildings.__table__.create(bind=engine, checkfirst=True)
    delete_hp_capacity(scenario=scenario)


def delete_mvgd_ts_status_quo(scenario):
    """Remove all mvgd ts for the selected status quo"""
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    delete_mvgd_ts(scenario=scenario)


def delete_mvgd_ts_2035(scenario):
    """Remove all mvgd ts for the selected scenario"""
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    delete_mvgd_ts(scenario=scenario)


def delete_heat_peak_loads_status_quo(scenario):
    """Remove all heat peak loads for status quo."""
    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)
    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.scenario == scenario
        ).delete(synchronize_session=False)


def delete_heat_peak_loads_2035(scenario):
    """Remove all heat peak loads for the selected scenario."""
    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)
    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.scenario == scenario
        ).delete(synchronize_session=False)


