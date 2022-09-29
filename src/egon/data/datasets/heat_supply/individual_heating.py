"""The central module containing all code dealing with
individual heat supply.

"""
from loguru import logger
import numpy as np
import pandas as pd
import random
import saio

from pathlib import Path
import time

from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import ARRAY, REAL, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd


from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand_timeseries.cts_buildings import (
    calc_cts_building_profiles,
    CtsBuildings,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    write_table_to_postgres,
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


class EgonEtragoTimeseriesIndividualHeating(Base):
    __tablename__ = "egon_etrago_timeseries_individual_heating"
    __table_args__ = {"schema": "demand"}
    bus_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    carrier = Column(String, primary_key=True)
    dist_aggregated_mw = Column(ARRAY(REAL))


class HeatPumpsPypsaEurSecAnd2035(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatPumpsPypsaEurSecAnd2035",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(create_peak_load_table,
                   delete_peak_loads_if_existing,
                   {determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_1,
                    determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_2,
                    determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_3,
                    determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_4,
                    determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_5,
                    }
                   ),
        )


class HeatPumps2050(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatPumps2050",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(determine_hp_cap_buildings_eGon100RE,),
        )


class BuildingHeatPeakLoads(Base):
    __tablename__ = "egon_building_heat_peak_loads"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    sector = Column(String, primary_key=True)
    peak_load_in_w = Column(REAL)


def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def log_to_file(name):
    """Simple only file logger"""
    logger.remove()
    logger.add(
        Path(f"{name}.log"),
        format="{time} {level} {message}",
        # filter="my_module",
        level="TRACE",
    )
    logger.trace("Start trace logging")
    return logger


def timeit(func):
    """
    Decorator for measuring function's running time.
    """

    def measure_time(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        print(
            "Processing time of %s(): %.2f seconds."
            % (func.__qualname__, time.time() - start_time)
        )
        return result

    return measure_time


def timeitlog(func):
    """
    Decorator for measuring running time of residential heat peak load and
    logging it.
    """

    def measure_time(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        process_time = time.time() - start_time
        try:
            mvgd = kw["mvgd"]
        except KeyError:
            mvgd = "bulk"
        statement = (
            f"MVGD={mvgd} | Processing time of {func.__qualname__} | "
            f"{time.strftime('%H h, %M min, %S s', time.gmtime(process_time))}"
        )
        logger.trace(statement)
        print(statement)
        return result

    return measure_time


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
    2.) If the supply can not  meet the heat demand, solar thermal collectors
        are attached. This is not implemented yet, since individual
        solar thermal plants are not considered in eGon2035 scenario.
    3.) If this is not suitable, the mv grid is also supplied by heat pumps.
    4.) The last option are individual gas boilers.

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
    # TODO: Add gas boilers and solar themal (eGon100RE)
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
        resulting_capacities = resulting_capacities.append(
            append_df, ignore_index=True
        )

    if plotting:
        plot_heat_supply(resulting_capacities)

    return gpd.GeoDataFrame(
        resulting_capacities,
        geometry=geom_mv[resulting_capacities.mv_grid_id].values,
    )


# @timeit
def get_peta_demand(mvgd):
    """only residential"""

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonPetaHeat.demand.label("peta_2035"),
            )
            .filter(MapZensusGridDistricts.bus_id == mvgd)
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .filter(EgonPetaHeat.scenario == "eGon2035")
            .filter(EgonPetaHeat.sector == "residential")
        )

    df_peta_2035 = pd.read_sql(
        query.statement, query.session.bind, index_col="zensus_population_id"
    )

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonPetaHeat.demand.label("peta_2050"),
            )
            .filter(MapZensusGridDistricts.bus_id == mvgd)
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .filter(EgonPetaHeat.scenario == "eGon100RE")
            .filter(EgonPetaHeat.sector == "residential")
        )

    df_peta_100RE = pd.read_sql(
        query.statement, query.session.bind, index_col="zensus_population_id"
    )

    df_peta_demand = pd.concat(
        [df_peta_2035, df_peta_100RE], axis=1
    ).reset_index()

    return df_peta_demand


# @timeit
def get_profile_ids(mvgd):
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

    df_profiles_ids = df_profiles_ids.explode("selected_idp_profiles")
    df_profiles_ids["day_of_year"] = (
        df_profiles_ids.groupby("building_id").cumcount() + 1
    )
    return df_profiles_ids


# @timeit
def get_daily_profiles(profile_ids):
    saio.register_schema("demand", db.engine())
    from saio.demand import egon_heat_idp_pool

    with db.session_scope() as session:
        query = session.query(egon_heat_idp_pool).filter(
            egon_heat_idp_pool.index.in_(profile_ids)
        )

    df_profiles = pd.read_sql(
        query.statement, query.session.bind, index_col="index"
    )

    df_profiles = df_profiles.explode("idp")
    df_profiles["hour"] = df_profiles.groupby(axis=0, level=0).cumcount() + 1

    return df_profiles


# @timeit
def get_daily_demand_share(mvgd):

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonDailyHeatDemandPerClimateZone.day_of_year,
                EgonDailyHeatDemandPerClimateZone.daily_demand_share,
            )
            .filter(
                EgonMapZensusClimateZones.climate_zone
                == EgonDailyHeatDemandPerClimateZone.climate_zone
            )
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonMapZensusClimateZones.zensus_population_id
            )
            .filter(MapZensusGridDistricts.bus_id == mvgd)
        )

    df_daily_demand_share = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    )
    return df_daily_demand_share


@timeitlog
def calc_residential_heat_profiles_per_mvgd(mvgd):
    """
    Gets residential heat profiles per building in MV grid for both eGon2035 and
    eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.

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
            * eGon2035 : float
                Building's residential heat demand in MW, for specified hour of the
                year (specified through columns `day_of_year` and `hour`).
            * eGon100RE : float
                Building's residential heat demand in MW, for specified hour of the
                year (specified through columns `day_of_year` and `hour`).

    """
    df_peta_demand = get_peta_demand(mvgd)

    if df_peta_demand.empty:
        return None

    df_profiles_ids = get_profile_ids(mvgd)

    if df_profiles_ids.empty:
        return None

    df_profiles = get_daily_profiles(
        df_profiles_ids["selected_idp_profiles"].unique()
    )

    df_daily_demand_share = get_daily_demand_share(mvgd)

    # Merge profile ids to peta demand by zensus_population_id
    df_profile_merge = pd.merge(
        left=df_peta_demand, right=df_profiles_ids, on="zensus_population_id"
    )

    # Merge daily demand to daily profile ids by zensus_population_id and day
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_daily_demand_share,
        on=["zensus_population_id", "day_of_year"],
    )

    # Merge daily profiles by profile id
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_profiles[["idp", "hour"]],
        left_on="selected_idp_profiles",
        right_index=True,
    )

    # Scale profiles
    df_profile_merge["eGon2035"] = (
        df_profile_merge["idp"]
        .mul(df_profile_merge["daily_demand_share"])
        .mul(df_profile_merge["peta_2035"])
        .div(df_profile_merge["buildings"])
    )

    df_profile_merge["eGon100RE"] = (
        df_profile_merge["idp"]
        .mul(df_profile_merge["daily_demand_share"])
        .mul(df_profile_merge["peta_2050"])
        .div(df_profile_merge["buildings"])
    )

    columns = ["zensus_population_id", "building_id", "day_of_year", "hour",
               "eGon2035", "eGon100RE"]

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


@timeit
def get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id):
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
        Zensus cell IDs (as int) of buildings with decentral heating systems in given
        MV grid. Type is pandas Index to avoid errors later on when it is
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

    # convert to pd.Index (otherwise type is np.int64, which will for some
    # reason throw an error when used in a query)
    zensus_population_ids = pd.Index(zensus_population_ids)

    # get zensus cells with district heating
    from egon.data.datasets.district_heating_areas import (
        MapZensusDistrictHeatingAreas,
    )

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
    return zensus_population_ids


@timeit
def get_residential_buildings_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id):
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
        Building IDs (as int) of buildings with decentral heating system in given
        MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """
    # get zensus cells with decentral heating
    zensus_population_ids = get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
        scenario, mv_grid_id)

    # get buildings with decentral heat demand
    engine = db.engine()
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


@timeit
def get_cts_buildings_with_decentral_heat_demand_in_mv_grid(
    scenario, mv_grid_id):
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
        Building IDs (as int) of buildings with decentral heating system in given
        MV grid. Type is pandas Index to avoid errors later on when it is
        used in a query.

    """

    # get zensus cells with decentral heating
    zensus_population_ids = get_zensus_cells_with_decentral_heat_demand_in_mv_grid(
        scenario, mv_grid_id)

    # get buildings with decentral heat demand
    # ToDo @Julian, sind das alle CTS buildings in der Tabelle?
    with db.session_scope() as session:
        query = session.query(
            CtsBuildings.id,
        ).filter(
            CtsBuildings.zensus_population_id.in_(
                zensus_population_ids
            )
        )

    buildings_with_heat_demand = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).id.values

    return pd.Index(buildings_with_heat_demand)


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
    ).capacity.values[0]

    return hp_cap_mv_grid


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
    engine = db.engine()
    saio.register_schema("supply", engine)
    # TODO Adhoc Pv rooftop fix
    # from saio.supply import egon_power_plants_pv_roof_building
    #
    # with db.session_scope() as session:
    #     query = session.query(
    #         egon_power_plants_pv_roof_building.building_id
    #     ).filter(
    #         egon_power_plants_pv_roof_building.building_id.in_(building_ids)
    #     )
    #
    # buildings_with_pv = pd.read_sql(
    #     query.statement, query.session.bind, index_col=None
    # ).building_id.values
    buildings_with_pv = []
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

    # choose random heat pumps until remaining heat pumps are larger than remaining
    # heat pump capacity
    remaining_hp_cap = (
        hp_cap_mv_grid - min_hp_cap_per_building.loc[buildings_with_hp].sum())
    min_cap_buildings_wo_hp = min_hp_cap_per_building.loc[
        building_ids.drop(buildings_with_hp)]
    possible_buildings = min_cap_buildings_wo_hp[
        min_cap_buildings_wo_hp <= remaining_hp_cap].index
    while len(possible_buildings) > 0:
        random.seed(db.credentials()["--random-seed"])
        new_hp_building = random.choice(possible_buildings)
        # add new building to building with HP
        buildings_with_hp = buildings_with_hp.append(pd.Index([new_hp_building]))
        # determine if there are still possible buildings
        remaining_hp_cap = (
            hp_cap_mv_grid - min_hp_cap_per_building.loc[buildings_with_hp].sum())
        min_cap_buildings_wo_hp = min_hp_cap_per_building.loc[
            building_ids.drop(buildings_with_hp)]
        possible_buildings = min_cap_buildings_wo_hp[
            min_cap_buildings_wo_hp <= remaining_hp_cap].index

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
    return hp_cap_per_building


def determine_min_hp_cap_pypsa_eur_sec(peak_heat_demand, building_ids):
    """
    Determines minimum required HP capacity in MV grid in MW as input for
    pypsa-eur-sec.

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    building_ids : pd.Index(int)
        Building IDs (as int) of buildings with decentral heating system in given
        MV grid.

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


def determine_hp_cap_buildings_eGon2035(mv_grid_id, peak_heat_demand, building_ids):
    """
    Determines which buildings in the MV grid will have a HP (buildings with PV
    rooftop are more likely to be assigned) in the eGon2035 scenario, as well as
    their respective HP capacity in MW.

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

    if len(building_ids) > 0:
        peak_heat_demand = peak_heat_demand.loc[building_ids]

        # determine minimum required heat pump capacity per building
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            peak_heat_demand
        )

        # select buildings that will have a heat pump
        hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(
            "eGon2035", mv_grid_id
        )
        buildings_with_hp = determine_buildings_with_hp_in_mv_grid(
            hp_cap_grid, min_hp_cap_buildings
        )

        # distribute total heat pump capacity to all buildings with HP
        hp_cap_per_building = desaggregate_hp_capacity(
            min_hp_cap_buildings.loc[buildings_with_hp], hp_cap_grid
        )

        return hp_cap_per_building

    else:
        return pd.Series()


def determine_hp_cap_buildings_eGon100RE(mv_grid_id):
    """
    Main function to determine HP capacity per building in eGon100RE scenario.

    In eGon100RE scenario all buildings without district heating get a heat pump.

    """

    # determine minimum required heat pump capacity per building
    building_ids = get_buildings_with_decentral_heat_demand_in_mv_grid(
        "eGon100RE", mv_grid_id
    )

    # TODO get peak demand from db
    peak_heat_demand = get_peak_demand_per_building(
        "eGon100RE", building_ids
    )

    # determine minimum required heat pump capacity per building
    min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
        peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
    )

    # distribute total heat pump capacity to all buildings with HP
    hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(
        "eGon100RE", mv_grid_id
    )
    hp_cap_per_building = desaggregate_hp_capacity(
        min_hp_cap_buildings, hp_cap_grid
    )

    # ToDo Julian Write desaggregated HP capacity to table (same as for 2035 scenario)


@timeitlog
def determine_hp_capacity_eGon2035_pypsa_eur_sec(n, max_n=5):
    """
    Main function to determine HP capacity per building in eGon2035 scenario and
    minimum required HP capacity in MV for pypsa-eur-sec.
    Further, creates heat demand time series for all buildings with heat pumps
    (in eGon2035 and eGon100RE scenario) in MV grid, as well as for all buildings
    with gas boilers (only in eGon2035scenario), used in eTraGo.

    Parameters
    -----------
    n : int
        Number between [1;max_n].
    max_n : int
        Maximum number of bulks (MV grid sets run in parallel).

    """

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    log_to_file(determine_hp_capacity_eGon2035_pypsa_eur_sec.__qualname__ + f"_{n}")
    if n == 0:
        raise KeyError("n >= 1")

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
    mvgd_ids = pd.read_sql(query.statement, query.session.bind, index_col=None)

    mvgd_ids = mvgd_ids.sort_values("bus_id").reset_index(drop=True)

    mvgd_ids = np.array_split(mvgd_ids["bus_id"].values, max_n)

    # TODO mvgd_ids = [kleines mvgd]
    for mvgd in [1556]: #mvgd_ids[n - 1]:

        logger.trace(f"MVGD={mvgd} | Start")

        # ############### get residential heat demand profiles ###############
        df_heat_ts = calc_residential_heat_profiles_per_mvgd(
            mvgd=mvgd
        )

        # pivot to allow aggregation with CTS profiles
        df_heat_ts_2035 = df_heat_ts.loc[
                          :, ["building_id", "day_of_year", "hour", "eGon2035"]]
        df_heat_ts_2035 = df_heat_ts_2035.pivot(
            index=["day_of_year", "hour"],
            columns="building_id",
            values="eGon2035",
        )
        df_heat_ts_2035 = df_heat_ts_2035.sort_index().reset_index(drop=True)

        df_heat_ts_100RE = df_heat_ts.loc[
                          :, ["building_id", "day_of_year", "hour", "eGon100RE"]]
        df_heat_ts_100RE = df_heat_ts_100RE.pivot(
            index=["day_of_year", "hour"],
            columns="building_id",
            values="eGon100RE",
        )
        df_heat_ts_100RE = df_heat_ts_100RE.sort_index().reset_index(drop=True)

        del df_heat_ts

        # ############### get CTS heat demand profiles ###############
        heat_demand_cts_ts_2035 = calc_cts_building_profiles(
            bus_ids=[mvgd],
            scenario="eGon2035",
            sector="heat",
        )
        heat_demand_cts_ts_100RE = calc_cts_building_profiles(
            bus_ids=[mvgd],
            scenario="eGon100RE",
            sector="heat",
        )

        # ############# aggregate residential and CTS demand profiles #############
        df_heat_ts_2035 = pd.concat(
            [df_heat_ts_2035, heat_demand_cts_ts_2035], axis=1
        )
        df_heat_ts_2035 = df_heat_ts_2035.groupby(axis=1, level=0).sum()

        df_heat_ts_100RE = pd.concat(
            [df_heat_ts_100RE, heat_demand_cts_ts_100RE], axis=1
        )
        df_heat_ts_100RE = df_heat_ts_100RE.groupby(axis=1, level=0).sum()

        del heat_demand_cts_ts_2035, heat_demand_cts_ts_100RE

        # ##################### export peak loads to DB ###################

        df_peak_loads_2035 = df_heat_ts_2035.max()
        df_peak_loads_100RE = df_heat_ts_100RE.max()

        df_peak_loads_db_2035 = df_peak_loads_2035.reset_index().melt(
            id_vars="building_id",
            var_name="scenario",
            value_name="peak_load_in_w",
        )
        df_peak_loads_db_2035["scenario"] = "eGon2035"
        df_peak_loads_db_100RE = df_peak_loads_100RE.reset_index().melt(
            id_vars="building_id",
            var_name="scenario",
            value_name="peak_load_in_w",
        )
        df_peak_loads_db_100RE["scenario"] = "eGon100RE"
        df_peak_loads_db = pd.concat(
            [df_peak_loads_db_2035, df_peak_loads_db_100RE])

        del df_peak_loads_db_2035, df_peak_loads_db_100RE

        df_peak_loads_db["sector"] = "residential+CTS"
        # From MW to W
        df_peak_loads_db["peak_load_in_w"] = df_peak_loads_db["peak_load_in_w"] * 1e6

        logger.trace(f"MVGD={mvgd} | Export to DB")

        # TODO export peak loads all buildings both scenarios to db
        # write_table_to_postgres(
        #     df_peak_loads_db, BuildingHeatPeakLoads, engine=engine
        # )
        # logger.trace(f"MVGD={mvgd} | Done")

        # ######## determine HP capacity for NEP scenario and pypsa-eur-sec ##########

        # get residential buildings with decentral heating systems in both scenarios
        buildings_decentral_heating_2035_res = (
            get_residential_buildings_with_decentral_heat_demand_in_mv_grid(
                "eGon2035", mvgd
            )
        )
        buildings_decentral_heating_100RE_res = (
            get_residential_buildings_with_decentral_heat_demand_in_mv_grid(
                "eGon100RE", mvgd
            )
        )

        # get CTS buildings with decentral heating systems in both scenarios
        buildings_decentral_heating_2035_cts = (
            get_cts_buildings_with_decentral_heat_demand_in_mv_grid(
                "eGon2035", mvgd
            )
        )
        buildings_decentral_heating_100RE_cts = (
            get_cts_buildings_with_decentral_heat_demand_in_mv_grid(
                "eGon100RE", mvgd
            )
        )

        # merge residential and CTS buildings
        buildings_decentral_heating_2035 = (
            buildings_decentral_heating_2035_res.append(
                buildings_decentral_heating_2035_cts
            ).unique()
        )
        buildings_decentral_heating_100RE = (
            buildings_decentral_heating_100RE_res.append(
                buildings_decentral_heating_100RE_cts
            ).unique()
        )

        # determine HP capacity per building for NEP2035 scenario
        hp_cap_per_building_2035 = determine_hp_cap_buildings_eGon2035(
            mvgd, df_peak_loads_2035, buildings_decentral_heating_2035)
        buildings_hp_2035 = hp_cap_per_building_2035.index
        buildings_gas_2035 = pd.Index(buildings_decentral_heating_2035).drop(
            buildings_hp_2035)

        # determine minimum HP capacity per building for pypsa-eur-sec
        hp_min_cap_mv_grid_pypsa_eur_sec = determine_min_hp_cap_pypsa_eur_sec(
            df_peak_loads_100RE, buildings_decentral_heating_100RE)

        # ######################## write HP capacities to DB ######################

        # ToDo Julian Write HP capacity per building in 2035 (hp_cap_per_building_2035) to
        #  db table - neue Tabelle egon_hp_capacity_buildings

        # ToDo Julian Write minimum required capacity in pypsa-eur-sec
        #  (hp_min_cap_mv_grid_pypsa_eur_sec) to
        #  csv for pypsa-eur-sec input - im working directory gibt es directory
        #  input_pypsa_eur_sec - minimum_hp_capacity_mv_grid.csv

        # ################ write aggregated heat profiles to DB ###################

        # heat demand time series for buildings with heat pumps

        # ToDo Julian Write aggregated heat demand time series of buildings with HP to
        #  table to be used in eTraGo - egon_etrago_timeseries_individual_heating
        # TODO Clara uses this table already
        #     but will not need it anymore for eTraGo
        # EgonEtragoTimeseriesIndividualHeating
        df_heat_ts_2035.loc[:, buildings_hp_2035].sum(axis=1) # carrier heat_pump
        df_heat_ts_100RE.loc[:, buildings_decentral_heating_100RE].sum(axis=1) # carrier heat_pump

        # Change format
        # ToDo Julian
        # data = CTS_grid.drop(columns="scenario")
        # df_etrago_cts_heat_profiles = pd.DataFrame(
        #     index=data.index, columns=["scn_name", "p_set"]
        # )
        # df_etrago_cts_heat_profiles.p_set = data.values.tolist()
        # df_etrago_cts_heat_profiles.scn_name = CTS_grid["scenario"]
        # df_etrago_cts_heat_profiles.reset_index(inplace=True)

        # # Drop and recreate Table if exists
        # EgonEtragoTimeseriesIndividualHeating.__table__.drop(bind=db.engine(),
        #                                                      checkfirst=True)
        # EgonEtragoTimeseriesIndividualHeating.__table__.create(bind=db.engine(),
        #                                                        checkfirst=True)
        #
        # # Write heat ts into db
        # with db.session_scope() as session:
        #     session.bulk_insert_mappings(
        #         EgonEtragoTimeseriesIndividualHeating,
        #         df_etrago_cts_heat_profiles.to_dict(orient="records"),
        #     )

        # heat demand time series for buildings with gas boilers (only 2035 scenario)
        df_heat_ts_2035.loc[:, buildings_gas_2035].sum(axis=1) # carrier gas_boilers
        # ToDo Julian Write heat demand time series for buildings with gas boiler to
        #  database - in gleiche Tabelle wie Zeitreihen für WP Gebäude, falls Clara
        #  nichts anderes sagt; wird später weiter aggregiert nach gas voronoi
        #  (grid.egon_gas_voronoi mit carrier CH4) von Clara oder Amélia


def determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_1():
    determine_hp_capacity_eGon2035_pypsa_eur_sec(1, max_n=5)


def determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_2():
    determine_hp_capacity_eGon2035_pypsa_eur_sec(2, max_n=5)


def determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_3():
    determine_hp_capacity_eGon2035_pypsa_eur_sec(3, max_n=5)


def determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_4():
    determine_hp_capacity_eGon2035_pypsa_eur_sec(4, max_n=5)


def determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_5():
    determine_hp_capacity_eGon2035_pypsa_eur_sec(5, max_n=5)


def create_peak_load_table():

    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)


def delete_peak_loads_if_existing():
    """Remove all entries"""

    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.sector == "residential"
        ).delete(synchronize_session=False)


if __name__ == "__main__":
    determine_hp_capacity_eGon2035_pypsa_eur_sec_bulk_1()
