import numpy as np
import pandas as pd
import saio

from egon.data import db

saio.register_schema("demand", db.engine())
from pathlib import Path
import time

from loguru import logger
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import REAL, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

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
def calc_residential_heat_profiles_per_mvgd(
    mvgd, pivot=False, max_value=False, cum_max=False
):
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

    # if max_value:
    #     df_max_value = pd.concat(
    #         [
    #             df_profile_merge.groupby("building_id")["eGon2035"].max(),
    #             df_profile_merge.groupby("building_id")["eGon100RE"].max(),
    #         ],
    #         axis=1,
    #     )
    #     return df_max_value
    #
    # # both scenarios for etrago
    # # different district heating buildings in scenarios?
    # # gas boiler removed from cummulated profile in 2035
    # if cum_max:
    #     df_cum_max = pd.Series(
    #         [
    #             df_profile_merge.groupby(["day_of_year", "hour"])["eGon2035"]
    #             .sum()
    #             .max(),
    #             df_profile_merge.groupby(["day_of_year", "hour"])["eGon100RE"]
    #             .sum()
    #             .max(),
    #         ],
    #         index=["cum_peak_2035", "cum_peak_2050"],
    #     )
    #     return df_cum_max
    #
    # if pivot:
    #     # Reformat
    #     df_profile_merge = df_profile_merge.pivot(
    #         index=["day_of_year", "hour"],
    #         columns="building_id",
    #         values=["eGon2035", "eGon100RE"],
    #     )

    return df_profile_merge


@timeitlog
def residential_heat_peak_load_export_bulk(n, max_n=5):
    """n= [1;max_n]"""

    # ========== Register np datatypes with SQLA ==========
    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

    log_to_file(residential_heat_peak_load_export_bulk.__qualname__ + f"_{n}")
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
            .filter(EgonPetaHeat.sector == "residential")
            .distinct(MapZensusGridDistricts.bus_id)
        )
    mvgd_ids = pd.read_sql(query.statement, query.session.bind, index_col=None)

    mvgd_ids = mvgd_ids.sort_values("bus_id").reset_index(drop=True)

    mvgd_ids = np.array_split(mvgd_ids["bus_id"].values, max_n)

    # TODO mvgd_ids = [kleines mvgd]
    for mvgd in mvgd_ids[n - 1]:

        logger.trace(f"MVGD={mvgd} | Start")
        # TODO return timeseries without pivot
        df_peak_loads = calc_residential_heat_profiles_per_mvgd(
            mvgd=mvgd, pivot=False, max_value=True, cum_max=False
        )
        # TODO add CTS building profiles
        # TODO peak load all buildings both scenarios
        # TODO export peak loads all buildings both scenarios to db
        # TODO remove district heating buildings both scenarios maybe different
        # TODO desagg biggi > select gas buildings 2035
        # TODO remove gas buildings for scenario 2035
        # TODO calc cumulated time series for buildings with heatpumps both scenarios
        # TODO calc cumulated time series for buildings with gas boilers for 2035

        if df_peak_loads is None:
            continue

        df_peak_loads = df_peak_loads.reset_index().melt(
            id_vars="building_id",
            var_name="scenario",
            value_name="peak_load_in_w",
        )
        df_peak_loads["sector"] = "residential"
        # From MW to W
        df_peak_loads["peak_load_in_w"] = df_peak_loads["peak_load_in_w"] * 1e6
        logger.trace(f"MVGD={mvgd} | Export to DB")

        write_table_to_postgres(
            df_peak_loads, BuildingHeatPeakLoads, engine=engine
        )
        logger.trace(f"MVGD={mvgd} | Done")


def residential_heat_peak_load_export_bulk_1():
    residential_heat_peak_load_export_bulk(1, max_n=5)


def residential_heat_peak_load_export_bulk_2():
    residential_heat_peak_load_export_bulk(2, max_n=5)


def residential_heat_peak_load_export_bulk_3():
    residential_heat_peak_load_export_bulk(3, max_n=5)


def residential_heat_peak_load_export_bulk_4():
    residential_heat_peak_load_export_bulk(4, max_n=5)


def residential_heat_peak_load_export_bulk_5():
    residential_heat_peak_load_export_bulk(5, max_n=5)


def create_peak_load_table():

    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)


def delete_peak_loads_if_existing():
    """Remove all entries"""

    with db.session_scope() as session:
        # Buses
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.sector == "residential"
        ).delete(synchronize_session=False)
