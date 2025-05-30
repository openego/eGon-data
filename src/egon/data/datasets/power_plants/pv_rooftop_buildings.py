"""
Distribute MaStR PV rooftop capacities to OSM and synthetic buildings. Generate
new PV rooftop generators for scenarios eGon2035 and eGon100RE.

See documentation section :ref:`pv-rooftop-ref` for more information.

"""
from __future__ import annotations

from collections import Counter
from functools import wraps
from time import perf_counter
import datetime
import json

from geoalchemy2 import Geometry
from loguru import logger
from numpy.random import RandomState, default_rng
from omi.dialects import get_dialect
from pyproj.crs.crs import CRS
from sqlalchemy import BigInteger, Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    OsmBuildingsSynthetic,
)
from egon.data.datasets.power_plants.mastr_db_classes import EgonPowerPlantsPv
from egon.data.datasets.scenario_capacities import EgonScenarioCapacities
from egon.data.datasets.scenario_parameters import get_scenario_year
from egon.data.datasets.zensus_vg250 import Vg250Gem
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_dedl,
    license_odbl,
    meta_metadata,
    meta_metadata,
    sources,
)

engine = db.engine()
Base = declarative_base()
SEED = int(config.settings()["egon-data"]["--random-seed"])

# TODO: move to yml
MASTR_INDEX_COL = "gens_id"

EPSG = 4326
SRID = 3035

# data cleaning
MAX_REALISTIC_PV_CAP = 23500 / 10**3
MIN_REALISTIC_PV_CAP = 0.1 / 10**3

# show additional logging information
VERBOSE = False

# Number of quantiles
Q = 5

# Scenario Data
SCENARIOS = config.settings()["egon-data"]["--scenarios"]
SCENARIO_TIMESTAMP = {
    "status2019": pd.Timestamp("2020-01-01", tz="UTC"),
    "status2023": pd.Timestamp("2024-01-01", tz="UTC"),
    "eGon2035": pd.Timestamp("2035-01-01", tz="UTC"),
    "eGon100RE": pd.Timestamp("2050-01-01", tz="UTC"),
}
PV_ROOFTOP_LIFETIME = pd.Timedelta(20 * 365, unit="D")

# Example Modul Trina Vertex S TSM-400DE09M.08 400 Wp
# https://www.photovoltaik4all.de/media/pdf/92/64/68/Trina_Datasheet_VertexS_DE09-08_2021_A.pdf
MODUL_CAP = 0.4 / 10**3  # MWp
MODUL_SIZE = 1.096 * 1.754  # m²
PV_CAP_PER_SQ_M = MODUL_CAP / MODUL_SIZE

# Estimation of usable roof area
# Factor for the conversion of building area to roof area
# estimation mean roof pitch: 35°
# estimation usable roof share: 80%
# estimation that only the south side of the building is used for pv
# see https://mediatum.ub.tum.de/doc/%20969497/969497.pdf
# AREA_FACTOR = 1.221
# USABLE_ROOF_SHARE = 0.8
# SOUTH_SHARE = 0.5
# ROOF_FACTOR = AREA_FACTOR * USABLE_ROOF_SHARE * SOUTH_SHARE
ROOF_FACTOR = 0.5

CAP_RANGES = [
    (0, 30 / 10**3),
    (30 / 10**3, 100 / 10**3),
    (100 / 10**3, float("inf")),
]

MIN_BUILDING_SIZE = 10.0
UPPER_QUANTILE = 0.95
LOWER_QUANTILE = 0.05

COLS_TO_EXPORT = [
    "scenario",
    "bus_id",
    "building_id",
    "gens_id",
    "capacity",
    "orientation_uniform",
    "orientation_primary",
    "orientation_primary_angle",
    "voltage_level",
    "weather_cell_id",
]

# TODO
INCLUDE_SYNTHETIC_BUILDINGS = True
ONLY_BUILDINGS_WITH_DEMAND = True
TEST_RUN = False


def timer_func(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        total_time = end_time - start_time
        logger.debug(
            f"Function {func.__name__} took {total_time:.4f} seconds."
        )
        return result

    return timeit_wrapper


@timer_func
def mastr_data(
    index_col: str | int | list[str] | list[int],
) -> gpd.GeoDataFrame:
    """
    Read MaStR data from database.

    Parameters
    -----------
    index_col : str, int or list of str or int
        Column(s) to use as the row labels of the DataFrame.
    Returns
    -------
    pandas.DataFrame
        DataFrame containing MaStR data.
    """
    with db.session_scope() as session:
        query = session.query(EgonPowerPlantsPv).filter(
            EgonPowerPlantsPv.status == "InBetrieb",
            EgonPowerPlantsPv.site_type
            == ("Bauliche Anlagen (Hausdach, Gebäude und Fassade)"),
        )

    gdf = gpd.read_postgis(
        query.statement, query.session.bind, index_col=index_col
    ).drop(columns="id")

    logger.debug("MaStR data loaded.")

    return gdf


@timer_func
def clean_mastr_data(
    mastr_gdf: gpd.GeoDataFrame,
    max_realistic_pv_cap: int | float,
    min_realistic_pv_cap: int | float,
    seed: int,
) -> gpd.GeoDataFrame:
    """
    Clean the MaStR data from implausible data.

    * Drop MaStR ID duplicates.
    * Drop generators with implausible capacities.

    Parameters
    -----------
    mastr_gdf : pandas.DataFrame
        DataFrame containing MaStR data.
    max_realistic_pv_cap : int or float
        Maximum capacity, which is considered to be realistic.
    min_realistic_pv_cap : int or float
        Minimum capacity, which is considered to be realistic.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    Returns
    -------
    pandas.DataFrame
        DataFrame containing cleaned MaStR data.
    """
    init_len = len(mastr_gdf)

    # drop duplicates
    mastr_gdf = mastr_gdf.loc[~mastr_gdf.index.duplicated()]

    # drop generators without any capacity info
    # and capacity of zero
    # and if the capacity is > 23.5 MW, because
    # Germanies largest rooftop PV is 23 MW
    # https://www.iwr.de/news/groesste-pv-dachanlage-europas-wird-in-sachsen-anhalt-gebaut-news37379
    mastr_gdf = mastr_gdf.loc[
        ~mastr_gdf.capacity.isna()
        & (mastr_gdf.capacity <= max_realistic_pv_cap)
        & (mastr_gdf.capacity > min_realistic_pv_cap)
    ]

    # get consistent start-up date
    # randomly and weighted fill missing start-up dates
    pool = mastr_gdf.loc[
        ~mastr_gdf.commissioning_date.isna()
    ].commissioning_date.to_numpy()

    size = len(mastr_gdf) - len(pool)

    if size > 0:
        rng = default_rng(seed=seed)

        choice = rng.choice(
            pool,
            size=size,
            replace=False,
        )

        mastr_gdf.loc[mastr_gdf.commissioning_date.isna()] = mastr_gdf.loc[
            mastr_gdf.commissioning_date.isna()
        ].assign(commissioning_date=choice)

        logger.info(
            f"Randomly and weigthed added start-up date to {size} generators."
        )

    mastr_gdf = mastr_gdf.assign(
        commissioning_date=pd.to_datetime(
            mastr_gdf.commissioning_date, utc=True
        )
    )

    end_len = len(mastr_gdf)
    logger.debug(
        f"Dropped {init_len - end_len} "
        f"({((init_len - end_len) / init_len) * 100:g}%)"
        f" of {init_len} rows from MaStR DataFrame."
    )

    return mastr_gdf


@timer_func
def municipality_data() -> gpd.GeoDataFrame:
    """
    Get municipality data from eGo^n Database.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with municipality data.
    """
    with db.session_scope() as session:
        query = session.query(Vg250Gem.ags, Vg250Gem.geometry.label("geom"))

    return gpd.read_postgis(
        query.statement, query.session.bind, index_col="ags"
    )


@timer_func
def add_ags_to_gens(
    mastr_gdf: gpd.GeoDataFrame,
    municipalities_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Add information about AGS ID to generators.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data.
    municipalities_gdf : geopandas.GeoDataFrame
        GeoDataFrame with municipality data.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data
        with AGS ID added.
    """
    return mastr_gdf.sjoin(
        municipalities_gdf,
        how="left",
        predicate="intersects",
    ).rename(columns={"index_right": "ags"})


def drop_gens_outside_muns(
    mastr_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop all generators outside of municipalities.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data
        with generatos without an AGS ID dropped.
    """
    gdf = mastr_gdf.loc[~mastr_gdf.ags.isna()]

    logger.debug(
        f"{len(mastr_gdf) - len(gdf)} ("
        f"{(len(mastr_gdf) - len(gdf)) / len(mastr_gdf) * 100:g}%)"
        f" of {len(mastr_gdf)} values are outside of the municipalities"
        " and are therefore dropped."
    )

    return gdf


def load_mastr_data():
    """Read PV rooftop data from MaStR CSV
    Note: the source will be replaced as soon as the MaStR data is available
    in DB.

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data with geocoded locations.
    """
    mastr_gdf = mastr_data(
        MASTR_INDEX_COL,
    )

    clean_mastr_gdf = clean_mastr_data(
        mastr_gdf,
        max_realistic_pv_cap=MAX_REALISTIC_PV_CAP,
        min_realistic_pv_cap=MIN_REALISTIC_PV_CAP,
        seed=SEED,
    )

    municipalities_gdf = municipality_data()

    clean_mastr_gdf = add_ags_to_gens(clean_mastr_gdf, municipalities_gdf)

    return drop_gens_outside_muns(clean_mastr_gdf)


class OsmBuildingsFiltered(Base):
    """
    Class definition of table openstreetmap.osm_buildings_filtered.

    """
    __tablename__ = "osm_buildings_filtered"
    __table_args__ = {"schema": "openstreetmap"}

    osm_id = Column(BigInteger)
    amenity = Column(String)
    building = Column(String)
    name = Column(String)
    geom = Column(Geometry(srid=SRID), index=True)
    area = Column(Float)
    geom_point = Column(Geometry(srid=SRID), index=True)
    tags = Column(HSTORE)
    id = Column(BigInteger, primary_key=True, index=True)


@timer_func
def osm_buildings(
    to_crs: CRS,
) -> gpd.GeoDataFrame:
    """
    Read OSM buildings data from eGo^n Database.

    Parameters
    -----------
    to_crs : pyproj.crs.crs.CRS
        CRS to transform geometries to.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    """
    with db.session_scope() as session:
        query = session.query(
            OsmBuildingsFiltered.id,
            OsmBuildingsFiltered.area,
            OsmBuildingsFiltered.geom_point.label("geom"),
        )

    return gpd.read_postgis(
        query.statement, query.session.bind, index_col="id"
    ).to_crs(to_crs)


@timer_func
def synthetic_buildings(
    to_crs: CRS,
) -> gpd.GeoDataFrame:
    """
    Read synthetic buildings data from eGo^n Database.

    Parameters
    -----------
    to_crs : pyproj.crs.crs.CRS
        CRS to transform geometries to.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    """
    with db.session_scope() as session:
        query = session.query(
            OsmBuildingsSynthetic.id,
            OsmBuildingsSynthetic.area,
            OsmBuildingsSynthetic.geom_point.label("geom"),
        )

    return gpd.read_postgis(
        query.statement, query.session.bind, index_col="id"
    ).to_crs(to_crs)


@timer_func
def add_ags_to_buildings(
    buildings_gdf: gpd.GeoDataFrame,
    municipalities_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Add information about AGS ID to buildings.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    municipalities_gdf : geopandas.GeoDataFrame
        GeoDataFrame with municipality data.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data
        with AGS ID added.
    """
    return buildings_gdf.sjoin(
        municipalities_gdf,
        how="left",
        predicate="intersects",
    ).rename(columns={"index_right": "ags"})


def drop_buildings_outside_muns(
    buildings_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop all buildings outside of municipalities.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data
        with buildings without an AGS ID dropped.
    """
    gdf = buildings_gdf.loc[~buildings_gdf.ags.isna()]

    logger.debug(
        f"{len(buildings_gdf) - len(gdf)} "
        f"({(len(buildings_gdf) - len(gdf)) / len(buildings_gdf) * 100:g}%) "
        f"of {len(buildings_gdf)} values are outside of the municipalities "
        "and are therefore dropped."
    )

    return gdf


def egon_building_peak_loads():

    # use active scenario wich is closest to today
    scenario = sorted(SCENARIOS, key=get_scenario_year)[0]

    sql = f"""
    SELECT building_id
    FROM demand.egon_building_electricity_peak_loads
    WHERE scenario = '{scenario}'
    """

    return (
        db.select_dataframe(sql).building_id.astype(int).sort_values().unique()
    )


@timer_func
def load_building_data():
    """
    Read buildings from DB
    Tables:

    * `openstreetmap.osm_buildings_filtered` (from OSM)
    * `openstreetmap.osm_buildings_synthetic` (synthetic, created by us)

    Use column `id` for both as it is unique hence you concat both datasets.
    If INCLUDE_SYNTHETIC_BUILDINGS is False synthetic buildings will not be
    loaded.

    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with buildings without an
        AGS ID dropped.
    """

    municipalities_gdf = municipality_data()

    osm_buildings_gdf = osm_buildings(municipalities_gdf.crs)

    if INCLUDE_SYNTHETIC_BUILDINGS:
        synthetic_buildings_gdf = synthetic_buildings(municipalities_gdf.crs)

        buildings_gdf = gpd.GeoDataFrame(
            pd.concat(
                [
                    osm_buildings_gdf,
                    synthetic_buildings_gdf,
                ]
            ),
            geometry="geom",
            crs=osm_buildings_gdf.crs,
        ).rename(columns={"area": "building_area"})

        buildings_gdf.index = buildings_gdf.index.astype(int)

    else:
        buildings_gdf = osm_buildings_gdf.rename(
            columns={"area": "building_area"}
        )

    if ONLY_BUILDINGS_WITH_DEMAND:
        building_ids = egon_building_peak_loads()

        init_len = len(building_ids)

        building_ids = np.intersect1d(
            list(map(int, building_ids)),
            list(map(int, buildings_gdf.index.to_numpy())),
        )

        end_len = len(building_ids)

        logger.debug(
            f"{end_len/init_len * 100: g} % ({end_len} / {init_len}) "
            f"of buildings have peak load."
        )

        buildings_gdf = buildings_gdf.loc[building_ids]

    buildings_ags_gdf = add_ags_to_buildings(buildings_gdf, municipalities_gdf)

    buildings_ags_gdf = drop_buildings_outside_muns(buildings_ags_gdf)

    grid_districts_gdf = grid_districts(EPSG)

    federal_state_gdf = federal_state_data(grid_districts_gdf.crs)

    grid_federal_state_gdf = overlay_grid_districts_with_counties(
        grid_districts_gdf,
        federal_state_gdf,
    )

    buildings_overlay_gdf = add_overlay_id_to_buildings(
        buildings_ags_gdf,
        grid_federal_state_gdf,
    )

    logger.debug("Loaded buildings.")

    buildings_overlay_gdf = drop_buildings_outside_grids(buildings_overlay_gdf)

    # overwrite bus_id with data from new table
    sql = (
        "SELECT building_id, bus_id FROM "
        "boundaries.egon_map_zensus_mvgd_buildings"
    )
    map_building_bus_df = db.select_dataframe(sql)

    building_ids = np.intersect1d(
        list(map(int, map_building_bus_df.building_id.unique())),
        list(map(int, buildings_overlay_gdf.index.to_numpy())),
    )

    buildings_within_gdf = buildings_overlay_gdf.loc[building_ids]

    gdf = (
        buildings_within_gdf.reset_index()
        .drop(columns=["bus_id"])
        .merge(
            how="left",
            right=map_building_bus_df,
            left_on="id",
            right_on="building_id",
        )
        .drop(columns=["building_id"])
        .set_index("id")
        .sort_index()
    )

    return gdf[~gdf.index.duplicated(keep="first")]


@timer_func
def sort_and_qcut_df(
    df: pd.DataFrame | gpd.GeoDataFrame,
    col: str,
    q: int,
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Determine the quantile of a given attribute in a (Geo)DataFrame.
    Sort the (Geo)DataFrame in ascending order for the given attribute.

    Parameters
    -----------
    df : pandas.DataFrame or geopandas.GeoDataFrame
        (Geo)DataFrame to sort and qcut.
    col : str
        Name of the attribute to sort and qcut the (Geo)DataFrame on.
    q : int
        Number of quantiles.
    Returns
    -------
    pandas.DataFrame or gepandas.GeoDataFrame
        Sorted and qcut (Geo)DataFrame.
    """
    df = df.sort_values(col, ascending=True)

    return df.assign(
        quant=pd.qcut(
            df[col],
            q=q,
            labels=range(q),
        )
    )


@timer_func
def allocate_pv(
    q_mastr_gdf: gpd.GeoDataFrame,
    q_buildings_gdf: gpd.GeoDataFrame,
    seed: int,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Allocate the MaStR pv generators to the OSM buildings.
    This will determine a building for each pv generator if there are more
    buildings than generators within a given AGS. Primarily generators are
    distributed with the same qunatile as the buildings. Multiple assignment
    is excluded.

    Parameters
    -----------
    q_mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded and qcut MaStR data.
    q_buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing qcut OSM buildings data.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    Returns
    -------
    tuple with two geopandas.GeoDataFrame s
        GeoDataFrame containing MaStR data allocated to building IDs.
        GeoDataFrame containing building data allocated to MaStR IDs.
    """
    rng = default_rng(seed=seed)

    q_buildings_gdf = q_buildings_gdf.assign(gens_id=np.nan).sort_values(
        by=["ags", "quant"]
    )
    q_mastr_gdf = q_mastr_gdf.assign(building_id=np.nan).sort_values(
        by=["ags", "quant"]
    )

    ags_list = q_buildings_gdf.ags.unique()

    if TEST_RUN:
        ags_list = ags_list[:250]

    num_ags = len(ags_list)

    t0 = perf_counter()

    for count, ags in enumerate(ags_list):
        buildings = q_buildings_gdf.loc[q_buildings_gdf.ags == ags]
        gens = q_mastr_gdf.loc[q_mastr_gdf.ags == ags]

        len_build = len(buildings)
        len_gens = len(gens)

        if len_build < len_gens:
            gens = gens.sample(len_build, random_state=RandomState(seed=seed))
            logger.error(
                f"There are {len_gens} generators and only {len_build}"
                f" buildings in AGS {ags}. {len_gens - len(gens)} "
                "generators were truncated to match the amount of buildings."
            )

            assert len_build == len(gens)

        for quant in gens.quant.unique():
            q_buildings = buildings.loc[buildings.quant == quant]
            q_gens = gens.loc[gens.quant == quant]

            len_build = len(q_buildings)
            len_gens = len(q_gens)

            if len_build < len_gens:
                delta = len_gens - len_build

                logger.warning(
                    f"There are {len_gens} generators and only {len_build} "
                    f"buildings in AGS {ags} and quantile {quant}. {delta} "
                    f"buildings from AGS {ags} will be added randomly."
                )

                add_buildings = pd.Index(
                    rng.choice(
                        list(set(buildings.index) - set(q_buildings.index)),
                        size=delta,
                        replace=False,
                    )
                )

                chosen_buildings = q_buildings.index.union(add_buildings)

            else:
                chosen_buildings = rng.choice(
                    q_buildings.index,
                    size=len_gens,
                    replace=False,
                )

            q_buildings_gdf.loc[chosen_buildings, "gens_id"] = q_gens.index
            buildings = buildings.drop(chosen_buildings)

        if count % 500 == 0:
            logger.debug(
                f"Allocation of {count / num_ags * 100:g} % of AGS done. "
                f"It took {perf_counter() - t0:g} seconds."
            )

            t0 = perf_counter()

    assigned_buildings = q_buildings_gdf.loc[~q_buildings_gdf.gens_id.isna()]

    assert len(assigned_buildings) == len(assigned_buildings.gens_id.unique())

    q_mastr_gdf.loc[assigned_buildings.gens_id, "building_id"] = (
        assigned_buildings.index
    )

    assigned_gens = q_mastr_gdf.loc[~q_mastr_gdf.building_id.isna()]

    assert len(assigned_buildings) == len(assigned_gens)

    logger.debug("Allocated status quo generators to buildings.")

    return frame_to_numeric(q_mastr_gdf), frame_to_numeric(q_buildings_gdf)


def frame_to_numeric(
    df: pd.DataFrame | gpd.GeoDataFrame,
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Try to convert all columns of a DataFrame to numeric ignoring errors.

    Parameters
    ----------
    df : pandas.DataFrame or geopandas.GeoDataFrame
    Returns
    -------
    pandas.DataFrame or geopandas.GeoDataFrame
    """
    if str(df.index.dtype) == "object":
        df.index = pd.to_numeric(df.index, errors="ignore")

    for col in df.columns:
        if str(df[col].dtype) == "object":
            df[col] = pd.to_numeric(df[col], errors="ignore")

    return df


def validate_output(
    desagg_mastr_gdf: pd.DataFrame | gpd.GeoDataFrame,
    desagg_buildings_gdf: pd.DataFrame | gpd.GeoDataFrame,
) -> None:
    """
    Validate output.

    * Validate that there are exactly as many buildings with a pv system as
      there are pv systems with a building
    * Validate that the building IDs with a pv system are the same building
      IDs as assigned to the pv systems
    * Validate that the pv system IDs with a building are the same pv system
      IDs as assigned to the buildings

    Parameters
    -----------
    desagg_mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data allocated to building IDs.
    desagg_buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing building data allocated to MaStR IDs.
    """
    assert len(
        desagg_mastr_gdf.loc[~desagg_mastr_gdf.building_id.isna()]
    ) == len(desagg_buildings_gdf.loc[~desagg_buildings_gdf.gens_id.isna()])
    assert (
        np.sort(
            desagg_mastr_gdf.loc[
                ~desagg_mastr_gdf.building_id.isna()
            ].building_id.unique()
        )
        == np.sort(
            desagg_buildings_gdf.loc[
                ~desagg_buildings_gdf.gens_id.isna()
            ].index.unique()
        )
    ).all()
    assert (
        np.sort(
            desagg_mastr_gdf.loc[
                ~desagg_mastr_gdf.building_id.isna()
            ].index.unique()
        )
        == np.sort(
            desagg_buildings_gdf.loc[
                ~desagg_buildings_gdf.gens_id.isna()
            ].gens_id.unique()
        )
    ).all()

    logger.debug("Validated output.")


def drop_unallocated_gens(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop generators which did not get allocated.

    Parameters
    -----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data allocated to building IDs.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data with generators dropped which did
        not get allocated.
    """
    init_len = len(gdf)
    gdf = gdf.loc[~gdf.building_id.isna()]
    end_len = len(gdf)

    logger.debug(
        f"Dropped {init_len - end_len} "
        f"({((init_len - end_len) / init_len) * 100:g}%)"
        f" of {init_len} unallocated rows from MaStR DataFrame."
    )

    return gdf


@timer_func
def allocate_to_buildings(
    mastr_gdf: gpd.GeoDataFrame,
    buildings_gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Allocate status quo pv rooftop generators to buildings.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data with geocoded locations.
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with buildings without an
        AGS ID dropped.
    Returns
    -------
    tuple with two geopandas.GeoDataFrame s
        GeoDataFrame containing MaStR data allocated to building IDs.
        GeoDataFrame containing building data allocated to MaStR IDs.
    """
    logger.debug("Starting allocation of status quo.")

    q_mastr_gdf = sort_and_qcut_df(mastr_gdf, col="capacity", q=Q)
    q_buildings_gdf = sort_and_qcut_df(buildings_gdf, col="building_area", q=Q)

    desagg_mastr_gdf, desagg_buildings_gdf = allocate_pv(
        q_mastr_gdf, q_buildings_gdf, SEED
    )

    validate_output(desagg_mastr_gdf, desagg_buildings_gdf)

    return drop_unallocated_gens(desagg_mastr_gdf), desagg_buildings_gdf


@timer_func
def grid_districts(
    epsg: int,
) -> gpd.GeoDataFrame:
    """
    Load mv grid district geo data from eGo^n Database as
    geopandas.GeoDataFrame.

    Parameters
    -----------
    epsg : int
        EPSG ID to use as CRS.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing mv grid district ID and geo shapes data.
    """
    gdf = db.select_geodataframe(
        """
        SELECT bus_id, geom
        FROM grid.egon_mv_grid_district
        ORDER BY bus_id
        """,
        index_col="bus_id",
        geom_col="geom",
        epsg=epsg,
    )

    gdf.index = gdf.index.astype(int)

    logger.debug("Grid districts loaded.")

    return gdf


def scenario_data(
    carrier: str = "solar_rooftop",
    scenario: str = "eGon2035",
) -> pd.DataFrame:
    """
    Get scenario capacity data from eGo^n Database.

    Parameters
    -----------
    carrier : str
        Carrier type to filter table by.
    scenario : str
        Scenario to filter table by.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with scenario capacity data in GW.
    """
    with db.session_scope() as session:
        query = session.query(EgonScenarioCapacities).filter(
            EgonScenarioCapacities.carrier == carrier,
            EgonScenarioCapacities.scenario_name == scenario,
        )

    df = pd.read_sql(
        query.statement, query.session.bind, index_col="index"
    ).sort_index()

    logger.debug("Scenario capacity data loaded.")

    return df


class Vg250Lan(Base):
    """
    Class definition of table boundaries.vg250_lan.

    """
    __tablename__ = "vg250_lan"
    __table_args__ = {"schema": "boundaries"}

    id = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(String)
    ags = Column(String)
    sdv_ars = Column(String)
    gen = Column(String)
    bez = Column(String)
    ibz = Column(BigInteger)
    bem = Column(String)
    nbd = Column(String)
    sn_l = Column(String)
    sn_r = Column(String)
    sn_k = Column(String)
    sn_v1 = Column(String)
    sn_v2 = Column(String)
    sn_g = Column(String)
    fk_s3 = Column(String)
    nuts = Column(String)
    ars_0 = Column(String)
    ags_0 = Column(String)
    wsk = Column(String)
    debkg_id = Column(String)
    rs = Column(String)
    sdv_rs = Column(String)
    rs_0 = Column(String)
    geometry = Column(Geometry(srid=EPSG), index=True)


def federal_state_data(to_crs: CRS) -> gpd.GeoDataFrame:
    """
    Get feder state data from eGo^n Database.

    Parameters
    -----------
    to_crs : pyproj.crs.crs.CRS
        CRS to transform geometries to.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with federal state data.
    """
    with db.session_scope() as session:
        query = session.query(
            Vg250Lan.id, Vg250Lan.nuts, Vg250Lan.geometry.label("geom")
        )

        gdf = gpd.read_postgis(
            query.statement, session.connection(), index_col="id"
        ).to_crs(to_crs)

    logger.debug("Federal State data loaded.")

    return gdf


@timer_func
def overlay_grid_districts_with_counties(
    mv_grid_district_gdf: gpd.GeoDataFrame,
    federal_state_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Calculate the intersections of mv grid districts and counties.

    Parameters
    -----------
    mv_grid_district_gdf : gpd.GeoDataFrame
        GeoDataFrame containing mv grid district ID and geo shapes data.
    federal_state_gdf : gpd.GeoDataFrame
        GeoDataFrame with federal state data.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    """
    logger.debug(
        "Calculating intersection overlay between mv grid districts and "
        "counties. This may take a while..."
    )

    gdf = gpd.overlay(
        federal_state_gdf.to_crs(mv_grid_district_gdf.crs),
        mv_grid_district_gdf.reset_index(),
        how="intersection",
        keep_geom_type=True,
    )

    logger.debug("Done!")

    return gdf


@timer_func
def add_overlay_id_to_buildings(
    buildings_gdf: gpd.GeoDataFrame,
    grid_federal_state_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Add information about overlay ID to buildings.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    grid_federal_state_gdf : geopandas.GeoDataFrame
        GeoDataFrame with intersection shapes between counties and grid
        districts.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with overlay ID added.
    """
    gdf = (
        buildings_gdf.to_crs(grid_federal_state_gdf.crs)
        .sjoin(
            grid_federal_state_gdf,
            how="left",
            predicate="intersects",
        )
        .rename(columns={"index_right": "overlay_id"})
    )

    logger.debug("Added overlay ID to OSM buildings.")

    return gdf


def drop_buildings_outside_grids(
    buildings_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop all buildings outside of grid areas.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data
        with buildings without an bus ID dropped.
    """
    gdf = buildings_gdf.loc[~buildings_gdf.bus_id.isna()]

    logger.debug(
        f"{len(buildings_gdf) - len(gdf)} "
        f"({(len(buildings_gdf) - len(gdf)) / len(buildings_gdf) * 100:g}%) "
        f"of {len(buildings_gdf)} values are outside of the grid areas "
        "and are therefore dropped."
    )

    return gdf


def cap_per_bus_id(
    scenario: str,
) -> pd.DataFrame:
    """
    Get table with total pv rooftop capacity per grid district.

    Parameters
    -----------
    scenario : str
        Scenario name.
    Returns
    -------
    pandas.DataFrame
        DataFrame with total rooftop capacity per mv grid.
    """
    if "status" in scenario:
        sources = config.datasets()["solar_rooftop"]["sources"]

        sql = f"""
        SELECT bus_id, SUM(el_capacity) as capacity
        FROM {sources['power_plants']['schema']}.{sources['power_plants']['table']}
        WHERE carrier = 'solar_rooftop'
        AND scenario = '{scenario}'
        GROUP BY bus_id
        """

        df = db.select_dataframe(sql, index_col="bus_id")

    else:
        targets = config.datasets()["solar_rooftop"]["targets"]

        sql = f"""
        SELECT bus as bus_id, control, p_nom as capacity
        FROM {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE carrier = 'solar_rooftop'
        AND scn_name = '{scenario}'
        """

        df = db.select_dataframe(sql, index_col="bus_id")
        df = df.loc[df.control != "Slack"]

    return df


def determine_end_of_life_gens(
    mastr_gdf: gpd.GeoDataFrame,
    scenario_timestamp: pd.Timestamp,
    pv_rooftop_lifetime: pd.Timedelta,
) -> gpd.GeoDataFrame:
    """
    Determine if an old PV system has reached its end of life.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    scenario_timestamp : pandas.Timestamp
        Timestamp at which the scenario takes place.
    pv_rooftop_lifetime : pandas.Timedelta
        Average expected lifetime of PV rooftop systems.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data and info if the system
        has reached its end of life.
    """
    before = mastr_gdf.capacity.sum()

    mastr_gdf = mastr_gdf.assign(
        age=scenario_timestamp - mastr_gdf.commissioning_date
    )

    mastr_gdf = mastr_gdf.assign(
        end_of_life=pv_rooftop_lifetime < mastr_gdf.age
    )

    after = mastr_gdf.loc[~mastr_gdf.end_of_life].capacity.sum()

    logger.debug(
        f"Determined if pv rooftop systems reached their end of life.\nTotal "
        f"capacity: {before}\nActive capacity: {after}"
    )

    return mastr_gdf


def calculate_max_pv_cap_per_building(
    buildings_gdf: gpd.GeoDataFrame,
    mastr_gdf: gpd.GeoDataFrame,
    pv_cap_per_sq_m: float | int,
    roof_factor: float | int,
) -> gpd.GeoDataFrame:
    """
    Calculate the estimated maximum possible PV capacity per building.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    pv_cap_per_sq_m : float, int
        Average expected, installable PV capacity per square meter.
    roof_factor : float, int
        Average for PV usable roof area share.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with estimated maximum PV
        capacity.
    """
    gdf = (
        buildings_gdf.reset_index()
        .rename(columns={"index": "id"})
        .merge(
            mastr_gdf[
                [
                    "capacity",
                    "end_of_life",
                    "building_id",
                    "orientation_uniform",
                    "orientation_primary",
                    "orientation_primary_angle",
                ]
            ],
            how="left",
            left_on="id",
            right_on="building_id",
        )
        .set_index("id")
        .drop(columns="building_id")
    )

    return gdf.assign(
        max_cap=gdf.building_area.multiply(roof_factor * pv_cap_per_sq_m),
        end_of_life=gdf.end_of_life.fillna(True).astype(bool),
        bus_id=gdf.bus_id.astype(int),
    )


def calculate_building_load_factor(
    mastr_gdf: gpd.GeoDataFrame,
    buildings_gdf: gpd.GeoDataFrame,
    rounding: int = 4,
) -> gpd.GeoDataFrame:
    """
    Calculate the roof load factor from existing PV systems.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    rounding : int
        Rounding to use for load factor.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data with calculated load
        factor.
    """
    gdf = mastr_gdf.merge(
        buildings_gdf[["max_cap", "building_area"]]
        .loc[~buildings_gdf["max_cap"].isna()]
        .reset_index(),
        how="left",
        left_on="building_id",
        right_on="id",
    ).set_index("id")

    return gdf.assign(load_factor=(gdf.capacity / gdf.max_cap).round(rounding))


def get_probability_for_property(
    mastr_gdf: gpd.GeoDataFrame,
    cap_range: tuple[int | float, int | float],
    prop: str,
) -> tuple[np.array, np.array]:
    """
    Calculate the probability of the different options of a property of the
    existing PV plants.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    cap_range : tuple(int, int)
        Capacity range of PV plants to look at.
    prop : str
        Property to calculate probabilities for. String needs to be in columns
        of mastr_gdf.
    Returns
    -------
    tuple
        numpy.array
            Unique values of property.
        numpy.array
            Probabilties per unique value.
    """
    cap_range_gdf = mastr_gdf.loc[
        (mastr_gdf.capacity > cap_range[0])
        & (mastr_gdf.capacity <= cap_range[1])
    ]

    if prop == "load_factor":
        cap_range_gdf = cap_range_gdf.loc[cap_range_gdf[prop] <= 1]

    count = Counter(
        cap_range_gdf[prop].loc[
            ~cap_range_gdf[prop].isna()
            & ~cap_range_gdf[prop].isnull()
            & ~(cap_range_gdf[prop] == "None")
        ]
    )

    values = np.array(list(count.keys()))
    probabilities = np.fromiter(count.values(), dtype=float)
    probabilities = probabilities / np.sum(probabilities)

    return values, probabilities


@timer_func
def probabilities(
    mastr_gdf: gpd.GeoDataFrame,
    cap_ranges: list[tuple[int | float, int | float]] | None = None,
    properties: list[str] | None = None,
) -> dict:
    """
    Calculate the probability of the different options of properties of the
    existing PV plants.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    cap_ranges : list(tuple(int, int))
        List of capacity ranges to distinguish between. The first tuple should
        start with a zero and the last one should end with infinite.
    properties : list(str)
        List of properties to calculate probabilities for. Strings need to be
        in columns of mastr_gdf.
    Returns
    -------
    dict
        Dictionary with values and probabilities per capacity range.
    """
    if cap_ranges is None:
        cap_ranges = [
            (0, 30 / 10**3),
            (30 / 10**3, 100 / 10**3),
            (100 / 10**3, float("inf")),
        ]
    if properties is None:
        properties = [
            "orientation_uniform",
            "orientation_primary",
            "orientation_primary_angle",
            "load_factor",
        ]

    prob_dict = {}

    for cap_range in cap_ranges:
        prob_dict[cap_range] = {
            "values": {},
            "probabilities": {},
        }

        for prop in properties:
            v, p = get_probability_for_property(
                mastr_gdf,
                cap_range,
                prop,
            )

            prob_dict[cap_range]["values"][prop] = v
            prob_dict[cap_range]["probabilities"][prop] = p

    return prob_dict


def cap_share_per_cap_range(
    mastr_gdf: gpd.GeoDataFrame,
    cap_ranges: list[tuple[int | float, int | float]] | None = None,
) -> dict[tuple[int | float, int | float], float]:
    """
    Calculate the share of PV capacity from the total PV capacity within
    capacity ranges.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    cap_ranges : list(tuple(int, int))
        List of capacity ranges to distinguish between. The first tuple should
        start with a zero and the last one should end with infinite.
    Returns
    -------
    dict
        Dictionary with share of PV capacity from the total PV capacity within
        capacity ranges.
    """
    if cap_ranges is None:
        cap_ranges = [
            (0, 30 / 10**3),
            (30 / 10**3, 100 / 10**3),
            (100 / 10**3, float("inf")),
        ]

    cap_share_dict = {}

    total_cap = mastr_gdf.capacity.sum()

    for cap_range in cap_ranges:
        cap_share = (
            mastr_gdf.loc[
                (mastr_gdf.capacity > cap_range[0])
                & (mastr_gdf.capacity <= cap_range[1])
            ].capacity.sum()
            / total_cap
        )

        cap_share_dict[cap_range] = cap_share

    return cap_share_dict


def mean_load_factor_per_cap_range(
    mastr_gdf: gpd.GeoDataFrame,
    cap_ranges: list[tuple[int | float, int | float]] | None = None,
) -> dict[tuple[int | float, int | float], float]:
    """
    Calculate the mean roof load factor per capacity range from existing PV
    plants.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    cap_ranges : list(tuple(int, int))
        List of capacity ranges to distinguish between. The first tuple should
        start with a zero and the last one should end with infinite.
    Returns
    -------
    dict
        Dictionary with mean roof load factor per capacity range.
    """
    if cap_ranges is None:
        cap_ranges = [
            (0, 30 / 10**3),
            (30 / 10**3, 100 / 10**3),
            (100 / 10**3, float("inf")),
        ]

    load_factor_dict = {}

    for cap_range in cap_ranges:
        load_factor = mastr_gdf.loc[
            (mastr_gdf.load_factor <= 1)
            & (mastr_gdf.capacity > cap_range[0])
            & (mastr_gdf.capacity <= cap_range[1])
        ].load_factor.mean()

        load_factor_dict[cap_range] = load_factor

    return load_factor_dict


def building_area_range_per_cap_range(
    mastr_gdf: gpd.GeoDataFrame,
    cap_ranges: list[tuple[int | float, int | float]] | None = None,
    min_building_size: int | float = 10.0,
    upper_quantile: float = 0.95,
    lower_quantile: float = 0.05,
) -> dict[tuple[int | float, int | float], tuple[int | float, int | float]]:
    """
    Estimate normal building area range per capacity range.
    Calculate the mean roof load factor per capacity range from existing PV
    plants.

    Parameters
    -----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    cap_ranges : list(tuple(int, int))
        List of capacity ranges to distinguish between. The first tuple should
        start with a zero and the last one should end with infinite.
    min_building_size : int, float
        Minimal building size to consider for PV plants.
    upper_quantile : float
        Upper quantile to estimate maximum building size per capacity range.
    lower_quantile : float
        Lower quantile to estimate minimum building size per capacity range.
    Returns
    -------
    dict
        Dictionary with estimated normal building area range per capacity
        range.
    """
    if cap_ranges is None:
        cap_ranges = [
            (0, 30 / 10**3),
            (30 / 10**3, 100 / 10**3),
            (100 / 10**3, float("inf")),
        ]

    building_area_range_dict = {}

    n_ranges = len(cap_ranges)

    for count, cap_range in enumerate(cap_ranges):
        cap_range_gdf = mastr_gdf.loc[
            (mastr_gdf.capacity > cap_range[0])
            & (mastr_gdf.capacity <= cap_range[1])
        ]

        if count == 0:
            building_area_range_dict[cap_range] = (
                min_building_size,
                cap_range_gdf.building_area.quantile(upper_quantile),
            )
        elif count == n_ranges - 1:
            building_area_range_dict[cap_range] = (
                cap_range_gdf.building_area.quantile(lower_quantile),
                float("inf"),
            )
        else:
            building_area_range_dict[cap_range] = (
                cap_range_gdf.building_area.quantile(lower_quantile),
                cap_range_gdf.building_area.quantile(upper_quantile),
            )

    values = list(building_area_range_dict.values())

    building_area_range_normed_dict = {}

    for count, (cap_range, (min_area, max_area)) in enumerate(
        building_area_range_dict.items()
    ):
        if count == 0:
            building_area_range_normed_dict[cap_range] = (
                min_area,
                np.mean((values[count + 1][0], max_area)),
            )
        elif count == n_ranges - 1:
            building_area_range_normed_dict[cap_range] = (
                np.mean((values[count - 1][1], min_area)),
                max_area,
            )
        else:
            building_area_range_normed_dict[cap_range] = (
                np.mean((values[count - 1][1], min_area)),
                np.mean((values[count + 1][0], max_area)),
            )

    return building_area_range_normed_dict


@timer_func
def desaggregate_pv_in_mv_grid(
    buildings_gdf: gpd.GeoDataFrame,
    pv_cap: float | int,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    Desaggregate PV capacity on buildings within a given grid district.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing buildings within the grid district.
    pv_cap : float, int
        PV capacity to desaggregate.
    Other Parameters
    -----------
    prob_dict : dict
        Dictionary with values and probabilities per capacity range.
    cap_share_dict : dict
        Dictionary with share of PV capacity from the total PV capacity within
        capacity ranges.
    building_area_range_dict : dict
        Dictionary with estimated normal building area range per capacity
        range.
    load_factor_dict : dict
        Dictionary with mean roof load factor per capacity range.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    pv_cap_per_sq_m : float, int
        Average expected, installable PV capacity per square meter.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM building data with desaggregated PV
        plants.
    """
    bus_id = int(buildings_gdf.bus_id.iat[0])

    rng = default_rng(seed=kwargs["seed"])
    random_state = RandomState(seed=kwargs["seed"])

    results_df = pd.DataFrame(columns=buildings_gdf.columns)

    for cap_range, share in kwargs["cap_share_dict"].items():
        pv_cap_range = pv_cap * share

        b_area_min, b_area_max = kwargs["building_area_range_dict"][cap_range]

        cap_range_buildings_gdf = buildings_gdf.loc[
            ~buildings_gdf.index.isin(results_df.index)
            & (buildings_gdf.building_area > b_area_min)
            & (buildings_gdf.building_area <= b_area_max)
        ]

        mean_load_factor = kwargs["load_factor_dict"][cap_range]
        cap_range_buildings_gdf = cap_range_buildings_gdf.assign(
            mean_cap=cap_range_buildings_gdf.max_cap * mean_load_factor,
            load_factor=np.nan,
            capacity=np.nan,
        )

        total_mean_cap = cap_range_buildings_gdf.mean_cap.sum()

        if total_mean_cap == 0:
            logger.warning(
                f"There are no matching roof for capacity range {cap_range} "
                f"kW in grid {bus_id}. Using all buildings as fallback."
            )

            cap_range_buildings_gdf = buildings_gdf.loc[
                ~buildings_gdf.index.isin(results_df.index)
            ]

            if len(cap_range_buildings_gdf) == 0:
                logger.warning(
                    "There are no roofes available for capacity range "
                    f"{cap_range} kW in grid {bus_id}. Allowing dual use."
                )
                cap_range_buildings_gdf = buildings_gdf.copy()

            cap_range_buildings_gdf = cap_range_buildings_gdf.assign(
                mean_cap=cap_range_buildings_gdf.max_cap * mean_load_factor,
                load_factor=np.nan,
                capacity=np.nan,
            )

            total_mean_cap = cap_range_buildings_gdf.mean_cap.sum()

        elif total_mean_cap < pv_cap_range:
            logger.warning(
                f"Average roof utilization of the roof area in grid {bus_id} "
                f"and capacity range {cap_range} kW is not sufficient. The "
                "roof utilization will be above average."
            )

        frac = max(
            pv_cap_range / total_mean_cap,
            1 / len(cap_range_buildings_gdf),
        )

        samples_gdf = cap_range_buildings_gdf.sample(
            frac=min(1, frac),
            random_state=random_state,
        )

        cap_range_dict = kwargs["prob_dict"][cap_range]

        values_dict = cap_range_dict["values"]
        p_dict = cap_range_dict["probabilities"]

        load_factors = rng.choice(
            a=values_dict["load_factor"],
            size=len(samples_gdf),
            p=p_dict["load_factor"],
        )

        samples_gdf = samples_gdf.assign(
            load_factor=load_factors,
            capacity=(
                samples_gdf.building_area
                * load_factors
                * kwargs["pv_cap_per_sq_m"]
            ).clip(lower=0.4),
        )

        missing_factor = pv_cap_range / samples_gdf.capacity.sum()

        samples_gdf = samples_gdf.assign(
            capacity=(samples_gdf.capacity * missing_factor),
            load_factor=(samples_gdf.load_factor * missing_factor),
        )

        assert np.isclose(
            samples_gdf.capacity.sum(),
            pv_cap_range,
            rtol=1e-03,
        ), f"{samples_gdf.capacity.sum()} != {pv_cap_range}"

        results_df = pd.concat(
            [
                results_df,
                samples_gdf,
            ],
        )

    total_missing_factor = pv_cap / results_df.capacity.sum()

    results_df = results_df.assign(
        capacity=(results_df.capacity * total_missing_factor),
    )

    assert np.isclose(
        results_df.capacity.sum(),
        pv_cap,
        rtol=1e-03,
    ), f"{results_df.capacity.sum()} != {pv_cap}"

    return gpd.GeoDataFrame(
        results_df,
        crs=samples_gdf.crs,
        geometry="geom",
    )


@timer_func
def desaggregate_pv(
    buildings_gdf: gpd.GeoDataFrame,
    cap_df: pd.DataFrame,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    Desaggregate PV capacity on buildings within a given grid district.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    cap_df : pandas.DataFrame
        DataFrame with total rooftop capacity per mv grid.
    Other Parameters
    -----------
    prob_dict : dict
        Dictionary with values and probabilities per capacity range.
    cap_share_dict : dict
        Dictionary with share of PV capacity from the total PV capacity within
        capacity ranges.
    building_area_range_dict : dict
        Dictionary with estimated normal building area range per capacity
        range.
    load_factor_dict : dict
        Dictionary with mean roof load factor per capacity range.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    pv_cap_per_sq_m : float, int
        Average expected, installable PV capacity per square meter.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM building data with desaggregated PV
        plants.
    """
    allocated_buildings_gdf = buildings_gdf.loc[~buildings_gdf.end_of_life]

    building_bus_ids = set(buildings_gdf.bus_id)
    cap_bus_ids = set(cap_df.index)

    logger.debug(
        f"Bus IDs from buildings: {len(building_bus_ids)}\nBus IDs from "
        f"capacity: {len(cap_bus_ids)}"
    )

    if len(building_bus_ids) > len(cap_bus_ids):
        missing = building_bus_ids - cap_bus_ids
    else:
        missing = cap_bus_ids - building_bus_ids

    logger.debug(str(missing))

    bus_ids = np.intersect1d(list(building_bus_ids), list(cap_bus_ids))

    # assert set(buildings_gdf.bus_id.unique()) == set(cap_df.index)

    for bus_id in bus_ids:
        buildings_grid_gdf = buildings_gdf.loc[buildings_gdf.bus_id == bus_id]

        pv_installed_gdf = buildings_grid_gdf.loc[
            ~buildings_grid_gdf.end_of_life
        ]

        pv_installed = pv_installed_gdf.capacity.sum()

        pot_buildings_gdf = buildings_grid_gdf.drop(
            index=pv_installed_gdf.index
        )

        if len(pot_buildings_gdf) == 0:
            logger.error(
                f"In grid {bus_id} there are no potential buildings to "
                f"allocate PV capacity to. The grid is skipped. This message "
                f"should only appear doing test runs with few buildings."
            )

            continue

        pv_target = cap_df.at[bus_id, "capacity"]

        logger.debug(f"pv_target: {pv_target}")

        pv_missing = pv_target - pv_installed

        if pv_missing <= 0:
            logger.warning(
                f"In grid {bus_id} there is more PV installed "
                f"({pv_installed: g} kW) in status Quo than allocated within "
                f"the scenario ({pv_target: g} kW). "
                f"No new generators are added."
            )

            continue

        if pot_buildings_gdf.max_cap.sum() < pv_missing:
            logger.error(
                f"In grid {bus_id} there is less PV potential ("
                f"{pot_buildings_gdf.max_cap.sum():g} MW) than allocated PV "
                f"capacity ({pv_missing:g} MW). The average roof utilization "
                f"will be very high."
            )

        gdf = desaggregate_pv_in_mv_grid(
            buildings_gdf=pot_buildings_gdf,
            pv_cap=pv_missing,
            **kwargs,
        )

        logger.debug(f"New cap in grid {bus_id}: {gdf.capacity.sum()}")
        logger.debug(f"Installed cap in grid {bus_id}: {pv_installed}")
        logger.debug(
            f"Total cap in grid {bus_id}: {gdf.capacity.sum() + pv_installed}"
        )

        if not np.isclose(
            gdf.capacity.sum() + pv_installed, pv_target, rtol=1e-3
        ):
            logger.warning(
                f"The desired capacity and actual capacity in grid {bus_id} "
                f"differ.\n"
                f"Desired cap: {pv_target}\nActual cap: "
                f"{gdf.capacity.sum() + pv_installed}"
            )

        pre_cap = allocated_buildings_gdf.capacity.sum()
        new_cap = gdf.capacity.sum()

        allocated_buildings_gdf = pd.concat(
            [
                allocated_buildings_gdf,
                gdf,
            ]
        )

        total_cap = allocated_buildings_gdf.capacity.sum()

        assert np.isclose(pre_cap + new_cap, total_cap)

    logger.debug("Desaggregated scenario.")
    logger.debug(f"Scenario capacity: {cap_df.capacity.sum(): g}")
    logger.debug(
        f"Generator capacity: " f"{allocated_buildings_gdf.capacity.sum(): g}"
    )

    return gpd.GeoDataFrame(
        allocated_buildings_gdf,
        crs=gdf.crs,
        geometry="geom",
    )


@timer_func
def add_buildings_meta_data(
    buildings_gdf: gpd.GeoDataFrame,
    prob_dict: dict,
    seed: int,
) -> gpd.GeoDataFrame:
    """
    Randomly add additional metadata to desaggregated PV plants.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with desaggregated PV
        plants.
    prob_dict : dict
        Dictionary with values and probabilities per capacity range.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM building data with desaggregated PV
        plants.
    """
    rng = default_rng(seed=seed)
    buildings_gdf = buildings_gdf.reset_index().rename(
        columns={
            "index": "building_id",
        }
    )

    for (min_cap, max_cap), cap_range_prob_dict in prob_dict.items():
        cap_range_gdf = buildings_gdf.loc[
            (buildings_gdf.capacity >= min_cap)
            & (buildings_gdf.capacity < max_cap)
        ]

        for key, values in cap_range_prob_dict["values"].items():
            if key == "load_factor":
                continue

            gdf = cap_range_gdf.loc[
                cap_range_gdf[key].isna()
                | cap_range_gdf[key].isnull()
                | (cap_range_gdf[key] == "None")
            ]

            key_vals = rng.choice(
                a=values,
                size=len(gdf),
                p=cap_range_prob_dict["probabilities"][key],
            )

            buildings_gdf.loc[gdf.index, key] = key_vals

    return buildings_gdf


def add_commissioning_date(
    buildings_gdf: gpd.GeoDataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    seed: int,
):
    """
    Randomly and linear add start-up date to new pv generators.

    Parameters
    ----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with desaggregated PV
        plants.
    start : pandas.Timestamp
        Minimum Timestamp to use.
    end : pandas.Timestamp
        Maximum Timestamp to use.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with start-up date added.
    """
    rng = default_rng(seed=seed)

    date_range = pd.date_range(start=start, end=end, freq="1D")

    return buildings_gdf.assign(
        commissioning_date=rng.choice(date_range, size=len(buildings_gdf))
    )


@timer_func
def allocate_scenarios(
    mastr_gdf: gpd.GeoDataFrame,
    valid_buildings_gdf: gpd.GeoDataFrame,
    last_scenario_gdf: gpd.GeoDataFrame,
    scenario: str,
):
    """
    Desaggregate and allocate scenario pv rooftop ramp-ups onto buildings.

    Parameters
    ----------
    mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing geocoded MaStR data.
    valid_buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    last_scenario_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings matched with pv generators from
        temporally preceding scenario.
    scenario : str
        Scenario to desaggrgate and allocate.
    Returns
    -------
    tuple
        geopandas.GeoDataFrame
            GeoDataFrame containing OSM buildings matched with pv generators.
        pandas.DataFrame
            DataFrame containing pv rooftop capacity per grid id.
    """
    cap_per_bus_id_df = cap_per_bus_id(scenario)

    if cap_per_bus_id_df.empty:
        print(f"No PV rooftop in scenario {scenario}")
        return

    logger.debug(
        f"cap_per_bus_id_df total capacity: {cap_per_bus_id_df.capacity.sum()}"
    )

    last_scenario_gdf = determine_end_of_life_gens(
        last_scenario_gdf,
        SCENARIO_TIMESTAMP[scenario],
        PV_ROOFTOP_LIFETIME,
    )

    buildings_gdf = calculate_max_pv_cap_per_building(
        valid_buildings_gdf,
        last_scenario_gdf,
        PV_CAP_PER_SQ_M,
        ROOF_FACTOR,
    )

    mastr_gdf = calculate_building_load_factor(
        mastr_gdf,
        buildings_gdf,
    )

    probabilities_dict = probabilities(
        mastr_gdf,
        cap_ranges=CAP_RANGES,
    )

    cap_share_dict = cap_share_per_cap_range(
        mastr_gdf,
        cap_ranges=CAP_RANGES,
    )

    load_factor_dict = mean_load_factor_per_cap_range(
        mastr_gdf,
        cap_ranges=CAP_RANGES,
    )

    building_area_range_dict = building_area_range_per_cap_range(
        mastr_gdf,
        cap_ranges=CAP_RANGES,
        min_building_size=MIN_BUILDING_SIZE,
        upper_quantile=UPPER_QUANTILE,
        lower_quantile=LOWER_QUANTILE,
    )

    allocated_buildings_gdf = desaggregate_pv(
        buildings_gdf=buildings_gdf,
        cap_df=cap_per_bus_id_df,
        prob_dict=probabilities_dict,
        cap_share_dict=cap_share_dict,
        building_area_range_dict=building_area_range_dict,
        load_factor_dict=load_factor_dict,
        seed=SEED,
        pv_cap_per_sq_m=PV_CAP_PER_SQ_M,
    )

    allocated_buildings_gdf = allocated_buildings_gdf.assign(scenario=scenario)

    meta_buildings_gdf = frame_to_numeric(
        add_buildings_meta_data(
            allocated_buildings_gdf,
            probabilities_dict,
            SEED,
        )
    )

    return (
        add_commissioning_date(
            meta_buildings_gdf,
            start=last_scenario_gdf.commissioning_date.max(),
            end=SCENARIO_TIMESTAMP[scenario],
            seed=SEED,
        ),
        cap_per_bus_id_df,
    )


class EgonPowerPlantPvRoofBuilding(Base):
    """
    Class definition of table supply.egon_power_plants_pv_roof_building.

    """
    __tablename__ = "egon_power_plants_pv_roof_building"
    __table_args__ = {"schema": "supply"}

    index = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    bus_id = Column(Integer, nullable=True)
    building_id = Column(Integer)
    gens_id = Column(String, nullable=True)
    capacity = Column(Float)
    orientation_uniform = Column(Float)
    orientation_primary = Column(String)
    orientation_primary_angle = Column(String)
    voltage_level = Column(Integer)
    weather_cell_id = Column(Integer)


def add_metadata():
    schema = "supply"
    table = "egon_power_plants_pv_roof_building"
    name = f"{schema}.{table}"
    deposit_id_mastr = config.datasets()["mastr_new"]["deposit_id"]
    deposit_id_data_bundle = config.datasets()["data-bundle"]["sources"][
        "zenodo"
    ]["deposit_id"]

    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-16"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."

    meta = {
        "name": name,
        "title": "eGon power plants rooftop solar",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "eGon power plants rooftop solar systems allocated to buildings"
        ),
        "language": "en-US",
        "keywords": ["photovoltaik", "solar", "pv", "mastr", "status quo"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "Germany",
            "resolution": "building",
        },
        "temporal": {
            "referenceDate": (
                config.datasets()["mastr_new"]["egon2021_date_max"].split(" ")[
                    0
                ]
            ),
            "timeseries": {},
        },
        "sources": [
            {
                "title": "Data bundle for egon-data",
                "description": (
                    "Data bundle for egon-data: A transparent and "
                    "reproducible data processing pipeline for energy "
                    "system modeling"
                ),
                "path": (
                    "https://zenodo.org/record/"
                    f"{deposit_id_data_bundle}#.Y_dWM4CZMVM"
                ),
                "licenses": [license_dedl(attribution="© Cußmann, Ilka")],
            },
            {
                "title": ("open-MaStR power unit registry for eGo^n project"),
                "description": (
                    "Data from Marktstammdatenregister (MaStR) data using "
                    "the data dump from 2022-11-17 for eGon-data."
                ),
                "path": (
                    f"https://zenodo.org/record/{deposit_id_mastr}"
                ),
                "licenses": [license_dedl(attribution="© Amme, Jonathan")],
            },
            sources()["openstreetmap"],
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
        ],
        "licenses": [license_odbl("© eGon development team")],
        "contributors": contris,
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": name,
                "path": "None",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": generate_resource_fields_from_db_table(
                        schema,
                        table,
                    ),
                    "primaryKey": "index",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github."
                "com/OpenEnergyPlatform/oemetadata/blob/master/metadata/"
                "v141/metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time "
                "zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, "
                "en-US, de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(f"oep-v{meta_metadata()['metadataVersion'][4:7]}")()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        schema,
        table,
    )


def create_scenario_table(buildings_gdf):
    """Create mapping table pv_unit <-> building for scenario"""
    EgonPowerPlantPvRoofBuilding.__table__.drop(bind=engine, checkfirst=True)
    EgonPowerPlantPvRoofBuilding.__table__.create(bind=engine, checkfirst=True)

    buildings_gdf[COLS_TO_EXPORT].reset_index().to_sql(
        name=EgonPowerPlantPvRoofBuilding.__table__.name,
        schema=EgonPowerPlantPvRoofBuilding.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    add_metadata()


def add_weather_cell_id(buildings_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    sql = """
    SELECT building_id, zensus_population_id
    FROM boundaries.egon_map_zensus_mvgd_buildings
    """

    buildings_gdf = buildings_gdf.merge(
        right=db.select_dataframe(sql).drop_duplicates(subset="building_id"),
        how="left",
        on="building_id",
    )

    sql = """
    SELECT zensus_population_id, w_id as weather_cell_id
    FROM boundaries.egon_map_zensus_weather_cell
    """

    buildings_gdf = buildings_gdf.merge(
        right=db.select_dataframe(sql).drop_duplicates(
            subset="zensus_population_id"
        ),
        how="left",
        on="zensus_population_id",
    )

    if buildings_gdf.weather_cell_id.isna().any():
        missing = buildings_gdf.loc[
            buildings_gdf.weather_cell_id.isna(), "building_id"
        ].tolist()

        raise ValueError(
            f"Following buildings don't have a weather cell id: {missing}"
        )

    return buildings_gdf


def add_bus_ids_sq(
    buildings_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Add bus ids for status_quo units

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with desaggregated PV
        plants.
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing OSM building data with bus_id per
        generator.
    """
    grid_districts_gdf = grid_districts(EPSG)

    mask = buildings_gdf.scenario == "status_quo"

    buildings_gdf.loc[mask, "bus_id"] = (
        buildings_gdf.loc[mask]
        .sjoin(grid_districts_gdf, how="left")
        .index_right
    )

    return buildings_gdf


def infer_voltage_level(
    units_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Infer nan values in voltage level derived from generator capacity to
    the power plants.

    Parameters
    -----------
    units_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing units with voltage levels from MaStR
    Returnsunits_gdf: gpd.GeoDataFrame
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing units all having assigned a voltage level.
    """

    def voltage_levels(p: float) -> int:
        if p <= 0.1:
            return 7
        elif p <= 0.2:
            return 6
        elif p <= 5.5:
            return 5
        elif p <= 20:
            return 4
        elif p <= 120:
            return 3
        return 1

    units_gdf["voltage_level_inferred"] = False
    mask = units_gdf.voltage_level.isna()
    units_gdf.loc[mask, "voltage_level_inferred"] = True
    units_gdf.loc[mask, "voltage_level"] = units_gdf.loc[mask].capacity.apply(
        voltage_levels
    )

    return units_gdf


def pv_rooftop_to_buildings():
    """Main script, executed as task"""

    mastr_gdf = load_mastr_data()

    status_quo = "status2023" # FIXME: Hard coded

    ts = pd.Timestamp(
        config.datasets()["mastr_new"][f"{status_quo}_date_max"], tz="UTC"
    )

    mastr_gdf = mastr_gdf.loc[
        mastr_gdf.commissioning_date <= ts
    ]

    buildings_gdf = load_building_data()

    desagg_mastr_gdf, desagg_buildings_gdf = allocate_to_buildings(
        mastr_gdf, buildings_gdf
    )

    all_buildings_gdf = (
        desagg_mastr_gdf.assign(scenario=status_quo)
        .reset_index()
        .rename(columns={"geometry": "geom"})
        .set_geometry("geom")
    )

    scenario_buildings_gdf = all_buildings_gdf.copy()

    cap_per_bus_id_df = pd.DataFrame()

    for scenario in SCENARIOS:
        if scenario == status_quo:
            continue
        elif "status" in scenario:
            ts = pd.Timestamp(
                config.datasets()["mastr_new"][f"{scenario}_date_max"], tz="UTC"
            )

            scenario_buildings_gdf = scenario_buildings_gdf.loc[
                scenario_buildings_gdf.commissioning_date <= ts
            ]

        else:
            logger.debug(f"Desaggregating scenario {scenario}.")

            (
                scenario_buildings_gdf,
                cap_per_bus_id_scenario_df,
            ) = allocate_scenarios(  # noqa: F841
                desagg_mastr_gdf,
                desagg_buildings_gdf,
                scenario_buildings_gdf,
                scenario,
            )

            cap_per_bus_id_df = pd.concat(
                [cap_per_bus_id_df, cap_per_bus_id_scenario_df]
            )

        all_buildings_gdf = gpd.GeoDataFrame(
            pd.concat(
                [all_buildings_gdf, scenario_buildings_gdf], ignore_index=True
            ),
            crs=scenario_buildings_gdf.crs,
            geometry="geom",
        )

    # add weather cell
    all_buildings_gdf = add_weather_cell_id(all_buildings_gdf)

    # add bus IDs for status quo scenario
    all_buildings_gdf = add_bus_ids_sq(all_buildings_gdf)

    # export scenario
    create_scenario_table(infer_voltage_level(all_buildings_gdf))
