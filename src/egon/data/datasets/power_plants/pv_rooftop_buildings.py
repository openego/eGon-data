"""
Distribute PV rooftop capacities to buildings
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path, PurePath
from typing import Any

from geoalchemy2 import Geometry
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from loguru import logger
from numpy.random import default_rng
from pyproj.crs.crs import CRS
from sqlalchemy import BigInteger, Column, Float, String
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    OsmBuildingsSynthetic,
)

# from egon.data.datasets.scenario_parameters import EgonScenario
from egon.data.datasets.zensus_vg250 import Vg250Gem

engine = db.engine()
Base = declarative_base()
SEED = config.settings()["egon-data"]["--random-seed"]

# TODO: move to yml
# mastr data
MASTR_PATH = Path("/home/kilian/Documents/PythonProjects/eGon/data/eGon-gogs/")

MASTR_RELEVANT_COLS = [
    "EinheitMastrNummer",
    "Bruttoleistung",
    "StatisikFlag",
    "Bruttoleistung_extended",
    "Nettonennleistung",
    "InstallierteLeistung",
    "zugeordneteWirkleistungWechselrichter",
    "EinheitBetriebsstatus",
    "Standort",
    "Bundesland",
    "Land",
    "Landkreis",
    "Gemeinde",
    "Postleitzahl",
    "Ort",
    "GeplantesInbetriebnahmedatum",
    "Inbetriebnahmedatum",
    "GemeinsamerWechselrichterMitSpeicher",
    "Lage",
    "Leistungsbegrenzung",
    "EinheitlicheAusrichtungUndNeigungswinkel",
    "Hauptausrichtung",
    "HauptausrichtungNeigungswinkel",
    "Nebenausrichtung",
]

MASTR_DTYPES = {
    "EinheitMastrNummer": str,
    "Bruttoleistung": float,
    "StatisikFlag": str,
    "Bruttoleistung_extended": float,
    "Nettonennleistung": float,
    "InstallierteLeistung": float,
    "zugeordneteWirkleistungWechselrichter": float,
    "EinheitBetriebsstatus": str,
    "Standort": str,
    "Bundesland": str,
    "Land": str,
    "Landkreis": str,
    "Gemeinde": str,
    # "Postleitzahl": int,  # fails because of nan values
    "Ort": str,
    "GemeinsamerWechselrichterMitSpeicher": str,
    "Lage": str,
    "Leistungsbegrenzung": str,
    # this will parse nan values as false wich is not always correct
    # "EinheitlicheAusrichtungUndNeigungswinkel": bool,
    "Hauptausrichtung": str,
    "HauptausrichtungNeigungswinkel": str,
    "Nebenausrichtung": str,
    "NebenausrichtungNeigungswinkel": str,
}

MASTR_PARSE_DATES = [
    "GeplantesInbetriebnahmedatum",
    "Inbetriebnahmedatum",
]

MASTR_INDEX_COL = "EinheitMastrNummer"

EPSG = 4326
SRID = 3035

# data cleaning
MAX_REALISTIC_PV_CAP = 23500
MIN_REALISTIC_PV_CAP = 0.1
ROUNDING = 1

# geopy
MIN_DELAY_SECONDS = 1
USER_AGENT = "rli_kh_geocoder"

# show additional logging information
VERBOSE = False

EXPORT_DIR = Path(__name__).resolve().parent / "data"
EXPORT_FILE = "mastr_geocoded.gpkg"
EXPORT_PATH = EXPORT_DIR / EXPORT_FILE
DRIVER = "GPKG"

# Number of quantiles
Q = 10


def mastr_data(
    mastr_path: PurePath,
    index_col: str | int | list[str] | list[int],
    usecols: list[str],
    dtype: dict[str, Any] | None,
    parse_dates: list[str] | None,
) -> pd.DataFrame:
    """
    Read MaStR data from csv.

    TODO: change this to from DB

    Parameters
    -----------
    mastr_path : pathlib.PurePath
        Path to the cleaned MaStR data.
    index_col : str, int or list of str or int
        Column(s) to use as the row labels of the DataFrame.
    usecols : list of str
        Return a subset of the columns.
    dtype : dict of column (str) -> type (any), optional
        Data type for data or columns.
    parse_dates : list of names (str), optional
        Try to parse given columns to datetime.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing MaStR data.
    """
    mastr_df = pd.read_csv(
        mastr_path,
        index_col=index_col,
        usecols=usecols,
        dtype=dtype,
        parse_dates=parse_dates,
    )

    mastr_df = mastr_df.loc[
        (mastr_df.StatisikFlag == "B")
        & (mastr_df.EinheitBetriebsstatus == "InBetrieb")
        & (mastr_df.Land == "Deutschland")
        & (mastr_df.Lage == "BaulicheAnlagen")
    ]

    logger.debug("MaStR data loaded.")

    return mastr_df


def clean_mastr_data(
    mastr_df: pd.DataFrame,
    max_realistic_pv_cap: int | float,
    min_realistic_pv_cap: int | float,
    rounding: int,
    seed: int,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Clean the MaStR data from implausible data.

    Drop MaStR ID duplicates.
    Drop generators with implausible capacities.
    Drop generators without any kind of start-up date.
    Clean up Standort column and capacity.

    Parameters
    -----------
    mastr_df : pandas.DataFrame
        DataFrame containing MaStR data.
    max_realistic_pv_cap : int or float
        Maximum capacity, which is considered to be realistic.
    min_realistic_pv_cap : int or float
        Minimum capacity, which is considered to be realistic.
    rounding : int
        Rounding to use when cleaning up capacity. E.g. when
        rounding is 1 a capacity of 9.93 will be rounded to 9.9.
    seed : int
        Seed to use for random operations with NumPy and pandas.
    verbose : bool
        Logs additional info if True.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing cleaned MaStR data.
    """
    init_len = len(mastr_df)

    # drop duplicates
    mastr_df = mastr_df.loc[~mastr_df.index.duplicated()]

    # drop invalid entries in standort
    index_to_drop = mastr_df.loc[
        (mastr_df.Standort.isna()) | (mastr_df.Standort.isnull())
    ].index

    mastr_df = mastr_df.loc[~mastr_df.index.isin(index_to_drop)]

    df = mastr_df[
        [
            "Bruttoleistung",
            "Bruttoleistung_extended",
            "Nettonennleistung",
            "zugeordneteWirkleistungWechselrichter",
            "InstallierteLeistung",
        ]
    ].round(rounding)

    # use only the smallest capacity rating if multiple are given
    mastr_df = mastr_df.assign(
        capacity=[
            most_plausible(p_tub, min_realistic_pv_cap)
            for p_tub in df.itertuples(index=False)
        ]
    )

    # drop generators without any capacity info
    # and capacity of zero
    # and if the capacity is > 23.5 MW, because
    # Germanies largest rooftop PV is 23 MW
    # https://www.iwr.de/news/groesste-pv-dachanlage-europas-wird-in-sachsen-anhalt-gebaut-news37379
    mastr_df = mastr_df.loc[
        (~mastr_df.capacity.isna())
        & (mastr_df.capacity <= max_realistic_pv_cap)
        & (mastr_df.capacity > min_realistic_pv_cap)
    ]

    # get zip and municipality
    mastr_df[["zip_and_municipality", "drop_this"]] = pd.DataFrame(
        mastr_df.Standort.astype(str)
        .apply(
            zip_and_municipality_from_standort,
            args=(verbose,),
        )
        .tolist(),
        index=mastr_df.index,
    )

    # drop invalid entries
    mastr_df = mastr_df.loc[mastr_df.drop_this].drop(columns="drop_this")

    # add ", Deutschland" just in case
    mastr_df = mastr_df.assign(
        zip_and_municipality=(mastr_df.zip_and_municipality + ", Deutschland")
    )

    # get consistent start-up date
    mastr_df = mastr_df.assign(
        start_up_date=mastr_df.Inbetriebnahmedatum,
    )

    mastr_df.loc[mastr_df.start_up_date.isna()] = mastr_df.loc[
        mastr_df.start_up_date.isna()
    ].assign(
        start_up_date=mastr_df.GeplantesInbetriebnahmedatum.loc[
            mastr_df.start_up_date.isna()
        ]
    )

    # randomly and weighted fill missing start-up dates
    pool = mastr_df.loc[
        ~mastr_df.start_up_date.isna()
    ].start_up_date.to_numpy()

    size = len(mastr_df) - len(pool)

    if size > 0:
        np.random.seed(seed)

        choice = np.random.choice(
            pool,
            size=size,
            replace=False,
        )

        mastr_df.loc[mastr_df.start_up_date.isna()] = mastr_df.loc[
            mastr_df.start_up_date.isna()
        ].assign(start_up_date=choice)

        logger.info(
            f"Randomly and weigthed added start-up date to {size} generators."
        )

    mastr_df = mastr_df.assign(
        start_up_date=pd.to_datetime(mastr_df.start_up_date, utc=True)
    )

    end_len = len(mastr_df)
    logger.debug(
        f"Dropped {init_len - end_len} "
        f"({((init_len - end_len) / init_len) * 100:g}%)"
        f" of {init_len} rows from MaStR DataFrame."
    )

    return mastr_df


def zip_and_municipality_from_standort(
    standort: str,
    verbose: bool = False,
) -> str:
    """
    Get zip code and municipality from Standort string split into a list.

    Parameters
    -----------
    standort : str
        Standort as given from MaStR data.
    verbose : bool
        Logs additional info if True.

    Returns
    -------
    str
        Standort with only the zip code and municipality
        as well a ', Germany' added.
    """
    if verbose:
        logger.debug(f"Uncleaned String: {standort}")

    standort_list = standort.split()

    found = False

    for count, elem in enumerate(standort_list):
        if len(elem) != 5:
            continue
        elif not elem.isnumeric():
            continue

        found = True

        break

    if found:
        cleaned_str = " ".join(standort_list[count:])

        if verbose:
            logger.debug(f"Cleaned String:   {cleaned_str}")

        return cleaned_str, found
    else:
        logger.warning(
            "Couldn't identify zip code. This entry will be dropped."
            f" Original standort: {standort}."
        )

        return standort, found


def most_plausible(
    p_tub: tuple,
    min_realistic_pv_cap: int | float,
) -> float:
    """
    Try to determine the most plausible capacity.

    Try to determine the most plausible capacity from a given
    generator from MaStR data.

    Parameters
    -----------
    p_tub : tuple
        Tuple containing the different capacities given in
        the MaStR data.
    min_realistic_pv_cap : int or float
        Minimum capacity, which is considered to be realistic.

    Returns
    -------
    float
        Capacity of the generator estimated as the most realistic.
    """
    count = Counter(p_tub).most_common(3)

    if len(count) == 1:
        return count[0][0]

    val1 = count[0][0]
    val2 = count[1][0]

    if len(count) == 2:
        min_val = min(val1, val2)
        max_val = max(val1, val2)
    else:
        val3 = count[2][0]

        min_val = min(val1, val2, val3)
        max_val = max(val1, val2, val3)

    if min_val < min_realistic_pv_cap:
        return max_val

    return min_val


def geocoder(
    user_agent: str,
    min_delay_seconds: int,
) -> RateLimiter:
    """
    Setup Nominatim geocoding class.

    Parameters
    -----------
    user_agent : str
        The app name.
    min_delay_seconds : int
        Delay in seconds to use between requests to Nominatim.
        A minimum of 1 is advised.

    Returns
    -------
    geopy.extra.rate_limiter.RateLimiter
        Nominatim RateLimiter geocoding class to use for geocoding.
    """
    locator = Nominatim(user_agent=user_agent)
    return RateLimiter(
        locator.geocode,
        min_delay_seconds=min_delay_seconds,
    )


def geocoding_data(
    clean_mastr_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Setup DataFrame to geocode.

    Parameters
    -----------
    clean_mastr_df : pandas.DataFrame
        DataFrame containing cleaned MaStR data.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing all unique combinations of
        zip codes with municipalities for geocoding.

    """
    return pd.DataFrame(
        data=clean_mastr_df.zip_and_municipality.unique(),
        columns=["zip_and_municipality"],
    )


def geocode_data(
    geocoding_df: pd.DataFrame,
    ratelimiter: RateLimiter,
    epsg: int,
) -> gpd.GeoDataFrame:
    """
    Geocode zip code and municipality.

    Extract latitude, longitude and altitude.
    Transfrom latitude and longitude to shapely
    Point and return a geopandas GeoDataFrame.

    Parameters
    -----------
    geocoding_df : pandas.DataFrame
        DataFrame containing all unique combinations of
        zip codes with municipalities for geocoding.
    ratelimiter : geopy.extra.rate_limiter.RateLimiter
        Nominatim RateLimiter geocoding class to use for geocoding.
    epsg : int
        EPSG ID to use as CRS.

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing all unique combinations of
        zip codes with municipalities with matching geolocation.
    """
    geocode_df = geocoding_df.assign(
        location=geocoding_df.zip_and_municipality.apply(ratelimiter)
    )

    geocode_df = geocode_df.assign(
        point=geocode_df.location.apply(
            lambda loc: tuple(loc.point) if loc else None
        )
    )

    geocode_df[["latitude", "longitude", "altitude"]] = pd.DataFrame(
        geocode_df.point.tolist(), index=geocode_df.index
    )

    return gpd.GeoDataFrame(
        geocode_df,
        geometry=gpd.points_from_xy(geocode_df.longitude, geocode_df.latitude),
        crs=f"EPSG:{epsg}",
    )


def merge_geocode_with_mastr(
    clean_mastr_df: pd.DataFrame, geocode_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge geometry to original mastr data.

    Parameters
    -----------
    clean_mastr_df : pandas.DataFrame
        DataFrame containing cleaned MaStR data.
    geocode_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing all unique combinations of
        zip codes with municipalities with matching geolocation.

    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame containing cleaned MaStR data with
        matching geolocation from geocoding.
    """
    return gpd.GeoDataFrame(
        clean_mastr_df.merge(
            geocode_gdf[["zip_and_municipality", "geometry"]],
            how="left",
            left_on="zip_and_municipality",
            right_on="zip_and_municipality",
        ),
        crs=geocode_gdf.crs,
    ).set_index(clean_mastr_df.index)


def drop_invalid_entries_from_gdf(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop invalid entries from geopandas GeoDataFrame.

    TODO: how to omit the logging from geos here???

    Parameters
    -----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame to be checked for validity.

    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with rows with invalid geometries
        dropped.

    """
    valid_gdf = gdf.loc[gdf.is_valid]

    logger.debug(
        f"{len(gdf) - len(valid_gdf)} "
        f"({(len(gdf) - len(valid_gdf)) / len(gdf) * 100:g}%) "
        f"of {len(gdf)} values were invalid and are dropped."
    )

    return valid_gdf


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


def add_ags_to_gens(
    valid_mastr_gdf: gpd.GeoDataFrame,
    municipalities_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Add information about AGS ID to generators.

    Parameters
    -----------
    valid_mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data.
    municipalities_gdf : geopandas.GeoDataFrame
        GeoDataFrame with municipality data.

    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data
        with AGS ID added.
    """
    return valid_mastr_gdf.sjoin(
        municipalities_gdf,
        how="left",
        predicate="intersects",
    ).rename(columns={"index_right": "ags"})


def drop_gens_outside_muns(
    valid_mastr_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Drop all generators outside of municipalities.

    Parameters
    -----------
    valid_mastr_gdf : geopandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data.

    Returns
    -------
    gepandas.GeoDataFrame
        GeoDataFrame with valid and cleaned MaStR data
        with generatos without an AGS ID dropped.
    """
    gdf = valid_mastr_gdf.loc[~valid_mastr_gdf.ags.isna()]

    logger.debug(
        f"{len(valid_mastr_gdf) - len(gdf)} "
        f"({(len(valid_mastr_gdf) - len(gdf)) / len(valid_mastr_gdf) * 100:g}%) "
        f"of {len(valid_mastr_gdf)} values are outside of the municipalities"
        " and are therefore dropped."
    )

    return gdf


def load_mastr_data():
    """Read PV rooftop data from MaStR CSV

    Note: the source will be replaced as soon as the MaStR data is available
    in DB.
    """
    cfg = config.datasets()["power_plants"]
    mastr_path = MASTR_PATH / cfg["sources"]["mastr_pv"]

    mastr_df = mastr_data(
        mastr_path,
        MASTR_INDEX_COL,
        MASTR_RELEVANT_COLS,
        MASTR_DTYPES,
        MASTR_PARSE_DATES,
    )

    clean_mastr_df = clean_mastr_data(
        mastr_df,
        max_realistic_pv_cap=MAX_REALISTIC_PV_CAP,
        min_realistic_pv_cap=MIN_REALISTIC_PV_CAP,
        seed=SEED,
        rounding=ROUNDING,
        verbose=VERBOSE,
    )

    geocoding_df = geocoding_data(clean_mastr_df)

    ratelimiter = geocoder(USER_AGENT, MIN_DELAY_SECONDS)

    geocode_gdf = geocode_data(geocoding_df, ratelimiter, EPSG)

    mastr_gdf = merge_geocode_with_mastr(clean_mastr_df, geocode_gdf)

    valid_mastr_gdf = drop_invalid_entries_from_gdf(mastr_gdf)

    municipalities_gdf = municipality_data()

    valid_mastr_gdf = add_ags_to_gens(valid_mastr_gdf, municipalities_gdf)

    return drop_gens_outside_muns(valid_mastr_gdf)


class OsmBuildingsFiltered(Base):
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


def osm_buildings(
    limit: int | None,
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


def load_building_data():
    """Read buildings from DB

    Tables:
    * `openstreetmap.osm_buildings_filtered` (from OSM)
    * `openstreetmap.osm_buildings_synthetic` (synthetic, creaed by us)

    Use column `id` for both as it is unique hence you concat both datasets.
    """

    municipalities_gdf = municipality_data()

    osm_buildings_gdf = osm_buildings(municipalities_gdf.crs)
    synthetic_buildings_gdf = synthetic_buildings(municipalities_gdf.crs)

    buildings_gdf = gpd.GeoDataFrame(
        pd.concat(
            [
                osm_buildings_gdf,
                synthetic_buildings_gdf,
            ]
        ),
        crs=osm_buildings_gdf.crs,
    )

    buildings_ags_gdf = add_ags_to_buildings(buildings_gdf, municipalities_gdf)

    return drop_buildings_outside_muns(buildings_ags_gdf)


def sort_and_qcut_df(
    df: pd.DataFrame | gpd.GeoDataFrame,
    col: str,
    q: int,
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Determine the quantile of a given attribute in a (Geo)DataFrame.

    Sort the (Geo)DataFrame ascendingly for the given attribute.

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

    df = df.assign(
        quant=pd.qcut(
            df[col],
            q=q,
            labels=range(q),
        )
    )

    return df


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
    tuple with two geopandas.GeoDataFrame\s  # noqa: W605
        GeoDataFrame containing MaStR data allocated to building IDs.
        GeoDataFrame containing building data allocated to MaStR IDs.
    """
    rng = default_rng(seed=seed)

    q_buildings_gdf = q_buildings_gdf.assign(gens_id=np.nan)
    q_mastr_gdf = q_mastr_gdf.assign(building_id=np.nan)

    for ags in q_buildings_gdf.ags.unique():
        buildings = q_buildings_gdf.loc[
            (q_buildings_gdf.ags == ags) & (q_buildings_gdf.gens_id.isna())
        ]
        gens = q_mastr_gdf.loc[
            (q_mastr_gdf.ags == ags) & (q_mastr_gdf.building_id.isna())
        ]

        len_build = len(buildings)
        len_gens = len(gens)

        # TODO: @Jonathan FYI this part is probably unnecessary when all
        #  building data is loaded
        if len_build < len_gens:
            gens = gens.sample(len_build, random_state=seed)
            logger.error(
                f"There are {len_gens} generators and only {len_build}"
                f" buildings in AGS {ags}. {len_gens - len(gens)} "
                "generators were truncated to match the amount of buildings."
            )

            assert len_build == len(gens)

        for quant in gens.quant.unique():
            q_buildings = q_buildings_gdf.loc[
                (q_buildings_gdf.quant == quant)
                & (q_buildings_gdf.ags == ags)
                & (q_buildings_gdf.gens_id.isna())
            ]
            q_gens = gens.loc[gens.quant == quant]

            len_build = len(q_buildings)
            len_gens = len(q_gens)

            # TODO: @Jonathan FYI this part is probably unnecessary when all
            #  building data is loaded
            if len_build < len_gens:
                delta = len_gens - len_build

                logger.warning(
                    f"There are {len_gens} generators and only {len_build} "
                    f"buildings in AGS {ags} and quantile {quant}. {delta} "
                    f"buildings from AGS {ags} will be added randomly."
                )

                add_buildings = pd.Index(
                    rng.choice(
                        q_buildings_gdf.loc[
                            (q_buildings_gdf.quant != quant)
                            & (q_buildings_gdf.ags == ags)
                            & (q_buildings_gdf.gens_id.isna())
                        ].index,
                        size=delta,
                        replace=False,
                    )
                )

                q_buildings = q_buildings_gdf.loc[
                    q_buildings.index.append(add_buildings)
                ]

                assert len(q_buildings) == len(q_gens)

            chosen_buildings = pd.Index(
                rng.choice(
                    q_buildings.index,
                    size=len(q_gens),
                    replace=False,
                )
            )

            q_mastr_gdf.loc[q_gens.index] = q_mastr_gdf.loc[
                q_gens.index
            ].assign(building_id=chosen_buildings)

            q_buildings_gdf.loc[chosen_buildings] = q_buildings_gdf.loc[
                chosen_buildings
            ].assign(gens_id=q_gens.index)

    return q_mastr_gdf, q_buildings_gdf


def validate_output(
    desagg_mastr_gdf: pd.DataFrame | gpd.GeoDataFrame,
    desagg_buildings_gdf: pd.DataFrame | gpd.GeoDataFrame,
) -> None:
    """
    Validate output.

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

    TODO: @Jonathan FYI this part is probably unnecessary when all
     building data is loaded

    Parameters
    -----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing MaStR data allocated to building IDs.
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


def allocate_to_buildings(
    mastr_gdf: gpd.GeoDataFrame,
    buildings_gdf: gpd.GeoDataFrame,
):
    """Do the allocation"""
    q_mastr_gdf = sort_and_qcut_df(mastr_gdf, col="capacity", q=Q)
    q_buildings_gdf = sort_and_qcut_df(buildings_gdf, col="area", q=Q)

    desagg_mastr_gdf, desagg_buildings_gdf = allocate_pv(
        q_mastr_gdf, q_buildings_gdf, SEED
    )
    validate_output(desagg_mastr_gdf, desagg_buildings_gdf)

    return drop_unallocated_gens(desagg_mastr_gdf)


# class EgonPowerPlantPvRoofBuildingMapping(Base):
#     __tablename__ = "egon_power_plants_pv_roof_building_mapping"
#     __table_args__ = {"schema": "supply"}
#
#     scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
#     osm_buildings_id = Column(Integer, primary_key=True)
#     pv_roof_unit_id = Column(
#         Integer, primary_key=True
#     )  # will later point to new power plant table
#
#
# def create_mapping_table(alloc_data):
#     """Create mapping table pv_unit <-> building"""
#     EgonPowerPlantPvRoofBuildingMapping.__table__.drop(
#         bind=engine, checkfirst=True
#     )
#     EgonPowerPlantPvRoofBuildingMapping.__table__.create(
#         bind=engine, checkfirst=True
#     )
#
#     alloc_data.to_sql( # or .to_postgis()
#         name=EgonPowerPlantPvRoofBuildingMapping.__table__.name,
#         schema=EgonPowerPlantPvRoofBuildingMapping.__table__.schema,
#         con=db.engine(),
#         if_exists="append",
#         index=False,
#         #dtype={}
#     )


def pv_rooftop_to_buildings():
    """Main script, executed as task"""

    mastr_gdf = load_mastr_data()

    buildings_gdf = load_building_data()

    allocate_to_buildings(mastr_gdf, buildings_gdf)
    # create_mapping_table(alloc_data)
