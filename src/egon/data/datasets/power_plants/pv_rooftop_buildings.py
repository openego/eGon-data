"""
Distribute MaStR PV rooftop capacities to OSM and synthetic buildings. Generate new
PV rooftop generators for scenarios eGon2035 and eGon100RE.

Data cleaning: Drop duplicates and entries with missing critical data. Determine most
plausible capacity from multiple values given in MaStR data. Drop generators which don't
have any plausible capacity data (23.5MW > P > 0.1). Randomly and weighted add a
start-up date if it is missing. Extract zip and municipality from 'Standort' given in
MaStR data. Geocode unique zip and municipality combinations with Nominatim (1sec
delay). Drop generators for which geocoding failed or which are located outside the
municipalities of Germany. Add some visual sanity checks for cleaned data.

Allocation of MaStR data: Allocate each generator to an existing building from OSM.
Determine the quantile each generator and building is in depending on the capacity of
the generator and the area of the polygon of the building. Randomly distribute
generators within each municipality preferably within the same building area quantile as
the generators are capacity wise. If not enough buildings exists within a municipality
and quantile additional buildings from other quantiles are chosen randomly.

Desegregation of pv rooftop scenarios: The scenario data per federal state is linear
distributed to the mv grid districts according to the pv rooftop potential per mv grid
district. The rooftop potential is estimated from the building area given from the OSM
buildings. Grid districts, which are located in several federal states, are allocated PV
capacity according to their respective roof potential in the individual federal states.

The desegregation of PV plants within a grid districts respects existing plants from
MaStR, which did not reach their end of life. New PV plants are randomly and weighted
generated using a breakdown of MaStR data as generator basis. Plant metadata (e.g. plant
orientation) is also added random and weighted from MaStR data as basis.

"""
from __future__ import annotations

from collections import Counter
from pathlib import Path, PurePath
from typing import Any

from geoalchemy2 import Geometry
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from loguru import logger
from numpy.random import RandomState, default_rng
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
from egon.data.datasets.scenario_capacities import EgonScenarioCapacities
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

# Scenario Data
COMPONENT = "generator"
CARRIER = "solar_rooftop"
SCENARIOS = ["eGon2035"]  # , "eGon100RE"]
SCENARIO_TIMESTAMP = pd.Timestamp("2035-01-01", tz="UTC")
PV_ROOFTOP_LIFETIME = pd.Timedelta(30 * 365, unit="D")
SEED = 5

# Example Modul Trina Vertex S TSM-400DE09M.08 400 Wp
# https://www.photovoltaik4all.de/media/pdf/92/64/68/Trina_Datasheet_VertexS_DE09-08_2021_A.pdf
MODUL_CAP = 0.4  # kWp
MODUL_SIZE = 1.096 * 1.754  # m²
PV_CAP_PER_SQ_M = MODUL_CAP / MODUL_SIZE

# Estimation of usable roof area
# @Jonathan absolutely speculative
# Factor for the conversion of building area to roof area
# estimation mean roof pitch: 25°
AREA_FACTOR = 1.1033
USABLE_ROOF_SHARE = 0.8  # wild guess
ROOF_FACTOR = AREA_FACTOR * USABLE_ROOF_SHARE

CAP_RANGES = [
    (0, 30),
    (30, 100),
    (100, float("inf")),
]

MIN_BUILDING_SIZE = 10.0
UPPER_QUNATILE = 0.95
LOWER_QUANTILE = 0.05

COLS_TO_RENAME = {
    "EinheitlicheAusrichtungUndNeigungswinkel": (
        "einheitliche_ausrichtung_und_neigungswinkel"
    ),
    "Hauptausrichtung": "hauptausrichtung",
    "HauptausrichtungNeigungswinkel": "hauptausrichtung_neigungswinkel",
}

COLS_TO_EXPORT = [
    "scenario",
    "building_id",
    "gens_id",
    "capacity",
    "einheitliche_ausrichtung_und_neigungswinkel",
    "hauptausrichtung",
    "hauptausrichtung_neigungswinkel",
]


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

    if (
        config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        init_len = len(mastr_df)

        mastr_df = mastr_df.loc[mastr_df.Bundesland == "SchleswigHolstein"]

        logger.info(
            f"Using only MaStR data within Schleswig-Holstein. "
            f"{init_len - len(mastr_df)} of {init_len} generators are dropped."
        )

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
    logger.info(f"Geocoding {len(geocoding_df)} locations.")

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


class EgonMastrPvRoofGeocoded(Base):
    __tablename__ = "egon_mastr_pv_roof_geocoded"
    __table_args__ = {"schema": "supply"}

    zip_and_municipality = Column(String, primary_key=True, index=True)
    location = Column(String)
    point = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    geometry = Column(Geometry(srid=EPSG))


def create_geocoded_table(geocode_gdf):
    """Create geocoded table mastr pv rooftop"""
    EgonMastrPvRoofGeocoded.__table__.drop(bind=engine, checkfirst=True)
    EgonMastrPvRoofGeocoded.__table__.create(bind=engine, checkfirst=True)

    geocode_gdf.to_postgis(
        name=EgonMastrPvRoofGeocoded.__table__.name,
        schema=EgonMastrPvRoofGeocoded.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
        # dtype={}
    )


def geocode_mastr_data():
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

    create_geocoded_table(geocode_gdf)


def geocoded_data_from_db(
    epsg: str | int,
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
            EgonMastrPvRoofGeocoded.zip_and_municipality,
            EgonMastrPvRoofGeocoded.geometry,
        )

    return gpd.read_postgis(
        query.statement, query.session.bind, geom_col="geometry"
    ).to_crs(f"EPSG:{epsg}")


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

    geocode_gdf = geocoded_data_from_db(EPSG)

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
    ).rename(columns={"area": "building_area"})

    buildings_ags_gdf = add_ags_to_buildings(buildings_gdf, municipalities_gdf)

    logger.debug("Loaded buildings.")

    return drop_buildings_outside_muns(buildings_ags_gdf)


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
    random_state = RandomState(seed=seed)

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
            gens = gens.sample(len_build, random_state=random_state)
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
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Do the allocation"""
    q_mastr_gdf = sort_and_qcut_df(mastr_gdf, col="capacity", q=Q)
    q_buildings_gdf = sort_and_qcut_df(buildings_gdf, col="building_area", q=Q)

    desagg_mastr_gdf, desagg_buildings_gdf = allocate_pv(
        q_mastr_gdf, q_buildings_gdf, SEED
    )

    validate_output(desagg_mastr_gdf, desagg_buildings_gdf)

    return drop_unallocated_gens(desagg_mastr_gdf), desagg_buildings_gdf


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
    component: str = "generator",
    carrier: str = "solar_rooftop",
    scenario: str = "eGon2035",
) -> pd.DataFrame:
    """
    Get scenario capacity data from eGo^n Database.

    Parameters
    -----------
    component : str
        Component type to filter table by.
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
            EgonScenarioCapacities.component == component,
            EgonScenarioCapacities.carrier == carrier,
            EgonScenarioCapacities.scenario_name == scenario,
        )

    df = pd.read_sql(
        query.statement, query.session.bind, index_col="index"
    ).sort_index()

    logger.debug("Scenario capacity data loaded.")

    return df


class Vg250Lan(Base):
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
        query.statement, query.session.bind, index_col="id"
    ).to_crs(to_crs)

    logger.debug("Federal State data loaded.")

    return gdf


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
        GeoDataFrame with intersection shapes between counties and grid districts.

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


def buildings_area_per_overlay_id(
    valid_buildings_gdf: gpd.GeoDataFrame,
    grid_federal_state_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Calculate total area of building per overlay ID.

    TODO: This is very simplified at the moment. If possible add some kind
     of weights to the area per building to differentiate between single-
     family and multi-family houses as well as agricultural and industrial
     buildings.

    Parameters
    -----------
    valid_buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data with overlay ID added.
    grid_federal_state_gdf : geopandas.GeoDataFrame
        GeoDataFrame with intersection shapes between counties and grid districts.

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with grid data and total buildings area.
    """
    return grid_federal_state_gdf.merge(
        valid_buildings_gdf[
            [
                "building_area",
                "overlay_id",
            ]
        ]
        .groupby("overlay_id")
        .sum(),
        how="left",
        left_index=True,
        right_index=True,
    )


def cap_per_bus_id(
    overlay_gdf: gpd.GeoDataFrame,
    scenario_df: pd.DataFrame,
    conversion: int | float = 10**3,
) -> pd.DataFrame:
    """
    Calculate total pv rooftop capacity per grid district dependent on
    available buildings area.

    TODO: This is very simplified at the moment as it assumes an evenly
     distribution of generators depending on the available buildings
     area per grid district.

    Parameters
    -----------
    overlay_gdf : geopandas.GeoDataFrame
        GeoDataFrame with grid data and total buildings area.
    conversion : int, float
        Conversion factor to match units. E.g. MW -> kW

    Returns
    -------
    pandas.DataFrame
        DataFrame with total rooftop capacity per mv grid.
    """
    overlay_gdf = overlay_gdf.assign(capacity=np.nan)

    for cap, nuts in scenario_df[["capacity", "nuts"]].itertuples(index=False):
        nuts_gdf = overlay_gdf.loc[overlay_gdf.nuts == nuts]

        capacity = nuts_gdf.building_area.multiply(
            cap / nuts_gdf.building_area.sum()
        )

        overlay_gdf.loc[nuts_gdf.index] = overlay_gdf.loc[
            nuts_gdf.index
        ].assign(capacity=capacity.multiply(conversion).to_numpy())

    return overlay_gdf[["bus_id", "capacity"]].groupby("bus_id").sum()


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
    mastr_gdf = mastr_gdf.assign(
        age=scenario_timestamp - mastr_gdf.start_up_date
    )

    logger.debug("Determined if pv rooftop systems reached their end of life.")

    return mastr_gdf.assign(end_of_life=pv_rooftop_lifetime < mastr_gdf.age)


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
        .merge(
            mastr_gdf[
                [
                    "capacity",
                    "end_of_life",
                    "building_id",
                    "EinheitlicheAusrichtungUndNeigungswinkel",
                    "Hauptausrichtung",
                    "HauptausrichtungNeigungswinkel",
                ]
            ],
            how="left",
            left_on="id",
            right_on="building_id",
        )
        .set_index("id")
        .drop(columns="building_id")
    )

    gdf = gdf.assign(
        max_cap=gdf.building_area.multiply(roof_factor * pv_cap_per_sq_m),
        end_of_life=gdf.end_of_life.fillna(True).astype(bool),
        bus_id=gdf.bus_id.astype(int),
    )

    return gdf


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
        GeoDataFrame containing geocoded MaStR data with calculated load factor.
    """
    gdf = mastr_gdf.merge(
        buildings_gdf[["max_cap", "building_area"]].loc[
            ~buildings_gdf["max_cap"].isna()
        ],
        how="left",
        left_on="building_id",
        right_index=True,
    )

    gdf = gdf.assign(load_factor=(gdf.capacity / gdf.max_cap).round(rounding))

    return gdf


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
        List of properties to calculate probabilities for. Strings needs to be
        in columns of mastr_gdf.

    Returns
    -------
    dict
        Dictionary with values and probabilities per capacity range.
    """
    if cap_ranges is None:
        cap_ranges = [
            (0, 30),
            (30, 100),
            (100, float("inf")),
        ]
    if properties is None:
        properties = [
            "EinheitlicheAusrichtungUndNeigungswinkel",
            "Hauptausrichtung",
            "HauptausrichtungNeigungswinkel",
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
            (0, 30),
            (30, 100),
            (100, float("inf")),
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
            (0, 30),
            (30, 100),
            (100, float("inf")),
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
            (0, 30),
            (30, 100),
            (100, float("inf")),
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


def desaggregate_pv_in_mv_grid(
    buildings_gdf: gpd.GeoDataFrame,
    pv_cap: float | int,
    prob_dict: dict,
    cap_share_dict: dict[tuple[int | float, int | float], float],
    building_area_range_dict: dict[
        tuple[int | float, int | float], tuple[float | int, float]
    ],
    load_factor_dict: dict[tuple[int | float, int | float], float],
    seed: int,
    pv_cap_per_sq_m: float | int,
) -> gpd.GeoDataFrame:
    """
    Desaggregate PV capacity on buildings within a given grid district.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing buildings within the grid district.
    pv_cap : float, int
        PV capacity to desaggregate.
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

    rng = default_rng(seed=seed)
    random_state = RandomState(seed=seed)

    results_df = pd.DataFrame(columns=buildings_gdf.columns)

    for cap_range, share in cap_share_dict.items():
        pv_cap_range = pv_cap * share

        b_area_min, b_area_max = building_area_range_dict[cap_range]

        cap_range_buildings_gdf = buildings_gdf.loc[
            ~buildings_gdf.index.isin(results_df.index)
            & (buildings_gdf.building_area > b_area_min)
            & (buildings_gdf.building_area <= b_area_max)
        ]

        mean_load_factor = load_factor_dict[cap_range]
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

        cap_range_dict = prob_dict[cap_range]

        values_dict = cap_range_dict["values"]
        p_dict = cap_range_dict["probabilities"]

        load_factors = rng.choice(
            a=values_dict["load_factor"],
            size=len(samples_gdf),
            p=p_dict["load_factor"],
        )

        samples_gdf = samples_gdf.assign(
            load_factor=load_factors,
            capacity=samples_gdf.building_area
            * load_factors
            * pv_cap_per_sq_m,
        )

        missing_factor = pv_cap_range / samples_gdf.capacity.sum()

        samples_gdf = samples_gdf.assign(
            capacity=(samples_gdf.capacity * missing_factor),
            load_factor=(samples_gdf.load_factor * missing_factor),
        )

        assert np.isclose(
            samples_gdf.capacity.sum(),
            pv_cap_range,
        ), f"{samples_gdf.capacity.sum()} != {pv_cap_range}"

        results_df = pd.concat(
            [
                results_df,
                samples_gdf,
            ],
        )

    assert np.isclose(
        results_df.capacity.sum(),
        pv_cap,
    ), f"{results_df.capacity.sum()} != {pv_cap}"

    return gpd.GeoDataFrame(
        results_df,
        crs=samples_gdf.crs,
        geometry="geom",
    )


def desaggregate_pv(
    buildings_gdf: gpd.GeoDataFrame,
    cap_df: pd.DataFrame,
    prob_dict: dict,
    cap_share_dict: dict[tuple[int | float, int | float], float],
    building_area_range_dict: dict[
        tuple[int | float, int | float], tuple[float | int, float]
    ],
    load_factor_dict: dict[tuple[int | float, int | float], float],
    seed: int,
    pv_cap_per_sq_m: float | int,
) -> gpd.GeoDataFrame:
    """
    Desaggregate PV capacity on buildings within a given grid district.

    Parameters
    -----------
    buildings_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing OSM buildings data.
    cap_df : pandas.DataFrame
        DataFrame with total rooftop capacity per mv grid.
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

    for bus_id in buildings_gdf.bus_id.unique():
        buildings_grid_gdf = buildings_gdf.loc[buildings_gdf.bus_id == bus_id]

        pv_installed_gdf = buildings_grid_gdf.loc[
            ~buildings_grid_gdf.end_of_life
        ]

        pv_installed = pv_installed_gdf.capacity.sum()

        pot_buildings_gdf = buildings_grid_gdf.loc[
            ~buildings_grid_gdf.index.isin(pv_installed_gdf.index)
        ]

        if len(pot_buildings_gdf) == 0:
            logger.error(
                f"In grid {bus_id} there are no potential buildings to allocate "
                "PV capacity to. The grid is skipped. This message should only "
                "appear doing test runs with few buildings."
            )

            continue

        pv_target = cap_df.at[
            bus_id,
            "capacity",
        ]

        pv_missing = pv_target - pv_installed

        if pv_missing <= 0:
            logger.info(
                f"In grid {bus_id} there is more PV installed in status Quo than"
                " allocated within the scenario. No new generators are added."
            )

            continue

        if pot_buildings_gdf.max_cap.sum() < pv_missing:
            logger.error(
                f"In grid {bus_id} there is less PV potential ("
                f"{pot_buildings_gdf.max_cap.sum():g} kW) than allocated PV "
                f"capacity ({pv_missing:g} kW). The grid is skipped. This message "
                "should only appear doing test runs with few buildings."
            )

            continue

        gdf = desaggregate_pv_in_mv_grid(
            buildings_gdf=pot_buildings_gdf,
            pv_cap=pv_missing,
            prob_dict=prob_dict,
            cap_share_dict=cap_share_dict,
            building_area_range_dict=building_area_range_dict,
            load_factor_dict=load_factor_dict,
            seed=seed,
            pv_cap_per_sq_m=pv_cap_per_sq_m,
        )

        allocated_buildings_gdf = pd.concat(
            [
                allocated_buildings_gdf,
                gdf,
            ]
        )

    logger.debug("Desaggregated scenario.")

    return gpd.GeoDataFrame(
        allocated_buildings_gdf,
        crs=gdf.crs,
        geometry="geom",
    )


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


def allocate_scenarios(
    mastr_gdf: gpd.GeoDataFrame,
    buildings_gdf: gpd.GeoDataFrame,
    scenario: str,
):
    grid_districts_gdf = grid_districts(EPSG)

    scenario_df = scenario_data(COMPONENT, CARRIER, scenario)

    federal_state_gdf = federal_state_data(grid_districts_gdf.crs)

    grid_federal_state_gdf = overlay_grid_districts_with_counties(
        grid_districts_gdf,
        federal_state_gdf,
    )

    buildings_overlay_gdf = add_overlay_id_to_buildings(
        buildings_gdf,
        grid_federal_state_gdf,
    )

    valid_buildings_gdf = drop_buildings_outside_grids(buildings_overlay_gdf)

    buildings_area_per_overlay_gdf = buildings_area_per_overlay_id(
        valid_buildings_gdf,
        grid_federal_state_gdf,
    )

    cap_per_bus_id_df = cap_per_bus_id(
        buildings_area_per_overlay_gdf, scenario_df
    )

    mastr_gdf = determine_end_of_life_gens(
        mastr_gdf,
        SCENARIO_TIMESTAMP,
        PV_ROOFTOP_LIFETIME,
    )

    buildings_gdf = calculate_max_pv_cap_per_building(
        valid_buildings_gdf,
        mastr_gdf,
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
        upper_quantile=UPPER_QUNATILE,
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

    return frame_to_numeric(
        add_buildings_meta_data(
            allocated_buildings_gdf,
            probabilities_dict,
            SEED,
        )
    )


class EgonPowerPlantPvRoofBuildingMapping(Base):
    __tablename__ = "egon_power_plants_pv_roof_building_mapping"
    __table_args__ = {"schema": "supply"}

    einheit_mastr_nummer = Column(
        String,
        primary_key=True,
        index=True,
    )
    building_id = Column(Integer, primary_key=True)


def create_mapping_table(desagg_mastr_gdf):
    """Create mapping table pv_unit <-> building"""
    EgonPowerPlantPvRoofBuildingMapping.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonPowerPlantPvRoofBuildingMapping.__table__.create(
        bind=engine, checkfirst=True
    )

    desagg_mastr_gdf[["building_id"]].astype(int).reset_index().rename(
        columns={"EinheitMastrNummer": "einheit_mastr_nummer"}
    ).to_sql(
        name=EgonPowerPlantPvRoofBuildingMapping.__table__.name,
        schema=EgonPowerPlantPvRoofBuildingMapping.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
    )


class EgonPowerPlantPvRoofBuildingScenario(Base):
    __tablename__ = "egon_power_plant_pv_roof_building_scenario"
    __table_args__ = {"schema": "supply"}

    scenario = Column(String)
    building_id = Column(
        Integer,
        primary_key=True,
        index=True,
    )
    gens_id = Column(
        String,
        nullable=True,
    )
    capacity = Column(Float)
    einheitliche_ausrichtung_und_neigungswinkel = Column(Float)
    hauptausrichtung = Column(String)
    hauptausrichtung_neigungswinkel = Column(String)


def create_scenario_table(buildings_gdf):
    """Create mapping table pv_unit <-> building for scenario"""
    EgonPowerPlantPvRoofBuildingScenario.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonPowerPlantPvRoofBuildingScenario.__table__.create(
        bind=engine, checkfirst=True
    )

    buildings_gdf.rename(columns=COLS_TO_RENAME)[COLS_TO_EXPORT].to_sql(
        name=EgonPowerPlantPvRoofBuildingScenario.__table__.name,
        schema=EgonPowerPlantPvRoofBuildingScenario.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
    )


def pv_rooftop_to_buildings():
    """Main script, executed as task"""

    mastr_gdf = load_mastr_data()

    buildings_gdf = load_building_data()

    desagg_mastr_gdf, desagg_buildings_gdf = allocate_to_buildings(
        mastr_gdf, buildings_gdf
    )

    # export status quo
    create_mapping_table(desagg_mastr_gdf)

    for count, scenario in enumerate(SCENARIOS):
        logger.debug(f"Desaggregating scenario {scenario}.")
        scenario_buildings_gdf = allocate_scenarios(  # noqa: F841
            desagg_mastr_gdf, desagg_buildings_gdf, scenario
        )

        scenario_buildings_gdf = scenario_buildings_gdf.assign(
            scenario=scenario
        )

        if count == 0:
            all_buildings_gdf = scenario_buildings_gdf.copy()
        else:
            all_buildings_gdf = gpd.GeoDataFrame(
                pd.concat([all_buildings_gdf, scenario_buildings_gdf]),
                crs=scenario_buildings_gdf.crs,
            )

    # export scenario
    create_scenario_table(all_buildings_gdf)