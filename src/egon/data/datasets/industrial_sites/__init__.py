"""The central module containing all code dealing with the spatial
   distribution of industrial electricity demands.
   Industrial demands from DemandRegio are distributed from nuts3 level down
   to osm landuse polygons and/or industrial sites also identified within this
   processing step bringing three different inputs together.

"""


import egon.data.config
import geopandas as gpd
import pandas as pd
import os
from urllib.request import urlretrieve
from egon.data import db, subprocess
from egon.data.datasets import Dataset
from sqlalchemy import Column, String, Float, Integer, Sequence
from geoalchemy2.types import Geometry
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path


Base = declarative_base()


class HotmapsIndustrialSites(Base):
    __tablename__ = "egon_hotmaps_industrial_sites"
    __table_args__ = {"schema": "demand"}
    siteid = Column(Integer, primary_key=True)
    sitename = Column(String)
    companyname = Column(String(200))
    address = Column(String(170))
    citycode = Column(String(50))
    city = Column(String(50))
    country = Column(String(50))
    location = Column(String(130))
    subsector = Column(String(50))
    datasource = Column(String)
    emissions_ets_2014 = Column(Float)
    emissions_eprtr_2014 = Column(Float)
    production = Column(Float)
    fuel_demand = Column(Float)
    excess_heat_100_200C = Column(Float)
    excess_heat_200_500C = Column(Float)
    excess_heat_500C = Column(Float)
    excess_heat_total = Column(Float)
    geom = Column(Geometry("POINT", 4326), index=True)
    wz = Column(Integer)


class SeenergiesIndustrialSites(Base):
    __tablename__ = "egon_seenergies_industrial_sites"
    __table_args__ = {"schema": "demand"}
    objectid = Column(Integer, primary_key=True)
    siteid = Column(Integer)
    companyname = Column(String(100))
    address = Column(String(100))
    country = Column(String(2))
    eu28 = Column(String(3))
    subsector = Column(String(30))
    lat = Column(Float)
    lon = Column(Float)
    nuts1 = Column(String(3))
    nuts3 = Column(String(5))
    excess_heat = Column(String(3))
    level_1_tj = Column(Float)
    level_2_tj = Column(Float)
    level_3_tj = Column(Float)
    level_1_r_tj = Column(Float)
    level_2_r_tj = Column(Float)
    level_3_r_tj = Column(Float)
    level_1_pj = Column(Float)
    level_2_pj = Column(Float)
    level_3_pj = Column(Float)
    level_1_r_pj = Column(Float)
    level_2_r_pj = Column(Float)
    level_3_r_pj = Column(Float)
    electricitydemand_tj = Column(Float)
    fueldemand_tj = Column(Float)
    globalid = Column(String(50))
    geom = Column(Geometry("POINT", 4326), index=True)
    wz = Column(Integer)


class SchmidtIndustrialSites(Base):
    __tablename__ = "egon_schmidt_industrial_sites"
    __table_args__ = {"schema": "demand"}
    id = Column(Integer, primary_key=True)
    application = Column(String(50))
    plant = Column(String(100))
    landkreis_number = Column(String(5))
    annual_tonnes = Column(Float)
    capacity_production = Column(String(10))
    lat = Column(Float)
    lon = Column(Float)
    geom = Column(Geometry("POINT", 4326), index=True)
    wz = Column(Integer)


class IndustrialSites(Base):
    __tablename__ = "egon_industrial_sites"
    __table_args__ = {"schema": "demand"}
    id = Column(
        Integer,
        Sequence("industrial_sites_id_seq", schema="demand"),
        server_default=Sequence(
            "industrial_sites_id_seq", schema="demand"
        ).next_value(),
        primary_key=True,
    )
    companyname = Column(String(100))
    address = Column(String(170))
    subsector = Column(String(100))
    wz = Column(Integer)
    el_demand = Column(Float)
    nuts3 = Column(String(10))
    geom = Column(Geometry("POINT", 4326), index=True)


def create_tables():
    """Create tables for industrial sites and distributed industrial demands
    Returns
    -------
    None.
    """

    # Get data config
    targets_sites = egon.data.config.datasets()["industrial_sites"]["targets"]

    # Create target schema
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")

    # Drop outdated tables if still present, might be removed later
    db.execute_sql(
        """DROP TABLE IF EXISTS demand.industrial_sites CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS demand.hotmaps_industrial_sites CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS demand.seenergies_industrial_sites CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS demand.schmidt_industrial_sites CASCADE;"""
    )


    # Drop tables and sequences before recreating them
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
                   {targets_sites['hotmaps']['schema']}.
                   {targets_sites['hotmaps']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
                   {targets_sites['seenergies']['schema']}.
                   {targets_sites['seenergies']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
                   {targets_sites['schmidt']['schema']}.
                   {targets_sites['schmidt']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
                   {targets_sites['sites']['schema']}.
                   {targets_sites['sites']['table']} CASCADE;"""
    )

    # Drop sequence
    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS
            {targets_sites['sites']['schema']}.
            {targets_sites['sites']['table']}_id_seq CASCADE;"""
    )

    engine = db.engine()

    HotmapsIndustrialSites.__table__.create(bind=engine, checkfirst=True)

    SeenergiesIndustrialSites.__table__.create(bind=engine, checkfirst=True)

    SchmidtIndustrialSites.__table__.create(bind=engine, checkfirst=True)

    IndustrialSites.__table__.create(bind=engine, checkfirst=True)


def download_hotmaps():
    """Download csv file on hotmap's industrial sites."""
    hotmaps_config = egon.data.config.datasets()["industrial_sites"][
        "sources"
    ]["hotmaps"]

    download_directory = "industrial_sites"

    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = Path(".") / "industrial_sites" / hotmaps_config["path"]

    if not os.path.isfile(target_file):
        subprocess.run(
            f"curl {hotmaps_config['url']} > {target_file}", shell=True
        )


def download_seenergies():
    """Download csv file on s-eenergies' industrial sites."""
    see_config = egon.data.config.datasets()["industrial_sites"]["sources"][
        "seenergies"
    ]

    download_directory = "industrial_sites"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = Path(".") / "industrial_sites" / see_config["path"]

    if not os.path.isfile(target_file):
        urlretrieve(see_config["url"], target_file)


def hotmaps_to_postgres():
    """Import hotmaps data to postgres database"""
    # Get information from data configuration file

    hotmaps_targets = egon.data.config.datasets()["industrial_sites"][
        "targets"
    ]["hotmaps"]
    hotmaps_sources = egon.data.config.datasets()["industrial_sites"][
        "sources"
    ]["hotmaps"]

    input_file = Path(".") / "industrial_sites" / hotmaps_sources["path"]

    engine = db.engine()

    db.execute_sql(
        f"DELETE FROM {hotmaps_targets['schema']}.{hotmaps_targets['table']}"
    )
    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=";")

    # Adjust column names
    df = df.rename(
        columns={
            "SiteID": "siteid",
            "CompanyName": "companyname",
            "SiteName": "sitename",
            "Address": "address",
            "CityCode": "citycode",
            "City": "city",
            "Country": "country",
            "geom": "geom",
            "Subsector": "subsector",
            "DataSource": "datasource",
            "Emissions_ETS_2014": "emissions_ets_2014",
            "Emissions_EPRTR_2014": "emissions_eprtr_2014",
            "Production": "production",
            "Fuel_Demand": "fuel_demand",
            "Excess_Heat_100-200C": "excess_heat_100_200C",
            "Excess_Heat_200-500C": "excess_heat_200_500C",
            "Excess_Heat_500C": "excess_heat_500C",
            "Excess_Heat_Total": "excess_heat_total",
        }
    )

    # Remove entries without geometry
    df = df[df.country == "Germany"]
    df = df[df.geom.notnull()]

    # From EWKT to WKT
    for i in df.index:
        df.loc[i, "geom"] = df.loc[i, "geom"].split(";")[1]

    # Create geometry with shapely
    geom = gpd.GeoSeries.from_wkt(df["geom"])

    # Import as geodataframe
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(geom.x, geom.y), crs="EPSG:4326"
    )

    # Select boundaries
    boundaries = db.select_geodataframe(
        "SELECT * FROM boundaries.vg250_sta_union",
        geom_col="geometry",
        epsg=4326,
    )

    # Choose only sites inside Germany or testmode boundaries
    gdf = gpd.sjoin(gdf, boundaries).drop(
        ["gid", "bez", "area_ha", "index_right", "geom"], axis=1
    )

    # Rename geometry column
    gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

    # Remove duplicates on columns 'plant' and 'geom'
    gdf = gdf.drop_duplicates(subset=["subsector", "geom"])

    # Add additional column for sector information (wz)
    gdf["wz"] = gdf["subsector"]

    # Map subsector information and WZ definition for hotmaps data
    wz_definition = pd.Series(
        {
            "Paper and printing": 1718,
            "Refineries": 19,
            "Cement": 23,
            "Glass": 23,
            "Iron and steel": 24,
            "Non-ferrous metals": 24,
            "Non-metallic mineral products": 23,
            "Chemical industry": 20,
        }
    )
    # Map WZ ids and subsectors from hotmaps

    gdf["wz"] = gdf["wz"].map(wz_definition)

    # Write data to db
    gdf.to_postgis(
        hotmaps_targets["table"],
        engine,
        schema=hotmaps_targets["schema"],
        if_exists="append",
        index=df.index,
    )


def seenergies_to_postgres():
    """Import seenergies data to postgres database"""
    # Get information from data configuration file
    see_targets = egon.data.config.datasets()["industrial_sites"]["targets"][
        "seenergies"
    ]
    see_sources = egon.data.config.datasets()["industrial_sites"]["sources"][
        "seenergies"
    ]

    input_file = Path(".") / "industrial_sites" / see_sources["path"]
    engine = db.engine()

    db.execute_sql(
        f"DELETE FROM {see_targets['schema']}.{see_targets['table']}"
    )

    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=",")
    df = df.drop(["X", "Y"], axis=1)

    # Adjust column names
    df = df.rename(
        columns={
            "SiteName": "sitename",
            "OBJECTID": "objectid",
            "SiteId": "siteid",
            "CompanyName": "companyname",
            "StreetNameAndNumber": "address",
            "Country": "country",
            "EU28": "eu28",
            "Eurostat_Name": "subsector",
            "Latitude": "lat",
            "Longitude": "lon",
            "NUTS1ID": "nuts1",
            "NUTS3ID": "nuts3",
            "Excess_Heat": "excess_heat",
            "level_1_Tj": "level_1_tj",
            "level_2_Tj": "level_2_tj",
            "level_3_Tj": "level_3_tj",
            "level_1_r_Tj": "level_1_r_tj",
            "level_2_r_Tj": "level_2_r_tj",
            "level_3_r_Tj": "level_3_r_tj",
            "level_1_Pj": "level_1_pj",
            "level_2_Pj": "level_2_pj",
            "level_3_Pj": "level_3_pj",
            "level_1_r_Pj": "level_1_r_pj",
            "level_2_r_Pj": "level_2_r_pj",
            "level_3_r_Pj": "level_3_r_pj",
            "ElectricityDemand_TJ_a": "electricitydemand_tj",
            "FuelDemand_TJ_a": "fueldemand_tj",
            "GlobalID": "globalid",
        }
    )

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326"
    )

    gdf = gdf.rename({"geometry": "geom"}, axis=1).set_geometry("geom")

    boundaries = db.select_geodataframe(
        "SELECT * FROM boundaries.vg250_sta_union",
        geom_col="geometry",
        epsg=4326,
    )

    # Choose only sites inside Germany or testmode boundaries
    gdf = gpd.sjoin(gdf, boundaries).drop(
        ["gid", "bez", "area_ha", "index_right"], axis=1
    )

    # Remove duplicates on columns 'plant' and 'geom'
    gdf = gdf.drop_duplicates(subset=["subsector", "geom"])

    # Add additional column for sector information (wz)
    gdf["wz"] = gdf["subsector"]

    # Map subsector information and WZ definition for seenergies data
    wz_definition = pd.Series(
        {
            "Paper and printing": 1718,
            "Refineries": 19,
            "Cement": 23,
            "Glass": 23,
            "Iron and steel": 24,
            "Non-ferrous metals": 24,
            "Non-metallic minerals": 23,
            "Chemical industry": 20,
        }
    )

    # Map WZ ids and subsectors from seenergies

    gdf["wz"] = gdf["wz"].map(wz_definition)

    # Write data to db
    gdf.to_postgis(
        see_targets["table"],
        engine,
        schema=see_targets["schema"],
        if_exists="append",
        index=df.index,
    )


def schmidt_to_postgres():
    """Import data from Thesis by Danielle Schmidt to postgres database"""
    # Get information from data configuration file
    schmidt_targets = egon.data.config.datasets()["industrial_sites"][
        "targets"
    ]["schmidt"]
    schmidt_sources = egon.data.config.datasets()["industrial_sites"][
        "sources"
    ]["schmidt"]

    input_file = (
            Path(".") /
            "data_bundle_egon_data" /
            "industrial_sites" /
            schmidt_sources["path"])

    engine = db.engine()

    db.execute_sql(
        f"DELETE FROM {schmidt_targets['schema']}.{schmidt_targets['table']}"
    )

    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=";")

    # Adjust column names
    df = df.rename(
        columns={
            "Application": "application",
            "Plant": "plant",
            "Landkreis Number": "landkreis_number",
            "Annual Tonnes": "annual_tonnes",
            "Capacity or Production": "capacity_production",
            "Latitude": "lat",
            "Longitude": "lon",
        }
    )

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326"
    )

    gdf = gdf.rename({"geometry": "geom"}, axis=1).set_geometry("geom")

    boundaries = db.select_geodataframe(
        "SELECT * FROM boundaries.vg250_sta_union",
        geom_col="geometry",
        epsg=4326,
    )

    # Choose only sites inside Germany or testmode boundaries
    gdf = gpd.sjoin(gdf, boundaries).drop(
        ["gid", "bez", "area_ha", "index_right"], axis=1
    )

    # Remove duplicates on columns 'plant' 'lon' and 'lat'
    gdf = gdf.drop_duplicates(subset=["plant", "lat", "lon"])

    # Add additional column for sector information (wz)
    gdf["wz"] = gdf["application"]

    # Map subsector information and WZ definition for hotmaps data
    wz_definition = pd.Series(
        {
            "Mechanical Pulp": 1718,
            "Packing Paper and Board": 1718,
            "Cement Mill": 23,
            "Technical/Special Paper and Board": 1718,
            "Graphic Paper": 1718,
            "Hygiene Paper": 1718,
            "Recycled Paper": 1718,
        }
    )

    # Map WZ ids and subsectors from hotmaps
    gdf["wz"] = gdf["wz"].map(wz_definition)

    # Write data to db
    gdf.to_postgis(
        schmidt_targets["table"],
        engine,
        schema=schmidt_targets["schema"],
        if_exists="append",
        index=df.index,
    )


def download_import_industrial_sites():
    """
    Wraps different functions to create tables, download csv files containing
    information on industrial sites in Germany and write this data to the
    local postgresql database

    Returns
    -------
    None.

    """

    create_tables()

    download_hotmaps()

    download_seenergies()

    hotmaps_to_postgres()

    seenergies_to_postgres()

    schmidt_to_postgres()


def merge_inputs():
    """Merge and clean data from different sources
    (hotmaps, seenergies, Thesis Schmidt)
    """

    # Get information from data configuration file

    hotmaps_targets = egon.data.config.datasets()["industrial_sites"][
        "targets"
    ]["hotmaps"]
    see_targets = egon.data.config.datasets()["industrial_sites"]["targets"][
        "seenergies"
    ]
    schmidt_targets = egon.data.config.datasets()["industrial_sites"][
        "targets"
    ]["schmidt"]
    sites_targets = egon.data.config.datasets()["industrial_sites"]["targets"][
        "sites"
    ]

    sites_table = f"{sites_targets['schema']}" f".{sites_targets['table']}"

    hotmaps_table = (
        f"{hotmaps_targets['schema']}" f".{hotmaps_targets['table']}"
    )

    seenergies_table = f"{see_targets['schema']}" f".{see_targets['table']}"

    schmidt_table = (
        f"{schmidt_targets['schema']}" f".{schmidt_targets['table']}"
    )

    # Insert data from s-EEnergies
    db.execute_sql(
        f"""INSERT INTO {sites_table}
              (companyname, address, subsector, wz, el_demand, geom)
                SELECT  s.companyname,
                        s.address,
                        s.subsector,
                        s.wz,
                        s.electricitydemand_tj * 9/2500,
                        s.geom
                FROM {seenergies_table} s
                WHERE   s.country = 'DE'
                AND     geom IS NOT NULL"""
    )
    # Insert data from Hotmaps

    db.execute_sql(
        f"""INSERT INTO {sites_table}
              (companyname, address, subsector, wz, geom)
                  SELECT h.companyname, h.address, h.subsector, h.wz, h.geom
                  FROM {hotmaps_table} h
                  WHERE h.country = 'Germany'
                  AND h.geom IS NOT NULL
                  AND h.siteid NOT IN
                      (SELECT a.siteid
                          FROM {seenergies_table} a
                          WHERE   a.country = 'DE'
                          AND     a.geom IS NOT NULL)
                  AND h.geom NOT IN
                      (SELECT a.geom
                          FROM {seenergies_table} a
                          WHERE   a.country = 'DE'
                          AND     a.geom IS NOT NULL);"""
    )

    # Insert data from Schmidt's Master thesis
    db.execute_sql(
        f"""INSERT INTO {sites_table}
              (companyname, subsector, wz, geom)
                SELECT h.plant, h.application, h.wz, h.geom
                FROM {schmidt_table} h
                WHERE geom IS NOT NULL
                AND h.plant NOT IN
                    (SELECT h.plant
                      FROM  {schmidt_table} h,
                            {sites_table} s
                      WHERE ST_DWithin (h.geom, s.geom, 0.01)
 					  AND	(h.wz = s.wz)
 					  AND	(LOWER (SUBSTRING(h.plant, 1, 3)) =
                              LOWER (SUBSTRING(s.companyname, 1, 3))));"""
    )

    # Replace geometry by spatial information from table 'demand.schmidt_industrial_sites' if possible

    db.execute_sql(
        f"""UPDATE {sites_table} s
              SET geom = g.geom
              FROM {schmidt_table} g
              WHERE ST_DWithin (g.geom, s.geom, 0.01)
              AND (g.wz = s.wz)
              AND  (LOWER (SUBSTRING(g.plant, 1, 3)) =
                    LOWER (SUBSTRING(s.companyname, 1, 3)));"""
    )

    # Delete remaining duplicates

    db.execute_sql(
        f"""DELETE FROM
                {sites_table} a
                    USING {sites_table} b
            WHERE a.id > b.id
            AND a.geom = b.geom
            AND a.wz = b.wz;"""
    )


def map_nuts3():
    """
    Match resulting industrial sites with nuts3 codes and fill column 'nuts3'


    Returns
    -------
    None.

    """
    # Get information from data configuration file
    sites_targets = egon.data.config.datasets()["industrial_sites"]["targets"][
        "sites"
    ]

    sites_table = f"{sites_targets['schema']}" f".{sites_targets['table']}"

    db.execute_sql(
        f"""UPDATE {sites_table} s
              SET nuts3 = krs.nuts
              FROM boundaries.vg250_krs krs
              WHERE ST_WITHIN(s.geom, ST_TRANSFORM(krs.geometry,4326));"""
    )


class MergeIndustrialSites(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Merge_industrial_sites",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(download_import_industrial_sites, merge_inputs, map_nuts3),
        )
