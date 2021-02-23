"""The central module containing all code to download and import data on
   industrial consumers with information on their georeferencing.
"""

from urllib.request import urlretrieve
import os
import pandas as pd
from sqlalchemy import Column, String, Float, Integer, Sequence
from geoalchemy2.types import Geometry
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db, subprocess
import egon.data.config
Base = declarative_base()

class HotmapsIndustrialSites(Base):
    __tablename__ = 'hotmaps_industrial_sites'
    __table_args__ = {'schema': 'demand'}
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
    emissions_ets_2014 = Column(Integer)
    emissions_eprtr_2014 = Column(Float)
    production = Column(Float)
    fuel_demand = Column(Float)
    excess_heat_100_200C = Column(Float)
    excess_heat_200_500C = Column(Float)
    excess_heat_500C = Column(Float)
    excess_heat_total = Column(Float)
    geom = Column(Geometry('POINT', 4326), index=True)
    

class SeenergiesIndustrialSites(Base):
    __tablename__ = 'seenergies_industrial_sites'
    __table_args__ = {'schema': 'demand'}
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
    level_1_Tj = Column(Float)
    level_2_Tj = Column(Float)
    level_3_Tj = Column(Float)
    level_1_r_Tj = Column(Float)
    level_2_r_Tj = Column(Float)
    level_3_r_Tj = Column(Float)
    level_1_Pj = Column(Float)
    level_2_Pj = Column(Float)
    level_3_Pj = Column(Float)
    level_1_r_Pj = Column(Float)
    level_2_r_Pj = Column(Float)
    level_3_r_Pj = Column(Float)
    electricitydemand_Tj = Column(Float)
    fueldemand_Tj = Column(Float)
    globalid = Column(String(50))
    geom = Column(Geometry('POINT', 4326), index=True)
    

class SchmidtIndustrialSites(Base):
    __tablename__ = 'schmidt_industrial_sites'
    __table_args__ = {'schema': 'demand'}
    id = Column(Integer, primary_key=True)
    application = Column(String(50))
    plant = Column(String(100))
    landkreis_number = Column(String(5))
    annual_tonnes = Column(Integer)
    capacity_production = Column(String(10))
    lat = Column(Float)
    lon = Column(Float)
    geom = Column(Geometry('POINT', 4326), index=True)
    
    
class IndustrialSites(Base):
    __tablename__ = 'industrial_sites'
    __table_args__ = {'schema': 'demand'}
    id = Column(Integer, Sequence('ind_id_seq'), primary_key=True)
    companyname = Column(String(100))
    adress = Column(String(170))
    subsector = Column(String(100))
    wz = Column(Integer)
    el_demand = Column(Float)
    nuts3 = Column(String(10))
    geom = Column(Geometry('POINT', 4326), index=True)    
    
    
        
def create_tables():
    """Create tables for data on industrial
    Returns
    -------
    None.
    """
    data_config = egon.data.config.datasets()
    hotmaps_config = data_config["hotmaps"][
        "processed"
    ]
    # Create target schema
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {hotmaps_config['schema']};"
    )
    db.execute_sql("DROP TABLE IF EXISTS demand.hotmaps_industrial_sites CASCADE;")
    db.execute_sql("DROP TABLE IF EXISTS demand.seenergies_industrial_sites CASCADE;")
    db.execute_sql("DROP TABLE IF EXISTS demand.schmidt_industrial_sites CASCADE;")

    engine = db.engine()
    HotmapsIndustrialSites.__table__.create(bind=engine, checkfirst=True)
    SeenergiesIndustrialSites.__table__.create(bind=engine, checkfirst=True)
    SchmidtIndustrialSites.__table__.create(bind=engine, checkfirst=True)
    IndustrialSites.__table__.create(bind=engine, checkfirst=True)

create_tables()

def download_hotmaps():
    """Download csv file on hotmap's industrial sites."""
    data_config = egon.data.config.datasets()
    hotmaps_config = data_config["hotmaps"][
        "original_data"
    ]

    target_file = os.path.join(
        os.path.dirname(__file__), hotmaps_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(hotmaps_config["source"]["url"], target_file)

        
def download_seenergies():
    """Download csv file on s-eenergies' industrial sites."""
    data_config = egon.data.config.datasets()
    see_config = data_config["seenergies"][
        "original_data"
    ]

    target_file = os.path.join(
        os.path.dirname(__file__), see_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(see_config["source"]["url"], target_file)
        

def hotmaps_to_postgres():
    """Import hotmaps data to postgres database"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    hotmaps_orig = data_config["hotmaps"][
        "original_data"
    ]
    hotmaps_proc = data_config["hotmaps"][
        "processed"
    ]
    
    input_file = os.path.join(
        os.path.dirname(__file__), hotmaps_orig["target"]["path"]
    )
    engine = db.engine()
    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=';')
    
    # Adjust column names 
    df = df.rename(columns={
        'SiteID' : 'siteid',
        'CompanyName' : 'companyname',
        'SiteName' : 'sitename',
        'Address' : 'address',
        'CityCode' : 'citycode',
        'City' : 'city',
        'Country' : 'country',
        'geom' : 'location',
        'Subsector' : 'subsector',
        'DataSource' : 'datasource',
        'Emissions_ETS_2014' : 'emissions_ets_2014',
        'Emissions_EPRTR_2014' : 'emissions_eprtr_2014',
        'Production' : 'production',
        'Fuel_Demand' : 'fuel_demand',
        'Excess_Heat_100-200C' : 'excess_heat_100_200C',
        'Excess_Heat_200-500C' : 'excess_heat_200_500C',
        'Excess_Heat_500C' : 'excess_heat_500C',
        'Excess_Heat_Total' : 'excess_heat_total'})
    
    # Write data to db 
    df.to_sql(hotmaps_proc['table'], 
                      engine, 
                      schema = hotmaps_proc['schema'],
                      if_exists='append',
                      index=df.index)
    
    db.execute_sql(
        f"UPDATE {hotmaps_proc['schema']}.{hotmaps_proc['table']} a"
        " SET geom=ST_GeomFromEWKT(a.location);"
    )
        


def seenergies_to_postgres(): 
    """Import seenergies data to postgres database"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    seenergies_orig = data_config["seenergies"][
        "original_data"
    ]
    seenergies_proc = data_config["seenergies"][
        "processed"
    ]
    
    input_file = os.path.join(
        os.path.dirname(__file__), seenergies_orig["target"]["path"]
    )
    engine = db.engine()
    
    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=',')
    df = df.drop(['X', 'Y'], axis=1)
    
    # Adjust column names 
    df = df.rename(columns={
        'SiteName' : 'sitename',
        'OBJECTID' : 'objectid',
        'SiteId' : 'siteid',
        'CompanyName' : 'companyname',
        'StreetNameAndNumber' : 'address',
        'Country' : 'country',
        'EU28' : 'eu28',
        'Eurostat_Name' : 'subsector',
        'Latitude' : 'lat',
        'Longitude' : 'lon',
        'NUTS1ID' : 'nuts1',
        'NUTS3ID' : 'nuts3',
        'Excess_Heat' : 'excess_heat',
        'level_1_Tj' : 'level_1_Tj',
        'level_2_Tj' : 'level_2_Tj',
        'level_3_Tj' : 'level_3_Tj',
        'level_1_r_Tj' : 'level_1_r_Tj',
        'level_2_r_Tj' : 'level_2_r_Tj',
        'level_3_r_Tj' : 'level_3_r_Tj',
        'level_1_Pj' : 'level_1_Pj',
        'level_2_Pj' : 'level_2_Pj',
        'level_3_Pj' : 'level_3_Pj',
        'level_1_r_Pj' : 'level_1_r_Pj',
        'level_2_r_Pj' : 'level_2_r_Pj',
        'level_3_r_Pj' : 'level_3_r_Pj',
        'ElectricityDemand_TJ_a' : 'electricitydemand_Tj',
        'FuelDemand_TJ_a' : 'fueldemand_Tj',
        'GlobalID' : 'globalid'})
    
    # Write data to db 
    df.to_sql(seenergies_proc['table'], 
                      engine, 
                      schema = seenergies_proc['schema'],
                      if_exists='append',
                      index=df.index)
    db.execute_sql(
        f"UPDATE {seenergies_proc['schema']}.{seenergies_proc['table']} a"
        " SET geom=ST_SetSRID(ST_MakePoint(a.lon, a.lat), 4326);"
    )



def schmidt_to_postgres():
    """Import data from Thesis by Danielle Schmidt to postgres database"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    schmidt_orig = data_config["schmidt"][
        "original_data"
    ]
    schmidt_proc = data_config["schmidt"][
        "processed"
    ]
    
    input_file = os.path.join(
        os.path.dirname(__file__), schmidt_orig["target"]["path"]
    )
    engine = db.engine()
    
    # Read csv to dataframe
    df = pd.read_csv(input_file, delimiter=';')
    
    # Adjust column names 
    df = df.rename(columns={
        'Application' : 'application',
        'Plant' : 'plant',
        'Landkreis Number' : 'landkreis_number',
        'Annual Tonnes' : 'annual_tonnes',
        'Capacity or Production' : 'capacity_production',
        'Latitude' : 'lat',
        'Longitude' : 'lon'})
    
    df.insert(0, 'id', df.index + 1)

    # Write data to db 
    df.to_sql(schmidt_proc['table'], 
                      engine, 
                      schema = schmidt_proc['schema'],
                      if_exists='append',
                      index=df.index)
    db.execute_sql(
        f"UPDATE {schmidt_proc['schema']}.{schmidt_proc['table']} a"
        " SET geom=ST_SetSRID(ST_MakePoint(a.lon, a.lat), 4326);"
    )
    

def merge_inputs(): 
    """ Merge and clean data from different sources 
    (hotmaps, seenergies, Thesis Schmidt)
    """
    
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    sites_proc = data_config["industrial_sites"][
        "processed"
    ]
    sites_table = (
        f"{sites_proc['schema']}"
        f".{sites_proc['table']}"
    )
    hotmaps_proc = data_config["hotmaps"][
        "processed"
    ]
    hotmaps_table = (
        f"{hotmaps_proc['schema']}"
        f".{hotmaps_proc['table']}"
    )
    
    seenergies_proc = data_config["seenergies"][
        "processed"
    ]
    seenergies_table = (
        f"{seenergies_proc['schema']}"
        f".{seenergies_proc['table']}"
    )
    
    schmidt = data_config["seenergies"][
        "processed"
    ]
    schmidt_table = (
        f"{seenergies_proc['schema']}"
        f".{seenergies_proc['table']}"
    )
    
    # Insert data from Hotmaps
    db.execute_sql(
        f"""INSERT INTO {sites_table}
                (companyname, address, subsector, geom)
                SELECT hs.companyname, hs.address, hs.subsector, hs.geom
                FROM {hotmaps_table} hs
                WHERE hs.country = 'Germany';"""
    )
    
    # Insert data from s-EEnergies
    db.execute_sql(
        f"""INSERT INTO {sites_table}
                (companyname, address, subsector, geom)
                SELECT se.companyname, se.address, se.subsector, se.geom
                FROM {seenergies_table} se
                WHERE se.country = 'DE' AND se.companyname NOT IN 
                 (SELECT se.companyname 
                      FROM {seenergies_table} se , {sites_table} s 
                      WHERE ST_DWithin (se.geom, s.geom, 0.0001)) ;"""
    )
    
    # Insert data from Schmidt's Master thesis 
    db.execute_sql(
        f"""INSERT INTO {sites_table}
                (companyname, subsector, geom)
                SELECT sm.plant, sm.application, sm.geom
                FROM {schmidt_table} sm
                WHERE sm.plant NOT IN 
                 (SELECT sm.plant 
                      FROM {schmidt_table} sm , {sites_table} s 
                      WHERE ST_DWithin (se.geom, s.geom, 0.0001)) ;"""
    )
    
    