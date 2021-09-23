
"""
The central module containing all code dealing with chp.
"""

import pandas as pd
import geopandas as gpd
from egon.data import db, config
from egon.data.datasets import Dataset
from egon.data.datasets.chp.match_nep import insert_large_chp
from egon.data.datasets.chp.small_chp import existing_chp_smaller_10mw, extension_per_federal_state, select_target
from egon.data.datasets.etrago_setup import link_geom_from_buses
from sqlalchemy import Column, String, Float, Integer, Sequence, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from shapely.ops import nearest_points

Base = declarative_base()

class EgonChp(Base):
    __tablename__ = "egon_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("chp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    district_heating = Column(Boolean)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    electrical_bus_id = Column(Integer)
    district_heating_area_id = Column(Integer)
    gas_bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))

class EgonMaStRConventinalWithoutChp(Base):
    __tablename__ = "egon_mastr_conventional_without_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("mastr_conventional_seq"), primary_key=True)
    EinheitMastrNummer = Column(String)
    carrier = Column(String)
    el_capacity = Column(Float)
    geometry = Column(Geometry("POINT", 4326))







def create_tables():
    """Create tables for chp data
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonChp.__table__.drop(bind=engine, checkfirst=True)
    EgonChp.__table__.create(bind=engine, checkfirst=True)
    EgonMaStRConventinalWithoutChp.__table__.drop(bind=engine, checkfirst=True)
    EgonMaStRConventinalWithoutChp.__table__.create(
        bind=engine, checkfirst=True)


def nearest(row, df, centroid= False,
            row_geom_col='geometry', df_geom_col='geometry', src_column=None):
    """
    Finds the nearest point and returns the specified column values

    Parameters
    ----------
    row : pandas.Series
        Data to which the nearest data of df is assigned.
    df : pandas.DataFrame
        Data which includes all options for the nearest neighbor alogrithm.
    centroid : boolean
        Use centroid geoemtry. The default is False.
    row_geom_col : str, optional
        Name of row's geometry column. The default is 'geometry'.
    df_geom_col : str, optional
        Name of df's geometry column. The default is 'geometry'.
    src_column : str, optional
        Name of returned df column. The default is None.

    Returns
    -------
    value : pandas.Series
        Values of specified column of df

    """

    if centroid:
        unary_union = df.centroid.unary_union
    else:
        unary_union = df[df_geom_col].unary_union

    # Find the geometry that is closest
    nearest = df[df_geom_col] == nearest_points(
        row[row_geom_col], unary_union)[1]

    # Get the corresponding value from df (matching is based on the geometry)
    value = df[nearest][src_column].values[0]

    return value

def assign_heat_bus(scenario='eGon2035'):
    """ Selects heat_bus for chps used in district heating.

    Parameters
    ----------
    scenario : str, optional
        Name of the corresponding scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    sources = config.datasets()["chp_location"]["sources"]
    target = config.datasets()["chp_location"]["targets"]["chp_table"]

    # Select CHP with use_case = 'district_heating'
    chp = db.select_geodataframe(
        f"""
        SELECT * FROM
        {target['schema']}.{target['table']}
        WHERE scenario = '{scenario}'
        AND district_heating = True
        """,
        index_col='id',
        epsg=4326)

    # Select district heating areas and their centroid
    district_heating = db.select_geodataframe(
        f"""
        SELECT area_id, ST_Centroid(geom_polygon) as geom
        FROM
        {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = '{scenario}'
        """,
        epsg=4326)

    # Assign district heating area_id to district_heating_chp
    # According to nearest centroid of district heating area
    chp['district_heating_area_id'] = chp.apply(
        nearest, df=district_heating, row_geom_col='geom', df_geom_col='geom',
        centroid=True, src_column='area_id', axis=1)

    # Drop district heating CHP without heat_bus_id
    db.execute_sql(
        f"""
        DELETE FROM {target['schema']}.{target['table']}
        WHERE scenario = '{scenario}'
        AND district_heating = True
        """)

    # Insert district heating CHP with heat_bus_id
    session = sessionmaker(bind=db.engine())()
    for i, row in chp.iterrows():
        entry = EgonChp(
                id = i,
                sources=row.sources,
                source_id=row.source_id,
                carrier=row.carrier,
                el_capacity=row.el_capacity,
                th_capacity= row.th_capacity,
                electrical_bus_id = row.electrical_bus_id,
                gas_bus_id = row.gas_bus_id,
                district_heating_area_id = row.district_heating_area_id,
                district_heating=row.district_heating,
                voltage_level = row.voltage_level,
                scenario=scenario,
                geom=f"SRID=4326;POINT({row.geom.x} {row.geom.y})",
            )
        session.add(entry)
    session.commit()

def insert_chp_egon2035():
    """ Insert CHP plants for eGon2035 considering NEP and MaStR data

    Returns
    -------
    None.

    """

    create_tables()

    sources = config.datasets()["chp_location"]["sources"]

    targets = config.datasets()["chp_location"]["targets"]

    # Insert large CHPs based on NEP's list of conventional power plants
    MaStR_konv = insert_large_chp(sources, targets["chp_table"], EgonChp)

    # Insert smaller CHPs (< 10MW) based on existing locations from MaStR
    existing_chp_smaller_10mw(sources, MaStR_konv, EgonChp)

    gpd.GeoDataFrame(MaStR_konv[['EinheitMastrNummer', 'el_capacity',
                'geometry', 'carrier']]).to_postgis(
                    targets["mastr_conventional_without_chp"]["table"],
                    schema=targets["mastr_conventional_without_chp"]["schema"],
                    con=db.engine(),
                    if_exists = "replace")

def extension_BW():
    extension_per_federal_state('BadenWuerttemberg', EgonChp)

def extension_BY():
    extension_per_federal_state('Bayern', EgonChp)

def extension_HB():
    extension_per_federal_state('Bremen', EgonChp)

def extension_BB():
    extension_per_federal_state('Brandenburg', EgonChp)

def extension_HH():
    extension_per_federal_state('Hamburg', EgonChp)

def extension_HE():
    extension_per_federal_state('Hessen', EgonChp)

def extension_MV():
    extension_per_federal_state('MecklenburgVorpommern', EgonChp)

def extension_NS():
    extension_per_federal_state('Niedersachsen', EgonChp)

def extension_NW():
    extension_per_federal_state('NordrheinWestfalen', EgonChp)

def extension_SN():
    extension_per_federal_state('Sachsen', EgonChp)

def extension_TH():
    extension_per_federal_state('Thueringen', EgonChp)

def extension_SL():
    extension_per_federal_state('Saarland', EgonChp)

def extension_ST():
    extension_per_federal_state('SachsenAnhalt', EgonChp)

def extension_RP():
    extension_per_federal_state('RheinlandPfalz', EgonChp)

def extension_BE():
    extension_per_federal_state('Berlin', EgonChp)

def extension_SH():
    extension_per_federal_state('SchleswigHolstein', EgonChp)

if config.settings()[
        "egon-data"]["--dataset-boundary"] == 'Schleswig-Holstein':
    extension = extension_SH

else:
    extension = {extension_BW, extension_BY, extension_HB, extension_BB,
                 extension_HE, extension_MV, extension_NS, extension_NW,
                 extension_SH, extension_HH, extension_RP, extension_SL,
                 extension_SN, extension_ST, extension_TH, extension_BE}

class Chp(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Chp",
            version="0.0.1.dev",
            dependencies=dependencies,
            tasks=(create_tables, insert_chp_egon2035,
                   assign_heat_bus,
                   extension
                   ),
        )
