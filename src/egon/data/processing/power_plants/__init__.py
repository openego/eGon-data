"""The central module containing all code dealing with power plant data.
"""
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, Sequence, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
import pandas as pd
import geopandas as gpd
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class EgonPowerPlants(Base):
    __tablename__ = 'egon_power_plants'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, Sequence('pp_seq'), primary_key=True)
    sources = Column(JSONB)     # source of data (MaStr/NEP/...)
    source_id = Column(JSONB)  # id used in original source
    carrier = Column(String)
    chp = Column(Boolean)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    subst_id = Column(Integer)
    voltage_level = Column(Integer)
    w_id = Column(Integer)      # id of corresponding weather grid cell
    scenario = Column(String)
    geom = Column(Geometry('POINT', 4326))

def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonPowerPlants.__table__.create(bind=engine, checkfirst=True)

def scale_prox2now(df, target, level='federal_state'):

    # TODO: check if Bruttoleistung or Nettonennleistung is needed
    if level=='federal_state':
        df.loc[:,'Nettonennleistung'] = df.groupby(
            df.Bundesland).Nettonennleistung.apply(
            lambda grp: grp/grp.sum()).mul(
                target[df.Bundesland.values].values)
    else:
        df.loc[:,'Nettonennleistung'] = df.Nettonennleistung.apply(
            lambda x: x/x.sum()).mul(target.values)

    return df


def insert_power_plants(carrier = 'biomass', scenario='eGon2035'):

    # import target values from NEP 2021, scneario C 2035
    target = pd.read_sql(f"""SELECT DISTINCT ON (b.gen)
                         REPLACE(REPLACE(b.gen, '-', ''), 'ü', 'ue') as state,
                         a.capacity
                         FROM supply.egon_scenario_capacities a,
                         boundaries.vg250_lan b
                         WHERE a.nuts = b.nuts
                         AND scenario_name = '{scenario}'
                         AND carrier = '{carrier}'
                         AND b.gen NOT IN ('Baden-Württemberg (Bodensee)',
                                           'Bayern (Bodensee)')""",
                         con=db.engine()).set_index('state').capacity

    # temporary use local data for MaStR
    path ='/home/clara/GitHub/eGon-data/src/egon/data/importing/'
    mastr = pd.read_csv(path+f'bnetza_mastr_{carrier}_cleaned.csv')

    # Drop entries without federal state or 'AusschließlichWirtschaftszone'
    mastr = mastr[mastr.Bundesland.isin(pd.read_sql(
        """SELECT DISTINCT ON (gen)
        REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
        FROM boundaries.vg250_lan""",
        con=db.engine()).states.values)]

    # Scale capacities to meet target values
    mastr = scale_prox2now(mastr, target, level='federal_state')


    # Drop entries without geometry for insert
    mastr_loc = mastr[
        mastr.Laengengrad.notnull() & mastr.Breitengrad.notnull()]

    # Create geodataframe
    mastr_loc = gpd.GeoDataFrame(
        mastr_loc, geometry=gpd.points_from_xy(
            mastr_loc.Laengengrad, mastr_loc.Breitengrad, crs=4326))

    # Drop entries outside of germany
    mastr_loc = gpd.sjoin(
        gpd.read_postgis(
            "SELECT geometry as geom FROM boundaries.vg250_sta_union",
             con = db.engine()).to_crs(4326),
        mastr_loc, how='right').drop('index_left', axis=1)

    # Insert entries with location
    # TODO: change to gdf.to_postgis and update for sources to avoid loop?
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_loc.iterrows():
        entry = EgonPowerPlants(
            sources ={'chp': 'MaStR',
                          'el_capacity': 'MaStR scaled with NEP 2021',
                          'th_capacity': 'MaStR'},
            source_id = {'MastrNummer': row.EinheitMastrNummer},
            carrier = carrier,
            chp = type(row.KwkMastrNummer)!=float,
            el_capacity = row.Nettonennleistung,
            th_capacity = row.ThermischeNutzleistung/1000,
            scenario = scenario,
            geom = f'SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})'
            )
        session.add(entry)

    session.commit()

