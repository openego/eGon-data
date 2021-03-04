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
from pathlib import Path
import egon.data.config
Base = declarative_base()

class EgonPowerPlants(Base):
    __tablename__ = 'egon_power_plants'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, Sequence('pp_seq'), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    chp = Column(Boolean)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    subst_id = Column(Integer)
    voltage_level = Column(Integer)
    w_id = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry('POINT', 4326))

def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """

    cfg = egon.data.config.datasets()["power_plants"]
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target']['schema']};")
    engine = db.engine()
    EgonPowerPlants.__table__.create(bind=engine, checkfirst=True)

def scale_prox2now(df, target, level='federal_state'):
    """ Scale installed capacities linear to status quo power plants

    Parameters
    ----------
    df : pandas.DataFrame
        Status Quo power plants
    target : pandas.Series
        Target values for future sceanrio
    level : str, optional
        Scale per 'federal_state' or 'country'. The default is 'federal_state'.

    Returns
    -------
    df : pandas.DataFrame
        Future power plants

    """

    if level=='federal_state':
        df.loc[:,'Nettonennleistung'] = df.groupby(
            df.Bundesland).Nettonennleistung.apply(
            lambda grp: grp/grp.sum()).mul(
                target[df.Bundesland.values].values)
    else:
        df.loc[:,'Nettonennleistung'] = df.Nettonennleistung.apply(
            lambda x: x/x.sum()).mul(target.values)

    df = df[df.Nettonennleistung>0]

    return df

def select_target(carrier, scenario):
    """ Select installed capacity per scenario and carrier

    Parameters
    ----------
    carrier : str
        Name of energy carrier
    scenario : str
        Name of scenario

    Returns
    -------
    pandas.Series
        Target values for carrier and scenario

    """
    cfg = egon.data.config.datasets()["power_plants"]

    return pd.read_sql(f"""SELECT DISTINCT ON (b.gen)
                         REPLACE(REPLACE(b.gen, '-', ''), 'ü', 'ue') as state,
                         a.capacity
                         FROM {cfg['sources']['capacities']} a,
                         {cfg['sources']['geom_federal_states']} b
                         WHERE a.nuts = b.nuts
                         AND scenario_name = '{scenario}'
                         AND carrier = '{carrier}'
                         AND b.gen NOT IN ('Baden-Württemberg (Bodensee)',
                                           'Bayern (Bodensee)')""",
                         con=db.engine()).set_index('state').capacity

def filter_mastr_geometry(mastr):
    """ Filter data from MaStR by geometry

    Parameters
    ----------
    mastr : pandas.DataFrame
        All power plants listed in MaStR

    Returns
    -------
    mastr_loc : pandas.DataFrame
        Power plants listed in MaStR with valid geometry

    """
    cfg = egon.data.config.datasets()["power_plants"]

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
            f"SELECT geometry as geom FROM {cfg['sources']['geom_germany']}",
             con = db.engine()).to_crs(4326),
        mastr_loc,
        how='right').query("index_left==0").drop('index_left', axis=1)

    return mastr_loc


def insert_biomass_plants(scenario='eGon2035'):
    """ Insert biomass power plants of future scenario

    Parameters
    ----------
    scenario : str, optional
        Name of scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # import target values from NEP 2021, scneario C 2035
    target = select_target('biomass', scenario)

    # import data for MaStR
    path = Path(__file__).parent.parent.parent/'importing/'
    mastr = pd.read_csv(path/'bnetza_mastr_biomass_cleaned.csv')

    # Drop entries without federal state or 'AusschließlichWirtschaftszone'
    mastr = mastr[mastr.Bundesland.isin(pd.read_sql(
        f"""SELECT DISTINCT ON (gen)
        REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
        FROM {cfg['sources']['geom_federal_states']}""",
        con=db.engine()).states.values)]

    # Scale capacities to meet target values
    mastr = scale_prox2now(mastr, target, level='federal_state')

    # Choose only entries with valid geometries
    mastr_loc = filter_mastr_geometry(mastr)
    # TODO: Deal with power plants without geometry

    # Insert entries with location
    # TODO: change to gdf.to_postgis and update for sources to avoid loop?
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_loc.iterrows():
        entry = EgonPowerPlants(
            sources ={'chp': 'MaStR',
                          'el_capacity': 'MaStR scaled with NEP 2021',
                          'th_capacity': 'MaStR'},
            source_id = {'MastrNummer': row.EinheitMastrNummer},
            carrier = 'biomass',
            chp = type(row.KwkMastrNummer)!=float,
            el_capacity = row.Nettonennleistung,
            th_capacity = row.ThermischeNutzleistung/1000,
            scenario = scenario,
            geom = f'SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})'
            )
        session.add(entry)

    session.commit()

def insert_hydro_plants(scenario='eGon2035'):
    """ Insert hydro power plants of future scenario

    Parameters
    ----------
    scenario : str, optional
        Name of scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # Map MaStR carriers to eGon carriers
    map_carrier = {
        'run_of_river': ['Laufwasseranlage',
                         'WasserkraftanlageInTrinkwassersystem',
                         'WasserkraftanlageInBrauchwassersystem'],
         'reservoir': ['Speicherwasseranlage']}

    for carrier in map_carrier.keys():
        # import target values from NEP 2021, scneario C 2035
        target = select_target(carrier, scenario)

        # import data for MaStR
        path = Path(__file__).parent.parent.parent/'importing/'
        mastr = pd.read_csv(path/'bnetza_mastr_hydro_cleaned.csv')

        # Choose only plants with specific carriers
        mastr = mastr[mastr.ArtDerWasserkraftanlage.isin(map_carrier[carrier])]

        # Drop entries without federal state or 'AusschließlichWirtschaftszone'
        mastr = mastr[mastr.Bundesland.isin(pd.read_sql(
            f"""SELECT DISTINCT ON (gen)
            REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
            FROM {cfg['sources']['geom_federal_states']}""",
            con=db.engine()).states.values)]

        # Scale capacities to meet target values
        mastr = scale_prox2now(mastr, target, level='federal_state')

        # Choose only entries with valid geometries
        mastr_loc = filter_mastr_geometry(mastr)
        # TODO: Deal with power plants without geometry

        # Insert entries with location
        # TODO: change to gdf.to_postgis and update for sources to avoid loop?
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_loc.iterrows():
            entry = EgonPowerPlants(
                sources ={'chp':'MaStR',
                          'el_capacity': 'MaStR scaled with NEP 2021'},
                source_id = {'MastrNummer': row.EinheitMastrNummer},
                carrier = carrier,
                chp = type(row.KwkMastrNummer)!=float,
                el_capacity = row.Nettonennleistung,
                scenario = scenario,
                geom = f'SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})'
                )
            session.add(entry)

        session.commit()


def insert_power_plants():
    """ Insert power plants in database

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]
    db.execute_sql(
        f"DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}")
    for scenario in ['eGon2035']:
        insert_biomass_plants(scenario)
        insert_hydro_plants(scenario)
