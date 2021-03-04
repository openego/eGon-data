"""The central module containing all code dealing with power plant data.
"""
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, Sequence, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
import pandas as pd
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


def insert_biomass_plants(scenario='eGon2035'):
    # temporary use local data for MaStR
    path ='/home/clara/Dokumente/eGo^n/2021-02-11_StatistikFlagB'
    mastr = pd.read_csv(path+'/bnetza_mastr_biomass_raw.csv')
    mastr = mastr[mastr.Bundesland.notnull()]
    # import target values from NEP 2021, scneario C 2035
    target = pd.read_sql(f"""SELECT DISTINCT ON (b.gen)
                         REPLACE(REPLACE(b.gen, '-', ''), 'ü', 'ue') as state,
                         a.capacity
                         FROM supply.egon_scenario_capacities a,
                         boundaries.vg250_lan b
                         WHERE a.nuts = b.nuts
                         AND scenario_name = '{scenario}'
                         AND carrier = 'biomass'
                         AND b.gen NOT IN ('Baden-Württemberg (Bodensee)',
                                           'Bayern (Bodensee)')""",
                         con=db.engine()).set_index('state').capacity

    df = scale_prox2now(mastr, target, level='federal_state')

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    for i, row in df.iterrows():
        if row.Laengengrad == row.Laengengrad:
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
        else:
            entry = EgonPowerPlants(
                sources ={'chp': 'MaStR',
                          'el_capacity': 'MaStR scaled with NEP 2021',
                          'th_capacity': 'MaStR'},
                source_id = {'MastrNummer': row.EinheitMastrNummer},
                carrier = 'biomass',
                chp = type(row.KwkMastrNummer)!=float,
                el_capacity = row.Nettonennleistung,
                th_capacity = row.ThermischeNutzleistung/1000,
                scenario = scenario)
        session.add(entry)

    session.commit()

