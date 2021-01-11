"""The central module containing all code dealing with importing data from
demandRegio

TODO: Add description
"""
from __future__ import absolute_import
import sys, os
import subprocess
import pandas as pd
import egon.data.config
import sqlalchemy as sql
from egon.data import db
from sqlalchemy import Column, String, Float, func, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# clone disaggregator code from oopenego-fork
# TODO: Ok to depend on git???
if not os.path.exists('disaggregator'):
    subprocess.run(
        "git clone " +
        egon.data.config.datasets()['demandregio']['disaggregator_code']['url'],
        shell=True,
        cwd=os.path.dirname(__file__),
        )

from disaggregator.disaggregator import data, spatial

### will be later imported from another file ###
Base = declarative_base()
# TODO: Add metadata for tables
class EgonDemandRegioHH(Base):
    __tablename__ = 'egon_demandregio_hh'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    demand = Column(Float)

class EgonDemandRegioCtsInd(Base):
    __tablename__ = 'egon_demandregio_cts_ind'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    wz = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    demand = Column(Float)

class EgonDemandRegioPopulation(Base):
    __tablename__ = 'egon_demandregio_population'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)

class EgonDemandRegioHouseholds(Base):
    __tablename__ = 'egon_demandregio_households'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Integer)

def create_tables():
    """Create tables for demandregio data
    Returns
    -------
    None.
    """
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg['demand_data']['schema']};")
    engine = db.engine()
    EgonDemandRegioHH.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioCtsInd.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioPopulation.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioHouseholds.__table__.create(bind=engine, checkfirst=True)

def insert_demands():
    """ Insert electricity demands per nuts3-region in Germany according to
    demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()['demandregio']['demand_data']
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    for table in cfg['table_names']:
        db.execute_sql(f"DELETE FROM {cfg['schema']}.{cfg['table_names'][table]};")

    for year in cfg['target_years']:
        # Insert demands of private households
        ec_hh = spatial.disagg_households_power(
            by='households',
            weight_by_income=False,
            year=year)

        for hh_size in ec_hh.columns:
            df = pd.DataFrame(ec_hh[hh_size])
            df['year'] = year
            df['hh_size'] = hh_size
            df = df.rename({hh_size: 'demand'}, axis='columns')
            df.to_sql(cfg['table_names']['household'],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')

        # Insert demands of CTS and industry, data only available for years before 2036
        if not year > 2035:
            for sector in ['CTS', 'industry']:
                ec_cts_ind = spatial.disagg_CTS_industry(
                    use_nuts3code=True,
                    source='power',
                    sector=sector,
                    year=year).transpose()

                for wz in ec_cts_ind.columns:
                    df = pd.DataFrame(ec_cts_ind[wz])
                    df['year'] = year
                    df['wz'] = wz
                    df = df.rename({wz: 'demand'}, axis='columns')
                    df.index = df.index.rename('nuts3')
                    df.to_sql(cfg['table_names'][sector],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')

def insert_society_data():
    """ Insert population and number of households per nuts3-region in Germany
    according to demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()['demandregio']['society_data']
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    for table in cfg['table_names']:
        db.execute_sql(f"DELETE FROM {cfg['schema']}.{cfg['table_names'][table]};")

    for year in cfg['target_years']:
        df_pop = pd.DataFrame(data.population(year=year))
        df_pop['year'] = year
        df_pop = df_pop.rename({'value': 'population'}, axis = 'columns')
        df_pop.to_sql(cfg['table_names']['population'],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')
        df_hh = pd.DataFrame(data.households_per_size(year=year))

        for hh_size in df_hh.columns:
            df = pd.DataFrame(df_hh[hh_size])
            df['year'] = year
            df['hh_size'] = hh_size
            df = df.rename({hh_size:'household'})
            df.to_sql(cfg['table_names']['households'],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')

def insert_data():
    """ Overall function for importing data from demandregio

    Returns
    -------
    None.

    """
    create_tables()
    insert_demands()
    insert_society_data()

