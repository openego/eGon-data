#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#import saio
import pandas as pd
#import geopandas as gpd
import sqlalchemy as sql
from egon.data import utils
#from shapely import wkt
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Float, func
from sqlalchemy.ext.declarative import declarative_base

def connect_to_engine():
    # Read database configuration from docker-compose.yml
    docker_db_config = utils.egon_data_db_credentials()

    return sql.create_engine((
        'postgresql+psycopg2://' + docker_db_config['POSTGRES_USER'] + ':'
        + docker_db_config['POSTGRES_PASSWORD']+ '@'
        + docker_db_config['HOST']+ ':'
        + docker_db_config['PORT']+ '/'
        + docker_db_config['POSTGRES_DB']), echo=True)

def select_table_input(tablename):
    session = sessionmaker(bind=engine)()
    query = session.query(tablename)
    return pd.read_sql_query(query.statement,
                                 session.bind)

def add_schema():
    for schema in ['model_draft']:
        utils.execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS {schema};")

def create_input_tables_nep():

    Base = declarative_base()
    # TODO: Add metadata for both tables
    class EgoSupplyScenarioCapacities(Base):
        __tablename__ = 'ego_supply_scenario_capacities'
        __table_args__ = {'schema': 'model_draft'}
        state = Column(String(50), primary_key=True)
        generation_type = Column(String(25), primary_key=True)
        capacity = Column(Float)
        nuts = Column(String(12))
        scenario_name = Column(String(50), primary_key=True)

    class NEP2021Kraftwerksliste(Base):
        __tablename__ = 'nep_2021_kraftwerksliste'
        __table_args__ = {'schema': 'model_draft'}
        index =  Column(String(50), primary_key=True)
        bnetza_id = Column(String(50))
        kraftwerksname = Column(String(100))
        blockname = Column(String(50))
        energietraeger = Column(String(12))
        kwk_ja_nein = Column(String(12))
        plz = Column(String(12))
        ort = Column(String(50))
        bundesland_land = Column(String(12))
        inbetriebnamejahr = Column(String(12))
        status = Column(String(50))
        el_leistung = Column(Float)
        a2035_kwk_ersatz = Column(String(12))
        a2035_leistung = Column(Float)
        b2035_kwk_ersatz = Column(String(12))
        b2035_leistung = Column(Float)
        c2035_kwk_ersatz = Column(String(12))
        c2035_leistung = Column(Float)
        b2040_kwk_ersatz = Column(String(12))
        b2040_leistung = Column(Float)

    # Read database configuration from docker-compose.yml
    docker_db_config = utils.egon_data_db_credentials()

    engine = sql.create_engine((
            'postgresql+psycopg2://' + docker_db_config['POSTGRES_USER'] + ':'
            + docker_db_config['POSTGRES_PASSWORD']+ '@'
            + docker_db_config['HOST']+ ':'
            + docker_db_config['PORT']+ '/'
            + docker_db_config['POSTGRES_DB']), echo=True)

    EgoSupplyScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    NEP2021Kraftwerksliste.__table__.create(bind=engine, checkfirst=True)

def manipulate_federal_state_numbers(path_input, df, carrier, scn = 'C 2035'):
    """
    Temporary function that adjusts installed capacities per federal state of
    the first draft to the approval of the scenario data NEP 2021

    Parameters
    ----------
    df : pandas.DataFrame
        Capacities per federal state.
    carrier : dict
        rename carriers
    scn : str, optional
        Name of the NEP-scenario. The default is 'C 2035'.

    Returns
    -------
    df : pandas.DataFrame
        Adjusted capacities per federal state.

    """
    # read-in installed capacities in germany (Genehmigung des Szenariorahmens)
    df_g = pd.read_csv(path_input+'NEP_2021_Genehmiging_C2035.csv',
                       delimiter=';', decimal=',',
                       index_col='Unnamed: 0')

    target_cap = df_g[~df_g.index.map(carrier).isnull()][scn]

    target_cap.index = target_cap.index.map(carrier)

    for c in target_cap.index:
        mask = df.generation_type == c
        df.loc[mask, 'capacity']= \
            df[mask].capacity /\
                df[mask].capacity.sum() * target_cap[c]
    return df

def insert_capacities_per_federal_state_nep(path_input):

    engine = connect_to_engine()

    # read-in installed capacities per federal state of germany (Entwurf des Szenariorahmens)
    df = pd.read_csv(path_input + 'NEP_2021_C2035.csv',
                     delimiter=';', decimal=',',
                     index_col='Unnamed: 0')

    # sort NEP-carriers:
    rename_carrier = {'Windenergie onshore': 'wind_onshore',
                     'Windenergie offshore': 'wind_offshore',
                     'Sonstige konventionelle': 'other_non_renewable',
                     'Speicherwasser': 'reservoir',
                     'Laufwasser': 'run_of_river',
                     'Biomasse': 'biomass',
                     'Erdgas': 'gas',
                     'Kuppelgas': 'gas',
                     'PV (Aufdach)': 'solar',
                     'PV (Freiflaeche)': 'solar',
                     'Pumpspeicher': 'pumped_hydro',
                     'Sonstige EE': 'other_renewable',
                     'Photovoltaik': 'solar',
                     'Oel': 'oil'}

    # nuts1 to federal state in Germany
    ## TODO: Can this be replaced by a sql-query?
    nuts1 = {'Baden-Wuerttemberg': 'DE1',
             'Bayern': 'DE2',
             'Berlin': 'DE3',
             'Brandenburg': 'DE4',
             'Bremen': 'DE5',
             'Hamburg': 'DE6',
             'Hessen': 'DE7',
             'Mecklenburg-Vorpommern': 'DE8',
             'Niedersachsen': 'DE9',
             'Nordrhein-Westfalen': 'DEA',
             'Rheinland-Pfalz': 'DEB',
             'Saarland': 'DEC',
             'Sachsen': 'DED',
             'Sachsen-Anhalt': 'DEE',
             'Schleswig-Holstein': 'DEF',
             'Thueringen': 'DEG'}

    insert_data = pd.DataFrame()

    for bl in nuts1.keys():

        data = df[df.index==bl].transpose()
        data['generation_type'] = data.index.map(rename_carrier)
        data = data.groupby(data.generation_type).sum().reset_index()
        data['state'] = bl
        data['nuts'] = nuts1[bl]
        data['scenario_name'] = 'NEP 2035'
        data = data.rename(columns={bl: 'capacity'})
        insert_data = insert_data.append(data)

    # Scale numbers for federal states based on BNetzA (can be removed later)
    if True:
        insert_data = manipulate_federal_state_numbers(
            path_input, insert_data, rename_carrier, scn = 'C 2035')

    # Set Multiindex to fit to primary keys in table
    insert_data.set_index(['state', 'scenario_name', 'generation_type'],
                          inplace=True)

    # Insert data to db
    insert_data.to_sql('ego_supply_scenario_capacities',
                       engine,
                       schema='model_draft',
                       if_exists='replace')

def insert_nep_list_powerplants(path_input):
    # Connect to database
    engine = connect_to_engine()

    # Read-in data from csv-file
    kw_liste_nep = pd.read_csv(path_input+'Kraftwerksliste_NEP_2021_konv.csv',
                               delimiter=';', decimal=',')

    # Adjust column names
    kw_liste_nep = kw_liste_nep.rename(columns={'BNetzA-ID': 'bnetza_id',
                                 'Kraftwerksname': 'kraftwerksname',
                                 'Blockname': 'blockname',
                                 'Energieträger': 'energietraeger',
                                 'KWK\nJa/Nein': 'kwk_ja_nein',
                                 'PLZ': 'plz',
                                 'Ort': 'ort',
                                 'Bundesland/\nLand': 'bundesland_land',
                                 'Inbetrieb-\nnahmejahr': 'inbetriebnamejahr',
                                 'Status': 'status',
                                 'el. Leistung\n06.02.2020': 'el_leistung',
                                 'A 2035:\nKWK-Ersatz': 'a2035_kwk_ersatz',
                                 'A 2035:\nLeistung': 'a2035_leistung',
                                 'B 2035\nKWK-Ersatz':'b2035_kwk_ersatz',
                                 'B 2035:\nLeistung':'b2035_leistung',
                                 'C 2035:\nKWK-Ersatz': 'c2035_kwk_ersatz',
                                 'C 2035:\nLeistung': 'c2035_leistung',
                                 'B 2040:\nKWK-Ersatz': 'b2040_kwk_ersatz',
                                 'B 2040:\nLeistung': 'b2040_leistung'})

    # Insert data to db
    kw_liste_nep.to_sql('nep_2021_kraftwerksliste',
                       engine,
                       schema='model_draft',
                       if_exists='replace')

def setup_nep_scenario():
    # TODO: Change input path
    path_input_files = '/home/clara/GitHub/dp_new/input_data/'
    add_schema()
    create_input_tables_nep()
    insert_capacities_per_federal_state_nep(path_input=path_input_files)
    insert_nep_list_powerplants(path_input=path_input_files)
