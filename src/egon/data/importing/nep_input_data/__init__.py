#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#import saio
import os
import pandas as pd
import sqlalchemy as sql
from egon.data import utils
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Float, func, Integer
from sqlalchemy.ext.declarative import declarative_base

### will be later imported from another file ###
Base = declarative_base()
# TODO: Add metadata for both tables
class EgonScenarioCapacities(Base):
        __tablename__ = 'egon_scenario_capacities'
        __table_args__ = {'schema': 'model_draft'}
        index = Column(Integer, primary_key=True)
        country = Column(String(50))
        component = Column(String(25))
        carrier = Column(String(50))
        capacity = Column(Float)
        nuts = Column(String(12))
        scenario_name = Column(String(50))

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

    engine = connect_to_engine()

    EgonScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    NEP2021Kraftwerksliste.__table__.create(bind=engine, checkfirst=True)

def manipulate_federal_state_numbers(df, carrier, scn = 'C 2035'):
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

    target_file = os.path.join(
        os.path.dirname(__file__), 'NEP_2021_Genehmiging_C2035.csv')

    df_g = pd.read_csv(target_file,
                       delimiter=';', decimal=',',
                       index_col='Unnamed: 0')

    target_cap = df_g[~df_g.index.map(carrier).isnull()][scn]

    target_cap.index = target_cap.index.map(carrier)

    for c in target_cap.index:
        mask = df.carrier == c
        df.loc[mask, 'capacity']= \
            df[mask].capacity /\
                df[mask].capacity.sum() * target_cap[c]
    return df

def insert_capacities_per_federal_state_nep():

    engine = connect_to_engine()

    # read-in installed capacities per federal state of germany (Entwurf des Szenariorahmens)
    target_file = os.path.join(
        os.path.dirname(__file__), 'NEP_2021_C2035.csv')

    df = pd.read_csv(target_file,
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
                     'Oel': 'oil',
                     'Haushaltswaermepumpen': 'residential_rural_heat_pump',
                     'Elektromobilitaet gesamt': 'transport',
                     'Elektromobilitaet privat': 'transport'}

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

        data = pd.DataFrame(df[bl])
        data['carrier'] = data.index.map(rename_carrier)
        data = data.groupby(data.carrier).sum().reset_index()
        data['component'] = 'generator'
        data['country'] = 'Deutschland'
        data['nuts'] = nuts1[bl]
        data['scenario_name'] = 'NEP 2035'

        # According to NEP, each heatpump has 3kW_el installed capacity
        data.loc[data.carrier == 'residential_rural_heat_pump', bl] *= 3e-6
        # TODO: how to deal with number of EV?

        data.loc[data.carrier ==
                 'residential_rural_heat_pump', 'component'] = 'link'
        data.loc[data.carrier == 'transport', 'component'] = 'load'
        data = data.rename(columns={bl: 'capacity'})

        insert_data = insert_data.append(data)

    # Scale numbers for federal states based on BNetzA (can be removed later)
    if True:
        insert_data = manipulate_federal_state_numbers(
            insert_data, rename_carrier, scn = 'C 2035')

    # # Set Multiindex to fit to primary keys in table
    # insert_data.set_index(['state', 'scenario_name', 'carrier', 'component'],
    #                       inplace=True)

    # Insert data to db
    try:
        insert_data.to_sql('egon_scenario_capacities',
                       engine,
                       schema='model_draft',
                       if_exists='append',
                       index=insert_data.index)
    except:
        print('data already exists')

    # Add district heating data accordning to energy and full load hours
    district_heating_input()

def insert_nep_list_powerplants():
    # Connect to database
    engine = connect_to_engine()

    # Read-in data from csv-file
    target_file = os.path.join(
        os.path.dirname(__file__), 'Kraftwerksliste_NEP_2021_konv.csv')
    kw_liste_nep = pd.read_csv(target_file,
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

def district_heating_input():

    file = os.path.join(
        os.path.dirname(__file__), 'NEP_2021_C2035_district_heating.csv')

    df = pd.read_csv(file, delimiter=';', dtype={'Wert':float})

    df.set_index(['Energietraeger', 'Name'], inplace=True)

    # Connect to database
    engine = connect_to_engine()
    session = sessionmaker(bind=engine)()

    for c in ['Grosswaermepumpe', 'Elektrodenheizkessel']:
        entry = EgonScenarioCapacities(
            component = 'link',
            scenario_name = 'NEP 2035',
            country = 'Deutschland',
            carrier = 'urban_central_'+ (
                'heat_pump' if c=='Grosswaermepumpe' else 'resistive_heater'),
            capacity = df.loc[(c, 'Fernwaermeerzeugung'), 'Wert']*1e3/
                        df.loc[(c, 'Volllaststunden'), 'Wert']/
                            df.loc[(c, 'Wirkungsgrad'), 'Wert'])

        session.add(entry)

    for c in ['Geothermie', 'Solarthermie']:
        entry = EgonScenarioCapacities(
        component = 'generator',
        scenario_name = 'NEP 2035',
        country = 'Deutschland',
        carrier = 'urban_central_'+ (
                'solar_thermal_collector' if c =='Solarthermie'
                                else 'geo_thermal'),
        capacity = df.loc[(c, 'Fernwaermeerzeugung'), 'Wert']*1e3/
                        df.loc[(c, 'Volllaststunden'), 'Wert'])

        session.add(entry)

    session.commit()

def setup_nep_scenario():

    add_schema()
    create_input_tables_nep()
    insert_capacities_per_federal_state_nep()
    insert_nep_list_powerplants()

#setup_nep_scenario()
test = os.path.join(
        os.path.dirname(__file__), 'Kraftwerksliste_NEP_2021_konv.csv')