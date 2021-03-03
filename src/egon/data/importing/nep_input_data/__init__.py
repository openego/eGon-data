"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2031, Szenario C
"""

import os
import egon.data.config
import pandas as pd
from egon.data import db
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

### will be later imported from another file ###
Base = declarative_base()

class EgonScenarioCapacities(Base):
    __tablename__ = 'egon_scenario_capacities'
    __table_args__ = {'schema': 'supply'}
    index = Column(Integer, primary_key=True)
    country = Column(String(50))
    component = Column(String(25))
    carrier = Column(String(50))
    capacity = Column(Float)
    nuts = Column(String(12))
    scenario_name = Column(String(50))

class NEP2021Kraftwerksliste(Base):
    __tablename__ = 'nep_2021_conv_powerplants'
    __table_args__ = {'schema': 'supply'}
    index =  Column(String(50), primary_key=True)
    bnetza_id = Column(String(50))
    name = Column(String(100))
    name_unit = Column(String(50))
    carrier = Column(String(12))
    chp = Column(String(12))
    postcode = Column(String(12))
    city = Column(String(50))
    federal_state = Column(String(12))
    commissioned = Column(String(12))
    status = Column(String(50))
    capacity = Column(Float)
    a2035_chp = Column(String(12))
    a2035_capacity = Column(Float)
    b2035_chp = Column(String(12))
    b2035_capacity = Column(Float)
    c2035_chp = Column(String(12))
    c2035_capacity = Column(Float)
    b2040_chp = Column(String(12))
    b2040_capacity = Column(Float)

def scenario_config(scn_name):
    """Get scenario settings from datasets.yml

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    dict
        Configuration data for the specified scenario

    """
    data_config = egon.data.config.datasets()

    return data_config["scenario_input"][scn_name]

def create_scenario_input_tables():
    """Create input tables for scenario setup

    Returns
    -------
    None.

    """

    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    EgonScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    NEP2021Kraftwerksliste.__table__.create(bind=engine, checkfirst=True)

def insert_capacities_per_federal_state_nep():
    """Inserts installed capacities per federal state accordning to
    NEP 2035 (version 2021), scenario 2035 C

    Returns
    -------
    None.

    """

    # Connect to local database
    engine = db.engine()

    # Delete rows if already exist
    db.execute_sql("DELETE FROM supply.egon_scenario_capacities "
                   "WHERE scenario_name = 'eGon2035' "
                   "AND country = 'Deutschland'")

    # read-in installed capacities per federal state of germany
    target_file = os.path.join(
        os.path.dirname(__file__),
        scenario_config('eGon2035')['paths']['capacities'])

    df = pd.read_excel(target_file, sheet_name='1.Entwurf_NEP2035_V2021',
                     index_col='Unnamed: 0')

    df_draft = pd.read_excel(target_file,
                             sheet_name='Entwurf_des_Szenariorahmens',
                             index_col='Unnamed: 0')

    # sort NEP-carriers:
    rename_carrier = {'Wind onshore': 'wind_onshore',
                     'Wind offshore': 'wind_offshore',
                     'Sonstige Konventionelle': 'other_non_renewable',
                     'Speicherwasser': 'reservoir',
                     'Laufwasser': 'run_of_river',
                     'Biomasse': 'biomass',
                     'Erdgas': 'gas',
                     'Kuppelgas': 'gas',
                     'PV (Aufdach)': 'solar_rooftop',
                     'PV (Freiflaeche)': 'solar',
                     'Pumpspeicher': 'pumped_hydro',
                     'Sonstige EE': 'other_renewable',
                     'Oel': 'oil',
                     'Haushaltswaermepumpen': 'residential_rural_heat_pump',
                     'KWK < 10 MW': 'small_chp'}
                     #'Elektromobilitaet gesamt': 'transport',
                    # 'Elektromobilitaet privat': 'transport'}

    # nuts1 to federal state in Germany
    map_nuts = pd.read_sql(
        "SELECT DISTINCT ON (nuts) gen, nuts FROM boundaries.vg250_lan",
        engine, index_col='gen')

    insert_data = pd.DataFrame()

    scaled_carriers = ['Haushaltswaermepumpen',
                       'PV (Aufdach)', 'PV (Freiflaeche)']

    for bl in map_nuts.index:

        data = pd.DataFrame(df[bl])

        # if distribution to federal states is not provided,
        # use data from draft of scenario report
        for c in scaled_carriers:
            data.loc[c, bl] = (
                df_draft.loc[c, bl]/ df_draft.loc[c, 'Summe']
                * df.loc[c, 'Summe'])

        # split hydro into run of river and reservoir
        # according to draft of scenario report
        if data.loc['Lauf- und Speicherwasser', bl] > 0:
            for c in ['Speicherwasser', 'Laufwasser']:
                data.loc[c, bl] = data.loc['Lauf- und Speicherwasser', bl] *\
                    df_draft.loc[c, bl]/\
                        df_draft.loc[['Speicherwasser', 'Laufwasser'], bl].sum()


        data['carrier'] = data.index.map(rename_carrier)
        data = data.groupby(data.carrier).sum().reset_index()
        data['component'] = 'generator'
        data['country'] = 'Deutschland'
        data['nuts'] = map_nuts.nuts[bl]
        data['scenario_name'] = 'eGon2035'


        # According to NEP, each heatpump has 3kW_el installed capacity
        data.loc[data.carrier == 'residential_rural_heat_pump', bl] *= 3e-6
        data.loc[data.carrier ==
                 'residential_rural_heat_pump', 'component'] = 'link'

        data = data.rename(columns={bl: 'capacity'})

        # convert GW to MW
        data.capacity *= 1e3

        insert_data = insert_data.append(data)

    # Insert data to db
    insert_data.to_sql('egon_scenario_capacities',
                       engine,
                       schema='supply',
                       if_exists='append',
                       index=insert_data.index)


    # Add district heating data accordning to energy and full load hours
    district_heating_input()

def insert_nep_list_powerplants():
    """Insert list of conventional powerplants attachd to the approval
    of the scenario report by BNetzA

    Returns
    -------
    None.

    """
    # Connect to local database
    engine = db.engine()

    # Read-in data from csv-file
    target_file = os.path.join(
        os.path.dirname(__file__),
        scenario_config('eGon2035')['paths']['list_conv_pp'])
    kw_liste_nep = pd.read_csv(target_file,
                               delimiter=';', decimal=',')

    # Adjust column names
    kw_liste_nep = kw_liste_nep.rename(columns={'BNetzA-ID': 'bnetza_id',
                                 'Kraftwerksname': 'name',
                                 'Blockname': 'name_unit',
                                 'Energieträger': 'carrier',
                                 'KWK\nJa/Nein': 'chp',
                                 'PLZ': 'postcode',
                                 'Ort': 'city',
                                 'Bundesland/\nLand': 'federal_state',
                                 'Inbetrieb-\nnahmejahr': 'commissioned',
                                 'Status': 'status',
                                 'el. Leistung\n06.02.2020': 'capacity',
                                 'A 2035:\nKWK-Ersatz': 'a2035_chp',
                                 'A 2035:\nLeistung': 'a2035_capacity',
                                 'B 2035\nKWK-Ersatz':'b2035_chp',
                                 'B 2035:\nLeistung':'b2035_capacity',
                                 'C 2035:\nKWK-Ersatz': 'c2035_chp',
                                 'C 2035:\nLeistung': 'c2035_capacity',
                                 'B 2040:\nKWK-Ersatz': 'b2040_chp',
                                 'B 2040:\nLeistung': 'b2040_capacity'})

    # Insert data to db
    kw_liste_nep.to_sql('nep_2021_kraftwerksliste',
                       engine,
                       schema='supply',
                       if_exists='replace')

def district_heating_input():
    """Imports data for district heating networks in Germany

    Returns
    -------
    None.

    """
    # import data to dataframe
    file = os.path.join(
        os.path.dirname(__file__),
        scenario_config('eGon2035')['paths']['capacities'])
    df = pd.read_excel(file, sheet_name='Kurzstudie_KWK', dtype={'Wert':float})
    df.set_index(['Energietraeger', 'Name'], inplace=True)

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    # insert heatpumps and resistive heater as link
    for c in ['Grosswaermepumpe', 'Elektrodenheizkessel']:
        entry = EgonScenarioCapacities(
            component = 'link',
            scenario_name = 'eGon2035',
            country = 'Deutschland',
            carrier = 'urban_central_'+ (
                'heat_pump' if c=='Grosswaermepumpe' else 'resistive_heater'),
            capacity = df.loc[(c, 'Fernwaermeerzeugung'), 'Wert']*1e6/
                        df.loc[(c, 'Volllaststunden'), 'Wert']/
                            df.loc[(c, 'Wirkungsgrad'), 'Wert'])

        session.add(entry)

    # insert solar- and geothermal as generator
    for c in ['Geothermie', 'Solarthermie']:
        entry = EgonScenarioCapacities(
        component = 'generator',
        scenario_name = 'eGon2035',
        country = 'Deutschland',
        carrier = 'urban_central_'+ (
                'solar_thermal_collector' if c =='Solarthermie'
                                else 'geo_thermal'),
        capacity = df.loc[(c, 'Fernwaermeerzeugung'), 'Wert']*1e6/
                        df.loc[(c, 'Volllaststunden'), 'Wert'])

        session.add(entry)

    session.commit()

def insert_data_nep():
    """Overall function for importing scenario input data for eGon2035 scenario

    Returns
    -------
    None.

    """

    insert_capacities_per_federal_state_nep()

    insert_nep_list_powerplants()
