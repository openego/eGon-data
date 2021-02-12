"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2031, Szenario C
"""

import os
import zipfile
from urllib.request import urlretrieve
import egon.data.config
import pandas as pd
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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

class EgonScenarioTimeseries(Base):
    __tablename__ = 'egon_scenario_timeseries'
    __table_args__ = {'schema': 'model_draft'}
    index = Column(Integer, primary_key=True)
    node = Column(String(50))
    component = Column(String(25))
    carrier = Column(String(50))
    data = Column(ARRAY(Float))
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

def add_schema():
    """Add missing schemas to local database.
    Will be removed to a central place later.

    Returns
    -------
    None.

    """
    for schema in ['model_draft']:
        db.execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS {schema};")

def create_scenario_input_tables():
    """Create input tables for scenario setup

    Returns
    -------
    None.

    """

    engine = db.engine()

    EgonScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    EgonScenarioTimeseries.__table__.create(bind=engine, checkfirst=True)
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
    db.execute_sql("DELETE FROM model_draft.egon_scenario_capacities "
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
                       schema='model_draft',
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

def download_tyndp_data():
    """ Download input data from TYNDP 2020

    Returns
    -------
    None.

    """
    config = scenario_config('eGon2035')['tyndp']

    for dataset in ['capacities', 'demand_2030', 'demand_2040']:
        target_file = os.path.join(
            os.path.dirname(__file__), config[dataset]["target_path"]
        )

        if not os.path.isfile(target_file):
            urlretrieve(config[dataset]["url"], target_file)

def map_carriers_tyndp():
    """ Map carriers from TYNDP-data to carriers used in eGon

    Returns
    -------
    dict
        Carrier from TYNDP and eGon

    """
    return {
        'Battery': 'battery',
        'DSR': 'demand_side_response',
        'Gas CCGT new': 'gas',
        'Gas CCGT old 2': 'gas',
        'Gas CCGT present 1': 'gas',
        'Gas CCGT present 2': 'gas',
        'Gas conventional old 1': 'gas',
        'Gas conventional old 2': 'gas',
        'Gas OCGT new': 'gas',
        'Gas OCGT old': 'gas',
        'Gas CCGT old 1': 'gas',
        'Gas CCGT old 2 Bio': 'biogas',
        'Gas conventional old 2 Bio': 'biogas',
        'Hard coal new': 'coal',
        'Hard coal old 1': 'coal',
        'Hard coal old 2': 'coal',
        'Hard coal old 2 Bio': 'coal',
        'Heavy oil old 1': 'oil',
        'Heavy oil old 1 Bio': 'oil',
        'Heavy oil old 2': 'oil',
        'Light oil': 'oil',
        'Lignite new': 'lignite',
        'Lignite old 1': 'lignite',
        'Lignite old 2': 'lignite',
        'Lignite old 1 Bio': 'lignite',
        'Lignite old 2 Bio': 'lignite',
        'Nuclear': 'nuclear',
        'Offshore Wind': 'wind_offshore',
        'Onshore Wind': 'wind_onshore',
        'Other non-RES': 'other_non_renewable',
        'Other RES': 'other_renewable',
        'P2G': 'power_to_gas',
        'PS Closed': 'pumped_hydro',
        'PS Open': 'reservoir',
        'Reservoir': 'reservoir',
        'Run-of-River': 'run_of_river',
        'Solar PV': 'solar',
        'Solar Thermal':'other_renewable',
        'Waste': 'Other RES'


}

def insert_typnd_capacities():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.

    """
    # Delete rows if already exist
    db.execute_sql("DELETE FROM model_draft.egon_scenario_capacities "
                   "WHERE scenario_name = 'eGon2035' "
                   "AND country != 'Deutschland'")

    config = scenario_config('eGon2035')['tyndp']

    # insert installed capacities
    file = zipfile.ZipFile(os.path.join(
        os.path.dirname(__file__),
        scenario_config('eGon2035')['tyndp']['capacities']['target_path']))

    df = pd.read_excel(
        file.open('TYNDP-2020-Scenario-Datafile.xlsx').read(),
        sheet_name='Capacity')

    # differneces between different climate years are very small (<1MW)
    # choose 1984 because it is the mean value
    df_2030 = df.rename(
        {'Climate Year':'Climate_Year'}, axis = 'columns').query(
            'Scenario == "Distributed Energy" & Year == 2030 & '
            'Climate_Year == 1984'
            ).set_index(['Node/Line', 'Generator_ID'])

    df_2040 =  df.rename(
        {'Climate Year':'Climate_Year'}, axis = 'columns').query(
            'Scenario == "Distributed Energy" & Year == 2040 & '
            'Climate_Year == 1984'
            ).set_index(['Node/Line', 'Generator_ID'])

    # interpolate linear between 2030 and 2040 for 2035 accordning to
    # scenario report of TSO's and the approval by BNetzA
    df_2035 = pd.DataFrame(index=df_2030.index)
    df_2035['cap_2030'] = df_2030.Value
    df_2035['cap_2040'] = df_2040.Value
    df_2035['cap_2035'] = df_2035['cap_2030'] + (
        df_2035['cap_2040']-df_2035['cap_2030'])/2
    df_2035 = df_2035.reset_index()
    df_2035['carrier'] = df_2035.Generator_ID.map(map_carriers_tyndp())

    # group capacities by new carriers
    grouped_capacities = df_2035.groupby(
        ['carrier', 'Node/Line']).cap_2035.sum().reset_index()

    # choose capacities for considered countries
    grouped_capacities = grouped_capacities[
        grouped_capacities['Node/Line'].str[:2].isin(config['countries'])]

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    # insert data
    for i in grouped_capacities.index:
        if grouped_capacities['carrier'][i] in [
                'battery', 'pumped_hydro', 'demand_side_response']:
            comp = 'storage_unit'
        elif grouped_capacities['carrier'][i] == 'power_to_gas':
            comp = 'link'
        else:
            comp = 'generator'
        entry = EgonScenarioCapacities(
            component = comp,
            scenario_name = 'eGon2035',
            country = grouped_capacities['Node/Line'][i], # not country but node (DK/UK)
            carrier = grouped_capacities['carrier'][i],
            capacity = grouped_capacities['cap_2035'][i])

        session.add(entry)

    session.commit()



def insert_tyndp_timeseries():
    """Copy load timeseries data from TYNDP 2020.
    According to NEP 2021, the data for 2030 and 2040 is interpolated linearly.

    Returns
    -------
    None.

    """
    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    config = scenario_config('eGon2035')['tyndp']
    data_config = egon.data.config.datasets()

    nodes = ['AT00', 'BE00', 'CH00', 'CZ00', 'DKE1', 'DKW1', 'FR00', 'NL00',
             'LUB1', 'LUF1', 'LUG1', 'NOM1', 'NON1', 'NOS0', 'SE01', 'SE02',
             'SE03', 'SE04', 'PL00', 'UK00', 'UKNI']

    dataset_2030 = pd.read_excel(
        os.path.join(os.path.dirname(__file__),
                     config['demand_2030']['target_path']),
        sheet_name=nodes, skiprows=10)

    dataset_2040 = pd.read_excel(
        os.path.join(os.path.dirname(__file__),
                     config['demand_2040']['target_path']),
        sheet_name=None, skiprows=10)

    for node in nodes:

        data_2030 = dataset_2030[node][data_config['weather']['year']]

        try:
            data_2040 = dataset_2040[node][data_config['weather']['year']]
        except:
            data_2040 = data_2030

        data_2035 = ((data_2030+data_2040)/2)[:8760]*1e-3

        entry = EgonScenarioTimeseries(
            component = 'load',
            scenario_name = 'eGon2035',
            node = node,
            carrier = 'all',
            data = list(data_2035.values))

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

    download_tyndp_data()

    insert_typnd_capacities()

    insert_tyndp_timeseries()
