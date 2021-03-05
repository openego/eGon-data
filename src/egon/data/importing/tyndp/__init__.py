"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2031, Szenario C
"""

import os
import zipfile
from urllib.request import urlretrieve
import egon.data.config
import pandas as pd
import geopandas as gpd
from egon.data import db
from sqlalchemy.orm import sessionmaker
import egon.data.importing.etrago as etrago



def download_tyndp_data():
    """ Download input data from TYNDP 2020

    Returns
    -------
    None.

    """
    config = egon.data.config.datasets()["scenario_input"]['eGon2035']['tyndp']

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

def insert_buses(map_buses):

    cfg = egon.data.config.datasets()["scenario_input"]['eGon2035']['tyndp']

    # insert installed capacities
    file = zipfile.ZipFile(os.path.join(
        os.path.dirname(__file__), cfg['capacities']['target_path']))

    # Select buses in neighbouring countries
    buses = pd.read_excel(
        file.open('TYNDP-2020-Scenario-Datafile.xlsx').read(),
        sheet_name='Nodes - Dict')
    buses = buses[buses.country.isin(cfg['countries'])]

    buses = buses[~buses.node_id.isin(map_buses.keys())]

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    for i, row in buses.iterrows():
        entry = etrago.EgonPfHvBus(
            version = '0.0.0',
            scn_name = 'eGon2035',
            bus_id = i,
            v_nom = 380,
            carrier = 'AC',
            x = row.longitude,
            y = row.latitude,
            geom = f'SRID=4326;POINT({row.longitude} {row.latitude})'
            )
        session.add(entry)

    session.commit()

    return buses

def get_foreign_bus_id():

    bus_id = gpd.read_postgis(
        """SELECT bus_id, geom
        FROM grid.egon_pf_hv_bus
        WHERE version = '0.0.0'""",
        con=db.engine())

    cfg = egon.data.config.datasets()["scenario_input"]['eGon2035']['tyndp']

    # insert installed capacities
    file = zipfile.ZipFile(os.path.join(
        os.path.dirname(__file__), cfg['capacities']['target_path']))

    # Select buses in neighbouring countries
    buses = pd.read_excel(
        file.open('TYNDP-2020-Scenario-Datafile.xlsx').read(),
        sheet_name='Nodes - Dict').query("longitude==longitude")

    buses = gpd.GeoDataFrame(buses, crs = 4326,
                       geometry=gpd.points_from_xy(buses.longitude,
                                                   buses.latitude))

    return gpd.sjoin(buses, bus_id).set_index('node_id').bus_id

def calc_capacities():
    config = egon.data.config.datasets()["scenario_input"]['eGon2035']['tyndp']

    # insert installed capacities
    file = zipfile.ZipFile(os.path.join(
         '/home/clara/GitHub/eGon-data/src/egon/data/importing/tyndp/', config['capacities']['target_path']))

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
    return grouped_capacities[
        grouped_capacities['Node/Line'].str[:2].isin(config['countries'])]


def insert_generators(capacities, map_buses):

    gen = capacities[capacities.carrier.isin([
        'other_non_renewable', 'wind_offshore',
       'wind_onshore', 'solar', 'other_renewable', 'reservoir',
       'run_of_river', 'lignite', 'coal',
       'oil', 'nuclear'])]

    # Set bus_id
    gen['bus'] = 0
    gen.loc[:, 'bus'] = gen.loc[:,'Node/Line'].map(get_foreign_bus_id())
    gen.loc[gen[gen.bus.isnull()].index, 'bus'] = (
        gen.loc[gen[gen.bus.isnull()].index,'Node/Line']
        .map(map_buses).map(get_foreign_bus_id()))
    gen.loc[:, 'bus'] = gen.bus.astype(int)

    # insert data
    session = sessionmaker(bind=db.engine())()
    for i, row in gen.iterrows():
        entry = etrago.EgonPfHvGenerator(
            version = '0.0.0',
            scn_name = 'eGon2035',
            generator_id = i,
            bus = row.bus,
            p_nom = row.cap_2035)

        session.add(entry)
    session.commit()

def insert_stores(capacities, map_buses):

    store = capacities[capacities.carrier.isin([
        'battery', 'pumped_hydro'])]



def insert_links(capacities, map_buses):

    link = capacities[capacities.carrier.isin([
        'power_to_gas', 'gas', 'biogas'])]


def insert_tyndp():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.

    """
    map_buses = {
        'DK00': 'DKW1',
        'DKKF': 'DKE1',
        'FR15': 'FR00',
        'NON1': 'NOM1',
        'NOS0': 'NOM1',
        'NOS1': 'NOM1',
        'PLE0': 'PL00',
        'PLI0': 'PL00',
        'SE00': 'SE02',
        'SE01': 'SE02',
        'SE03': 'SE02',
        'SE04': 'SE02'
        }

    insert_buses(map_buses)

    capacities = calc_capacities()

    insert_generators(capacities, map_buses)

    insert_stores(capacities, map_buses)

    insert_links(capacities, map_buses)

# def insert_tyndp_timeseries():
#     """Copy load timeseries data from TYNDP 2020.
#     According to NEP 2021, the data for 2030 and 2040 is interpolated linearly.

#     Returns
#     -------
#     None.

#     """
#     # Connect to database
#     engine = db.engine()
#     session = sessionmaker(bind=engine)()

#     config = egon.data.config.datasets()["scenario_input"]['eGon2035']['tyndp']
#     data_config = egon.data.config.datasets()

#     nodes = ['AT00', 'BE00', 'CH00', 'CZ00', 'DKE1', 'DKW1', 'FR00', 'NL00',
#              'LUB1', 'LUF1', 'LUG1', 'NOM1', 'NON1', 'NOS0', 'SE01', 'SE02',
#              'SE03', 'SE04', 'PL00', 'UK00', 'UKNI']

#     dataset_2030 = pd.read_excel(
#         os.path.join(os.path.dirname(__file__),
#                      config['demand_2030']['target_path']),
#         sheet_name=nodes, skiprows=10)

#     dataset_2040 = pd.read_excel(
#         os.path.join(os.path.dirname(__file__),
#                      config['demand_2040']['target_path']),
#         sheet_name=None, skiprows=10)

#     for node in nodes:

#         data_2030 = dataset_2030[node][data_config['weather']['year']]

#         try:
#             data_2040 = dataset_2040[node][data_config['weather']['year']]
#         except:
#             data_2040 = data_2030

#         data_2035 = ((data_2030+data_2040)/2)[:8760]*1e-3

#         entry = EgonScenarioTimeseries(
#             component = 'load',
#             scenario_name = 'eGon2035',
#             node = node,
#             carrier = 'all',
#             data = list(data_2035.values))

#         session.add(entry)

#     session.commit()