"""The central module containing all code dealing with electrical neighbours
"""

import geopandas as gpd
import pandas as pd
from egon.data import db, config
from egon.data.datasets import Dataset
from egon.data.datasets.heat_etrago import next_id
from shapely.geometry import LineString
import zipfile
from sqlalchemy.orm import sessionmaker
import egon.data.importing.etrago as etrago

class ElectricalNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ElectricalNeighbours",
            version="0.0.0.dev",
            dependencies=dependencies,
            tasks=(grid, {tyndp_generation, tyndp_demand}),
        )

def get_cross_border_buses(sources, targets):
    return db.select_geodataframe(
        f"""
        SELECT *
        FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE
        NOT ST_INTERSECTS (
            geom,
            (SELECT ST_Transform(ST_Buffer(geometry, 5), 4326) FROM
             {sources['german_borders']['schema']}.
            {sources['german_borders']['table']}))
        AND (bus_id IN (
            SELECT bus0 FROM
            {sources['lines']['schema']}.{sources['lines']['table']})
            OR bus_id IN (
            SELECT bus1 FROM
            {sources['lines']['schema']}.{sources['lines']['table']}))
        AND scn_name = 'eGon2035';
        """,
        epsg=4326)

def get_cross_border_lines(sources, targets):
    return db.select_geodataframe(
    f"""
    SELECT *
    FROM {sources['lines']['schema']}.{sources['lines']['table']} a
    WHERE
    ST_INTERSECTS (
        a.topo,
        (SELECT ST_Transform(ST_boundary(geometry), 4326)
         FROM {sources['german_borders']['schema']}.
        {sources['german_borders']['table']}))
    AND scn_name = 'eGon2035';
    """,
    epsg=4326)

def central_buses_egon100(sources, targets):
    return db.select_dataframe(
        f"""
        SELECT *
        FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE country != 'DE'
        AND scn_name = 'eGon100RE'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        AND carrier = 'AC'
        """)

def buses_egon2035(sources, targets):

    # Delete existing buses
    db.execute_sql(
        f"""
        DELETE FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE country != 'DE' AND scn_name = 'eGon2035' AND carrier = 'AC'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """)

    central_buses = central_buses_egon100(sources, targets)

    next_bus_id = next_id('bus')+1

    # if in test mode, add bus in center of Germany
    if config.settings()['egon-data']['--dataset-boundary'] != 'Everything':
        central_buses = central_buses.append({
                'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': next_bus_id,
             'x': 10.4234469,
             'y': 51.0834196,
             'country': 'DE',
             'carrier': 'AC',
             'v_nom': 380.,
             },
            ignore_index=True)
        next_bus_id += 1

    # Add buses for other voltage levels
    foreign_buses = get_cross_border_buses(sources, targets)
    vnom_per_country = foreign_buses.groupby('country').v_nom.unique().copy()
    for cntr in vnom_per_country.index:
        if 110. in vnom_per_country[cntr]:
            central_buses = central_buses.append({
            'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': next_bus_id,
             'x': central_buses[central_buses.country==cntr].x.unique()[0],
             'y': central_buses[central_buses.country==cntr].y.unique()[0],
             'country': cntr,
             'carrier': 'AC',
             'v_nom': 110.,
             },
            ignore_index=True)
            next_bus_id += 1
        if 220. in vnom_per_country[cntr]:
            central_buses = central_buses.append({
            'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': next_bus_id,
             'x': central_buses[central_buses.country==cntr].x.unique()[0],
             'y': central_buses[central_buses.country==cntr].y.unique()[0],
             'country': cntr,
             'carrier': 'AC',
             'v_nom': 220.,
             },
            ignore_index=True)
            next_bus_id += 1

    # Add geometry column
    central_buses = gpd.GeoDataFrame(
        central_buses,
        geometry=gpd.points_from_xy(central_buses.x, central_buses.y),
        crs="EPSG:4326")
    central_buses['geom'] = central_buses.geometry.copy()
    central_buses = central_buses.set_geometry('geom').drop(
        'geometry', axis='columns')

    # insert central buses for eGon2035 based on pypsa-eur-sec
    central_buses.scn_name='eGon2035'
    central_buses.to_postgis(
        targets['buses']['table'], schema=targets['buses']['schema'],
        if_exists = 'append', con=db.engine(), index=False)

    return central_buses

def cross_border_lines(sources, targets, central_buses):
    db.execute_sql(
        f"""
        DELETE FROM {targets['lines']['schema']}.
        {targets['lines']['table']}
        WHERE scn_name = 'eGon2035'
        AND line_id NOT IN (
            SELECT branch_id
            FROM  {sources['osmtgmod_branch']['schema']}.
            {sources['osmtgmod_branch']['table']}
              WHERE result_id = 1 and (link_type = 'line' or
                                       link_type = 'cable'))
        """
        )
    foreign_buses = get_cross_border_buses(sources, targets)

    lines = get_cross_border_lines(sources, targets)

    lines.loc[lines[lines.bus0.isin(foreign_buses.bus_id)].index, 'foreign_bus'] = \
      lines.loc[lines[lines.bus0.isin(foreign_buses.bus_id)].index, 'bus0']

    lines.loc[lines[lines.bus1.isin(foreign_buses.bus_id)].index, 'foreign_bus'] = \
      lines.loc[lines[lines.bus1.isin(foreign_buses.bus_id)].index, 'bus1']

    # Drop lines with start and endpoint in Germany
    lines = lines[lines.foreign_bus.notnull()]
    lines.loc[:, 'foreign_bus'] = lines.loc[:, 'foreign_bus'].astype(int)

    new_lines = lines.copy()

    new_lines.bus0 = new_lines.foreign_bus.copy()

    new_lines['country'] = foreign_buses.set_index('bus_id').loc[
        lines.foreign_bus, 'country'].values

    new_lines.line_id = range(next_id('line'), next_id('line')+len(lines))

    for i, row in new_lines.iterrows():
        new_lines.loc[i, 'bus1'] = central_buses.bus_id[
            (central_buses.country==row.country)
            & (central_buses.v_nom==row.v_nom)].values[0]

    # Create geoemtry for new lines
    new_lines['geom_bus0'] = foreign_buses.set_index('bus_id').geom[new_lines.bus0].values
    new_lines['geom_bus1'] = central_buses.set_index('bus_id').geom[new_lines.bus1].values
    new_lines['topo']=new_lines.apply(
            lambda x: LineString([x['geom_bus0'], x['geom_bus1']]),axis=1)

    # Set topo as geometry column
    new_lines = new_lines.set_geometry('topo')

    # Calcultae length of lines based on topology
    old_length = new_lines['length'].copy()
    new_lines['length'] = new_lines.to_crs(3035).length/1000

    # Set electrical parameters based on lines from osmtgmod
    for parameter in ['x', 'r']:
        new_lines[parameter] = (new_lines[parameter]/old_length
                                *new_lines['length'])
    for parameter in ['b', 'g']:
        new_lines[parameter] = (new_lines[parameter]*old_length
                                /new_lines['length'])
    # Drop intermediate columns
    new_lines.drop(
        ['foreign_bus', 'country', 'geom_bus0', 'geom_bus1', 'geom'],
        axis='columns', inplace=True)



    # Insert lines to the database
    new_lines.to_postgis(
        targets['lines']['table'], schema=targets['lines']['schema'],
        if_exists = 'append', con=db.engine(), index=False)

    return new_lines

def choose_transformer(s_nom):

    if s_nom <= 600:
        return 600, 0.0002
    elif (s_nom > 600) & (s_nom<=1200):
        return 1200, 0.0001
    elif (s_nom > 1200) & (s_nom<=1600):
        return 1600, 0.000075
    elif (s_nom > 1600) & (s_nom<=2100):
        return 2100, 0.00006667
    elif (s_nom > 2100) & (s_nom<=2600):
        return 2600, 0.0000461538
    elif (s_nom > 2600) & (s_nom<=4800):
        return 4800, 0.000025
    elif (s_nom > 4800) & (s_nom<=6000):
        return 6000, 0.0000225
    elif (s_nom > 6000) & (s_nom<=7200):
        return 7200, 0.0000194444
    elif (s_nom > 7200) & (s_nom<=8000):
        return 8000, 0.000016875
    elif (s_nom > 8000) & (s_nom<=9000):
        return 9000, 0.000015
    elif (s_nom > 9000) & (s_nom<=13000):
        return 13000, 0.0000103846
    elif (s_nom > 13000) & (s_nom<=20000):
        return 20000, 0.00000675
    elif (s_nom > 20000) & (s_nom<=33000):
        return 33000, 0.00000409091


def central_transformer(sources, targets, central_buses, new_lines):

    trafo = gpd.GeoDataFrame(columns=['trafo_id', 'bus0', 'bus1', 's_nom'],
                             dtype=int)

    trafo_id = next_id('transformer')

    for i, row in central_buses[central_buses.v_nom!=380].iterrows():

        s_nom_0 = new_lines[new_lines.bus0==row.bus_id].s_nom.sum()
        s_nom_1 = new_lines[new_lines.bus1==row.bus_id].s_nom.sum()
        if s_nom_0 == 0.:
            s_nom = s_nom_1
        elif s_nom_1 == 0.:
            s_nom = s_nom_0
        else:
            s_nom = min([s_nom_0, s_nom_1])

        s_nom, x = choose_transformer(s_nom)

        trafo = trafo.append(
            {'trafo_id': trafo_id,
             'bus0': row.bus_id,
             'bus1': central_buses[(central_buses.v_nom==380)
                                   &(central_buses.country == row.country)
                                   ].bus_id.values[0],
             's_nom': s_nom,
             'x':x
             }, ignore_index=True)
        trafo_id +=1

    trafo = trafo.astype({'trafo_id': 'int',
                          'bus0': 'int', 'bus1': 'int'})

    trafo['version'] = '0.0.0'
    trafo['scn_name'] = 'eGon2035'

    db.execute_sql(
        f"""
        DELETE FROM {targets['transformers']['schema']}.
        {targets['transformers']['table']}
        WHERE scn_name = 'eGon2035'
        AND trafo_id IN (
            SELECT branch_id
            FROM {sources['osmtgmod_branch']['schema']}.
            {sources['osmtgmod_branch']['table']}
              WHERE result_id = 1 and link_type = 'transformer')
        """
        )

    # Insert transformers to the database
    trafo.to_sql(
        targets['transformers']['table'],
        schema=targets['transformers']['schema'],
        if_exists = 'append', con=db.engine(), index=False)

def grid():
    # Select sources and targets from dataset configuration
    sources = config.datasets()['electrical_neighbours']['sources']
    targets = config.datasets()['electrical_neighbours']['targets']

    central_buses = buses_egon2035(sources, targets)

    foreign_lines = cross_border_lines(sources, targets, central_buses)

    central_transformer(sources, targets, central_buses, foreign_lines)


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

def get_foreign_bus_id():

    sources = config.datasets()['electrical_neighbours']['sources']

    bus_id = db.select_geodataframe(
        """SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_pf_hv_bus
        WHERE version = '0.0.0'
        AND scn_name = 'eGon2035'
        AND carrier = 'AC'
        AND v_nom = 380.
        AND country != 'DE'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM osmtgmod_results.bus_data)
        """, epsg=3035)

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

    # Select buses in neighbouring countries
    buses = pd.read_excel(
        file.open('TYNDP-2020-Scenario-Datafile.xlsx').read(),
        sheet_name='Nodes - Dict').query("longitude==longitude")

    buses = gpd.GeoDataFrame(
        buses, crs = 4326,
        geometry=gpd.points_from_xy(
            buses.longitude, buses.latitude)).to_crs(3035)

    buses['bus_id'] = 0

    for i, row in buses.iterrows():
        distance = bus_id.set_index('bus_id').geom.distance(row.geometry)
        buses.loc[i, 'bus_id'] = distance[
            distance==distance.min()].index.values[0]


    return buses.set_index('node_id').bus_id

def calc_capacities():

    sources = config.datasets()['electrical_neighbours']['sources']

    countries = ["AT", "BE", "CH", "CZ", "DK", "FR", "NL",
                 "NO", "SE", "PL", "UK"]

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
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
        grouped_capacities['Node/Line'].str[:2].isin(countries)]

def insert_generators(capacities, map_buses):

    db.execute_sql(
        """
        DELETE FROM grid.egon_pf_hv_generator
        WHERE bus IN (
            SELECT bus_id FROM
            grid.egon_pf_hv_bus
            WHERE country != 'DE'
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        """)

    gen = capacities[capacities.carrier.isin([
        'other_non_renewable', 'wind_offshore',
       'wind_onshore', 'solar', 'other_renewable', 'reservoir',
       'run_of_river', 'lignite', 'coal',
       'oil', 'nuclear'])]

    # Set bus_id
    gen.loc[
        gen[gen['Node/Line'].isin(map_buses.keys())].index, 'Node/Line'
        ] = gen.loc[
            gen[gen['Node/Line'].isin(map_buses.keys())].index,
            'Node/Line'].map(map_buses)

    gen.loc[:, 'bus'] = get_foreign_bus_id().loc[gen.loc[:,'Node/Line']].values

    # insert data
    session = sessionmaker(bind=db.engine())()
    for i, row in gen.iterrows():
        entry = etrago.EgonPfHvGenerator(
            version = '0.0.0',
            scn_name = 'eGon2035',
            generator_id = int(next_id('generator')),
            bus = row.bus,
            carrier = row.carrier,
            p_nom = row.cap_2035)

        session.add(entry)
        session.commit()

def insert_storage(capacities, map_buses):

    store = capacities[capacities.carrier.isin([
        'battery', 'pumped_hydro'])]

    # Set bus_id
    store.loc[
        store[store['Node/Line'].isin(map_buses.keys())].index, 'Node/Line'
        ] = store.loc[
            store[store['Node/Line'].isin(map_buses.keys())].index,
            'Node/Line'].map(map_buses)

    store.loc[:, 'bus'] = get_foreign_bus_id().loc[
        store.loc[:,'Node/Line']].values

    # insert data
    session = sessionmaker(bind=db.engine())()
    for i, row in store.iterrows():
        entry = etrago.EgonPfHvStorage(
            version = '0.0.0',
            scn_name = 'eGon2035',
            storage_id = int(next_id('storage')),
            bus = row.bus,
            max_hours = (6 if row.carrier=='battery' else 168),
            carrier = row.carrier,
            p_nom = row.cap_2035)

        session.add(entry)
        session.commit()

def insert_links(capacities, map_buses):

    link = capacities[capacities.carrier.isin([
        'power_to_gas', 'gas', 'biogas'])]

def get_map_buses():
    return {
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
def tyndp_generation():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040
    Returns
    -------
    None.
    """
    map_buses = get_map_buses()

    capacities = calc_capacities()

    insert_generators(capacities, map_buses)

    insert_storage(capacities, map_buses)

    #insert_links(capacities, map_buses)

def tyndp_demand():
    """Copy load timeseries data from TYNDP 2020.
    According to NEP 2021, the data for 2030 and 2040 is interpolated linearly.

    Returns
    -------
    None.

    """
    map_buses = get_map_buses()


    sources = config.datasets()['electrical_neighbours']['sources']
    targets = config.datasets()['electrical_neighbours']['targets']

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_pf_hv_load
        WHERE
        scn_name = 'eGon2035'
        AND carrier = 'AC'
        AND bus NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """)
    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    nodes = ['AT00', 'BE00', 'CH00', 'CZ00', 'DKE1', 'DKW1', 'FR00', 'NL00',
              'LUB1', 'LUF1', 'LUG1', 'NOM1', 'NON1', 'NOS0', 'SE01', 'SE02',
              'SE03', 'SE04', 'PL00', 'UK00', 'UKNI']

    buses = pd.DataFrame({'nodes':nodes})
    # Set bus_id
    buses.loc[
        buses[buses.nodes.isin(map_buses.keys())].index, 'nodes'] = buses[
        buses.nodes.isin(map_buses.keys())].nodes.map(map_buses)

    buses.loc[:, 'bus'] = get_foreign_bus_id().loc[buses.loc[:,'nodes']].values

    buses.set_index('nodes', inplace=True)
    buses = buses[~buses.index.duplicated(keep='first')]

    dataset_2030 = pd.read_excel(f"tyndp/{sources['tyndp_demand_2030']}",
         sheet_name=nodes, skiprows=10)

    dataset_2040 = pd.read_excel(f"tyndp/{sources['tyndp_demand_2040']}",
         sheet_name=None, skiprows=10)

    map_series = pd.Series(map_buses)
    map_series = map_series[map_series.index.isin(nodes)]
    for bus in buses.index:

        nodes = [bus]

        if bus in map_series.values:
            nodes.extend(list(map_series[map_series==bus].index.values))

        load_id = next_id('load')

        data_2030 = pd.Series(index = range(8760), data = 0.)

        for node in nodes :
            data_2030 = dataset_2030[node][2011]+data_2030

       # data_2030 = pd.read_excel(f"tyndp/{sources['tyndp_demand_2030']}",
       # sheet_name=node, skiprows=10, engine='openpyxl')[2011]

        try:
            data_2040 = pd.Series(index = range(8760), data = 0.)

            for node in nodes :
                data_2040 = dataset_2040[node][2011]+data_2040
        except:
            data_2040 = data_2030

        data_2035 = ((data_2030+data_2040)/2)[:8760]*1e-3

        entry = etrago.EgonPfHvLoad(
            version = '0.0.0',
            scn_name = 'eGon2035',
            load_id = int(load_id),
            carrier = 'AC',
            bus = int(buses.bus[bus]))

        entry_ts = etrago.EgonPfHvLoadTimeseries(
            version = '0.0.0',
            scn_name = 'eGon2035',
            load_id = int(load_id),
            temp_id = 1,
            p_set = list(data_2035.values))

        session.add(entry)
        session.add(entry_ts)
        session.commit()