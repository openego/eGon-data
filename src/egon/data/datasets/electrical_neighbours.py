"""The central module containing all code dealing with electrical neighbours
"""

import geopandas as gpd
from egon.data import db, config
from egon.data.datasets import Dataset
from egon.data.datasets.heat_etrago import next_id
from shapely.geometry import LineString

class ElectricalNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ElectricalNeighbours",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(insert_egon2035),
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
        """
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
        """
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

def insert_egon2035():
    # Select sources and targets from dataset configuration
    sources = config.datasets()['electrical_neighbours']['sources']
    targets = config.datasets()['electrical_neighbours']['targets']

    central_buses = buses_egon2035(sources, targets)

    foreign_lines = cross_border_lines(sources, targets, central_buses)

    central_transformer(sources, targets, central_buses, foreign_lines)
