#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 13:43:07 2021

@author: clara
"""

import geopandas as gpd
from egon.data import db, config
from egon.data.datasets.heat_etrago import next_id
from shapely.geometry import LineString


# Select sources and targets from dataset configuration
sources = config.datasets()['electrical_neighbours']['sources']
#target = config.datasets()['electrical_neighbours']['targets']

def get_cross_border_buses():
    return db.select_geodataframe(
        f"""
        SELECT *
        FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE
        NOT ST_INTERSECTS (
            geom,
            (SELECT ST_Transform(geometry, 4326) FROM
             {sources['german_borders']['schema']}.
            {sources['german_borders']['table']}))
        AND scn_name = 'eGon2035';
        """,
        epsg=4326)

def get_cross_border_lines():
    return db.select_geodataframe(
    f"""
    SELECT *
    FROM {sources['lines']['schema']}.{sources['lines']['table']}
    WHERE
    ST_INTERSECTS (
        grid.egon_pf_hv_line.topo,
        (SELECT ST_Transform(ST_boundary(geometry), 4326)
         FROM {sources['german_borders']['schema']}.
        {sources['german_borders']['table']}))
    AND scn_name = 'eGon2035';
    """,
    epsg=4326)

def buses_egon2035():

    # Delete existing buses
    db.execute_sql(
        f"""
        DELETE FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE country != 'DE'
        AND scn_name = 'eGon2035'
        AND bus_id NOT IN (
                SELECT bus0
                FROM {sources['lines']['schema']}.
                {sources['lines']['table']}
                WHERE scn_name = 'eGon2035')
        AND bus_id NOT IN (
                SELECT bus1
                FROM {sources['lines']['schema']}.
                {sources['lines']['table']}
                WHERE scn_name = 'eGon2035')
        AND carrier = 'AC'
        """)

    central_buses = db.select_dataframe(
        f"""
        SELECT *
        FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE country != 'DE'
        AND scn_name = 'eGon100RE'
        AND bus_id NOT IN (
                SELECT bus_id
                FROM {sources['electricity_buses']['schema']}.
                {sources['electricity_buses']['table']}
                WHERE
                NOT ST_INTERSECTS (
                    geom,
                    (SELECT ST_Transform(geometry, 4326) FROM
                     {sources['german_borders']['schema']}.
                     {sources['german_borders']['table']}))
                AND scn_name = 'eGon2035')
        AND carrier = 'AC'
        """)

    # if in test mode, add bus in center of Germany
    if config.settings()['egon-data']['--dataset-boundary'] != 'Everything':
        central_buses = central_buses.append({
                'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': next_id('bus'),
             'x': 10.4234469,
             'y': 51.0834196,
             'country': 'DE',
             'carrier': 'AC',
             'v_nom': 380.,
             },
            ignore_index=True)

    # Add buses for other voltage levels
    foreign_buses = get_cross_border_buses()
    vnom_per_country = foreign_buses.groupby('country').v_nom.unique().copy()
    for cntr in vnom_per_country.index:
        if 110. in vnom_per_country[cntr]:
            central_buses = central_buses.append({
            'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': central_buses.bus_id.max()+1,
             'x': central_buses[central_buses.country==cntr].x.unique()[0],
             'y': central_buses[central_buses.country==cntr].y.unique()[0],
             'country': cntr,
             'carrier': 'AC',
             'v_nom': 110.,
             },
            ignore_index=True)
        if 220. in vnom_per_country[cntr]:
            central_buses = central_buses.append({
            'version': '0.0.0',
             'scn_name': 'eGon2035',
             'bus_id': central_buses.bus_id.max()+1,
             'x': central_buses[central_buses.country==cntr].x.unique()[0],
             'y': central_buses[central_buses.country==cntr].y.unique()[0],
             'country': cntr,
             'carrier': 'AC',
             'v_nom': 220.,
             },
            ignore_index=True)

    # Add geometry column
    central_buses = gpd.GeoDataFrame(
        central_buses,
        geometry=gpd.points_from_xy(central_buses.x, central_buses.y),
        crs="EPSG:4326")

    central_buses['geom'] = central_buses.geometry.copy()

    central_buses = central_buses.set_geometry('geom')

    central_buses.drop('geometry', axis='columns', inplace=True)

    # insert central buses for eGon2035 based on pypsa-eur-sec
    central_buses.scn_name='eGon2035'
    central_buses.to_postgis('egon_pf_hv_bus', schema='grid',
                             if_exists = 'append', con=db.engine(), index=False)

    # # Update geometry column of central buses
    # db.execute_sql(
    #     f"""
    #     UPDATE {sources['electricity_buses']['schema']}.
    #         {sources['electricity_buses']['table']}
    #     SET geom = ST_SetSRID(ST_MakePoint(x, y), 4326)
    #     WHERE geom IS NULL
    #     AND carrier = 'AC'
    #     """)

    return central_buses

def cross_border_lines():

    central_buses = buses_egon2035()

    foreign_buses = get_cross_border_buses()

    lines = get_cross_border_lines()

    lines.loc[lines[lines.bus0.isin(foreign_buses.bus_id)].index, 'foreign_bus'] = \
      lines.loc[lines[lines.bus0.isin(foreign_buses.bus_id)].index, 'bus0']

    lines.loc[lines[lines.bus1.isin(foreign_buses.bus_id)].index, 'foreign_bus'] = \
      lines.loc[lines[lines.bus1.isin(foreign_buses.bus_id)].index, 'bus1']

    # Drop lines with start and endpoint in Germany
    lines = lines[lines.foreign_bus.notnull()]
    lines.loc[:, 'foreign_bus'] = lines.loc[:, 'foreign_bus'].astype(int)

    new_lines = lines.copy()

    new_lines.bus0= new_lines.foreign_bus.copy()

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

    # Drop intermediate columns
    new_lines.drop(
        ['foreign_bus', 'country', 'geom_bus0', 'geom_bus1', 'geom'],
        axis='columns', inplace=True)

    # Insert lines to the database
    new_lines.to_postgis('egon_pf_hv_line', schema='grid', if_exists = 'append', con=db.engine())


