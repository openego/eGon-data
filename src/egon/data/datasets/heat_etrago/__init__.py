"""The central module containing all code dealing with heat sector in etrago
"""
import pandas as pd
import geopandas as gpd
from egon.data import db, config
from egon.data.datasets.heat_etrago.power_to_heat import (
    insert_central_power_to_heat,insert_individual_power_to_heat)
from egon.data.datasets import Dataset

def insert_buses(carrier, version='0.0.0', scenario='eGon2035'):
    """ Insert heat buses to etrago table

    Heat buses are divided into central and individual heating

    Parameters
    ----------
    carrier : str
        Name of the carrier, either 'central_heat' or 'rural_heat'
    version : str, optional
        Version number. The default is '0.0.0'.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    """
    sources = config.datasets()['etrago_heat']['sources']
    target = config.datasets()['etrago_heat']['targets']['heat_buses']
    # Delete existing heat buses (central or rural)
    db.execute_sql(
        f"""
        DELETE FROM {target['schema']}.{target['table']}
        WHERE scn_name = '{scenario}'
        AND carrier = '{carrier}'
        AND version = '{version}'
        """)

    # Select unused index of buses
    next_bus_id = db.next_etrago_id('bus')

    # initalize dataframe for heat buses
    heat_buses = gpd.GeoDataFrame(columns = [
        'version', 'scn_name', 'bus_id', 'carrier',
        'x', 'y', 'geom']).set_geometry('geom').set_crs(epsg=4326)

    # If central heat, create one bus per district heating area
    if carrier == 'central_heat':
        areas = db.select_geodataframe(
            f"""
            SELECT area_id, geom_polygon as geom
            FROM  {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
            WHERE scenario = '{scenario}'
            """,
            index_col='area_id'
            )
        heat_buses.geom = areas.centroid.to_crs(epsg=4326)
    # otherwise create one heat bus per hvmv substation
    # which represents aggregated individual heating for etrago
    else:
        mv_grids = db.select_geodataframe(
            f"""
            SELECT ST_Centroid(geom) AS geom
            FROM {sources['mv_grids']['schema']}.
            {sources['mv_grids']['table']}
            """)
        heat_buses.geom = mv_grids.geom.to_crs(epsg=4326)

    # Insert values into dataframe
    heat_buses.version = '0.0.0'
    heat_buses.scn_name = scenario
    heat_buses.carrier = carrier
    heat_buses.x = heat_buses.geom.x
    heat_buses.y = heat_buses.geom.y
    heat_buses.bus_id = range(next_bus_id, next_bus_id+len(heat_buses))

    # Insert data into database
    heat_buses.to_postgis(target['table'],
                        schema=target['schema'],
                        if_exists='append',
                        con=db.engine())

def insert_central_direct_heat(version = '0.0.0', scenario='eGon2035'):
    """ Insert renewable heating technologies (solar and geo thermal)

    Parameters
    ----------
    version : str, optional
        Version number. The default is '0.0.0'.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    sources = config.datasets()['etrago_heat']['sources']
    targets = config.datasets()['etrago_heat']['targets']

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_generators']['schema']}.
        {targets['heat_generators']['table']}
        WHERE carrier IN ('solar_thermal_collector', 'geo_thermal')
        AND scn_name = '{scenario}'
        AND version = '{version}'
        """)

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_generator_timeseries']['schema']}.
        {targets['heat_generator_timeseries']['table']}
        WHERE scn_name = '{scenario}'
        AND generator_id NOT IN (
            SELECT generator_id FROM
            grid.egon_pf_hv_generator
            WHERE version = '{version}'
            AND scn_name = '{scenario}')
        """)

    central_thermal = db.select_geodataframe(
            f"""
            SELECT district_heating_id, capacity, geometry, carrier
            FROM  {sources['district_heating_supply']['schema']}.
            {sources['district_heating_supply']['table']}
            WHERE scenario = '{scenario}'
            AND carrier IN (
                'solar_thermal_collector', 'geo_thermal')
            """,
            geom_col='geometry',
            index_col='district_heating_id')

    map_dh_id_bus_id = db.select_dataframe(
        f"""
        SELECT bus_id, area_id, id FROM
        {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        JOIN {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
        ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
        WHERE carrier = 'central_heat'
        AND scenario = '{scenario}'
        """,
        index_col='id')

    new_id = db.next_etrago_id('generator')

    generator = pd.DataFrame(
        data = {'version': version,
                'scn_name': scenario,
                'carrier': central_thermal.carrier,
                'bus': map_dh_id_bus_id.bus_id[central_thermal.index],
                'p_nom': central_thermal.capacity,
                'generator_id': range(
                    new_id, new_id+len(central_thermal))})

    solar_thermal = central_thermal[
        central_thermal.carrier=='solar_thermal_collector']

    weather_cells = db.select_geodataframe(
        f"""
        SELECT w_id, geom
        FROM {sources['weather_cells']['schema']}.
            {sources['weather_cells']['table']}
        """,
        index_col='w_id'
        )

    # Map solar thermal collectors to weather cells
    join = gpd.sjoin(weather_cells, solar_thermal)[['index_right']]

    feedin = db.select_dataframe(
        f"""
        SELECT w_id, feedin
        FROM {sources['solar_thermal_feedin']['schema']}.
            {sources['solar_thermal_feedin']['table']}
        WHERE carrier = 'solar_thermal'
        AND weather_year = 2011
        """,
        index_col='w_id')

    timeseries =  pd.DataFrame(
        data = {'version': version,
                'scn_name': scenario,
                'temp_id': 1,
                'p_max_pu': feedin.feedin[join.index].values,
                'generator_id': generator.generator_id[
                    generator.carrier=='solar_thermal_collector'].values
                    }
        ).set_index('generator_id')

    generator = generator.set_index('generator_id')

    generator.to_sql(
        targets['heat_generators']['table'],
        schema=targets['heat_generators']['schema'],
        if_exists='append',
        con=db.engine())

    timeseries.to_sql(
        targets['heat_generator_timeseries']['table'],
        schema=targets['heat_generator_timeseries']['schema'],
        if_exists='append',
        con=db.engine())

def buses(version='0.0.0'):
    """ Insert individual and district heat buses into eTraGo-tables

    Parameters
    ----------
    version : str, optional
        Version number. The default is '0.0.0'.

    Returns
    -------
    None.

    """

    insert_buses('central_heat', version=version, scenario='eGon2035')
    insert_buses('rural_heat', version=version, scenario='eGon2035')

def supply(version='0.0.0'):
    """ Insert individual and district heat supply into eTraGo-tables

    Parameters
    ----------
    version : str, optional
        Version number. The default is '0.0.0'.

    Returns
    -------
    None.

    """

    insert_central_direct_heat(version = '0.0.0', scenario='eGon2035')
    insert_central_power_to_heat(version, scenario='eGon2035')
    insert_individual_power_to_heat(version, scenario='eGon2035')

class HeatEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatEtrago",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(buses, supply),
        )
