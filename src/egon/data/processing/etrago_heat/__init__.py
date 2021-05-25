"""The central module containing all code dealing with heat sector in etrago
"""
import pandas as pd
import geopandas as gpd
from egon.data import db
from egon.data.processing.etrago_heat.power_to_heat import (
    insert_central_power_to_heat, next_id)

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

    # Delete existing heat buses (central or rural)
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_pf_hv_bus
        WHERE scn_name = '{scenario}'
        AND carrier = '{carrier}'
        AND version = '{version}'
        """)

    # Select unused index of buses
    next_bus_id = next_id('bus')

    # initalize dataframe for heat buses
    heat_buses = gpd.GeoDataFrame(columns = [
        'version', 'scn_name', 'bus_id', 'carrier',
        'x', 'y', 'geom']).set_geometry('geom').set_crs(epsg=4326)

    # If central heat, create one bus per district heating area
    if carrier == 'central_heat':
        areas = db.select_geodataframe(
            f"""
            SELECT area_id, geom_polygon as geom
            FROM demand.district_heating_areas
            WHERE scenario = '{scenario}'
            """,
            index_col='area_id'
            )
        heat_buses.geom = areas.centroid.to_crs(epsg=4326)
    # otherwise create one heat bus per hvmv substation
    # which represents aggregated individual heating for etrago
    else:
        hvmv_substation = db.select_geodataframe(
            """
            SELECT point AS geom FROM grid.egon_hvmv_substation
            """)
        heat_buses.geom = hvmv_substation.geom.to_crs(epsg=4326)

    # Insert values into dataframe
    heat_buses.version = '0.0.0'
    heat_buses.scn_name = scenario
    heat_buses.carrier = carrier
    heat_buses.x = heat_buses.geom.x
    heat_buses.y = heat_buses.geom.y
    heat_buses.bus_id = range(next_bus_id, next_bus_id+len(heat_buses))

    # Insert data into database
    heat_buses.to_postgis('egon_pf_hv_bus',
                        schema='grid',
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

    db.execute_sql(
        """
        DELETE FROM grid.egon_pf_hv_generator
        WHERE carrier IN ('solar_thermal_collector', 'geo_thermal')
        AND scn_name = '{scenario}'
        AND version = '{version}'
        """)

    db.execute_sql(
        """
        DELETE FROM grid.egon_pf_hv_generator_timeseries
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
            FROM supply.egon_district_heating
            WHERE scenario = '{scenario}'
            AND carrier IN (
                'solar_thermal_collector', 'geo_thermal')
            """,
            geom_col='geometry',
            index_col='district_heating_id')

    map_dh_id_bus_id = db.select_dataframe(
        f"""
        SELECT bus_id, area_id, id FROM grid.egon_pf_hv_bus
        JOIN demand.district_heating_areas
        ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
        WHERE carrier = 'central_heat'
        AND scenario = '{scenario}'
        """,
        index_col='id')

    new_id = next_id('generator')

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
        """
        SELECT w_id, geom
        FROM supply.egon_era5_weather_cells
        """,
        index_col='w_id'
        )

    # Map solar thermal collectors to weather cells
    join = gpd.sjoin(weather_cells, solar_thermal)[['index_right']]

    feedin = db.select_dataframe(
        """
        SELECT w_id, feedin FROM supply.egon_era5_renewable_feedin
        WHERE carrier = 'solar_thermal'
        AND weather_year = 2011
        """,
        index_col='w_id')

    timeseries =  pd.DataFrame(
        data = {'version': version,
                'scn_name': scenario,
                'temp_id': 1,
                'p_max_pu': feedin.feedin[join.index].values,
                'generator_id': generator.generator_id[join.index_right].values
                    }
        ).set_index('generator_id')

    generator = generator.set_index('generator_id')

    generator.to_sql(
        'egon_pf_hv_generator',
        schema='grid',
        if_exists='append',
        con=db.engine())

    timeseries.to_sql(
        'egon_pf_hv_generator_timeseries',
        schema='grid',
        if_exists='append',
        con=db.engine())

def insert_heat_etrago(version='0.0.0'):

    insert_buses('central_heat', version=version, scenario='eGon2035')
    #insert_buses('rural_heat', version=version, scenario='eGon2035')
    insert_central_direct_heat(version = '0.0.0', scenario='eGon2035')
    insert_central_power_to_heat(version, scenario='eGon2035')