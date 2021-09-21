# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas industrial demand
"""
import requests
import pandas as pd
import numpy as np


from shapely import wkt 
from egon.data import db
from egon.data.datasets.gas_prod import assign_ch4_bus_id, assign_h2_bus_id
from egon.data.config import settings
from egon.data.datasets import Dataset

class IndustrialGasDemand(Dataset): 
     def __init__(self, dependencies): 
         super().__init__( 
             name="IndustrialGasDemand", 
             version="0.0.1.dev", 
             dependencies=dependencies, 
             tasks=(insert_industrial_gas_demand), 
         )       

def download_CH4_industrial_demand():
    """Download the CH4 industrial demand in Germany from the FfE open data portal
    
    Returns
    -------
    CH4_industrial_demand : 
        Dataframe containing the CH4 industrial demand in Germany
        
    """
    # Download the data and the id_region correspondance table
    correspondance_url = 'http://opendata.ffe.de:3000/region?id_region_type=eq.38'
    url = 'http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq.'
    
    internal_id = '2,11' # Natural_Gas
    year = '2035' # 2050
    datafilter = '&&internal_id=eq.{'+internal_id+'}'
    request = url + year + datafilter
    
    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)
    industrial_loads_list = industrial_loads_list[['id_region', 'values']].copy()
    industrial_loads_list = industrial_loads_list.set_index('id_region')
    
    result_corr = requests.get(correspondance_url)
    df_corr = pd.read_json(result_corr.content)
    df_corr = df_corr[['id_region', 'name_short']].copy()
    df_corr = df_corr.set_index('id_region')
    
    # Match the id_region to obtain the NUT3 region names
    industrial_loads_list = pd.concat([industrial_loads_list, df_corr], axis=1, join="inner")
    industrial_loads_list['NUTS0'] = (industrial_loads_list['name_short'].str)[0:2] 
    industrial_loads_list['NUTS1'] = (industrial_loads_list['name_short'].str)[0:3]   
    industrial_loads_list = industrial_loads_list[industrial_loads_list['NUTS0'].str.match('DE')]
    
    # Cut data to federal state if in testmode
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
                'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
                'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
                'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
                'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
                'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
                'Berlin': 'DE3', 'Bayern': 'DE2'}
        
        industrial_loads_list = industrial_loads_list[industrial_loads_list['NUTS1'].isin([map_states[boundary], np.nan])]
    
    industrial_loads_list = industrial_loads_list.rename(columns={"name_short": "nuts3", "values": "p_set"})
    industrial_loads_list = industrial_loads_list.set_index('nuts3')
    
    # Add the centroid point to each NUTS3 area
    sql_vg250 = """SELECT nuts as nuts3, geometry as geom
                    FROM boundaries.vg250_krs 
                    WHERE gf = 4 ;"""   
    gdf_vg250 = db.select_geodataframe(sql_vg250 , epsg=4326)
    
    point = []
    for index, row in gdf_vg250.iterrows():
        point.append(wkt.loads(str(row['geom'])).centroid)
    gdf_vg250['point'] = point
    gdf_vg250 = gdf_vg250.set_index('nuts3')
    gdf_vg250  = gdf_vg250 .drop(columns=['geom'])  
    
    # Match the load to the NUTS3 points
    industrial_loads_list = pd.concat([industrial_loads_list, gdf_vg250], axis=1, join="inner")
    industrial_loads_list = industrial_loads_list.rename(columns={'point': 'geom'}).set_geometry('geom', crs=4326)

    # Match to associated gas bus   
    industrial_loads_list = assign_ch4_bus_id(industrial_loads_list)
    
    # Add carrier   
    c = {'carrier':'CH4'}
    industrial_loads_list = industrial_loads_list.assign(**c)
    
    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(columns=['geom', 'NUTS0', 'NUTS1', 'bus_id']) 
        
    return industrial_loads_list


def download_H2_industrial_demand():
    """Download the H2 industrial demand in Germany from the FfE open data portal
    
    Returns
    -------
    H2_industrial_demand : 
        Dataframe containing the H2 industrial demand in Germany
        
    """
    # Download the data and the id_region correspondance table
    correspondance_url = 'http://opendata.ffe.de:3000/region?id_region_type=eq.38'
    url = 'http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq.'
    
    internal_id = '2,162' # Hydrogen
    year = '2035' # 2050
    datafilter = '&&internal_id=eq.{'+internal_id+'}'
    request = url + year + datafilter
    
    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)
    industrial_loads_list = industrial_loads_list[['id_region', 'values']].copy()
    industrial_loads_list = industrial_loads_list.set_index('id_region')
    
    result_corr = requests.get(correspondance_url)
    df_corr = pd.read_json(result_corr.content)
    df_corr = df_corr[['id_region', 'name_short']].copy()
    df_corr = df_corr.set_index('id_region')
    
    # Match the id_region to obtain the NUT3 region names
    industrial_loads_list = pd.concat([industrial_loads_list, df_corr], axis=1, join="inner")
    industrial_loads_list['NUTS0'] = (industrial_loads_list['name_short'].str)[0:2] 
    industrial_loads_list['NUTS1'] = (industrial_loads_list['name_short'].str)[0:3]   
    industrial_loads_list = industrial_loads_list[industrial_loads_list['NUTS0'].str.match('DE')]
    
    # Cut data to federal state if in testmode
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
                'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
                'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
                'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
                'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
                'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
                'Berlin': 'DE3', 'Bayern': 'DE2'}
        
        industrial_loads_list = industrial_loads_list[industrial_loads_list['NUTS1'].isin([map_states[boundary], np.nan])]
    
    industrial_loads_list = industrial_loads_list.rename(columns={"name_short": "nuts3", "values": "p_set"})
    industrial_loads_list = industrial_loads_list.set_index('nuts3')
    
    # Add the centroid point to each NUTS3 area
    sql_vg250 = """SELECT nuts as nuts3, geometry as geom
                    FROM boundaries.vg250_krs 
                    WHERE gf = 4 ;"""   
    gdf_vg250 = db.select_geodataframe(sql_vg250 , epsg=4326)
    
    point = []
    for index, row in gdf_vg250.iterrows():
        point.append(wkt.loads(str(row['geom'])).centroid)
    gdf_vg250['point'] = point
    gdf_vg250 = gdf_vg250.set_index('nuts3')
    gdf_vg250  = gdf_vg250 .drop(columns=['geom'])  
    
    # Match the load to the NUTS3 points
    industrial_loads_list = pd.concat([industrial_loads_list, gdf_vg250], axis=1, join="inner")
    industrial_loads_list = industrial_loads_list.rename(columns={'point': 'geom'}).set_geometry('geom', crs=4326)

    # Match to associated gas bus   
    industrial_loads_list = assign_h2_bus_id(industrial_loads_list)
    
    # Add carrier   
    c = {'carrier':'H2'}
    industrial_loads_list = industrial_loads_list.assign(**c)
    
    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(columns=['geom', 'NUTS0', 'NUTS1', 'bus_id'])
        
    return industrial_loads_list
    
    
def import_industrial_gas_demand():
    """Insert list of industrial gas demand (one per NUTS3) in database
    Returns
        industrial_gas_demand : Dataframe containing the industrial gas demand in Germany
    """
    # Connect to local database
    engine = db.engine()
    
    # Clean table
    db.execute_sql(
        """
    DELETE FROM grid.egon_etrago_load WHERE "carrier" = 'CH4';
    DELETE FROM grid.egon_etrago_load WHERE "carrier" = 'H2';
    """
    )
    
    # Select next id value
    new_id = db.next_etrago_id('load')
    
    industrial_gas_demand = pd.concat([download_CH4_industrial_demand(), download_H2_industrial_demand()])
    industrial_gas_demand['load_id'] = range(new_id, new_id + len(industrial_gas_demand))
     
    # Add missing columns
    c = {'scn_name':'eGon2035'}
    industrial_gas_demand = industrial_gas_demand.assign(**c)
    
    industrial_gas_demand =  industrial_gas_demand.reset_index(drop=True)
    
    # Remove useless columns
    egon_etrago_load_gas = industrial_gas_demand.drop(columns=['p_set'])  
    
    # Insert data to db
    egon_etrago_load_gas.to_sql('egon_etrago_load',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')
                              
    return industrial_gas_demand

    
def import_industrial_gas_demand_time_series(egon_etrago_load_gas):
    """Insert list of industrial gas demand time series (one per NUTS3) in database
    Returns
    -------
    None.
    """
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas
    
    # Connect to local database
    engine = db.engine()

    # Adjust columns
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas_timeseries.drop(columns=['carrier', 'bus']) 
    egon_etrago_load_gas_timeseries['temp_id'] = 1
    
    # Insert data to db
    egon_etrago_load_gas_timeseries.to_sql('egon_etrago_load_timeseries',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')

    
def insert_industrial_gas_demand():
    """Overall function for inserting the industrial gas demand
    Returns
    -------
    None.
    """
    
    egon_etrago_load_gas = import_industrial_gas_demand()
    
    import_industrial_gas_demand_time_series(egon_etrago_load_gas)
    