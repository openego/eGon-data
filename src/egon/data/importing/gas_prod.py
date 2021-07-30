# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas production data
"""
import os
import ast
import pandas as pd
import geopandas as gpd
import numpy as np
import geopandas

from pathlib import Path
from egon.data import db
from egon.data.importing.gas_grid import next_id
from egon.data.config import settings                     

def load_NG_generators():
    """Define the natural gas producion units in Germany
    
    Returns
    -------
    CH4_generators_list : 
        Dataframe containing the natural gas producion units in Germany
        
    """
    # Read-in data from csv-file
#    target_file = os.path.join(
#        Path(os.path.dirname(__file__)).parent, 'egon/data/importing/gas_grid/data/IGGIELGN_Productions.csv')
    target_file = os.path.join(
        "gas_data/",
        'data/IGGIELGN_Productions.csv')
    
    NG_generators_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['lat', 'long', 'country_code','param'])
    
    NG_generators_list = NG_generators_list[ NG_generators_list['country_code'].str.match('DE')]
    
    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row['param'])
        NUTS1.append(param['nuts_id_1'])
    NG_generators_list = NG_generators_list.assign(NUTS1 = NUTS1)
    
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
                'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
                'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
                'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
                'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
                'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
                'Berlin': 'DE3', 'Bayern': 'DE2'}
        
        NG_generators_list = NG_generators_list[NG_generators_list['NUTS1'].isin([map_states[boundary], np.nan])]
      
    NG_generators_list = NG_generators_list.rename(columns={'lat': 'y','long': 'x'})        
    NG_generators_list = geopandas.GeoDataFrame(NG_generators_list, 
                                                geometry=geopandas.points_from_xy(NG_generators_list['x'], 
                                                                                  NG_generators_list['y']))
    NG_generators_list = NG_generators_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)
    
    # Insert p_nom
    Total_NG_extracted_2035 = 36 # [TWh] Netzentwicklungsplan Gas 2020–2030
    Total_NG_capacity_2035 = Total_NG_extracted_2035 * 1000000 / (24 * 365)
    
    share = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row['param'])
        share.append(param['max_supply_M_m3_per_d'])
    
    NG_generators_list['p_nom'] = [(i/(sum(share)) * Total_NG_capacity_2035) for i in share]
    
    # Remove useless columns
    NG_generators_list = NG_generators_list.drop(columns=['x', 'y', 'param', 'country_code', 'NUTS1'])
    
    return NG_generators_list


def load_biogas_generators():
    """Define the biogas producion units in Germany
    
    Returns
    -------
    CH4_generators_list : 
        Dataframe containing the biogas producion units in Germany
        
    """
    
    # Read-in data from csv-file
    target_file = os.path.join(
        os.path.dirname(__file__), 'Biogaspartner_Einspeiseatlas_Deutschland_2021.csv')  # path to be changed when the data will be downloaded from Zenodo repository
    
    biogas_generators_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['lat', 'long', 'Einspeisung Biomethan [(N*m^3)/h)]'])
    
    biogas_generators_list = biogas_generators_list.rename(columns={'lat': 'y','long': 'x'})
    biogas_generators_list = geopandas.GeoDataFrame(biogas_generators_list, 
                                                    geometry=geopandas.points_from_xy(biogas_generators_list['x'], 
                                                                                      biogas_generators_list['y']))
    biogas_generators_list = biogas_generators_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)
    
    # Connect to local database
    engine = db.engine()
    
    # Cut data to federal state if in testmode
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        biogas_generators_list.to_postgis('egon_biogas_generator',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'replace')
        
        sql = '''SELECT *
            FROM grid.egon_biogas_generator, boundaries.vg250_sta_union  as vg
            WHERE ST_Transform(vg.geometry,4326) && egon_biogas_generator.geom
            AND ST_Contains(ST_Transform(vg.geometry,4326), egon_biogas_generator.geom)'''
        
        biogas_generators_list = gpd.GeoDataFrame.from_postgis(sql, con=engine, geom_col="geom", crs=4326)
        db.execute_sql(
            """
              DROP TABLE IF EXISTS grid.egon_biogas_generator CASCADE;
            """)

    # Insert p_nom
    Total_biogas_extracted_2035 = 10 # [TWh] Netzentwicklungsplan Gas 2020–2030
    Total_biogas_capacity_2035 = Total_biogas_extracted_2035 * 1000000 / (24 * 365)
        
    biogas_generators_list['p_nom'] = (biogas_generators_list['Einspeisung Biomethan [(N*m^3)/h)]'] 
                                       / biogas_generators_list['Einspeisung Biomethan [(N*m^3)/h)]'].sum() 
                                       * Total_biogas_capacity_2035)  
    # Remove useless columns
    biogas_generators_list = biogas_generators_list.drop(columns=['x', 'y', 'gid', 'bez', 
                                                                  'area_ha', 'geometry',
                                                                  'Einspeisung Biomethan [(N*m^3)/h)]'])
    return biogas_generators_list
    

def assign_gas_bus_id(dataframe):
    """Assigns bus_ids (for gas buses) to points (contained in a dataframe) according to location
    Parameters
    ----------
    dataframe : pandas.DataFrame cointaining points
    
    Returns
    -------
    power_plants : pandas.DataFrame
        Power plants (including voltage level) and bus_id
    """

    gas_voronoi = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_gas_voronoi
        """, epsg=4326)
    
    print(gas_voronoi)
    res = gpd.sjoin(dataframe, gas_voronoi)
    print(res)
    res['bus'] = res['bus_id']
    print(res)    
    res = res.drop(columns=['index_right', 'id'])
    print(res)
    
    # Assert that all power plants have a bus_id
    assert res.bus_id.notnull().all(), "Some points are not attached to a gas bus."

    return res

    
def import_gas_generators():
    """Insert list of gas production units in database
    
    Returns
    -------
        
    """
    # Connect to local database
    engine = db.engine()
    
    # Select next id value
    new_id = next_id('generator')
    
    CH4_generators_list = pd.concat([load_NG_generators(), load_biogas_generators()])   
    CH4_generators_list['generator_id'] = range(new_id, new_id + len(CH4_generators_list))
     
    # Add missing columns
    c = {'version':'0.0.0', 'scn_name':'eGon2035', 'carrier':'gas'}
    CH4_generators_list = CH4_generators_list.assign(**c)
    
    CH4_generators_list =  CH4_generators_list.reset_index(drop=True)

    # Match to associated gas bus   
    CH4_generators_list = assign_gas_bus_id(CH4_generators_list)
    
    # Remove useless columns
    CH4_generators_list = CH4_generators_list.drop(columns=['geom', 'bus_id'])
    
    # Insert data to db    
    CH4_generators_list.to_sql('egon_pf_hv_generator', #to_postgis
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')
        
# marginal_cost_fixed = Column(Float(53))


def insert_gas_generators_time_serie():
    """Insert gas production time series in database
    
    Returns
    -------
        
    """
    # Connect to local database
    engine = db.engine()
    

    sql = "SELECT generator_id, scn_name, p_nom, version FROM grid.egon_pf_hv_generator;"
    df = pd.read_sql(sql, con = engine)
    
    p_set = []
    for index, row in df.iterrows():
        p_set.append([row['p_nom']] * (365*24)) #Verify length
    df['p_set'] = p_set
    
    # Remove useless columns
    df = df.drop(columns=['p_nom'])

    # Insert data to db    
    df.to_sql('egon_pf_hv_generator_timeseries',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')
    
# #     temp_id = Column(Integer, primary_key=True, nullable=False)
# #     marginal_cost = Column(ARRAY(Float(precision=53)))

def insert_gas_prod():
    """Overall function for importing gas production data
    Returns
    -------
    None.
    """
    import_gas_generators()
    
#    insert_gas_generators_time_serie()
    