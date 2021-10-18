# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas storages data
"""
import ast
import pandas as pd
import numpy as np
import geopandas

from egon.data.datasets.gas_prod import assign_ch4_bus_id
from egon.data.datasets.gas_grid import ch4_nodes_number_G, define_gas_nodes_list
from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from pathlib import Path

class CH4Storages(Dataset):
     def __init__(self, dependencies):
         super().__init__(
             name="CH4Storages",
             version="0.0.0.dev",
             dependencies=dependencies,
             tasks=(import_ch4_storages),
         )

def import_installed_ch4_storages():
    """Define dataframe containing the ch4 storage units in Germany from the SciGRID_gas data

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the gas storages units in Germany
    
    """
    target_file = (
        Path(".") /
        "datasets" /
        "gas_data" /
        "data" /
        "IGGIELGN_Storages.csv")

    Gas_storages_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['lat', 'long', 'country_code','param'])

    Gas_storages_list = Gas_storages_list[ Gas_storages_list['country_code'].str.match('DE')]
    
    # Define new columns
    e_nom = []
    NUTS1 = []
    end_year = []
    for index, row in Gas_storages_list.iterrows():
        param = ast.literal_eval(row['param'])
        NUTS1.append(param['nuts_id_1'])
        end_year.append(param['end_year'])
        e_nom.append(param['max_power_MW'])
        
    Gas_storages_list = Gas_storages_list.assign(e_nom = e_nom)
    Gas_storages_list = Gas_storages_list.assign(NUTS1 = NUTS1)
    
    end_year = [float('inf') if x == None else x for x in end_year]
    Gas_storages_list = Gas_storages_list.assign(end_year = end_year)
    
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

        Gas_storages_list = Gas_storages_list[Gas_storages_list['NUTS1'].isin([map_states[boundary], np.nan])]
    
    # Remove unused storage units
    Gas_storages_list = Gas_storages_list[Gas_storages_list['end_year'] >= 2035]

    Gas_storages_list = Gas_storages_list.rename(columns={'lat': 'y','long': 'x'})
    Gas_storages_list = geopandas.GeoDataFrame(Gas_storages_list,
                                                geometry=geopandas.points_from_xy(Gas_storages_list['x'],
                                                                                  Gas_storages_list['y']))
    Gas_storages_list = Gas_storages_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)
    
    # Match to associated gas bus
    Gas_storages_list =  Gas_storages_list.reset_index(drop=True)
    Gas_storages_list = assign_ch4_bus_id(Gas_storages_list)
    
    # Add missing columns
    c = {'scn_name':'eGon2035', 'carrier':'CH4'}
    Gas_storages_list = Gas_storages_list.assign(**c)

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=['x', 'y', 
                                                        'param', 'country_code', 
                                                        'NUTS1', 'end_year', 
                                                        'geom', 'bus_id'])                                      
    return Gas_storages_list


def import_ch4_grid_capacity():
    """Define dataframe containing the gas storage modelling the ch4 grid storage capacity

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the gas stores in Germany modelling the gas grid storage capacity
    
    """ 
    Gas_grid_capacity = 130000 # G.Volk "Die Herauforderung an die Bundesnetzagentur die Energiewende zu meistern" Berlin, Dec 2012
    N_ch4_nodes_G = ch4_nodes_number_G(define_gas_nodes_list()) # Number of nodes in Germany
    print(N_ch4_nodes_G)
    Store_capacity = Gas_grid_capacity / N_ch4_nodes_G
    
    sql_gas = """SELECT bus_id, scn_name, carrier, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'CH4';"""
    Gas_storages_list = db.select_geodataframe(sql_gas, epsg=4326)
    
    # Add missing column    
    Gas_storages_list['e_nom'] = Store_capacity
    Gas_storages_list['bus'] = Gas_storages_list['bus_id']
    
    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=['bus_id', 'geom'])

    return Gas_storages_list    
    
    
def import_ch4_storages():
    """Insert list of gas storages units in database

    Returns
    -------
     None.
    """   
    # Connect to local database
    engine = db.engine()

    # Clean table
    db.execute_sql(
        """
    DELETE FROM grid.egon_etrago_store WHERE "carrier" = 'CH4';
    """
    )    
    
    # Select next id value
    new_id = db.next_etrago_id('store')
    
    gas_storages_list = pd.concat([import_installed_ch4_storages(), import_ch4_grid_capacity()])
    gas_storages_list['store_id'] = range(new_id, new_id + len(gas_storages_list))
    
    gas_storages_list =  gas_storages_list.reset_index(drop=True)
   
    # Insert data to db
    print(gas_storages_list)
    gas_storages_list.to_sql('egon_etrago_store',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')