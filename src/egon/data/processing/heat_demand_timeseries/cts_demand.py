
from disaggregator import config, data, spatial, temporal, plot

from egon.data import db
import psycopg2

import pandas as pd
import pandas.io.sql as sqlio
import geopandas as gpd
import numpy as np
import os


def psycop_df_AF(table_name):
       conn = db.engine()
       sql = "SELECT * FROM {}".format(table_name)
       data = sqlio.read_sql_query(sql, conn)
       conn = None
       return data

def psycop_gdf_AF(table_name,geom_column = 'geom'):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = gpd.read_postgis(sql, conn, geom_column)
        conn = None
        return data
    
def cts_demand_per_zensus_cell(zensus_id = []):
    ## dataframe interlinking the zensus_population_id to the nuts3 level
    nuts_zensus = psycop_gdf_AF('boundaries.egon_map_zensus_nuts3', geom_column = 'zensus_geom')
    nuts_zensus.drop('zensus_geom', axis=1, inplace=True)
          
    ##extracting the demand data for the service sector and conncting it to the nuts3 level
    demand = psycop_df_AF('demand.egon_peta_heat')
    demand = demand[(demand['sector']=='service') & (demand['scenario']=='eGon2035')]
    demand.drop(demand.columns.difference(['demand', 'zensus_population_id']),axis=1,inplace=True)
    demand.sort_values('zensus_population_id',inplace=True)
    
    demand_nuts = pd.merge(demand, nuts_zensus, how='left', on = 'zensus_population_id')
    
    
    ###CTS secotor NUTS§ level temperature profile
    #df_CTS_gas_2011 = temporal.disagg_temporal_gas_CTS(use_nuts3code=True, year=2011)
    df_CTS_gas_2011=pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/CTS/df_CTS_gas_2011.pickle')
    
    ##df linking ags_lk and natcode_nuts_3 ### this file is available in the nextcloud link
    #ags_lk = pd.read_json(os.path.join(os.getcwd(),'t_nuts3_lk.json'))
    ags_lk = pd.read_json(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/CTS/t_nuts3_lk.json')
    ags_lk =ags_lk.drop(ags_lk.columns.difference(['natcode_nuts3', 'ags_lk']),axis=1)
    
    ##replacing the ags_lk with natcode_nuts3
    CTS_profile = df_CTS_gas_2011.transpose()
    CTS_profile.reset_index(inplace=True)
    CTS_profile.ags_lk=CTS_profile.ags_lk.astype(int)
    CTS_profile=pd.merge(CTS_profile,ags_lk,on='ags_lk',how='inner')
    CTS_profile.set_index('natcode_nuts3',inplace=True)
    CTS_profile.drop('ags_lk', axis=1, inplace =True)
    CTS_profile =CTS_profile.transpose()
    
    CTS_profile_norm = CTS_profile.apply(lambda x: x/x.sum())
    CTS_profile_norm = CTS_profile_norm.transpose()
    
    CTS_profile_curve = pd.DataFrame(index=CTS_profile_norm.index,
                                     data={'profile':CTS_profile_norm.values.tolist()})
    
    ###generating CTS profile per zensus cell
    zensus_profile = pd.merge(demand_nuts,CTS_profile_curve, 
                              left_on='nuts3', right_on=CTS_profile_curve.index,
                              how='left')
    zensus_profile.profile=zensus_profile.profile.apply(lambda x: np.array(x))
    zensus_profile['cts_profile'] = zensus_profile.demand * zensus_profile.profile
    zensus_profile.drop(['demand','nuts3','profile'],axis=1,inplace=True)
    zensus_profile.set_index('zensus_population_id',inplace=True)
    zensus_profile =zensus_profile.transpose()
    if zensus_id ==[]:
        zensus_profile = zensus_profile.loc[:,zensus_profile.columns.tolist()]
    else:
        zensus_profile = zensus_profile.loc[:,zensus_id]
    zensus_profile = zensus_profile.apply(lambda x: x.explode())
    zensus_profile =zensus_profile.set_index(df_CTS_gas_2011.index)
    
    return zensus_profile
