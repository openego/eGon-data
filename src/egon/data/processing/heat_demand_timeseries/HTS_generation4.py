

import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import pickle
import db_processing4 as dbp

##generating dataframe with demand, householdstock count and the daily temperature interval
#annual_demand = dbp.annual_demand_generator() 
#direct import of the df. file available in next cloud
annual_demand = pd.read_pickle(os.path.join(os.getcwd(),'demand_count.pickle'))

##temperature profile of all the temperature statios extracted from cds database
#temperature_profile = dbp.temperature_profile()
temperature_profile =pd.read_pickle(os.path.join(os.getcwd(),'temperature_profile.pickle'))

##pool of idp for each class from 3-10 extracted from runnung.py
#imports individual dataframe with a pool of 24 hour profile for each class. Dataframes to be added to egon data
stock=['MFH','SFH']
class_list=[3,4,5,6,7,8,9,10]
for s in stock:
    for m in class_list:
        file_name=f'idp_collection_class_{m}_{s}_norm.pickle'
        globals()[f'idp_collection_class_{m}_{s}']=pd.read_pickle(os.path.join(os.getcwd(), file_name))

index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

##dataframe with temperature classes of each day for all 15 stations
all_temperature_interval = pd.DataFrame()
for x in range(len(temperature_profile.columns)):
    name_station = temperature_profile.columns[x]
    idp_this_station = dbp.IdpProfiles(index, temperature=temperature_profile[temperature_profile.columns[x]]).get_temperature_interval(how='geometric_series')
    all_temperature_interval[name_station]=idp_this_station['temperature_interval']


##os.chdir(r'/home/student/Documents/egon_AM/heat_demand_generation/idp pool generation/idp_pool_normalized_24hr')

idp_df = pd.DataFrame(columns=['idp', 'house', 'temperature_class'])

for s in stock:
    for m in class_list:
        file_name=f'idp_collection_class_{m}_{s}_norm.pickle'
        idp_df = idp_df.append(
            pd.DataFrame(
                data = {
                    'idp': pd.read_pickle(
                        os.path.join(os.getcwd(),
                         file_name)).transpose().values.tolist(),
                    'house': s,
                    'temperature_class': m
                    }))

annual_demand = dbp.annual_demand_generator()
##annual_demand = pd.read_pickle(os.path.join(os.getcwd(),'demand_count.pickle'))

Temperature_interval = pd.DataFrame(columns = range(365))

### r'/home/student/Documents/egon_AM/heat_demand_generation/data_15.04/all_temperature_interval.csv'
all_temperature_interval = pd.read_csv(os.join.path(os.getcwd(),'all_temperature_interval.csv', index_col=0)) 
all_temperature_interval.set_index(index,inplace=True)
all_temperature_interval = all_temperature_interval.resample('D').max()

all_temperature_interval.reset_index(inplace=True)
all_temperature_interval.drop('index',axis=1,inplace=True)

for x in all_temperature_interval:
    Station = x
    for c in range(365):
        Temperature_interval.loc[Station,c] =annual_demand[annual_demand.Station==Station].Temperature_interval.iloc[0][c]
####"single positional indexer is out-of-bounds") as all temperature zones are not included in the test data

idp_df = idp_df.reset_index()
idp_df.drop('index',axis=1,inplace=True)

# Set seed value to have reproducable results
np.random.seed(0)
#generates a dataframe with the idp index number of the selected profiles for each temperature 

for station in Temperature_interval.index:
    globals()[f'result_SFH_{station}']=pd.DataFrame(columns=Temperature_interval.columns)
    globals()[f'result_MFH_{station}']=pd.DataFrame(columns=Temperature_interval.columns)
    for day in Temperature_interval.columns:
        t_class = Temperature_interval.loc[station,day].astype(int)
        
        array_SFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                    &(idp_df.house=='SFH')].index.values)
        
        array_MFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                    &(idp_df.house=='MFH')].index.values) 
        
        globals()[f'result_SFH_{station}'][day] = np.random.choice(
            array_SFH, int(annual_demand[annual_demand.Station==station].SFH.sum()))
        
        globals()[f'result_MFH_{station}'][day] = np.random.choice(
            array_MFH, int(annual_demand[annual_demand.Station==station].MFH.sum()))


     
    globals()[f'result_SFH_{station}']['zensus_population_id'] = annual_demand[
        annual_demand.Station==station].loc[
            annual_demand[annual_demand.Station==station].index.repeat(
                annual_demand[annual_demand.Station==station].SFH.astype(int))].zensus_population_id.values

    globals()[f'result_MFH_{station}']['zensus_population_id'] =annual_demand[
        annual_demand.Station==station].loc[
            annual_demand[annual_demand.Station==station].index.repeat(
                annual_demand[annual_demand.Station==station].MFH.astype(int))].zensus_population_id.values
    
    globals()[f'result_SFH_{station}'].set_index('zensus_population_id',inplace=True)
    globals()[f'result_MFH_{station}'].set_index('zensus_population_id',inplace=True)


##hvalues for each temperature zone
h = dbp.h_value()

##generaing specific profiles of grid cell
###### normalizing and scaling up of profiles    
def profile_generator(station):
    x = globals()[f'result_SFH_{station}']
    y = idp_df['idp'].reset_index()
    heat_profile_idp = pd.DataFrame(index=x.index)
    #heat_profile_idp.reset_index()
    for i in x.columns:
        col=x[i]
        col=col.reset_index()
        y.rename(columns={'index':i},inplace=True)
        col=pd.merge(col,y[[i,'idp']],how='inner', on=i)
        col[i]=col['idp']
        col.drop('idp',axis=1,inplace=True)
        col.set_index('zensus_population_id', inplace=True)
        heat_profile_idp[i] = col[i].values
        y.rename(columns={i:'index'},inplace=True)
    heat_profile_idp = heat_profile_idp.transpose()
    heat_profile_idp = heat_profile_idp.apply(lambda x: x.explode())
    heat_profile_idp.set_index(index,inplace=True)
    heat_profile_idp = heat_profile_idp.groupby(lambda x:x, axis=1).sum()
    return heat_profile_idp

def demand_scale(station):
    heat_demand_profile = profile_generator(station)
    demand_curve = heat_demand_profile.multiply(h[station],axis=0)
    demand_curve = demand_curve.apply(lambda x: x/x.sum())
    heat_demand=demand_curve.transpose()
    heat_demand=heat_demand.reset_index()
    heat_demand.rename(columns={'index':'zensus_population_id'},inplace=True)
    heat_demand=pd.merge(heat_demand,annual_demand[['zensus_population_id','demand']],
                         how='inner', on='zensus_population_id')
        
    heat_demand.set_index('zensus_population_id', inplace=True)
    heat_demand=heat_demand[heat_demand.columns[:-1]].multiply(heat_demand.demand,axis=0)
    
    heat_demand=heat_demand.transpose()
    
    heat_demand.set_index(index,inplace=True)
    
    return heat_demand

station='Bremerhaven'

demand_profile = profile_generator(station)
