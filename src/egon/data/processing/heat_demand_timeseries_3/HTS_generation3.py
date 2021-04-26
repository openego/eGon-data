

import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import pickle
import db_processing3 as dbp

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
##dataframe for the fnal demand.
total_demand = pd.DataFrame()
all_demand = pd.DataFrame(index=index)
##generating the temperature profile with all cells with available demand value.

###column 'profile_name' consists of randomly selected column names
annual_demand['profile_name']=annual_demand.apply(lambda x: dbp.IdpProfiles.generator_Ax(x.Temperature_interval,x.SFH,x.MFH),axis=1)

#sample dataframe
sample_dc=annual_demand.head(20)
start=time.time()
sample_dc['profile_name']=sample_dc.apply(lambda x: dbp.generator_Ax(x.Temperature_interval,x.SFH,x.MFH),axis=1)
print(time.time()-start)