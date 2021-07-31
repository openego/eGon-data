#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 15:10:20 2021

@author: student
"""

import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import pickle
import db_processing as dbp

##dataframe with all the annual demand, csv file available as demand_zone_TC.csv
#annual_demand = dbp.annual_demand_generator()##csv of the dataframe avaialble to be added into the database
annual_demand =pd.read_csv(os.path.join(os.getcwd(),'demand_zone_TC.csv'))
annual_demand.drop(columns=['Unnamed: 0'], inplace=True)

##temperature profile of all the temperature statios extracted from cds database
#temperature_profile = dbp.temperature_profile()
temperature_profile =pd.read_pickle(os.path.join(os.getcwd(),'temperature_profile.pickle'))

##pool of idp for each class from 3-10 extracted from runnung.py
#imports individual dataframe with a pool of 24 hour profile for each class. Dataframes to be added to egon data
class_list=[3,4,5,6,7,8,9,10]
idp_path = os.path.join(os.getcwd(),'IDP_Pool')
for i in class_list:
    this_itteration = 'idp_collection_class_'+str(i)
    file_name  ='idp_pool_class_'+str(i)
    globals()[this_itteration]=pd.read_pickle(os.path.join(idp_path,file_name))

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
for i in range(len(annual_demand.head(1000))):
    cell_annual_demand =annual_demand.loc[i,'demand']
    cell_station=annual_demand.loc[i,'Station']
    temperature_profile_station= temperature_profile.loc[:,cell_station]  
    cell_demand_profile= dbp.IdpProfiles(index, temperature_int=all_temperature_interval, 
                                          idp_collection_class_3 = idp_collection_class_3,
                                          idp_collection_class_4 = idp_collection_class_4,
                                          idp_collection_class_5 = idp_collection_class_5, 
                                          idp_collection_class_6 = idp_collection_class_6, 
                                          idp_collection_class_7 = idp_collection_class_7,
                                          idp_collection_class_8 = idp_collection_class_8, 
                                          idp_collection_class_9 = idp_collection_class_9,
                                          idp_collection_class_10 = idp_collection_class_10,
                                        annual_heat_demand = cell_annual_demand, 
                                        station_name = cell_station).profile_normalizer()
    all_demand[annual_demand.loc[i,'grid_id']] = cell_demand_profile
total_demand = all_demand.sum(axis=1) 