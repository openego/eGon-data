import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as sqlio
import geopandas as gpd
import matplotlib.pyplot as plt
import os
import glob
from egon.data import db
import xarray as xr

import atlite
import netCDF4
from netCDF4 import Dataset

from math import ceil

index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

class IdpProfiles:    
    def __init__(self,df_index, **kwargs):
        
        self.df = pd.DataFrame(index = df_index)
        
        self.temperature = kwargs.get('temperature')
       
   #combination of weighted_temperature, get_normalized_bdew and temperature_inteval
    def get_temperature_interval(self, how='geometric_series'):
        """Appoints the corresponding temperature interval to each temperature
        in the temperature vector.
        """
        self.df['temperature']=self.temperature.values        
        
        temperature =self.df['temperature'].resample('D').mean().reindex(
            self.df.index).fillna(method='ffill').fillna(method='bfill')
        
        if how == 'geometric_series':
            temperature_mean = (temperature + 0.5 * np.roll(temperature, 24) +
                                    0.25 * np.roll(temperature, 48) +
                                    0.125 * np.roll(temperature, 72)) / 1.875
        elif how == 'mean':
            temperature_mean = temperature
        
        else:
            temperature_mean = None
        
        self.df['temperature_geo']= temperature_mean
        
        temperature_rounded=[]
        
        for i in self.df['temperature_geo']: 
            temperature_rounded.append(ceil(i))
        
        intervals = ({
            -20: 1, -19: 1, -18: 1, -17: 1, -16: 1, -15: 1, -14: 2,
            -13: 2, -12: 2, -11: 2, -10: 2, -9: 3, -8: 3, -7: 3, -6: 3, -5: 3,
            -4: 4, -3: 4, -2: 4, -1: 4, 0: 4, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,
            6: 6, 7: 6, 8: 6, 9: 6, 10: 6, 11: 7, 12: 7, 13: 7, 14: 7, 15: 7,
            16: 8, 17: 8, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 9, 24: 9,
            25: 9, 26: 10, 27: 10, 28: 10, 29: 10, 30: 10, 31: 10, 32: 10,
            33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 10, 40: 10})
        
        temperature_interval=[] 
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        
        self.df['temperature_interval'] = temperature_interval
        
        return self.df

def temperature_profile_extract():
    #downloading all data for Germany
    # load cutout
        #cutout = atlite.Cutout("Germany-2011-era5",
                                #cutout_dir = 'cutouts',
                                #module="era5",
                                #xs= slice(5., 16.), # ADJUST THIS IF YOU ONLY NEED GERMANY
                                #ys=slice(46., 56.), # ADJUST THIS IF YOU ONLY NEED GERMANY
                                #years= slice(2011, 2011)
                                #)
        #cutout.prepare()
        #file in the nextcloud shared link
        #will be directly taken from the dataframe??
        os.chdir(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/atlite/cutouts/Germany-2011-era5')
        ##### replace this with input from the dataframe
        temperature_profile=pd.DataFrame()
        for file in glob.glob('*.nc'):        
            weather_data_raw = xr.open_dataset(file)
            temperature_raw = weather_data_raw.temperature.values
            index = weather_data_raw.indexes._indexes
            lats= index['y']
            lon = index['x']
            time =index['time']
            weather_data = np.zeros(shape=(temperature_raw.size, 4))
            count = 0
            
            for hour in range(index['time'].size):
                for row in range(index['y'].size):
                    for column in range(index['x'].size):
                        weather_data[count, 0] = hour
                        weather_data[count, 1] = index['y'][row]
                        weather_data[count, 2] = index['x'][column]
                        weather_data[count, 3] = temperature_raw[hour, row, column] - 273.15
                        count += 1
            weather_df = pd.DataFrame(weather_data)
            temperature_month = pd.DataFrame()
            ##csv file available in the nextcloud folder
            station_location=pd.read_csv(os.path.join(os.getcwd(),'TRY_Climate_Zones','station_coordinates.csv'))
            
            for row in station_location.index:
                station_name=station_location.iloc[row,0]            
                longitude_station = station_location.iloc[row,1]
                latitude_station = station_location.iloc[row,2]
                sq_diff_lat =(lats-latitude_station)**2
                sq_diff_lon = (lon-longitude_station)**2
                min_index_lat = sq_diff_lat.argmin()
                min_index_lon = sq_diff_lon.argmin()
                current_cut = weather_df[(weather_df[1]==index['y'][min_index_lat]) & (weather_df[2]==index['x'][min_index_lon])]
                current_cut.set_index(time, inplace =True)
                df= pd.DataFrame(0, columns = [station_name], index = time)
                df[station_name]=current_cut.iloc[:,3]
                temperature_month=pd.concat([temperature_month,df],axis=1)
        
            temperature_profile = temperature_profile.append(temperature_month)
        
        temperature_profile.sort_index(inplace=True)
        
        return temperature_profile

def temp_interval():    
    temperature_interval = pd.DataFrame()
    temp_profile = temperature_profile_extract()
    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(index, temperature=temp_profile[temp_profile.columns[x]]).get_temperature_interval(how='geometric_series')
        temperature_interval[name_station]=idp_this_station['temperature_interval']
    
    return temperature_interval


def idp_pool():
    stock=['MFH','SFH']
    class_list=[3,4,5,6,7,8,9,10]
    for s in stock:
        for m in class_list:
            file_name=f'idp_collection_class_{m}_{s}_norm.pickle'
            globals()[f'idp_collection_class_{m}_{s}']=pd.read_pickle(
                os.path.join(r'/home/student/Documents/egon_AM/heat_demand_generation/idp pool generation/idp_pool_normalized_24hr', file_name))
    return stock,class_list
     
def idp_df_generator():
    stock,class_list = idp_pool()
    idp_df = pd.DataFrame(columns=['idp', 'house', 'temperature_class'])
    for s in stock:
        for m in class_list:
            var_name=globals()[f'idp_collection_class_{m}_{s}']
            idp_df = idp_df.append(
                pd.DataFrame(
                    data = {
                        'idp': var_name.transpose().values.tolist(),
                        'house': s,
                        'temperature_class': m
                        }))
    return idp_df


def annual_demand_generator():      
    # function for extracting demand data from pgadmin airflow database(new one added on 3003); 
    #converts the table into a dataframe   
    def psycop_df_AF(table_name):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = sqlio.read_sql_query(sql, conn)
        conn = None
        return data
    # the function extracts all spatial data in the form of geopandas dataframe      
    def psycop_gdf_AF(table_name):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = gpd.read_postgis(sql, conn)
        conn = None
        return data
           
    demand = psycop_df_AF('demand.egon_peta_heat')
    a_pha = psycop_df_AF('society.destatis_zensus_apartment_per_ha')
    b_pha = psycop_df_AF('society.destatis_zensus_building_per_ha')
    h_pha = psycop_df_AF('society.destatis_zensus_household_per_ha')
    
    zp_pha = psycop_gdf_AF('society.destatis_zensus_population_per_ha')
    
    ##considering only eGon2035 demand scenario for the residential sector
    demand = demand[demand['scenario'] == 'eGon2035']
    demand = demand[demand['sector'] == 'residential']
    
    a_pha_1 = a_pha[['zensus_population_id', 'grid_id']]
    a_pha_1 = a_pha_1.drop_duplicates(subset='zensus_population_id')
    b_pha_1 = b_pha[['zensus_population_id', 'grid_id']]
    b_pha_1 = b_pha_1.drop_duplicates(subset='zensus_population_id')
    h_pha_1 = h_pha[['zensus_population_id', 'grid_id']]
    h_pha_1 = h_pha_1.drop_duplicates(subset='zensus_population_id')
    
    all_grid = a_pha_1.append(b_pha_1).append(h_pha_1)
    all_grid = all_grid.drop_duplicates(subset='zensus_population_id')
    demand_grid = pd.merge(demand, all_grid, how='inner',
                           on='zensus_population_id')
    
    all_geom = zp_pha.drop(zp_pha.columns.difference(['grid_id', 'geom']), axis=1)
    demand_geom = pd.merge(demand_grid, all_geom, how='inner', on='grid_id')
    
    demand_geom = gpd.GeoDataFrame(demand_geom, geometry='geom')
    demand_geom = demand_geom.drop(
        demand_geom.columns.difference(['demand', 'grid_id', 'geom','zensus_population_id']), 1)
    demand_geom['geom'] = demand_geom['geom'].to_crs(epsg=4326)
    ####import demand geo ###shape file available in the next cloud link
    temperature_zones = gpd.read_file(
        '/home/student/Documents/egon_AM/Maps/Try_Climate_Zone.shp')
    temperature_zones.sort_values('Zone', inplace=True)
    temperature_zones.reset_index(inplace=True)
    temperature_zones.drop(columns=['index', 'Id'], inplace=True, axis=0)
    #####import temperature_zones  
    
    demand_zone = gpd.sjoin(demand_geom, temperature_zones,
                            how='inner', op='intersects')

    # 300 repeated overrlapping cells removed
    demand_zone.drop_duplicates(['grid_id'], inplace=True)
    
    ##classification of zensus 'characteritic_text' into household stock categories
    sfh_chartext = ['Freistehendes Einfamilienhaus', 'Einfamilienhaus: Doppelhaushälfte', 'Einfamilienhaus: Reihenhaus',
                    'Freistehendes Zweifamilienhaus', 'Zweifamilienhaus: Doppelhaushälfte', 'Zweifamilienhaus: Reihenhaus']
    mfh_chartext = ['Mehrfamilienhaus: 3-6 Wohnungen', 'Mehrfamilienhaus: 7-12 Wohnungen',
                    'Mehrfamilienhaus: 13 und mehr Wohnungen', 'Anderer Gebäudetyp']
    
    ###only builing types with attribute GEBTYPGROESSE considered for household stock characterization
    bg_pha = b_pha[b_pha['attribute'] == 'GEBTYPGROESSE']
    
    ##assigning household stock to each cell
    def household_stock(x):
        if x in sfh_chartext:
            output = 'SFH'
        if x in mfh_chartext:
            output = 'MFH'
        return output    
    bg_pha['Household Stock'] = bg_pha['characteristics_text'].apply(
        household_stock)
    ##counting sfh and mfh for each cell
    house_count = bg_pha[['grid_id', 'quantity', 'Household Stock']]
    house_count = house_count.groupby(
        ['grid_id', 'Household Stock']).sum('quantity')
    house_count = house_count.reset_index()
    house_count = house_count.pivot_table(
        values='quantity', index='grid_id', columns='Household Stock')
    house_count = house_count.fillna(0)
    
    demand_count = pd.merge(demand_zone, house_count, how='inner', on='grid_id')
    
    demand_count.drop('index_right', axis=1, inplace=True)
    #demand count consists of demand and household stock count for each cell
    
    
    ###assigning a array for each cell consiting of the day class. array length 365
    # all_temperature_interval is the output of HTS_generation.py
    # direct:
    all_temperature_interval = temp_interval()
    all_temperature_interval.set_index(pd.to_datetime(
        all_temperature_interval.index), inplace=True)
    all_temperature_interval = all_temperature_interval.resample('D').max()
    
    def interval_allocation(x):
        if x == 'Hamburg-Fuhlsbuettel':
            return np.array(all_temperature_interval['Hamburg-Fuhlsbuettel'])
        if x == 'Rostock-Warnemuende':
            return np.array(all_temperature_interval['Rostock-Warnemuende'])
        if x == 'Bremerhaven':
            return np.array(all_temperature_interval['Bremerhaven'])
    
    demand_count['Temperature_interval'] = demand_count['Station'].apply(
        interval_allocation)
    demand_count.drop(demand_count.columns.difference(
        ['grid_id', 'demand', 'SFH', 'MFH', 'Temperature_interval']), axis=1, inplace=True)
    
    return demand_count ##df with demand,hhstock count and daily temprature interval




##generates dataframe with randomly selected column names from the idp pool
def profile_selector():
    idp_df = idp_df_generator()
    annual_demand = annual_demand_generator()
    all_temperature_interval = temp_interval()
    
    Temperature_interval = pd.DataFrame(columns = range(365))
   
    all_temperature_interval.set_index(index,inplace=True)
    all_temperature_interval = all_temperature_interval.resample('D').max()

    all_temperature_interval.reset_index(inplace=True)
    all_temperature_interval.drop('index',axis=1,inplace=True)

    for x in all_temperature_interval.iloc[:,0:3]:###needs to be changed when running for the entire country 
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

        
##for building class 11, shlp_type =EFH, wind_impact=0, from shlp_sigmoid_factors.csv, demandlib
def h_value():
    a = 3.0469695
    
    b = -37.1833141
    
    c = 5.6727847
    
    d = 0.1163157
    
   
    temp_profile =temperature_profile_extract()
    temperature_profile_res =temp_profile.resample('D').mean().reindex(index).fillna(method='ffill').fillna(method='bfill')
    
    temp_profile_geom = (temperature_profile_res + 0.5 * np.roll(temperature_profile_res, 24) +
                                    0.25 * np.roll(temperature_profile_res, 48) +
                                    0.125 * np.roll(temperature_profile_res, 72)) / 1.875
    
    #for each temperature station h value created for hourly resolution
    h= (a / (1 + (b / (temp_profile_geom - 40)) ** c) + d)
    return h
    
##generaing specific profiles of grid cell as per the assigned index
###### normalizing and scaling up of profiles
def profile_generator(station):
    profile_selector()
    x = globals()[f'result_SFH_{station}']
    idp_df = idp_df_generator()
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

##scaling the profile as per the station h-value and the cell demand
def demand_scale(station):
    heat_demand_profile = profile_generator(station)
    h = h_value()
    annual_demand = annual_demand_generator()
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

