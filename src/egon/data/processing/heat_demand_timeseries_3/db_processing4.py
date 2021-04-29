
import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as sqlio
import geopandas as gpd
import matplotlib.pyplot as plt
import os
from egon.data import db

from feedinlib import era5
import netCDF4
from netCDF4 import Dataset

from math import ceil

###reading idp pool; all files import eg: idp_collection_class_3_SFH.
##### idp pool available in the nextcloud link
stock=['MFH','SFH']
class_list=[3,4,5,6,7,8,9,10]
for s in stock:
    for m in class_list:
        file_name=f'idp_collection_class_{m}_{s}_norm.pickle'
        globals()[f'idp_collection_class_{m}_{s}']=pd.read_pickle(
            os.path.join(os.getcwd(), file_name))
#r'/home/student/Documents/egon_AM/heat_demand_generation/idp pool generation/idp_pool_normalized_24hr'

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
index = pd.date_range(pd.datetime(2011,1,1,0), periods = 8760, freq='H')

#function for extracting demand data from pgadmin; converts the table into a dataframe 
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
    # needed
    b_pha = psycop_df_AF('society.destatis_zensus_building_per_ha')
    h_pha = psycop_df_AF('society.destatis_zensus_household_per_ha')
    
    zp_pha = psycop_gdf_AF('society.destatis_zensus_population_per_ha')
    #zp_pha2 = psycop_gdf_db_AF('society.destatis_zensus_population_per_ha')
    
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
####import demand geom    
    temperature_zones = gpd.read_file(
        '/home/student/Documents/egon_AM/Maps/Try_Climate_Zone.shp')
    temperature_zones.sort_values('Zone', inplace=True)
    temperature_zones.reset_index(inplace=True)
    temperature_zones.drop(columns=['index', 'Id'], inplace=True, axis=0)
#####import temperature_zones  
    
    demand_zone = gpd.sjoin(demand_geom, temperature_zones,
                            how='inner', op='intersects')
    ##demand_zone consists of df with demand, grid_id, geom and station name
    
    # direct_output
    #demand_zone = pd.read_csv(os.path.join(os.getcwd(),Demand_zone_new_Residential2035'), index_col=0)
    
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
    ##direct(output of all above codes)
    demand_count=pd.read_csv(r'/home/student/Documents/egon_AM/heat_demand_generation/data_15.04/Demand_with_HH_number.csv', index_col=0)
    demand_count.drop('index_right', axis=1, inplace=True)
    #demand count consists of demand and household stock count for each cell
    
    
    ###assigning a array for each cell consiting of the day class. array length 365
    # all_temperature_interval is the output of HTS_generation.py
    # direct:
    all_temperature_interval = pd.read_csv(
        r'/home/student/Documents/egon_AM/heat_demand_generation/data_15.04/all_temperature_interval.csv', index_col=0)
    all_temperature_interval.set_index(pd.to_datetime(
        all_temperature_interval.index), inplace=True)
    all_temperature_interval = all_temperature_interval.resample('D').max()
    
    def temp_interval(x):
        if x == 'Hamburg-Fuhlsbuettel':
            return np.array(all_temperature_interval['Hamburg-Fuhlsbuettel'])
        if x == 'Rostock-Warnemuende':
            return np.array(all_temperature_interval['Rostock-Warnemuende'])
        if x == 'Bremerhaven':
            return np.array(all_temperature_interval['Bremerhaven'])
    
    demand_count['Temperature_interval'] = demand_count['Station'].apply(
        temp_interval)
    demand_count.drop(demand_count.columns.difference(
        ['grid_id', 'demand', 'SFH', 'MFH', 'Temperature_interval']), axis=1, inplace=True)
    
    return demand_count ##df with demand,hhstock count and daily temprature interval

def temperature_profile():
    #downloading all data for Germany
    # latitude = [47,55] 
    # longitude =[5,16]       
    # # set start and end date (end date will be included 
    # # in the time period for which data is downloaded)
    # start_date, end_date = '2011-01-01', '2011-12-31'
    # # set variable set to download
    # variable = "windpowerlib"
    # # the downloaded fle will be saved as the target file
    # target_file = 'Germany_Weather2011.nc'
    # # get windpowerlib data for specified location
    # ds = era5.get_era5_data_from_datespan_and_position(
    #     variable=variable,
    #     start_date=start_date, end_date=end_date, 
    #     latitude=latitude, longitude=longitude,
    #     target_file=target_file)
    
    data = Dataset(os.path.join(os.getcwd(),'Germany_Weather2011.nc'),'r')
    lats = data.variables['latitude'][:]
    lon = data.variables['longitude'][:]
    time = data.variables['time'][:]
    temperature = data.variables['t2m'][:]
    temp = data.variables['t2m']
    starting_date = pd.to_datetime(data.variables['time'].units[11:12]+'2011-01-01 00:00:00.0')
    ending_date =  pd.to_datetime(data.variables['time'].units[11:12]+'2011-12-31 23:00:00.0')
    date_range = pd.date_range(start= starting_date, end = ending_date, freq = 'h')
    temperature_profile =pd.DataFrame()

    station_location=pd.read_csv(os.path.join(os.getcwd(),'TRY_Climate_Zones','station_coordinates.csv'))###replace file bz direc adding into the database
    ##extracting the netcdf file into a dataframe
    for row in station_location.index:
        station_name=station_location.iloc[row,0]
        longitude_station = station_location.iloc[row,1]
        latitude_station = station_location.iloc[row,2]
        sq_diff_lat =(lats-latitude_station)**2
        sq_diff_lon = (lon-longitude_station)**2
        min_index_lat = sq_diff_lat.argmin()
        min_index_lon = sq_diff_lon.argmin()
        df = pd.DataFrame(0, columns = [station_name], index = date_range)
        dt= np.arange(0, data.variables['time'].size)
        for time_index in dt:
            df.iloc[time_index] = temp[time_index,min_index_lat,min_index_lon]-273
        temperature_profile=pd.concat([temperature_profile,df],axis=1)
        
        return temperature_profile
    
    
###### normalizing and scaling up of profiles      
def profile_generator(station):
    x = globals()[f'result_SFH_{station}'].head()
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



class IdpProfiles:    
    def __init__(self,df_index, **kwargs):
        
        self.df = pd.DataFrame(index = df_index)
        self.annual_heat_demand=kwargs.get('annual_heat_demand')
        self.temperature = kwargs.get('temperature')
        self.sfh_count = kwargs.get('sfh_count')
        self.mfh_count = kwargs.get('mfh_count')
        self.idp_collection_class_3 = kwargs.get('idp_collection_class_3')
        self.idp_collection_class_4 = kwargs.get('idp_collection_class_4')
        self.idp_collection_class_5 = kwargs.get('idp_collection_class_5')
        self.idp_collection_class_6 = kwargs.get('idp_collection_class_6')
        self.idp_collection_class_7 = kwargs.get('idp_collection_class_7')
        self.idp_collection_class_8 = kwargs.get('idp_collection_class_8')
        self.idp_collection_class_9 = kwargs.get('idp_collection_class_9')
        self.idp_collection_class_10 = kwargs.get('idp_collection_class_10')
        self.temperature_int = kwargs.get('temperature_int')
        self.station_name = kwargs.get('station_name')
            
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


#######dataframe for the h-value
        
##for building class 11, shlp_type =EFH, wind_impact=0, from shlp_sigmoid_factors.csv, demandlib

def h_value():
    a = 3.0469695
    
    b = -37.1833141
    
    c = 5.6727847
    
    d = 0.1163157
    
    #from /home/student/Documents/egon_AM/heat_demand_generation
    temperature_profile =pd.read_pickle(os.path.join(r'/home/student/Documents/egon_AM/heat_demand_generation'
                                                     ,'temperature_profile.pickle'))
    
    temperature_profile_res =temperature_profile.resample('D').mean().reindex(index).fillna(method='ffill').fillna(method='bfill')
    
    temp_profile_geom = (temperature_profile_res + 0.5 * np.roll(temperature_profile, 24) +
                                    0.25 * np.roll(temperature_profile_res, 48) +
                                    0.125 * np.roll(temperature_profile_res, 72)) / 1.875
    
    #for each temperature station h value created for hourly resolution
    h= (a / (1 + (b / (temp_profile_geom - 40)) ** c) + d)
    return h