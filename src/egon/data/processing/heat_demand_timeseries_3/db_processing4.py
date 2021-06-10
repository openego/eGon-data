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
from sqlalchemy import Column, String, Float, Integer, ForeignKey, ARRAY
import egon.data.importing.era5 as era

import netCDF4
from netCDF4 import Dataset

from disaggregator import config, data, spatial, temporal, plot

from math import ceil


###generate temperature interval for each temperature zone
class IdpProfiles:
    def __init__(self,df_index, **kwargs):
        index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

        self.df = pd.DataFrame(index = df_index)

        self.temperature = kwargs.get('temperature')

   #combination of weighted_temperature, get_normalized_bdew and temperature_inteval
    def get_temperature_interval(self, how='geometric_series'):
        index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')
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

##extracting the temperature for each temperature zone from cds data
def temperature_profile_extract():

    cutout = era.import_cutout(boundary = 'Europe')
    coordinates_path = os.path.join(os.getcwd(),'TRY_Climate_Zones')
    station_location=pd.read_csv(
        os.path.join(coordinates_path,'station_coordinates.csv'))

    weather_cells = db.select_geodataframe(
        """
        SELECT geom FROM supply.egon_era5_weather_cells
        """,
        epsg=4326)

    gdf = gpd.GeoDataFrame(
        station_location,
        geometry=gpd.points_from_xy(
            station_location.Longitude, station_location.Latitude))

    selected_weather_cells = gpd.sjoin(weather_cells, gdf).set_index('Station')

    temperature_profile = cutout.temperature(
        shapes=selected_weather_cells.geom.values,
        index=selected_weather_cells.index).to_pandas().transpose()

    return temperature_profile

###generate temperature zones for each cell
def temp_interval():
    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')
    temperature_interval = pd.DataFrame()
    temp_profile = temperature_profile_extract()

    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(index, temperature=temp_profile[temp_profile.columns[x]]).get_temperature_interval(how='geometric_series')
        temperature_interval[name_station]=idp_this_station['temperature_interval']

    return temperature_interval

###generate idp pool from the profiles generated from the load profile generator
def idp_pool_generator():
    ###read hdf5 files with the generated profiles from the load profile generator
    #path = os.path.join(r'/home/student/Documents/egon_AM/heat_demand_generation/idp pool generation',
                        #'heat_data.hdf5')
    path = os.path.join(os.path.join(os.getcwd(), 'heat_data_new.hdf5'))

    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

    sfh = pd.read_hdf(path, key ='SFH')
    mfh = pd.read_hdf(path, key ='MFH')
    temp = pd.read_hdf(path, key ='temperature')

    ##############################################################################
    #demand per city
    globals()['luebeck_sfh'] = sfh[sfh.filter(like='Luebeck').columns]
    globals()['luebeck_mfh'] = mfh[mfh.filter(like='Luebeck').columns]

    globals()['kassel_sfh'] = sfh[sfh.filter(like='Kassel').columns]
    globals()['kassel_mfh'] = mfh[mfh.filter(like='Kassel').columns]

    globals()['wuerzburg_sfh'] = sfh[sfh.filter(like='Wuerzburg').columns]
    globals()['wuerzburg_mfh'] = mfh[mfh.filter(like='Wuerzburg').columns]

    globals()['chemnitz_sfh'] = sfh[sfh.filter(like='Chemnitz').columns]
    globals()['chemnitz_mfh'] = mfh[mfh.filter(like='Chemnitz').columns]

    ####dataframe with daily temperature in geometric series
    temp_daily = pd.DataFrame()
    for column in temp.columns:
        temp_current= temp[column].resample('D').mean().reindex(temp.index).fillna(method='ffill').fillna(method='bfill')
        # temp_current_geom = (temp_current + 0.5 * np.roll(temp_current, 24) +
        #                             0.25 * np.roll(temp_current, 48) +
        #                             0.125 * np.roll(temp_current, 72)) / 1.875 
        temp_current_geom = temp_current
        
        
        temp_daily=pd.concat([temp_daily,temp_current_geom], axis=1)

    def round_temperature(station):
        intervals = ({
            -20: 1, -19: 1, -18: 1, -17: 1, -16: 1, -15: 1, -14: 2,
            -13: 2, -12: 2, -11: 2, -10: 2, -9: 3, -8: 3, -7: 3, -6: 3, -5: 3,
            -4: 4, -3: 4, -2: 4, -1: 4, 0: 4, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,
            6: 6, 7: 6, 8: 6, 9: 6, 10: 6, 11: 7, 12: 7, 13: 7, 14: 7, 15: 7,
            16: 8, 17: 8, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 9, 24: 9,
            25: 9, 26: 10, 27: 10, 28: 10, 29: 10, 30: 10, 31: 10, 32: 10,
            33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 10, 40: 10})
        temperature_rounded=[]
        for i in temp_daily.loc[:,station]:
            temperature_rounded.append(ceil(i))
        temperature_interval =[]
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        temp_class_dic = {f'Class_{station}': temperature_interval}
        #temp_class_dic = {'Class_{}'.format(station): temperature_interval}
        temp_class = pd.DataFrame.from_dict(temp_class_dic)
        return temp_class

    temp_class_luebeck = round_temperature('Luebeck')
    temp_class_kassel = round_temperature('Kassel')
    temp_class_wuerzburg = round_temperature('Wuerzburg')
    temp_class_chemnitz = round_temperature('Chemnitz')
    temp_class =pd.concat([temp_class_luebeck, temp_class_kassel,temp_class_wuerzburg,temp_class_chemnitz],axis=1)
    temp_class.set_index(index, inplace=True)

    def unique_classes(station):
        classes=[]
        for x in temp_class[f'Class_{station}']:
        #for x in temp_class['Class_{}'.format(station)]:
            if x not in classes:
                classes.append(x)
        classes.sort()
        return classes

    globals()['luebeck_classes'] = unique_classes('Luebeck')
    globals()['kassel_classes'] = unique_classes('Kassel')
    globals()['wuerzburg_classes'] = unique_classes('Wuerzburg')
    globals()['chemnitz_classes'] = unique_classes('Chemnitz')

    stock=['MFH','SFH']
    class_list=[2,3,4,5,6,7,8,9,10]
    for s in stock:
         for m in class_list:
             globals()[f'idp_collection_class_{m}_{s}']=pd.DataFrame(index=range(24))
             #globals()['idp_collection_class_{}_{}'.format(m,s)]=pd.DataFrame(index=range(24))


    def splitter(station,household_stock):
        #this_classes = globals()[f'{station.lower()}_classes']
        this_classes = globals()['{}_classes'.format(station.lower())]
        for classes in this_classes:
            #this_itteration = globals()[f'{station.lower()}_{household_stock.lower()}'].loc[temp_class[f'Class_{station}']==classes,:]
            this_itteration = globals()['{}_{}'.format(station.lower(),household_stock.lower())].loc[temp_class['Class_{}'.format(station)]==classes,:]
            days = list(range(int(len(this_itteration)/24)))
            for day in days:
                this_day = this_itteration[day*24:(day+1)*24]
                this_day = this_day.reset_index(drop=True)
                globals()[f'idp_collection_class_{classes}_{household_stock}']=pd.concat(
                                [globals()[f'idp_collection_class_{classes}_{household_stock}'],
                                                                            this_day],axis=1,ignore_index=True)
                # globals()['idp_collection_class_{}_{}'.format(classes,household_stock)]=pd.concat(
                #                 [globals()['idp_collection_class_{}_{}'.format(classes,household_stock)],
                #                                                             this_day],axis=1,ignore_index=True)
    splitter('Luebeck','SFH')
    splitter('Kassel','SFH')
    splitter('Wuerzburg','SFH')
    splitter('Chemnitz','SFH')
    splitter('Luebeck','MFH')
    splitter('Kassel','MFH')
    splitter('Chemnitz','MFH')

    def pool_normalize(x):
            if x.sum()!=0:
                c=x.sum()
                return (x/c)
            else:
                return x

    stock=['MFH','SFH']
    class_list=[2,3,4,5,6,7,8,9,10]
    for s in stock:
         for m in class_list:
             df_name = globals()[f'idp_collection_class_{m}_{s}']
             #df_name = globals()['idp_collection_class_{}_{}'.format(m,s)]
             globals()[f'idp_collection_class_{m}_{s}_norm']=df_name.apply(pool_normalize)
             #globals()['idp_collection_class_{}_{}_norm'.format(m,s)]=df_name.apply(pool_normalize)

    return [idp_collection_class_2_SFH_norm,idp_collection_class_3_SFH_norm,idp_collection_class_4_SFH_norm,
            idp_collection_class_5_SFH_norm,idp_collection_class_6_SFH_norm,idp_collection_class_7_SFH_norm,
            idp_collection_class_8_SFH_norm,idp_collection_class_9_SFH_norm,idp_collection_class_10_SFH_norm,
            idp_collection_class_2_MFH_norm,idp_collection_class_3_MFH_norm,idp_collection_class_4_MFH_norm,
            idp_collection_class_5_MFH_norm, idp_collection_class_6_MFH_norm,idp_collection_class_7_MFH_norm,
            idp_collection_class_8_MFH_norm,idp_collection_class_9_MFH_norm,idp_collection_class_10_MFH_norm]

#convert the multiple idp pool into a single dataframe
def idp_df_generator():
    idp_list = idp_pool_generator()
    stock=['MFH','SFH']
    class_list=[2,3,4,5,6,7,8,9,10]
    idp_df = pd.DataFrame(columns=['idp', 'house', 'temperature_class'])
    for s in stock:
        for m in class_list:
            #var_name=globals()[f'idp_collection_class_{m}_{s}']
            if s =='SFH':
              i = class_list.index(m)
            if s == 'MFH':
              i = class_list.index(m) + 9
            current_pool = idp_list[i]
            idp_df = idp_df.append(
                pd.DataFrame(
                    data = {
                        'idp': current_pool.transpose().values.tolist(),
                        'house': s,
                        'temperature_class': m
                        }))
    idp_df = idp_df.reset_index(drop=True)


    ##writting to the database
    idp_df.to_sql('heat_idp_pool',con=db.engine(),schema='demand' ,if_exists ='replace', index=True,
                      dtype = {'index': Integer(),
                                'idp':ARRAY(Float()),
                                'house': String(),
                                'temperature_class':Integer()})

    idp_df['idp'] =idp_df.idp.apply(lambda x: np.array(x))

    idp_df.idp = idp_df.idp.apply(lambda x:  x.astype(np.float32))


    return idp_df


# function for extracting demand data from pgadmin airflow database
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

def psycop_gdf_AF(table_name,geom_column = 'geom'):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = gpd.read_postgis(sql, conn, geom_column)
        conn = None
        return data

##extracting and adjusting the demand data to obtain desired structure
## considering only residential for 2035 scenario
def annual_demand_generator():
    #os.chdir(r'/home/student')
    demand = psycop_df_AF('demand.egon_peta_heat')
    a_pha = psycop_df_AF('society.egon_destatis_zensus_apartment_per_ha')
    b_pha = psycop_df_AF('society.egon_destatis_zensus_building_per_ha')
    h_pha = psycop_df_AF('society.egon_destatis_zensus_household_per_ha')

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
    # temperature_zones = gpd.read_file(
    #     '/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/TRY_Climate_Zones/Try_Climate_Zone.shp')###change this file location
    temperature_zones = gpd.read_file(os.path.join(os.getcwd(),'TRY_Climate_Zones','Try_Climate_Zone.shp'))
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
    house_count = bg_pha[['zensus_population_id', 'quantity', 'Household Stock']]
    house_count = house_count.groupby(
        ['zensus_population_id', 'Household Stock']).sum('quantity')
    house_count = house_count.reset_index()
    house_count = house_count.pivot_table(
        values='quantity', index='zensus_population_id', columns='Household Stock')
    house_count = house_count.fillna(0)

    demand_count = pd.merge(demand_zone, house_count, how='inner', on='zensus_population_id')

    demand_count.drop('index_right', axis=1, inplace=True)
    #demand count consists of demand and household stock count for each cell

    demand_count.drop(demand_count.columns.difference(
        ['zensus_population_id', 'demand', 'SFH', 'MFH','Station']), axis=1, inplace=True)

    return demand_count ##df with demand,hhstock count and daily temprature interval

##generates dataframe with randomly selected column names from the idp pool
def profile_selector():
    #idp_df = idp_df_generator()
    idp_df = pd.read_pickle(os.path.join(os.getcwd(),'idp_df.pickle'))
    #idp_df = pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/idp_df.pickle')
    #idp_df = idp_df_sample #####################################to be read from the pgadmin database direclty
    #annual_demand = annual_demand_generator()
    annual_demand =  pd.read_pickle(os.path.join(os.getcwd(),'annual_demand.pickle'))
    #annual_demand = pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/annual_demand.pickle')
    #annual_demand.drop('Temperature_interval',axis=1,inplace=True)
    #all_temperature_interval = temp_interval()
    all_temperature_interval =  pd.read_pickle(os.path.join(os.getcwd(),'all_temperature_interval.pickle'))
    #all_temperature_interval = pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/all_temperature_interval.pickle')

    #Temperature_interval = pd.DataFrame(columns = range(365))
    #all_temperature_interval.set_index(index,inplace=True)
    all_temperature_interval = all_temperature_interval.resample('D').max()
    all_temperature_interval.reset_index(drop=True, inplace=True)
    
    station_count = annual_demand.Station.nunique()
        
    all_temperature_interval = all_temperature_interval.iloc[:,0:station_count]
    #all_temperature_interval = all_temperature_interval.iloc

    Temperature_interval = all_temperature_interval.transpose()

    # Set seed value to have reproducable results
    np.random.seed(0)

    db.execute_sql("DELETE FROM demand.selected_idp_names;")

    #generates a dataframe with the idp index number of the selected profiles for each temperature

    selected_idp_names = pd.DataFrame()
    length = 0
    for station in Temperature_interval.index:
        result_SFH =pd.DataFrame(columns=Temperature_interval.columns)
        result_MFH =pd.DataFrame(columns=Temperature_interval.columns)
        
        for day in Temperature_interval.columns:
            t_class = Temperature_interval.loc[station,day].astype(int)
            
            array_SFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                        &(idp_df.house=='SFH')].index.values)

            array_MFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                        &(idp_df.house=='MFH')].index.values)

            result_SFH[day] = np.random.choice(
                array_SFH, int(annual_demand[annual_demand.Station==station].SFH.sum()))

            result_MFH[day] = np.random.choice(
                array_MFH, int(annual_demand[annual_demand.Station==station].MFH.sum()))

        result_SFH['zensus_population_id'] = annual_demand[
            annual_demand.Station==station].loc[
                annual_demand[annual_demand.Station==station].index.repeat(
                    annual_demand[annual_demand.Station==station].SFH.astype(int))
                ].zensus_population_id.values
                

        result_MFH['zensus_population_id'] =annual_demand[
            annual_demand.Station==station].loc[
                annual_demand[annual_demand.Station==station].index.repeat(
                    annual_demand[annual_demand.Station==station].MFH.astype(int))
                ].zensus_population_id.values

        result_SFH.set_index('zensus_population_id',inplace=True)
        result_MFH.set_index('zensus_population_id',inplace=True)
        
        
        selected_idp_names = selected_idp_names.append(result_SFH)
        selected_idp_names = selected_idp_names.append(result_MFH)
        selected_idp_names = selected_idp_names.apply(lambda x: x.astype(np.int32))
        
        new_length = len(selected_idp_names)
        
        current_idp = selected_idp_names.iloc[length:new_length,:] 
        selected_array = pd.DataFrame(index=current_idp.index)
        selected_array['selected_idp'] =current_idp.values.tolist()
        
        selected_array.to_sql(
            'selected_idp_names',con=db.engine(),
            schema='demand' ,if_exists ='append', index=True) 
        
        length = new_length
        
    
    return annual_demand, idp_df, selected_idp_names



##for building class 11, shlp_type =EFH, wind_impact=0, from shlp_sigmoid_factors.csv, demandlib
def h_value():
    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

    a = 3.0469695

    b = -37.1833141

    c = 5.6727847

    d = 0.1163157


    temp_profile = temperature_profile_extract()
    temperature_profile_res =temp_profile.resample('D').mean().reindex(index).fillna(method='ffill').fillna(method='bfill')

    temp_profile_geom = ((temperature_profile_res.transpose() + 0.5 * np.roll(temperature_profile_res.transpose(), 24,axis=1) +
                                    0.25 * np.roll(temperature_profile_res.transpose(), 48,axis=1) +
                                    0.125 * np.roll(temperature_profile_res.transpose(), 72,axis=1)) / 1.875).transpose()

    #for each temperature station h value created for hourly resolution
    h= (a / (1 + (b / (temp_profile_geom - 40)) ** c) + d)

    return h

##generaing specific profiles of grid cell as per the assigned index
###### normalizing and scaling up of profiles
def profile_generator(aggregation_level):
    ###aggregation for district heating level
    annual_demand, idp_df, selected_profiles = profile_selector()
    #selected_profiles = pd.read_pickle('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/selected_profiles.pickle')
    #idp_df = pd.read_pickle('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/idp_df.pickle')

    y = idp_df['idp'].reset_index()
    heat_profile_idp= pd.DataFrame(index=selected_profiles.index.sort_values())   

    for i in selected_profiles.columns:
        col=selected_profiles[i]
        col=col.reset_index()
        y.rename(columns={'index':i},inplace=True)
        col=pd.merge(col,y[[i,'idp']],how='left', on=i)
        col[i]=col['idp']
        col.drop('idp',axis=1,inplace=True)
        col.set_index('zensus_population_id', inplace=True)
        #col = col.groupby(lambda x:x, axis=0).sum()
        heat_profile_idp[i] = col[i].values
        y.rename(columns={i:'index'},inplace=True)
        
    if aggregation_level == 'district':
        ###district heating aggregation
        district_heating = psycop_df_AF('demand.map_zensus_district_heating_areas')
        district_heating = district_heating[district_heating.scenario == 'eGon2035']
        
        heat_profile_dist = pd.merge(heat_profile_idp,
                                      district_heating[['area_id','zensus_population_id']]
                                      , on='zensus_population_id', how = 'inner' )
        heat_profile_dist.sort_values('area_id',inplace=True)
        heat_profile_dist.set_index('area_id',inplace=True)
        heat_profile_dist.drop('zensus_population_id',axis=1,inplace=True)
    
        ###mv_grid aggregation
        mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
        mv_grid = mv_grid.set_index('zensus_population_id')
        district_heating =district_heating.set_index('zensus_population_id')
    
        mv_grid_ind = mv_grid.loc[mv_grid.index.difference(district_heating.index),:]
    
        heat_profile_idp = pd.merge(heat_profile_idp,mv_grid_ind['subst_id'], left_on =selected_profiles.index,
                          right_on = mv_grid_ind.index, how='inner' )
        heat_profile_idp.sort_values('subst_id',inplace=True)
        heat_profile_idp.set_index('subst_id',inplace=True)
        heat_profile_idp.drop('key_0',axis=1,inplace=True)
        
        heat_profile_dist = heat_profile_dist.groupby(lambda x:x, axis=0).sum()
        heat_profile_dist =  heat_profile_dist.transpose()
        heat_profile_dist =  heat_profile_dist.apply(lambda x: x.explode())
        heat_profile_dist.reset_index(drop=True,inplace=True)
        heat_profile_dist = heat_profile_dist.apply(lambda x: x/x.sum())
        
        heat_profile_idp = heat_profile_idp.groupby(lambda x:x, axis=0).sum()
        heat_profile_idp =  heat_profile_idp.transpose()
        heat_profile_idp =  heat_profile_idp.apply(lambda x: x.explode())
        heat_profile_idp.reset_index(drop=True,inplace=True)
        heat_profile_idp = heat_profile_idp.apply(lambda x: x/x.sum())
    
    else:   
        heat_profile_dist = 0
        
        heat_profile_idp = heat_profile_idp.groupby(lambda x:x, axis=0).sum()
        heat_profile_idp =  heat_profile_idp.transpose()
        heat_profile_idp =  heat_profile_idp.apply(lambda x: x.explode())
        heat_profile_idp.reset_index(drop=True,inplace=True)
        heat_profile_idp = heat_profile_idp.apply(lambda x: x/x.sum())
        #heat_profile_idp =  heat_profile_idp.transpose()
    

    return annual_demand, heat_profile_dist, heat_profile_idp


def residential_demand_scale(aggregation_level):
    annual_demand, heat_profile_dist, heat_profile_idp = profile_generator(aggregation_level)
    #heat_profile = pd.read_pickle('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/heat_profile_idp_final_final.pickle')

    h = h_value()
    #h = pd.read_pickle('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/h.pickle')
    h= h.reset_index(drop=True)

    #annual_demand = annual_demand_generator()
    #annual_demand = pd.read_pickle('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/annual_demand.pickle')
    #annual_demand.drop('Temperature_interval',axis=1,inplace=True)

    if aggregation_level == 'district':
        
        ##district heating aggregation
        district_heating = psycop_df_AF('demand.map_zensus_district_heating_areas')
        district_heating = district_heating[district_heating.scenario == 'eGon2035']

        district_station=pd.merge(district_heating[['area_id','zensus_population_id']],
                          annual_demand[['zensus_population_id','Station','demand']],
                          on='zensus_population_id', how ='inner')

        district_station.sort_values('area_id',inplace=True)
        district_station.drop('zensus_population_id',axis=1, inplace =True)
        district_station=district_station.groupby(['area_id','Station']).sum()
        district_station.reset_index('Station', inplace =True)

        demand_curves_dist = pd.DataFrame()

        for j in range(len(heat_profile_dist.columns)):
            current_district = heat_profile_dist.iloc[:,j]
            area_id = heat_profile_dist.columns[j]
            station = district_station[district_station.index == area_id]['Station'][area_id]
            ##some area id cover two different stations
            if type(station)!=str:
                station = station.reset_index()
                multiple_stations = pd.DataFrame()
                for i in station.index:
                    current_station = station.Station[i]
                    multiple_stations[i] = current_district.multiply(h[current_station],axis=0)
                multiple_stations = multiple_stations.sum(axis=1)
                demand_curves_dist[area_id] = multiple_stations
            else:
                demand_curves_dist[area_id] = current_district.multiply(h[station],axis=0)
        demand_curves_dist = demand_curves_dist.apply(lambda x: x/x.sum())
        demand_curves_dist = demand_curves_dist.transpose()

        district_station.drop('Station',axis =1,inplace=True)
        district_station = district_station.groupby(district_station.index).sum()

        heat_demand_profile_dist = pd.merge(demand_curves_dist,district_station[['demand']],
                             how='inner', right_on = demand_curves_dist.index, left_on = district_station.index)

        heat_demand_profile_dist.rename(columns={'key_0': 'area_id'},inplace=True)
        heat_demand_profile_dist.set_index('area_id',inplace =True)
        heat_demand_profile_dist = heat_demand_profile_dist[ heat_demand_profile_dist.columns[:-1]].multiply( heat_demand_profile_dist.demand,axis=0)


        ##mv_grid aggregation
        mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
        
        mv_grid = mv_grid.set_index('zensus_population_id')
        district_heating =district_heating.set_index('zensus_population_id')
        mv_grid_ind = mv_grid.loc[mv_grid.index.difference(district_heating.index),:]
        mv_grid_ind = mv_grid_ind.reset_index()
        
        district_grid =pd.merge(mv_grid_ind[['subst_id','zensus_population_id']],
                      annual_demand[['zensus_population_id','Station','demand']],
                      on='zensus_population_id', how ='inner')
        
        district_grid.sort_values('subst_id',inplace=True)
        district_grid.drop('zensus_population_id',axis=1, inplace =True)
        district_grid=district_grid.groupby(['subst_id','Station']).sum()
        district_grid.reset_index('Station', inplace =True)

        demand_curves_mv = pd.DataFrame()
        for j in range(len(heat_profile_idp.columns)):
            current_district = heat_profile_idp.iloc[:,j]
            subst_id = heat_profile_idp.columns[j]
            station =  district_grid[ district_grid.index == subst_id]['Station'][subst_id]
            ##some area id cover two different stations
            if type(station)!=str:
                station = station.reset_index()
                multiple_stations = pd.DataFrame()
                for i in station.index:
                    current_station = station.Station[i]
                    multiple_stations[i] = current_district.multiply(h[current_station],axis=0)
                multiple_stations = multiple_stations.sum(axis=1)
                demand_curves_mv[subst_id] = multiple_stations
            else:
                demand_curves_mv[subst_id] = current_district.multiply(h[station],axis=0)
        demand_curves_mv = demand_curves_mv.apply(lambda x: x/x.sum())
        demand_curves_mv = demand_curves_mv.transpose()
        
        district_grid.drop('Station',axis =1,inplace=True)
        district_grid = district_grid.groupby(district_grid.index).sum()

        heat_demand_profile_mv = pd.merge(demand_curves_mv,district_grid[['demand']],
                             how='inner', right_on = demand_curves_mv.index, left_on = district_grid.index)

        heat_demand_profile_mv.rename(columns={'key_0': 'subst_id'},inplace=True)
        heat_demand_profile_mv.set_index('subst_id',inplace =True)
        heat_demand_profile_mv =  heat_demand_profile_mv[heat_demand_profile_mv.columns[:-1]].multiply(heat_demand_profile_mv.demand,axis=0)

        heat_demand_profile_zensus = 0
    else:
        heat_demand_profile_dist = 0
        heat_demand_profile_mv = 0
        heat_demand_profile_zensus = pd.DataFrame()
        for j in h.columns:
            station = j
            current_zensus = annual_demand[annual_demand.Station == station]
            heat_profile_station = pd.merge(current_zensus['zensus_population_id'],heat_profile_idp.transpose(),
                                           left_on ='zensus_population_id',
                                           right_on = heat_profile_idp.transpose().index, how= 'inner')
            heat_profile_station = heat_profile_station.set_index('zensus_population_id')
        
            heat_profile_station = heat_profile_station.multiply(h[station],axis=1)
            heat_profile_station = heat_profile_station.transpose()
            heat_profile_station =  heat_profile_station.apply(lambda x: x/x.sum())
            heat_profile_station = heat_profile_station.transpose()
            heat_demand_profile_zensus = pd.concat([heat_demand_profile_zensus, heat_profile_station],axis=1)

        heat_demand_profile_zensus.reset_index(inplace=True)
        
        heat_demand_profile_zensus = pd.merge(heat_demand_profile_zensus,annual_demand[['zensus_population_id','demand']],
                             how='inner', on = 'zensus_population_id') 

        heat_demand_profile_zensus.set_index('zensus_population_id',inplace =True)
        heat_demand_profile_zensus =  heat_demand_profile_zensus[heat_demand_profile_zensus.columns[:-1]].multiply(heat_demand_profile_zensus.demand,axis=0)
    #heat_demand_profile = heat_demand_profile.transpose()

    return heat_demand_profile_dist, heat_demand_profile_mv, heat_demand_profile_zensus



###assigning the nuts3 profile per zensus cell and aggregatint he cells per aggregation level
def cts_demand_per_aggregation_level(aggregation_level):
    ## dataframe interlinking the zensus_population_id to the nuts3 level
    nuts_zensus = psycop_gdf_AF('boundaries.egon_map_zensus_vg250', geom_column = 'zensus_geom')
    nuts_zensus.drop('zensus_geom', axis=1, inplace=True)

    ##extracting the demand data for the service sector and conncting it to the nuts3 level
    demand = psycop_df_AF('demand.egon_peta_heat')
    demand = demand[(demand['sector']=='service') & (demand['scenario']=='eGon2035')]
    demand.drop(demand.columns.difference(['demand', 'zensus_population_id']),axis=1,inplace=True)
    demand.sort_values('zensus_population_id',inplace=True)

    demand_nuts = pd.merge(demand, nuts_zensus, how='left', on = 'zensus_population_id')
    
    mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
    demand_nuts_grid = pd.merge(demand_nuts, mv_grid, how='left', on = 'zensus_population_id')
    
    ###CTS secotor NUTS§ level temperature profile
    if os.path.isfile('CTS_heat_demand_profile_nuts3.csv'):
        df_CTS_gas_2011 = pd.read_csv('CTS_heat_demand_profile_nuts3.csv',index_col=0)
        df_CTS_gas_2011.columns.name = 'ags_lk'
        df_CTS_gas_2011.index = pd.to_datetime(df_CTS_gas_2011.index)
        df_CTS_gas_2011 =  df_CTS_gas_2011.asfreq('H')
    else:
        df_CTS_gas_2011 = temporal.disagg_temporal_gas_CTS(use_nuts3code=True, year=2011)
        df_CTS_gas_2011.to_csv('CTS_heat_demand_profile_nuts3.csv')
           
    ##df linking ags_lk and natcode_nuts_3 ### this file is available in the nextcloud link
    ags_lk = pd.read_csv(os.path.join(os.getcwd(),'t_nuts3_lk.csv'),index_col =0)
    ags_lk =ags_lk.drop(ags_lk.columns.difference(['natcode_nuts3', 'ags_lk']),axis=1)

    ##replacing the ags_lk with natcode_nuts3
    CTS_profile = df_CTS_gas_2011.transpose()
    CTS_profile.reset_index(inplace=True)
    CTS_profile.ags_lk=CTS_profile.ags_lk.astype(int)
    CTS_profile=pd.merge(CTS_profile,ags_lk,on='ags_lk',how='inner')
    CTS_profile.set_index('natcode_nuts3',inplace=True)
    CTS_profile.drop('ags_lk', axis=1, inplace =True)

    CTS_per_zensus = pd.merge(demand_nuts[['zensus_population_id','vg250_nuts3']],CTS_profile,
                              left_on='vg250_nuts3', right_on=CTS_profile.index,
                              how='left')
    
    CTS_per_zensus = CTS_per_zensus.drop('vg250_nuts3',axis=1)


    if aggregation_level == 'district':
        district_heating = psycop_df_AF('demand.map_zensus_district_heating_areas')
        district_heating = district_heating[district_heating.scenario == 'eGon2035']
        
        CTS_per_district = pd.merge(CTS_per_zensus,district_heating[['area_id','zensus_population_id']],
                                     on='zensus_population_id',
                                     how='inner')
        CTS_per_district.set_index('area_id',inplace=True)
        CTS_per_district.drop('zensus_population_id',axis=1,inplace=True)
        
        
        CTS_per_district = CTS_per_district.groupby(lambda x:x, axis=0).sum()
        CTS_per_district = CTS_per_district.transpose()
        CTS_per_district = CTS_per_district.apply(lambda x: x/x.sum())
        CTS_per_district.columns.name = 'area_id'
        CTS_per_district.reset_index(drop=True,inplace=True)
                
        #mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
        
        mv_grid = mv_grid.set_index('zensus_population_id')
        district_heating =district_heating.set_index('zensus_population_id')
    
        mv_grid_ind = mv_grid.loc[mv_grid.index.difference(district_heating.index),:]
        mv_grid_ind = mv_grid_ind.reset_index()
        
        
        CTS_per_grid = pd.merge(CTS_per_zensus,mv_grid_ind[['subst_id','zensus_population_id']],
                                     on='zensus_population_id',
                                     how='inner') 
        CTS_per_grid.set_index('subst_id',inplace=True)
        CTS_per_grid.drop('zensus_population_id',axis=1,inplace=True)
        
        CTS_per_grid = CTS_per_grid.groupby(lambda x:x, axis=0).sum()
        CTS_per_grid = CTS_per_grid.transpose()
        CTS_per_grid = CTS_per_grid.apply(lambda x: x/x.sum())
        CTS_per_grid.columns.name = 'subst_id'
        CTS_per_grid.reset_index(drop=True,inplace=True)
        
        CTS_per_zensus = pd.DataFrame()
        
    else:
        CTS_per_district = pd.DataFrame()
        CTS_per_grid = pd.DataFrame()
        CTS_per_zensus.set_index('zensus_population_id',inplace=True)

        CTS_per_zensus = CTS_per_zensus.groupby(lambda x:x, axis=0).sum()
        CTS_per_zensus = CTS_per_zensus.transpose()
        CTS_per_zensus = CTS_per_zensus.apply(lambda x: x/x.sum())
        CTS_per_zensus.columns.name = 'zensus_population_id'
        CTS_per_zensus.reset_index(drop=True,inplace=True)

    return CTS_per_district, CTS_per_grid, CTS_per_zensus


def CTS_demand_scale(aggregation_level):
    #CTS_profiles = cts_demand_per_aggregation_level(aggregation_level).transpose()
    
    CTS_per_district, CTS_per_grid, CTS_per_zensus = cts_demand_per_aggregation_level(aggregation_level)
    CTS_per_district = CTS_per_district.transpose()
    CTS_per_grid = CTS_per_grid.transpose()
    CTS_per_zensus = CTS_per_zensus.transpose()
    
    demand = psycop_df_AF('demand.egon_peta_heat')
    demand = demand[(demand['sector']=='service') & (demand['scenario']=='eGon2035')]
    demand.drop(demand.columns.difference(['demand', 'zensus_population_id']),axis=1,inplace=True)
    demand.sort_values('zensus_population_id',inplace=True)

    if aggregation_level =='district':
        district_heating = psycop_df_AF('demand.map_zensus_district_heating_areas')
        district_heating = district_heating[district_heating.scenario == 'eGon2035']

        CTS_demands_district = pd.merge(demand, district_heating[['zensus_population_id','area_id']],
                                   on='zensus_population_id', how = 'inner')
        CTS_demands_district.drop('zensus_population_id', axis =1, inplace =True)
        CTS_demands_district= CTS_demands_district.groupby('area_id').sum()

        CTS_per_district =  pd.merge(CTS_per_district,CTS_demands_district[['demand']],
                         how='inner', right_on =  CTS_per_district.index, left_on = CTS_demands_district.index)

        CTS_per_district = CTS_per_district.rename(columns={'key_0':'area_id'})
        CTS_per_district.set_index('area_id', inplace =True)
        
        CTS_per_district =  CTS_per_district[CTS_per_district.columns[:-1]].multiply(CTS_per_district.demand,axis=0)
        
        ##on mv grid level
        mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
        mv_grid = mv_grid.set_index('zensus_population_id')
        district_heating =district_heating.set_index('zensus_population_id')
        mv_grid_ind = mv_grid.loc[mv_grid.index.difference(district_heating.index),:]
        mv_grid_ind = mv_grid_ind.reset_index()
        
        CTS_demands_grid = pd.merge(demand,mv_grid_ind[['subst_id','zensus_population_id']],
                                     on='zensus_population_id',
                                     how='inner') 
        
        CTS_demands_grid.drop('zensus_population_id', axis =1, inplace =True)
        CTS_demands_grid = CTS_demands_grid.groupby('subst_id').sum()
        
        CTS_per_grid =  pd.merge(CTS_per_grid,CTS_demands_grid[['demand']],
                         how='inner', right_on =  CTS_per_grid.index, left_on = CTS_demands_grid.index)

        CTS_per_grid =  CTS_per_grid.rename(columns={'key_0':'subst_id'})
        CTS_per_grid.set_index('subst_id', inplace =True)
        
        CTS_per_grid =   CTS_per_grid[ CTS_per_grid.columns[:-1]].multiply( CTS_per_grid.demand,axis=0)
        
        CTS_per_zensus = 0

    else:
        CTS_per_district = 0
        CTS_per_grid = 0
        
        CTS_per_zensus =  pd.merge(CTS_per_zensus,demand, how='inner',
                                 right_on = CTS_per_zensus.index, left_on = demand.zensus_population_id)
        CTS_per_zensus = CTS_per_zensus.drop('key_0',axis=1)
        CTS_per_zensus.set_index('zensus_population_id',inplace =True)

        CTS_per_zensus =  CTS_per_zensus[CTS_per_zensus.columns[:-1]].multiply(CTS_per_zensus.demand,axis=0)


    return CTS_per_district, CTS_per_grid,  CTS_per_zensus


def demand_profile_generator(aggregation_level = 'district'):
    print('****test***************************************test********')
    residential_demand_dist, residential_demand_grid, residential_demand_zensus = residential_demand_scale(aggregation_level)
    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(aggregation_level)

    
    if aggregation_level =='district':
        total_demands_dist = pd.concat([residential_demand_dist,CTS_demand_dist])
        total_demands_dist.sort_index(inplace=True)
        total_demands_dist = total_demands_dist.groupby(lambda x:x, axis=0).sum()
        total_demands_dist.index.name = 'area_id'
        
        final_heat_profiles_dist = pd.DataFrame(index= total_demands_dist.index)
        final_heat_profiles_dist['dist_aggregated_mw'] = total_demands_dist.values.tolist()
        final_heat_profiles_dist.to_sql('egon_heat_time_series_dist',con=db.engine(),schema='demand' ,
                                    if_exists ='replace',index=True, dtype=ARRAY(Float()))
        
        total_demands_grid = pd.concat([residential_demand_grid,CTS_demand_grid])
        total_demands_grid.sort_index(inplace=True)
        total_demands_grid = total_demands_grid.groupby(lambda x:x, axis=0).sum()
        total_demands_grid.index.name = 'subst_id'
        
        final_heat_profiles_grid = pd.DataFrame(index= total_demands_grid.index)
        final_heat_profiles_grid['grid_aggregated_mw'] = total_demands_grid.values.tolist()
        final_heat_profiles_grid.to_sql('egon_heat_time_series_grid',con=db.engine(),schema='demand' ,
                                    if_exists ='replace',index=True, dtype=ARRAY(Float()))
    else:
        total_demands_zensus = pd.concat([residential_demand_zensus,CTS_demand_zensus])
        total_demands_zensus.sort_index(inplace=True)
        total_demands_zensus = total_demands_zensus.groupby(lambda x:x, axis=0).sum()
        
        final_heat_profiles_zensus = pd.DataFrame(index= total_demands_dist.index)
        final_heat_profiles_zensus['zensus_aggregated_mw'] = total_demands_zensus.values.tolist()
        final_heat_profiles_zensus.to_sql('egon_heat_time_series_zensus',con=db.engine(),schema='demand' ,
                                    if_exists ='replace',index=True, dtype=ARRAY(Float()))

    return None

##call function
# heat_idp_pool = PythonOperator(
#         task_id="heat_idp_pool",
#         python_callable=db_porcessing4.demand_profile_generator,
#     )




