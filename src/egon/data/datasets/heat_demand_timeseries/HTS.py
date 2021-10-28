import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as sqlio
import geopandas as gpd
import matplotlib.pyplot as plt
import os
import glob

from egon.data import db, subprocess

import xarray as xr
from sqlalchemy import Column, String, Float, Integer, ForeignKey, ARRAY
import egon.data.datasets.era5 as era

from sqlalchemy.ext.declarative import declarative_base

try:
    from disaggregator import config, data, spatial, temporal, plot
except:
    print(
         "Could not import disaggregator. "
        "Please run task 'demandregio-installation'")

from math import ceil


from egon.data.datasets import Dataset


class IdpProfiles:
    def __init__(self,df_index, **kwargs):
        index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

        self.df = pd.DataFrame(index = df_index)

        self.temperature = kwargs.get('temperature')

   
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

def temperature_profile_extract():
    '''
    Description: Extract temperature data from atlite
    Returns
    -------
    temperature_profile : pandas.DataFrame
        Temperatur profile of all TRY Climate Zones 2011

    '''

    cutout = era.import_cutout(boundary = 'Europe')
    coordinates_path = os.path.join(os.getcwd(),'data_bundle_egon_data','climate_zones_Germany',
                                    'TRY_Climate_Zone')
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
        index=selected_weather_cells.index).to_pandas()

    return temperature_profile

def temp_interval():
    '''
    Description: Create Dataframe with temperature data for TRY Climate Zones
    Returns
    -------
    temperature_interval : pandas.DataFrame
        Hourly temperature intrerval of all 15 TRY Climate station#s temperature profile

    '''
    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')
    temperature_interval = pd.DataFrame()
    temp_profile = temperature_profile_extract()

    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(index, temperature=temp_profile[temp_profile.columns[x]]).get_temperature_interval(how='geometric_series')
        temperature_interval[name_station]=idp_this_station['temperature_interval']

    return temperature_interval


def idp_pool_generator():
    '''
   Description: Create List of Dataframes for each temperature class for each household stock
        
    Returns
    -------
    TYPE list
        List of dataframes with each element representing a dataframe 
        for every combination of household stock and temperature class 

    '''
    path = os.path.join(os.getcwd(),'data_bundle_egon_data',
                        'household_heat_demand_profiles','household_heat_demand_profiles.hdf5')
    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

    sfh = pd.read_hdf(path, key ='SFH')
    mfh = pd.read_hdf(path, key ='MFH')
    temp = pd.read_hdf(path, key ='temperature')


    globals()['luebeck_sfh'] = sfh[sfh.filter(like='Luebeck').columns]
    globals()['luebeck_mfh'] = mfh[mfh.filter(like='Luebeck').columns]

    globals()['kassel_sfh'] = sfh[sfh.filter(like='Kassel').columns]
    globals()['kassel_mfh'] = mfh[mfh.filter(like='Kassel').columns]

    globals()['wuerzburg_sfh'] = sfh[sfh.filter(like='Wuerzburg').columns]
    globals()['wuerzburg_mfh'] = mfh[mfh.filter(like='Wuerzburg').columns]

    globals()['chemnitz_sfh'] = sfh[sfh.filter(like='Chemnitz').columns]
    globals()['chemnitz_mfh'] = mfh[mfh.filter(like='Chemnitz').columns]

    temp_daily = pd.DataFrame()
    for column in temp.columns:
        temp_current= temp[column].resample('D').mean().reindex(temp.index).fillna(method='ffill').fillna(method='bfill')
        temp_current_geom = temp_current
        temp_daily=pd.concat([temp_daily,temp_current_geom], axis=1)

    def round_temperature(station):
        '''
        Description: Create dataframe to assign temperature class to each day of TRY climate zones 

        Parameters
        ----------
        station : str
            Name of the location

        Returns
        -------
        temp_class : pandas.DataFrame
            Each day assignd to their respective temperature class

        '''
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
        temp_class = pd.DataFrame.from_dict(temp_class_dic)
        return temp_class

    temp_class_luebeck = round_temperature('Luebeck')
    temp_class_kassel = round_temperature('Kassel')
    temp_class_wuerzburg = round_temperature('Wuerzburg')
    temp_class_chemnitz = round_temperature('Chemnitz')
    temp_class =pd.concat([temp_class_luebeck, temp_class_kassel,temp_class_wuerzburg,temp_class_chemnitz],axis=1)
    temp_class.set_index(index, inplace=True)

    def unique_classes(station):
        '''

        Returns
        -------
        classes : list
            Collection of temperature classes for each location

        '''
        classes=[]
        for x in temp_class[f'Class_{station}']:
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


    def splitter(station,household_stock):
        '''
        

        Parameters
        ----------
        station : str
            Name of the location
        household_stock : str
            SFH or MFH

        Returns
        -------
        None.

        '''
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
               
    splitter('Luebeck','SFH')
    splitter('Kassel','SFH')
    splitter('Wuerzburg','SFH')
    splitter('Chemnitz','SFH')
    splitter('Luebeck','MFH')
    splitter('Kassel','MFH')
    splitter('Chemnitz','MFH')

    def pool_normalize(x):
        '''
        

        Parameters
        ----------
        x : pandas.Series
            24-hour profiles of IDP pool

        Returns
        -------
        TYPE : pandas.Series
            Normalized to their daily total

        '''
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
             globals()[f'idp_collection_class_{m}_{s}_norm']=df_name.apply(pool_normalize)

    return [idp_collection_class_2_SFH_norm,idp_collection_class_3_SFH_norm,idp_collection_class_4_SFH_norm,
            idp_collection_class_5_SFH_norm,idp_collection_class_6_SFH_norm,idp_collection_class_7_SFH_norm,
            idp_collection_class_8_SFH_norm,idp_collection_class_9_SFH_norm,idp_collection_class_10_SFH_norm,
            idp_collection_class_2_MFH_norm,idp_collection_class_3_MFH_norm,idp_collection_class_4_MFH_norm,
            idp_collection_class_5_MFH_norm, idp_collection_class_6_MFH_norm,idp_collection_class_7_MFH_norm,
            idp_collection_class_8_MFH_norm,idp_collection_class_9_MFH_norm,idp_collection_class_10_MFH_norm]


def idp_df_generator():
    '''
    Description: Create dataframe with all temprature classes, 24hr. profiles and household stock

    Returns
    -------
    idp_df : pandas.DataFrame
        All IDP pool as classified as per household stock and temperature class

    '''
    idp_list = idp_pool_generator()
    stock=['MFH','SFH']
    class_list=[2,3,4,5,6,7,8,9,10]
    idp_df = pd.DataFrame(columns=['idp', 'house', 'temperature_class'])
    for s in stock:
        for m in class_list:
           
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

    idp_df.to_sql('heat_idp_pool',con=db.engine(),schema='demand' ,if_exists ='replace', index=True,
                      dtype = {'index': Integer(),
                                'idp':ARRAY(Float()),
                                'house': String(),
                                'temperature_class':Integer()})

    idp_df['idp'] =idp_df.idp.apply(lambda x: np.array(x))

    idp_df.idp = idp_df.idp.apply(lambda x:  x.astype(np.float32))


    return idp_df


def psycop_df_AF(table_name):
    '''
    Description: Read tables from database into pandas dataframe
    Parameters
    ----------
    table_name : str
        Name of the database table

    Returns
    -------
    data : pandas.DataFrame
        Imported database tables
        

    '''
    conn = db.engine()
    sql = "SELECT * FROM {}".format(table_name)
    data = sqlio.read_sql_query(sql, conn)
    conn = None
    return data

def psycop_gdf_AF(table_name,geom_column = 'geom'):
    '''
    
    Description: Read tables from database into geopandas dataframe
    Parameters
    ----------
    table_name : str
        Name of the database table
    geom_column : str, optional
        Column name with geometry. The default is 'geom'.

    Returns
    -------
    data : Tandas.DataFrame
        Imported database tables

    '''
    conn = db.engine()
    sql = "SELECT * FROM {}".format(table_name)
    data = gpd.read_postgis(sql, conn, geom_column)
    conn = None
    return data


def annual_demand_generator():
    '''
    
    Description: Create dataframe with annual demand and household count for each zensus cell
    Returns
    -------
    demand_count: pandas.DataFrame
        Annual demand of all zensus cell with MFH and SFH count and 
        respective associated Station

    '''
    
    demand = psycop_df_AF('demand.egon_peta_heat')
    a_pha = psycop_df_AF('society.egon_destatis_zensus_apartment_per_ha')
    b_pha = psycop_df_AF('society.egon_destatis_zensus_building_per_ha')
    h_pha = psycop_df_AF('society.egon_destatis_zensus_household_per_ha')

    zp_pha = psycop_gdf_AF('society.destatis_zensus_population_per_ha')

    
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

    temperature_zones = gpd.read_file(os.path.join(os.getcwd(),'data_bundle_egon_data',
                                                   'climate_zones_Germany','TRY_Climate_Zone',
                                                   'Climate_Zone.shp'))
    temperature_zones.sort_values('Zone', inplace=True)
    temperature_zones.reset_index(inplace=True)
    temperature_zones.drop(columns=['index', 'Id'], inplace=True, axis=0)


    demand_zone = gpd.sjoin(demand_geom, temperature_zones,
                            how='inner', op='intersects')

    demand_zone.drop_duplicates(['grid_id'], inplace=True)

    sfh_chartext = ['Freistehendes Einfamilienhaus', 'Einfamilienhaus: Doppelhaushälfte', 'Einfamilienhaus: Reihenhaus',
                    'Freistehendes Zweifamilienhaus', 'Zweifamilienhaus: Doppelhaushälfte', 'Zweifamilienhaus: Reihenhaus']
    mfh_chartext = ['Mehrfamilienhaus: 3-6 Wohnungen', 'Mehrfamilienhaus: 7-12 Wohnungen',
                    'Mehrfamilienhaus: 13 und mehr Wohnungen', 'Anderer Gebäudetyp']

    bg_pha = b_pha[b_pha['attribute'] == 'GEBTYPGROESSE']

    def household_stock(x):
        '''
        

        Parameters
        ----------
        x : str
            household characteristics

        Returns
        -------
        output : str
            Categorized to either SFH or MFH

        '''
        if x in sfh_chartext:
            output = 'SFH'
        if x in mfh_chartext:
            output = 'MFH'
        return output
    bg_pha['Household Stock'] = bg_pha['characteristics_text'].apply(
        household_stock)
   
    house_count = bg_pha[['zensus_population_id', 'quantity', 'Household Stock']]
    house_count = house_count.groupby(
        ['zensus_population_id', 'Household Stock']).sum('quantity')
    house_count = house_count.reset_index()
    house_count = house_count.pivot_table(
        values='quantity', index='zensus_population_id', columns='Household Stock')
    house_count = house_count.fillna(0)

    demand_count = pd.merge(demand_zone, house_count, how='inner', on='zensus_population_id')

    demand_count.drop('index_right', axis=1, inplace=True)
    

    demand_count.drop(demand_count.columns.difference(
        ['zensus_population_id', 'demand', 'SFH', 'MFH','Station']), axis=1, inplace=True)

    return demand_count


def profile_selector():
    '''
    
    Description: Random assignment of profiles to each day based on their temeprature class 
    and household stock count
    
    Returns
    -------
    annual_demand : pandas.DataFrame
        Annual demand of all zensus cell with MFH and SFH count and 
        respective associated Station
    
    idp_df : pandas.DataFrame
        All IDP pool as classified as per household stock and temperature class
    
    selected_idp_names: pandas.DataFrame
        Each cell of the table assigned with the column number (int) with the value 
        corresponding to the idp_df row number. This indicates the assignmnet of 
        24 hr. array to the day. 

    '''
    idp_df = idp_df_generator()
    annual_demand = annual_demand_generator()
    all_temperature_interval = temp_interval()
    all_temperature_interval = all_temperature_interval.resample('D').max()
    all_temperature_interval.reset_index(drop=True, inplace=True)
    
        
    all_temperature_interval = all_temperature_interval.loc[:,annual_demand.Station.unique()]


    Temperature_interval = all_temperature_interval.transpose()

    
    np.random.seed(0)
    if  os.path.isfile('selected_profiles.csv'):
        os.remove('selected_profiles.csv')

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
        selected_this_station = selected_idp_names.iloc[length:new_length,:] 
        selected_this_station['selected_idp']=selected_this_station.values.tolist()
        selected_this_station = selected_this_station.selected_idp
        selected_this_station = selected_this_station.reset_index()
        selected_this_station.index=range(length,new_length)
        
        
        if  os.path.isfile('selected_profiles.csv'):
            selected_this_station.to_csv('selected_profiles.csv', mode ='a', header = False)
        else:
            selected_this_station.to_csv('selected_profiles.csv')
        
        length = new_length
    
    Base = declarative_base()
    class EgonHeatTimeseries(Base):
        __tablename__ = 'heat_timeseries_selected_profiles'
        __table_args__ = {'schema': 'demand'}
        ID = Column(Integer, primary_key=True)
        zensus_population_id = Column(Integer, primary_key=True)
        selected_idp_profiles = Column(String)
            
    engine = db.engine()
    EgonHeatTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonHeatTimeseries.__table__.create(bind=engine, checkfirst=True)  
    
    
    heat_selected_profiles = {'schema':'demand', 'table':'heat_timeseries_selected_profiles'}
    
    filename_insert = 'selected_profiles.csv'

    docker_db_config = db.credentials()
    
    selected_profiles_table = (
       f"{heat_selected_profiles['schema']}"
       f".{heat_selected_profiles['table']}"
       )

    host = ["-h", f"{docker_db_config['HOST']}"]
    port = ["-p", f"{docker_db_config['PORT']}"]
    pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
    user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
    command = [
        "-c",
        rf"\copy {selected_profiles_table}"
        rf" FROM '{filename_insert}' DELIMITER ',' CSV HEADER;",
    ]
    
    
    subprocess.run(["psql"] + host + port + pgdb + user + command,
                   env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
                   )
        
    os.remove(filename_insert)
    
    return annual_demand, idp_df, selected_idp_names


def h_value():
    '''
    Description: Assignment of daily demand scaling factor to each day of all TRY Climate Zones

    Returns
    -------
    h : pandas.DataFrame
        Hourly factor values for each station corresponding to the temperature profile. 
        Extracted from demandlib.

    '''
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

    h= (a / (1 + (b / (temp_profile_geom - 40)) ** c) + d)

    return h


def profile_generator(aggregation_level):
    '''
    
    Descriptiion: Aggregation of profiles either based on district heating network and medium voltage 
    grid or zensus cell
    
    Parameters
    ----------
    aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    annual_demand : pandas.DataFrame
        Annual demand of all zensus cell with MFH and SFH count and 
        respective associated Station
    
    heat_profile_dist : pandas.DataFrame
        if aggreation_level = 'district'
            heat profiles for every distric heating id
        else
            0
    heat_profile_idp : pandas.DataFrame
        if aggreation_level = 'district'
            heat profiles for every mv grid subst_id
        else
            heat profiles for every zensus_poppulation_id
    '''
    annual_demand, idp_df, selected_profiles = profile_selector()
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
        heat_profile_idp[i] = col[i].values
        y.rename(columns={i:'index'},inplace=True)
        
    if aggregation_level == 'district':
        district_heating = psycop_df_AF('demand.egon_map_zensus_district_heating_areas')
        district_heating = district_heating[district_heating.scenario == 'eGon2035']
        
        heat_profile_dist = pd.merge(heat_profile_idp,
                                      district_heating[['area_id','zensus_population_id']]
                                      , on='zensus_population_id', how = 'inner' )
        heat_profile_dist.sort_values('area_id',inplace=True)
        heat_profile_dist.set_index('area_id',inplace=True)
        heat_profile_dist.drop('zensus_population_id',axis=1,inplace=True)
    
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
    

    return annual_demand, heat_profile_dist, heat_profile_idp


def residential_demand_scale(aggregation_level):
    '''
    
    Description: Scaling the demand curves to the annual demand of the respective aggregation level

    Parameters
    ----------
    aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    heat_demand_profile_dist : pandas.DataFrame
        if aggregation ='district'
            final demand profiles per district heating netowrk id
        else
            0
    heat_demand_profile_mv : pandas.DataFrame
        if aggregation ='district'
            final demand profiles per mv grid subst_id
        else
            0
    heat_demand_profile_zensus : pandas.DataFrame
        if aggregation ='district'
            0
        else
            final demand profiles per zensus_population_id

    '''
    annual_demand, heat_profile_dist, heat_profile_idp = profile_generator(aggregation_level)

    h = h_value()
    h= h.reset_index(drop=True)

    if aggregation_level == 'district':
        
        district_heating = psycop_df_AF('demand.egon_map_zensus_district_heating_areas')
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

    return heat_demand_profile_dist, heat_demand_profile_mv, heat_demand_profile_zensus



def cts_demand_per_aggregation_level(aggregation_level):
    '''
    
    Description: Create dataframe assigining the CTS demand curve to individual zensus cell
    based on their respective NUTS3 CTS curve

    Parameters
    ----------
    aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual 
            zensu cells and aggregated per district heat area id
        else
            empty dataframe
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual 
            zensu cells and aggregated per mv grid subst id
        else
            empty dataframe
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            empty dataframe
        else
            NUTS3 CTS profiles assigned to individual 
            zensu population id

    '''

    nuts_zensus = psycop_gdf_AF('boundaries.egon_map_zensus_vg250', geom_column = 'zensus_geom')
    nuts_zensus.drop('zensus_geom', axis=1, inplace=True)

    demand = psycop_df_AF('demand.egon_peta_heat')
    demand = demand[(demand['sector']=='service') & (demand['scenario']=='eGon2035')]
    demand.drop(demand.columns.difference(['demand', 'zensus_population_id']),axis=1,inplace=True)
    demand.sort_values('zensus_population_id',inplace=True)

    demand_nuts = pd.merge(demand, nuts_zensus, how='left', on = 'zensus_population_id')
    
    mv_grid = psycop_df_AF('boundaries.egon_map_zensus_grid_districts')
    
    if os.path.isfile('CTS_heat_demand_profile_nuts3.csv'):
        df_CTS_gas_2011 = pd.read_csv('CTS_heat_demand_profile_nuts3.csv',index_col=0)
        df_CTS_gas_2011.columns.name = 'ags_lk'
        df_CTS_gas_2011.index = pd.to_datetime(df_CTS_gas_2011.index)
        df_CTS_gas_2011 =  df_CTS_gas_2011.asfreq('H')
    else:
        df_CTS_gas_2011 = temporal.disagg_temporal_gas_CTS(use_nuts3code=True, year=2011)
        df_CTS_gas_2011.to_csv('CTS_heat_demand_profile_nuts3.csv')
           
    ags_lk = pd.read_csv(os.path.join(os.getcwd(),'demandregio-disaggregator/disaggregator/disaggregator/data_in/regional',
                                      't_nuts3_lk.csv'),index_col =0)
    ags_lk =ags_lk.drop(ags_lk.columns.difference(['natcode_nuts3', 'ags_lk']),axis=1)

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
        district_heating = psycop_df_AF('demand.egon_map_zensus_district_heating_areas')
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
    '''
    
    Description: caling the demand curves to the annual demand of the respective aggregation level
    

    Parameters
    ----------
    aggregation_level : str
        aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demand
        else
            0
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demandd
        else
            0
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            0
        else
           Profiles scaled up to annual demand

    '''
    
    CTS_per_district, CTS_per_grid, CTS_per_zensus = cts_demand_per_aggregation_level(aggregation_level)
    CTS_per_district = CTS_per_district.transpose()
    CTS_per_grid = CTS_per_grid.transpose()
    CTS_per_zensus = CTS_per_zensus.transpose()
    
    demand = psycop_df_AF('demand.egon_peta_heat')
    demand = demand[(demand['sector']=='service') & (demand['scenario']=='eGon2035')]
    demand.drop(demand.columns.difference(['demand', 'zensus_population_id']),axis=1,inplace=True)
    demand.sort_values('zensus_population_id',inplace=True)

    if aggregation_level =='district':
        district_heating = psycop_df_AF('demand.egon_map_zensus_district_heating_areas')
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
    '''
    
    Description: Creating final demand profiles

    Parameters
    ----------
    aggregation_level : str, optional
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'. The default is 'district'.

    Returns
    -------
    None.

    '''
    
    residential_demand_dist, residential_demand_grid, residential_demand_zensus = residential_demand_scale(aggregation_level)
    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(aggregation_level)

    
    if aggregation_level =='district':
        total_demands_dist = pd.concat([residential_demand_dist,CTS_demand_dist])
        total_demands_dist.sort_index(inplace=True)
        total_demands_dist = total_demands_dist.groupby(lambda x:x, axis=0).sum()
        total_demands_dist.index.name = 'area_id'
        
        final_heat_profiles_dist = pd.DataFrame(index= total_demands_dist.index)
        final_heat_profiles_dist['dist_aggregated_mw'] = total_demands_dist.values.tolist()
        final_heat_profiles_dist.to_sql('egon_timeseries_district_heating',con=db.engine(),schema='demand' ,
                                    if_exists ='replace',index=True, dtype=ARRAY(Float()))
        
        total_demands_grid = pd.concat([residential_demand_grid,CTS_demand_grid])
        total_demands_grid.sort_index(inplace=True)
        total_demands_grid = total_demands_grid.groupby(lambda x:x, axis=0).sum()
        total_demands_grid.index.name = 'subst_id'
        
        final_heat_profiles_grid = pd.DataFrame(index= total_demands_grid.index)
        final_heat_profiles_grid['grid_aggregated_mw'] = total_demands_grid.values.tolist()
        final_heat_profiles_grid.to_sql('egon_etrago_timeseries_individual_heating',con=db.engine(),schema='demand' ,
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




class HeatTimeSeries(Dataset):
     def __init__(self, dependencies):
        super().__init__(
            name="HeatTimeSeries",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(demand_profile_generator),
        )

