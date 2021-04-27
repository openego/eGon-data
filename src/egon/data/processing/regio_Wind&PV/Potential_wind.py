from egon.data import db
import geopandas as gpd
import pandas as pd
import psycopg2
import numpy as np
#import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, LineString, Point, MultiPoint


def wind_power_parks(target_power):
    # Due to errors in some inputs, some areas of existing wind farms
    # should be discarded using perimeter and area filters
    def filter_current_wf(wf_geometry):
        if wf_geometry.geom_type == 'Point':
            return True
        if wf_geometry.geom_type == 'Polygon':
            area = wf_geometry.area
            length = wf_geometry.length
            return ((area < 40000000) & (length < 40000)) #Based on the biggest (# of WT) wind farm
        if wf_geometry.geom_type == 'LineString':
            length = wf_geometry.length
            return (length < 1008) # 8 * rotor diameter (8*126m)
    
    # The function 'wf' returns the connexion point of a wind turbine
    def wf(x):    
        try:
            return(map_ap_wea[x])
        except:
            return(np.nan)
        
    #Connect to the data base
    con = db.engine()
    sql = "SELECT geom FROM supply.egon_re_potential_area_wind"
    # wf_areas has all the potential areas geometries for wind farms
    wf_areas = gpd.GeoDataFrame.from_postgis(sql, con)
    sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
    # hvmv_substation has the information about HV transmission lines in Germany
    hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "point")
    hvmv_substation = hvmv_substation.to_crs(3035)
    sql = "SELECT gen, geometry FROM boundaries.vg250_sta"
    # states has the administrative boundaries of Germany
    states = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "geometry")
    
    north = ['Schleswig-Holstein', 'Mecklenburg-Vorpommern', 'Niedersachsen']
    north_states = states[states['gen'].isin(north)].unary_union
    germany_borders = states.unary_union
    
    # bus has the connection points of the wind farms
    bus = pd.read_csv('bus.csv')
    # wea has info of each wind turbine in Germany.
    wea = pd.read_csv('bnetza_mastr_wind_cleaned.csv')
    
    # Delete all the rows without information about geographical location
    wea = wea[(pd.notna(wea['Laengengrad'])) & (pd.notna(wea['Breitengrad']))]
    # Delete all the offshore wind turbines
    wea = wea[wea['Kuestenentfernung'] == 0]
    # the variable map_ap_wea have the conection point of all the available wt
    # in the dataframe bus.
    map_ap_wea = {}
    for i in bus.index:
        for unit in bus['MaStR Nummern der Einheiten'][i].split(', '):
                map_ap_wea[unit] = bus['MaStR Nummer'][i]
    wea['connexion point'] = wea['EinheitMastrNummer'].apply(wf)
    
    # Delete all the offshore wind turbines
    wea = wea[wea['Kuestenentfernung'] == 0]
    # Create the columns 'geometry' which will have location of each WT in a point type
    wea = gpd.GeoDataFrame(wea,
                          geometry=gpd.points_from_xy(wea['Laengengrad'],
                          wea['Breitengrad'],
                          crs = 4326))
    
    ### THIS IS JUST FOR MY OWN VISUALIZATION - DELETE AFTER FINISHING ###
    wea = gpd.clip(gdf= wea, mask= germany_borders)
    ### THIS IS JUST FOR MY OWN VISUALIZATION - DELETE AFTER FINISHING ###
    
    # wf_size storages the number of WT connected to each connection point
    wf_size = wea['connexion point'].value_counts()
    # Delete all the connexion points with less than 3 WT
    wf_size = wf_size[wf_size >= 3]
    # Filter all the WT which are not part of a wind farm of at least 3 WT
    wea = wea[wea['connexion point'].isin(wf_size.index)]
    #current_wfs has all the geometries that represent the existing wind farms
    current_wfs = gpd.GeoDataFrame(index= wf_size.index, crs = 4326, columns = ['geometry'])
    for conn_point, wt_location in wea.groupby('connexion point'):
        current_wfs.at[conn_point, 'geometry'] = MultiPoint(wt_location['geometry'].values).convex_hull
    current_wfs['geometry2'] =  current_wfs['geometry'].to_crs(3035)
    current_wfs['area'] = current_wfs['geometry2'].apply( lambda x: x.area)
    current_wfs['length'] = current_wfs['geometry2'].apply( lambda x: x.length)
    #The 'filter_wts' is used to discard atypical values for the current wind farms
    current_wfs['filter2'] = current_wfs['geometry2'].apply( lambda x: filter_current_wf(x))
    
    # Apply the filter based on area and perimeter
    current_wfs = current_wfs[current_wfs['filter2']]
    current_wfs = current_wfs.drop(columns= ['geometry2', 'filter2'])
    
    wf_areas['area [km²]'] = wf_areas.area/1000000
    
    ################################ TASK 1 ######################################
    # Define if it is necessary to implement the small areas unification
    ################################ TASK 1 ######################################
    """
    # Unify small potential areas:
    wf_areas_big = wf_areas[wf_areas['area [m²]'] >= 1000000].copy()
    wf_areas_small = wf_areas[wf_areas['area [m²]'] < 1000000].copy()
    wf_areas.sort_values(by= 'area [m²]', ascending= False, inplace= True)
    wf_areas_small['geom2'] = wf_areas_small['geom'].buffer(10)
    wf_areas_small['parent'] = 0
    wf_areas_small.set_geometry('geom2', inplace=True)
    area = 1
    wf_areas_small = wf_areas_small.head(500)
    wf_areas_small['inters'] = False
    for i in wf_areas_small.index:
        wf_areas_small['inters'].loc[i:] = wf_areas_small.loc[i:].intersects(wf_areas_small['geom2'].loc[i])
        if wf_areas_small['parent'].loc[i] == 0:
            wf_areas_small['parent'].loc[i] = area
            area+=1
        group = wf_areas_small['parent'].loc[i]
        wf_areas_small['parent'] = wf_areas_small.apply(lambda x: group if (x['inters'] == True and x['parent'] == 0) else x['parent'], axis = 1)
        wf_areas_small['inters'] = False
        
    ###### JUST FOR VISUALIZATION ####
    wf_areas_small['geom2'] = wf_areas_small['geom2'].to_crs(4326)
    wf_areas_small[['geom2','area [m²]', 'parent']].to_file("small_areas.geojson", driver='GeoJSON')
    ###### JUST FOR VISUALIZATION ####
    """
    ################################ TASK 1 ######################################
    # Define if it is necessary to implement the small areas unification
    ################################ TASK 1 ######################################
    
    
    # Exclude areas smaller than X m². X was calculated as the area of
    # 3 WT in the corners of an equilateral triangle with l = 4*rotor_diameter
    min_area = 4 * (0.126**2) * np.sqrt(3) 
    wf_areas = wf_areas[wf_areas['area [km²]'] > min_area]
    
    # find the potential areas that intersects the convex hulls of current wind farms
    wf_areas= wf_areas.to_crs(4326)
    for i in wf_areas.index:
        wf_areas.at[i, 'intersection'] = current_wfs.intersects(wf_areas.at[i, 'geom']).any()
    wf_areas = wf_areas[wf_areas['intersection']]
    wf_areas = wf_areas.drop(columns= ['intersection'])
    
    # Create the centroid of the selected potential areas and assign an installed capacity
    power_north = 21.05 #MW/km²
    power_south = 16.81 #MW/km²
    # Set a maximum installed capacity to limit the power of big potential areas
    max_power_hv = 120 # in MW
    max_power_mv = 20 # in MW
    # Max distance between WF (connected to MV) and nearest HV substation that
    # allows its connexion to HV.
    max_dist_hv = 20000 # in meters
    
    wf_areas['centroid'] = wf_areas.centroid
    wf_areas['north'] = wf_areas['centroid'].within(north_states)
    wf_areas['inst capacity [MW]'] = wf_areas.apply(
        lambda x: power_north*x['area [km²]'] if x['north'] == True else 
        power_south*x['area [km²]'], axis= 1)
     
    # Divide selected areas based on voltage of connexion points
    ##################################################################################
    ### delele after Guido supplies the final version of tables with voltage level ###
    wf_areas['voltage'] = np.random.randint(100, 120, wf_areas['inst capacity [MW]'].size)
    ### delele after Guido supplies the final version of tables with voltage level ###
    ##################################################################################
    
    wf_mv = wf_areas[wf_areas['voltage'] < 110]
    wf_hv = wf_areas[wf_areas['voltage'] >= 110]
    
    # Wind farms connected to MV network will be connected to HV network if the distance
    # to the closest HV transmission line is =< max_dist_hv, and the installed capacity
    # is bigger than max_power_mv
    hvmv_substation['voltage'] = hvmv_substation['voltage'].apply(
        lambda x: int(x.split(';')[0]))
    hv_substations = hvmv_substation[hvmv_substation['voltage'] >= 110000]
    hv_substations = hv_substations.unary_union # join all the hv_substations
    wf_mv['dist_to_HV'] = wf_areas['geom'].to_crs(3035).distance(hv_substations)
    wf_mv_to_hv = wf_mv[(wf_mv['dist_to_HV'] <= max_dist_hv) &
                        (wf_mv['inst capacity [MW]'] >= max_power_mv)]
    wf_mv_to_hv = wf_mv_to_hv.drop(columns= ['dist_to_HV'])    
    
    wf_hv = wf_hv.append(wf_mv_to_hv)
    wf_mv = wf_mv[(wf_mv['dist_to_HV'] > max_dist_hv) |
                        (wf_mv['inst capacity [MW]'] < max_power_mv)]
    wf_mv = wf_mv.drop(columns= ['dist_to_HV'])
        
    wf_hv['inst capacity [MW]']= wf_hv['inst capacity [MW]'].apply(
    lambda x: x if x < max_power_hv else max_power_hv)
    
    wf_mv['inst capacity [MW]'] = wf_mv['inst capacity [MW]'].apply(
        lambda x: x if x < max_power_mv else max_power_mv)
    
    # Adjust the total installed capacity to the scenario
    total_wind_power = wf_hv['inst capacity [MW]'].sum() + wf_mv['inst capacity [MW]'].sum()
    if total_wind_power > target_power:
        scale_factor = target_power/total_wind_power
        wf_mv['inst capacity [MW]'] = wf_mv['inst capacity [MW]'] * scale_factor
        wf_hv['inst capacity [MW]'] = wf_hv['inst capacity [MW]'] * scale_factor
    else:
    ################################ TASK 5 ######################################
    # Define mothodology to build new farms in HV level
    ################################ TASK 5 ######################################
        wf_mv['inst capacity [MW]'] = wf_mv['inst capacity [MW]']
        wf_hv['inst capacity [MW]'] = wf_hv['inst capacity [MW]']
    
        wind_farms= wf_hv.append(wf_mv)
        print(wind_farms['inst capacity [MW]'].sum())
    ################################ TASK 6 ######################################
    # Fill the table in egon-data
    ################################ TASK 6 ######################################

    return wf_hv.append(wf_mv)

wind_farms = wind_power_parks(target_power= 2000)
print(wind_farms['inst capacity [MW]'].sum())






"""
wind_farms[['geom', 'inst capacity [MW]', 'area [km²]']].to_file("Selected_Pot_areas.geojson", driver='GeoJSON')
wind_farms = wind_farms.set_geometry('centroid')
wind_farms[['centroid', 'inst capacity [MW]', 'area [km²]']].to_file("Wind_farms_points.geojson", driver='GeoJSON')
"""

