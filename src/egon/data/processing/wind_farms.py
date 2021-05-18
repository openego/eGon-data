from egon.data import db
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, LineString, Point, MultiPoint


def wind_power_parks():
    con = db.engine()
    sql = "SELECT carrier, capacity, scenario_name FROM supply.egon_scenario_capacities"
    target_power_df = pd.read_sql(sql, con)
    target_power_df = target_power_df[target_power_df['carrier'] == "wind_onshore"]
    for scenario in target_power_df.index:
        target_power = target_power_df.at[scenario, 'capacity']
        scenario_year = target_power_df.at[scenario, 'scenario_name']
        source =  target_power_df.at[scenario, 'carrier']
        wind_farms_scenario(target_power, scenario_year, source)
    return 0


def wind_farms_scenario(target_power, scenario_year, source):
    # Due to typos in some inputs, some areas of existing wind farms
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
    
    # The function 'wind_farm' returns the connection point of a wind turbine
    def wind_farm(x):    
        try:
            return(map_ap_wea_farm[x])
        except:
            return(np.nan)
    
    # The function 'voltage' returns the voltage level a wind turbine operates 
    def voltage(x):    
        try:
            return(map_ap_wea_voltage[x])
        except:
            return(np.nan)
    
    def match_district_se(x):
        for sub in hvmv_substation.index:
            if x['geom'].contains(hvmv_substation.at[sub, 'point']):
                return hvmv_substation.at[sub, 'point']
            
    #Connect to the data base
    con = db.engine()
    sql = "SELECT geom FROM supply.egon_re_potential_area_wind"
    # wf_areas has all the potential areas geometries for wind farms
    wf_areas = gpd.GeoDataFrame.from_postgis(sql, con)
    sql= "SELECT geom FROM grid.mv_grid_districts"
    # mv_districts has geographic info of medium voltage districts in Germany
    mv_districts = gpd.GeoDataFrame.from_postgis(sql, con)
    sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
    # hvmv_substation has the information about HV transmission lines in Germany
    hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "point")
    sql = "SELECT gen, geometry FROM boundaries.vg250_sta"
    # states has the administrative boundaries of Germany
    states = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "geometry")
    north = ['Schleswig-Holstein', 'Mecklenburg-Vorpommern', 'Niedersachsen']
    north_states = states[states['gen'].isin(north)].unary_union
      
    # bus has the connection points of the wind farms
    bus = pd.read_csv('location_elec_generation_raw.csv')
    # Drop all the rows without connection point
    bus.dropna(subset=['NetzanschlusspunktMastrNummer'], inplace= True)
    # wea has info of each wind turbine in Germany.
    wea = pd.read_csv('bnetza_mastr_wind_cleaned.csv')
    
    # Delete all the rows without information about geographical location
    wea = wea[(pd.notna(wea['Laengengrad'])) & (pd.notna(wea['Breitengrad']))]
    # Delete all the offshore wind turbines
    wea = wea[wea['Kuestenentfernung'] == 0]
    # the variable map_ap_wea_farm have the connection point of all the available wt
    # in the dataframe bus.
    map_ap_wea_farm = {}
    map_ap_wea_voltage = {}
    for i in bus.index:
        for unit in bus['MaStRNummer'][i][1:-1].split(', '):
            map_ap_wea_farm[unit[1:-1]] = bus['NetzanschlusspunktMastrNummer'][i]
            map_ap_wea_voltage[unit[1:-1]] = bus['Spannungsebene'][i]
    wea['connection point'] = wea['EinheitMastrNummer'].apply(wind_farm)
    wea['voltage'] = wea['EinheitMastrNummer'].apply(voltage)
    
    # Create the columns 'geometry' which will have location of each WT in a point type
    wea = gpd.GeoDataFrame(wea,
                          geometry=gpd.points_from_xy(wea['Laengengrad'],
                          wea['Breitengrad'],
                          crs = 4326))
    
    
    # wf_size storages the number of WT connected to each connection point
    wf_size = wea['connection point'].value_counts()
    # Delete all the connection points with less than 3 WT
    wf_size = wf_size[wf_size >= 3]
    # Filter all the WT which are not part of a wind farm of at least 3 WT
    wea = wea[wea['connection point'].isin(wf_size.index)]
    #current_wfs has all the geometries that represent the existing wind farms
    current_wfs = gpd.GeoDataFrame(index= wf_size.index,
                                   crs = 4326,
                                   columns = ['geometry', 'voltage'])
    for conn_point, wt_location in wea.groupby('connection point'):
        current_wfs.at[conn_point, 'geometry'] = MultiPoint(wt_location['geometry'].values).convex_hull
        current_wfs.at[conn_point, 'voltage'] = wt_location['voltage'].iat[0]
    current_wfs['geometry2'] =  current_wfs['geometry'].to_crs(3035)
    current_wfs['area'] = current_wfs['geometry2'].apply( lambda x: x.area)
    current_wfs['length'] = current_wfs['geometry2'].apply( lambda x: x.length)
    #The 'filter_wts' is used to discard atypical values for the current wind farms
    current_wfs['filter2'] = current_wfs['geometry2'].apply( lambda x: filter_current_wf(x))
    
    # Apply the filter based on area and perimeter
    current_wfs = current_wfs[current_wfs['filter2']]
    current_wfs = current_wfs.drop(columns= ['geometry2', 'filter2'])
    
    wf_areas['area [km²]'] = wf_areas.area/1000000
    
    # Exclude areas smaller than X m². X was calculated as the area of
    # 3 WT in the corners of an equilateral triangle with l = 4*rotor_diameter
    min_area = 4 * (0.126**2) * np.sqrt(3) 
    wf_areas = wf_areas[wf_areas['area [km²]'] > min_area]
    
    # find the potential areas that intersects the convex hulls of current wind farms
    # and assign voltage levels
    wf_areas= wf_areas.to_crs(4326)
    for i in wf_areas.index:
        intersection = current_wfs.intersects(wf_areas.at[i, 'geom'])
        if intersection.any() == False:
            wf_areas.at[i, 'voltage'] = 'No Intersection'
        else:
            wf_areas.at[i, 'voltage'] = current_wfs[intersection].voltage[0]
    
    # wf_areas_ni has the potential areas which don't intersect any current wind farm
    wf_areas_ni = wf_areas[wf_areas['voltage'] == 'No Intersection']        
    wf_areas = wf_areas[wf_areas['voltage'] != 'No Intersection']
    
    # Create the centroid of the selected potential areas and assign an installed capacity
    power_north = 21.05 #MW/km²
    power_south = 16.81 #MW/km²
    # Set a maximum installed capacity to limit the power of big potential areas
    max_power_hv = 120 # in MW
    max_power_mv = 20 # in MW
    # Max distance between WF (connected to MV) and nearest HV substation that
    # allows its connection to HV.
    max_dist_hv = 20000 # in meters
    
    wf_areas['centroid'] = wf_areas.centroid
    wf_areas['north'] = wf_areas['centroid'].within(north_states)
    wf_areas['inst capacity [MW]'] = wf_areas.apply(
        lambda x: power_north*x['area [km²]']
        if x['north'] == True
        else power_south*x['area [km²]'], axis= 1)
    
    wf_areas.drop(columns= ['north'], inplace= True)
     
    # Divide selected areas based on voltage of connection points
    wf_mv = wf_areas[(wf_areas['voltage'] != 'Hochspannung') &
                     (wf_areas['voltage'] != 'Hoechstspannung') &
                     (wf_areas['voltage'] != 'UmspannungZurHochspannung')]
    
    wf_hv = wf_areas[(wf_areas['voltage'] == 'Hochspannung') |
                     (wf_areas['voltage'] == 'Hoechstspannung') |
                     (wf_areas['voltage'] == 'UmspannungZurHochspannung')]
    
    # Wind farms connected to MV network will be connected to HV network if the distance
    # to the closest HV substation is =< max_dist_hv, and the installed capacity
    # is bigger than max_power_mv
    hvmv_substation = hvmv_substation.to_crs(3035)
    hvmv_substation['voltage'] = hvmv_substation['voltage'].apply(
        lambda x: int(x.split(';')[0]))
    hv_substations = hvmv_substation[hvmv_substation['voltage'] >= 110000]
    hv_substations = hv_substations.unary_union # join all the hv_substations
    wf_mv['dist_to_HV'] = wf_areas['geom'].to_crs(3035).distance(hv_substations)
    wf_mv_to_hv = wf_mv[(wf_mv['dist_to_HV'] <= max_dist_hv) &
                        (wf_mv['inst capacity [MW]'] >= max_power_mv)]
    wf_mv_to_hv = wf_mv_to_hv.drop(columns= ['dist_to_HV'])
    wf_mv_to_hv['voltage'] = 'Hochspannung'
    
    wf_hv = wf_hv.append(wf_mv_to_hv)
    wf_mv = wf_mv[(wf_mv['dist_to_HV'] > max_dist_hv) |
                        (wf_mv['inst capacity [MW]'] < max_power_mv)]
    wf_mv = wf_mv.drop(columns= ['dist_to_HV'])
        
    wf_hv['inst capacity [MW]']= wf_hv['inst capacity [MW]'].apply(
        lambda x: x if x < max_power_hv else max_power_hv)
    
    wf_mv['inst capacity [MW]'] = wf_mv['inst capacity [MW]'].apply(
        lambda x: x if x < max_power_mv else max_power_mv)
    
    wind_farms= wf_hv.append(wf_mv)
    
    # Adjust the total installed capacity to the scenario
    total_wind_power = wf_hv['inst capacity [MW]'].sum() + wf_mv['inst capacity [MW]'].sum()
    if total_wind_power > target_power:
        scale_factor = target_power/total_wind_power
        wf_mv['inst capacity [MW]'] = wf_mv['inst capacity [MW]'] * scale_factor
        wf_hv['inst capacity [MW]'] = wf_hv['inst capacity [MW]'] * scale_factor
        wind_farms= wf_hv.append(wf_mv)
    
    else:
        wf_areas_ni['centroid'] = wf_areas_ni.to_crs(3035).centroid
        wf_areas_ni = wf_areas_ni.set_geometry('centroid')
        #wf_areas_ni['centroid'].to_file("centroid_districs.geojson", driver='GeoJSON')
        #wf_areas_ni['geom'].to_file("restastes pa.geojson", driver='GeoJSON')
        extra_wf = mv_districts.copy()
        # the column centroid has the coordinates of the substation corresponting
        # to each mv_grid_district
        extra_wf['centroid'] = extra_wf.apply(match_district_se, axis = 1)
        extra_wf = extra_wf.set_geometry("centroid")
        extra_wf['area [km²]'] = 0.0
        for district in extra_wf.index:
            pot_area_district = gpd.clip(wf_areas_ni, extra_wf.at[district, 'geom'])
            extra_wf.at[district, 'area [km²]'] = pot_area_district['area [km²]'].sum()
        extra_wf = extra_wf.to_crs(4326)
        extra_wf = extra_wf[extra_wf['area [km²]'] != 0]
        total_new_area = extra_wf['area [km²]'].sum()
        scale_factor = (target_power - total_wind_power) / total_new_area
        extra_wf['inst capacity [MW]'] = extra_wf['area [km²]'] * scale_factor
        extra_wf['voltage'] = 'Hochspannung'    
        wind_farms = wind_farms.append(extra_wf, ignore_index= True) 
    
    # Use Definition of thresholds for voltage level assignment
    wind_farms['voltage_level'] = 0
    for i in wind_farms.index:
        try:
            if wind_farms.at[i, 'inst capacity [MW]'] < 5.5:
                wind_farms.at[i, 'voltage_level'] = 5
                continue
            if wind_farms.at[i, 'inst capacity [MW]'] < 20:
                wind_farms.at[i, 'voltage_level'] = 4
                continue
            if wind_farms.at[i, 'inst capacity [MW]'] >= 20 :
                wind_farms.at[i, 'voltage_level'] = 3
                continue
        except:
            print(i)

    
    # Look for the maximum id in the table egon_power_plants
    sql = "SELECT MAX(id) FROM supply.egon_power_plants"
    max_id = pd.read_sql(sql,con)
    max_id = max_id['max'].iat[0]
    if max_id == None:
        wind_farm_id = 1
    else:
        wind_farm_id = int(max_id+1)

    # write_table in egon-data database:
        
    # Copy relevant columns from wind_farms
    insert_wind_farms = wind_farms[['inst capacity [MW]', 'voltage_level', 'centroid']]

    # Set static column values
    insert_wind_farms['carrier'] = source
    insert_wind_farms['chp'] = False
    insert_wind_farms['th_capacity'] = 0
    insert_wind_farms['scenario'] = scenario_year

    # Change name and crs of geometry column
    insert_wind_farms = insert_wind_farms.rename(
        {'centroid':'geom',
         'inst capacity [MW]': 'el_capacity'},
        axis=1).set_geometry('geom').to_crs(4326)

    # Reset index
    insert_wind_farms.index = pd.RangeIndex(
        start = wind_farm_id,
        stop = wind_farm_id + len(insert_wind_farms),
        name = 'id')

    # Insert into database
    insert_wind_farms.reset_index().to_postgis('egon_power_plants',
                               schema='supply',
                               con=db.engine(),
                               if_exists='append')
    return wind_farms

"""
    for wf in wind_farms.index:
        con = psycopg2.connect(host = db.credentials()['HOST'],
                               database = db.credentials()['POSTGRES_DB'],
                               user = db.credentials()['POSTGRES_USER'],
                               password = db.credentials()['POSTGRES_PASSWORD'],
                               port = db.credentials()['PORT']) 
        cur = con.cursor()
        sql = '''insert into supply.egon_power_plants
        (id, carrier, chp, el_capacity, th_capacity,
         voltage_level, scenario, geom) 
        values (%s, %s, %s, %s, %s, %s, %s, %s)'''      
        cur.execute(sql, (wind_farm_id,
                          source,
                          False,
                          wind_farms.loc[wf].at['inst capacity [MW]'],
                          0,
                          wind_farms.loc[wf].at['voltage level'],
                          scenario_year,
                          wkb.dumps(wind_farms.loc[wf].at['centroid'])))
        con.commit()
        cur.close()
        wind_farm_id+=1
"""




"""
wind_farms[['geom', 'inst capacity [MW]', 'area [km²]']].to_file("Selected_Pot_areas.geojson", driver='GeoJSON')
wind_farms = wind_farms.set_geometry('centroid')
wind_farms[['centroid', 'inst capacity [MW]', 'area [km²]']].to_file("Wind_farms_points.geojson", driver='GeoJSON')
"""