
#from egon.data import db
import psycopg2
import geopandas as gpd
import pandas as pd
import math
import numpy as np



def regio_of_pv_ground_mounted(path,con, pow_per_area, join_buffer, max_dist_hv, target_power):
    
    def mastr_exisiting_pv(path=path, pow_per_area=pow_per_area):
        
        # import MaStR data: locations, grid levels and installed capacities

        # get relevant pv plants: ground mounted
        df = pd.read_csv(path,usecols = ['Lage','Laengengrad','Breitengrad','Nettonennleistung'])
        df = df[df['Lage']=='Freiflaeche']
        
        # examine data concerning locations and drop NaNs
        x1 = df['Laengengrad'].isnull().sum()
        x2 = df['Breitengrad'].isnull().sum()
        print(' ')
        print('Untersuchung des MaStR-Datensatzes:')
        print('originale Anzahl der Zeilen im Datensatz: '+str(len(df)))
        print('NaNs für Längen- und Breitengrad: '+str(x1)+' & '+str(x2))
        df.dropna(inplace=True)
        print('Anzahl der Zeilen im Datensatz nach Dropping der NaNs:'+str(len(df)))
        print(' ')
        
        # derive dataframe for locations
        mastr = gpd.GeoDataFrame(index=df.index,geometry=gpd.points_from_xy(df['Laengengrad'], df['Breitengrad']), crs={'init' :'epsg:4326'})
        mastr = mastr.to_crs(3035)
        
        # derive grid levels
        # TODO: data will be integrated soon
        
        # derive installed capacities
        mastr['installed capacity in kW'] = df['Nettonennleistung'] 
        
        # create buffer around locations 
        
        # calculate bufferarea and -radius considering installed capacity
        df_radius = mastr['installed capacity in kW'].div(pow_per_area*math.pi)**0.5 # in m
        
        # create buffer
        df_buffer = gpd.GeoSeries()
        for index, row in mastr.iterrows():
            #row['buffer'] = row['geometry'].buffer(df_radius.loc[index]) ### funktioniert mit dieser Zeile nicht
            df_buffer.loc[index] = row['geometry'].buffer(df_radius.loc[index])
            
        mastr['buffer'] = df_buffer
        mastr['buffer'].crs=3035
        
        return mastr
        
    def potential_areas(con=con, join_buffer=join_buffer):
        
        # import potential areas: railways and roads & agriculture
        
        # railways and roads
        sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_road_railway"
        potentials_rora = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_rora = potentials_rora.set_index("id")
        
        # agriculture
        sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_agriculture"
        potentials_agri = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_agri = potentials_agri.set_index("id")
        
        # add areas < 1 ha to bigger areas if they are very close, otherwise exclude areas < 1 ha
        
        ### counting variables for examination
        count_small = 0
        count_join = 0
        count_del_join = 0
        before = len(potentials_rora)
        
        rora_join = gpd.GeoSeries()
        potentials_rora['area'] = potentials_rora.area 
        for index, row in potentials_rora.iterrows():
            if row['area'] < 10000: ### suche kleine Flächen
                buffer = row['geom'].buffer(join_buffer) # Buffer um kleine Fläche
                count_small = count_small+1
                for index2, row2 in potentials_rora.iterrows():
                    if ((row2['area'] > 10000) and (buffer.intersects(row2['geom']))): ### prüfe, ob sich Buffer mit großer Fläche überschneidet
                        count_join = count_join+1
                        #row2['geom']=gpd.GeoSeries([row2['geom'],row['geom']]).unary_union ### funktioniert mit dieser Zeile nicht
                        rora_join.loc[index2] = gpd.GeoSeries([row2['geom'],row['geom']]).unary_union ### join kleine zu große Fläche
                        break ### verhindere doppelte Zuordnung
                potentials_rora = potentials_rora.drop(index) ### danach oder falls keine Überschneidung lösche Zeile mit kleiner Fläche
                count_del_join = count_del_join + 1
        potentials_rora['joined'] = potentials_rora['geom'].copy()
        for i in range(len(rora_join)):
            index = rora_join.index[i]
            potentials_rora['joined'].loc[index] = rora_join.iloc[i]
            
        ### print counting variables for examination
        count_delete = count_del_join - count_join
        print(' ')
        print('Untersuchung der Zusammenfassung von Potentialflächen im Bereich Roads and Railways')
        print('Länge des Dataframes der Flächen vorher: '+str(before))
        print('Anzahl kleiner Flächen: '+str(count_small))
        print('Anzahl der durchgeführten Prozedur des Zusammenfassens: '+str(count_join))
        print('gelöschte Flächen (not joined): '+str(count_delete))
        print('Länge des Dataframes der Flächen danach: '+str(len(potentials_rora)))
        print(' ')
        
        ### counting variables for examination
        count_small = 0
        count_join = 0
        count_del_join = 0
        before = len(potentials_agri)
        
        agri_join = gpd.GeoSeries()     
        potentials_agri['area'] = potentials_agri.area  
        for index, row in potentials_agri.iterrows():
            if row['area'] < 10000:
                buffer = row['geom'].buffer(join_buffer)
                count_small = count_small+1
                for index2, row2 in potentials_agri.iterrows():
                    if ((row2['area'] > 10000) and (buffer.intersects(row2['geom']))):  
                        count_join = count_join+1
                        #row2['geom']=gpd.GeoSeries([row2['geom'],row['geom']]).unary_union
                        agri_join.loc[index2] = gpd.GeoSeries([row2['geom'],row['geom']]).unary_union
                        break
                potentials_agri = potentials_agri.drop(index)
                count_del_join = count_del_join + 1
        potentials_agri['joined'] = potentials_agri['geom'].copy()
        for i in range(len(agri_join)):
            index = agri_join.index[i]
            potentials_agri['joined'].loc[index] = agri_join.iloc[i]   
            
        ### print counting variables for examination
        count_delete = count_del_join - count_join
        print(' ')
        print('Untersuchung der Zusammenfassung von Potentialflächen im Bereich Roads and Railways')
        print('Länge des Dataframes der Flächen vorher: '+str(before))
        print('Anzahl kleiner Flächen: '+str(count_small))
        print('Anzahl der durchgeführten Prozedur des Zusammenfassens: '+str(count_join))
        print('gelöschte Flächen (not joined): '+str(count_delete))
        print('Länge des Dataframes der Flächen danach: '+str(len(potentials_agri)))
        print(' ')
        
        # edit dataframe to only save relevant information
        
        potentials_rora['geom'] = potentials_rora['joined']
        potentials_rora.drop(['joined'], axis=1, inplace=True)
        potentials_rora['area'] = potentials_rora['geom'].area
        potentials_agri['geom'] = potentials_agri['joined']
        potentials_agri.drop(['joined'], axis=1, inplace=True)
        potentials_agri['area'] = potentials_agri['geom'].area
        
        # check intersection of potential areas 
        
        ### counting variables
        agri_vorher = len(potentials_agri)
        x_inter = 0
        
        pot_r = potentials_rora['geom'].unary_union
        
        # if areas intersect, keep road & railway potential areas and drop agricultural ones
        for index, agri in potentials_agri.iterrows():
            if agri['geom'].intersects(pot_r):
                x_inter = x_inter + 1
                potentials_agri = potentials_agri.drop([index])
        
        ### examination
        agri_nachher = len(potentials_agri)
        print(' ')
        print('Überprüfung der Funktion zur Meidung der Intersection von Potentialflächen:')
        print('Länge potentials_agri vorher: '+str(agri_vorher))
        print('Anzahl der auftretenden Fälle: '+str(x_inter))
        print('Länge potentials_agri nachher: '+str(agri_nachher))
        print(' ')
        
        return potentials_rora, potentials_agri
    
    def select_pot_areas(mastr, potentials_pot):
        
        # select potential areas with existing pv plants
        # (potential areas intersect buffer around existing plants)
        
        '''### test data
        print(' ')
        print('Testdaten für intersect-Funktion an PV-Koordinaten und Buffers je mit Potentialflächen:')
        print(' - rora sollte TRUE liefern')
        print('POINT INTERSECTS: '+str(potentials_pot['geom'].loc[2136].intersects(mastr['geometry'].loc[798803])))
        print('BUFFER INTERSECTS: '+str(potentials_pot['geom'].loc[2136].intersects(mastr['buffer'].loc[798803])))
        print(' ')'''
        
        pot_sel = pd.Series()
        for index1, loc in mastr.iterrows():
            for index2, pot in potentials_pot.iterrows():
                    if pot['geom'].intersects(loc['buffer']):
                        pot_sel.loc[index2] = True 
        potentials_pot['selected'] = pot_sel                              
        pv_pot = potentials_pot.loc[potentials_pot['selected'] == True]
        
        '''pot_sel2 = pd.Series()
        for index1, loc in mastr.iterrows():
            for index2, pot in potentials_pot.iterrows():
                    if pot['geom'].intersects(loc['geometry']):
                        pot_sel2.loc[index2] = True                            
        pv_pot_test = pot_sel2.loc[pot_sel2 == True]
        
        ### examination of influence of buffer
        x_with_buffer=len(pv_pot)
        x_without_buffer=len(pv_pot_test)
        print(' ')
        print('Untersuchung des Einflusses des Buffers:')
        print('Anzahl ausgewählter Potentialflächen mit Buffer: '+str(x_with_buffer))
        print('Anzahl ausgewählter Potentialflächen ohne Buffer: '+str(x_without_buffer))
        print(' ')'''
        
        return pv_pot
    
    def build_pv(pv_pot, pow_per_area=pow_per_area):
        
        # build pv farms in selected areas

        # calculation of centroids
        pv_pot['centroid'] = pv_pot['geom'].centroid
        
        # calculation of power in kW
        pv_pot['installed capacity in kW'] = pd.Series()
        pv_pot['installed capacity in kW'] = pv_pot['area']*pow_per_area
        
        '''# check for maximal capacity for PV ground mounted
        # TODO: Do I need that? Choose value for limit!
        limit_cap = 100000 # in kW 
        pv_pot['installed capacity in kW'] = pv_pot['installed capacity in kW'].apply(
            lambda x: x if x < limit_cap else limit_cap)'''
        
        return pv_pot
    
    def adapt_grid_level(pv_pot, max_dist_hv=max_dist_hv, con=con):
        
        # check grid level

        # TODO: aus MaStR-Daten (jeweilige Originalanlagen)
        ### Zwischenlösung für fehlende Daten
        pv_pot['voltage'] = np.random.randint(100, 120, pv_pot['installed capacity in kW'].size)
        ### 
        
        # divide dataframe in MV and HV 
        pv_pot_mv = pv_pot[pv_pot['voltage'] < 110]
        pv_pot_hv = pv_pot[pv_pot['voltage'] >= 110]
        
        # check installed capacity in MV 
        
        max_cap_mv = 20000 # in kW
        
        # find PVs which need to be HV or to have reduced capacity
        pv_pot_mv_to_hv = pv_pot_mv[pv_pot_mv['installed capacity in kW'] > max_cap_mv]
        
        if len(pv_pot_mv_to_hv) > 0:
        
            # import data for HV substations
            
            ###
            #sql = "SELECT geom FROM grid.egon_pf_hv_line"
            #trans_lines = gpd.GeoDataFrame.from_postgis(sql, con)
            #trans_lines = trans_lines.to_crs(3035) 
            #trans_lines = trans_lines.unary_union # join all the transmission lines
            
            sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
            hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "point")
            hvmv_substation = hvmv_substation.to_crs(3035)
            hvmv_substation['voltage'] = hvmv_substation['voltage'].apply(
                lambda x: int(x.split(';')[0]))
            hv_substations = hvmv_substation[hvmv_substation['voltage'] >= 110000]
            hv_substations = hv_substations.unary_union # join all the hv_substations
            
            # check distance to HV substations of PVs with too high installed capacity for MV
            
            ###
            # calculate distance to lines
            # pv_pot_mv_to_hv['dist_to_HV'] = pv_pot_mv_to_hv['geom'].to_crs(3035).distance(trans_lines)
            
            # calculate distance to substations
            pv_pot_mv_to_hv['dist_to_HV'] = pv_pot_mv_to_hv['geom'].to_crs(3035).distance(hv_substations)
            
            # adjust grid level and keep capacity if transmission lines are close
            pv_pot_mv_to_hv = pv_pot_mv_to_hv[pv_pot_mv_to_hv['dist_to_HV'] <= max_dist_hv]
            pv_pot_mv_to_hv = pv_pot_mv_to_hv.drop(columns=['dist_to_HV'])
            pv_pot_hv = pv_pot_hv.append(pv_pot_mv_to_hv)
            
            # delete PVs which are now HV from MV dataframe
            for index, pot in pv_pot_mv_to_hv.iterrows():
                pv_pot_mv = pv_pot_mv.drop([index])
                
            # keep grid level adjust capacity if transmission lines are too far 
            pv_pot_mv['installed capacity in kW'] = pv_pot_mv['installed capacity in kW'].apply(
                lambda x: x if x < max_cap_mv else max_cap_mv)
        
        return pv_pot_mv, pv_pot_hv
    
    def build_additional_pv(potentials, pv, rest_cap, pow_per_area=pow_per_area, con=con): 
    
        # get MV grid districts
        sql = "SELECT subst_id, geom FROM grid.mv_grid_districts"
        distr = gpd.GeoDataFrame.from_postgis(sql, con)
        distr = distr.set_index("subst_id")            
        
        # identify potential areas where there are no PV parks yet 
        for index, pv in pv.iterrows():
            potentials = potentials.drop([index])
        
        # aggregate potential area per MV grid district 
        pot_per_distr=gpd.GeoDataFrame()
        pot_per_distr['geom'] = distr['geom'].copy()
        overlay = gpd.sjoin(potentials,distr)
        for index, distr in distr.iterrows():
            pot_per_distr['geom'].loc[index] = overlay[overlay['index_right']==index]['geom'].unary_union
        # TODO: assignment of potential area to district not quite right
            
        # calculate area per MV grid district and linearly distribute needed capacity considering pow_per_area
        pot_per_distr['area'] = pot_per_distr['geom'].area
        pot_per_distr['installed capacity in kW'] = pot_per_distr['area']*pow_per_area
        
        # assign HV substations to power per MV district
        
        # get  substations 
        sql = "SELECT point FROM grid.egon_hvmv_substation"
        hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "point")
        hvmv_substation = hvmv_substation.to_crs(3035)

        # assign substation per MV district
        sub = gpd.sjoin(hvmv_substation,distr)
        pot_per_distr['substation'] = gpd.GeoSeries()
        for index, distr in pot_per_distr.iterrows():
            pot_per_distr['substation'].loc[index] = sub[sub['index_right'] == index]['index_right']
        # TODO: check
          
        # files for depiction in QGis
        distr['geom'].to_file("mv_grid_districts.geojson", driver='GeoJSON',index=True)
        pot_per_distr['geom'].to_file("pot_per_distr.geojson", driver='GeoJSON',index=True)
            
        return pot_per_distr

    def check_target(pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, potentials_rora,pv_rora,pv_agri, target_power=target_power):
        
        # sum overall installed capacity for MV and HV

        total_pv_power = pv_rora_mv['installed capacity in kW'].sum() + pv_rora_hv['installed capacity in kW'].sum() + \
        pv_agri_mv['installed capacity in kW'].sum() + pv_agri_hv['installed capacity in kW'].sum()
        
        # check target value
        
        # linear scale farms to meet target if sum of installed capacity is too high
        if total_pv_power > target_power:
                scale_factor = target_power/total_pv_power
                pv_rora_mv['installed capacity in kW'] = pv_rora_mv['installed capacity in kW'] * scale_factor
                pv_rora_hv['installed capacity in kW'] = pv_rora_hv['installed capacity in kW'] * scale_factor
                pv_agri_mv['installed capacity in kW'] = pv_agri_mv['installed capacity in kW'] * scale_factor
                pv_agri_hv['installed capacity in kW'] = pv_agri_hv['installed capacity in kW'] * scale_factor
                 
        # build new pv parks if sum of installed capacity is below target value
        elif total_pv_power < target_power:
            
            # build pv parks in potential areas road & railway
            rest_cap = target_power - total_pv_power
            pow_per_distr = build_additional_pv(potentials_rora, pv_rora, rest_cap) 
            total_pv_power = total_pv_power + pow_per_distr['installed capacity in kw'].sum()
            
            # build pv parks on potential areas ariculture if still necessary
            if total_pv_power < target_power: 
                rest_cap = target_power - total_pv_power
                pow_per_distr_2 = build_additional_pv(potentials_agri, pv_agri, rest_cap)
                pow_per_distr.append(pow_per_distr_2)
                total_pv_power = total_pv_power + pow_per_distr['installed capacity in kw'].sum()
                
            # linear scale farms to meet target if sum of installed capacity is too high   
            if total_pv_power > target_power:
                scale_factor = target_power/total_pv_power
                pv_rora_mv['installed capacity in kW'] = pv_rora_mv['installed capacity in kW'] * scale_factor
                pv_rora_hv['installed capacity in kW'] = pv_rora_hv['installed capacity in kW'] * scale_factor
                pv_agri_mv['installed capacity in kW'] = pv_agri_mv['installed capacity in kW'] * scale_factor
                pv_agri_hv['installed capacity in kW'] = pv_agri_hv['installed capacity in kW'] * scale_factor
                pow_per_distr['installed capacity in kW'] = pow_per_distr['installed capacity in kW'] * scale_factor

        return pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, pow_per_distr
    
    
    # MaStR-data: existing PV farms 
    mastr = mastr_exisiting_pv()
    
    # files for depiction in QGis
    mastr['geometry'].to_file("MaStR_PVs.geojson", driver='GeoJSON',index=True)
    mastr['buffer'].to_file("MaStR_PVs_buffered.geojson", driver='GeoJSON')
    
    # database-data: potential areas for new PV farms
    potentials_rora, potentials_agri = potential_areas()
    
    # files for depiction in QGis        
    potentials_rora['geom'].to_file("potentials_rora_joined.geojson", driver='GeoJSON',index=True)
    potentials_agri['geom'].to_file("potentials_agri_joined.geojson", driver='GeoJSON',index=True)
    
    # select potential areas with existing PV farms to build new PV farms
    pv_rora = select_pot_areas(mastr, potentials_rora)
    pv_agri = select_pot_areas(mastr, potentials_agri)
    
    # files for depiction in QGis
    pv_rora['geom'].to_file("potential_rora_selected.geojson", driver='GeoJSON')
    pv_agri['geom'].to_file("potential_agri_selected.geojson", driver='GeoJSON')
    
    # build new PV farms
    pv_rora = build_pv(pv_rora)
    pv_agri = build_pv(pv_agri)
    
    # files for depiction in QGis
    pv_rora['centroid'].to_file("PVs_rora_new.geojson", driver='GeoJSON')
    pv_agri['centroid'].to_file("PVs_agri_new.geojson", driver='GeoJSON')
    
    # adapt grid level to new farms
    pv_rora_mv, pv_rora_hv = adapt_grid_level(pv_rora)
    pv_agri_mv, pv_agri_hv = adapt_grid_level(pv_agri)
    
    # check target value and adapt installed capacity if necessary
    pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, pow_per_distr = check_target(pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, potentials_rora, pv_rora, pv_agri)
      
    
    return pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, pow_per_distr


'''con = psycopg2.connect(host = "172.18.0.2",
                                   database = "egon-data",
                                   user = "egon",
                                  password = "data")'''
    
con = psycopg2.connect(host = "127.0.0.1",
                               database = "test2",
                               user = "egon",
                               password = "data",
                               port= 59734)

#con = db.engine()

path = '/home/kathiesterl/PYTHON/Potentials_PV/bnetza_mastr_solar_cleaned.csv'

pow_per_area = 0.04 # kW per m² 
# assumption for areas of existing pv farms and power of new built pv farms
# TODO: mark in issue?

join_buffer = 10 # m
# maximum distance for joining of potential areas (only small ones to big ones)

max_dist_hv = 20000 # m
# assumption for maximum distance of park with hv-power to next substation
# TODO: research

target_power = 1337984 # kW 
# assumption for target value of installed capacity in Germany per scenario

pv_rora_mv, pv_rora_hv, pv_agri_mv, pv_agri_hv, pow_per_distr = regio_of_pv_ground_mounted(path,con,
                                                pow_per_area, join_buffer, max_dist_hv, target_power)

# TODO: integrate data in table eGon power plants (look at other scripts, copy eg from biomass)

















