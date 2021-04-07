import psycopg2
import geopandas as gpd
import pandas as pd
import math

##############################################################################
# import MaStR data: locations, grid levels and installed capacities

# get relevant pv plants: ground mounted
df = pd.read_csv('/home/kathiesterl/PYTHON/bnetza_mastr_solar_cleaned.csv',usecols = ['Lage','Laengengrad','Breitengrad','Nettonennleistung'])
df = df[df['Lage']=='Freiflaeche']
df.to_csv('/home/kathiesterl/PYTHON/pv_ground_mounted.csv')
# examine data concerning locations and drop NaNs
x1 = df['Laengengrad'].isnull().sum()
x2 = df['Breitengrad'].isnull().sum()
print(' ')
print('Untersuchung des MaStR-Datensatzes:')
print('originale Anzahl der Zeilen im Datensatz: '+str(len(df)))
print('NaNs für Längen- und Breitengrad: '+str(x1)+' & '+str(x1))
df.dropna(inplace=True)
print('Anzahl der Zeilen im Datensatz nach dropping der NaNs:'+str(len(df)))
print(' ')

# derive dataframe for locations
gdf_loc = gpd.GeoDataFrame(index=df.index,geometry=gpd.points_from_xy(df['Laengengrad'], df['Breitengrad']), crs={'init' :'epsg:4326'})
gdf_loc = gdf_loc.to_crs(3035)
# file for depiction in QGis
gdf_loc.to_file("PVs.geojson", driver='GeoJSON',index=True)

# derive dataframe for grid levels
# TODO: data will be integrated soon

# derive dataframe for installed capacities
df_pow = df['Nettonennleistung'] # in kW

##############################################################################
# create buffer around locations
# TODO: calculate one version with and one without buffer to examine influence

# calculate bufferarea and -radius considering installed capacity
# TODO: remark this value in issue and set it as parameter for function in the end
pow_per_area = 0.04  # in kW / m²
df_radius = df_pow.div(pow_per_area*math.pi)**0.5 # in m

# create buffer
df_buffer = gpd.GeoSeries()
for index, row in gdf_loc.iterrows():
    #row['buffer'] = row['geometry'].buffer(df_radius.loc[index]) ### funktioniert mit dieser Zeile nicht
    df_buffer.loc[index] = row['geometry'].buffer(df_radius.loc[index])
    
# file for depiction in QGis
gdf_loc['buffer'] = df_buffer
gdf_loc['buffer'].crs=3035
gdf_loc['buffer'].to_file("PVs_buffered.geojson", driver='GeoJSON')

##############################################################################
# import potential areas: railways and roads & agriculture 

con = psycopg2.connect(host = "172.18.0.2",
                           database = "egon-data",
                           user = "egon",
                          password = "data")

# railways and roads
sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_road_railway"
gdf_rora = gpd.GeoDataFrame.from_postgis(sql, con)
gdf_rora = gdf_rora.set_index("id")

# agriculture
sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_agriculture"
gdf_agri = gpd.GeoDataFrame.from_postgis(sql, con)
gdf_agri = gdf_agri.set_index("id")

##############################################################################
# add areas < 1 ha to bigger areas if they are very close, otherwise exclude areas < 1 ha
# TODO: examine impact of joining of areas

join_buffer = 30 # maximum distance for joining of areas 
# TODO: remark this value in issue and set it as parameter for function in the end

# counting variables for examination
count_small = 0
count_join = 0
count_delete = len(gdf_rora)

gdf_rora_join = gpd.GeoSeries()
gdf_rora['area'] = gdf_rora.area 
for index, row in gdf_rora.iterrows():
    if row['area'] < 10000: ### suche kleine Flächen
        buffer = row['geom'].buffer(join_buffer) # Buffer um kleine Fläche
        count_small = count_small+1
        for index2, row2 in gdf_rora.iterrows():
            if ((row2['area'] > 10000) and (buffer.intersects(row2['geom']))): #### prüfe, ob sich Buffer mit großer Fläche überschneidet
                count_join = count_join+1
                #row2['geom']=gpd.GeoSeries([row2['geom'],row['geom']]).unary_union ### funktioniert mit dieser Zeile nicht
                gdf_rora_join.loc[index2] = gpd.GeoSeries([row2['geom'],row['geom']]).unary_union ### join kleine zu große Fläche
                break ### verhindere doppelte Zuordnung
        gdf_rora.drop(index,inplace=True) ### danach oder falls keine Überschneidung lösche Zeile mit kleiner Fläche
gdf_rora['joined'] = gdf_rora['geom'].copy()
for i in range(len(gdf_rora_join)):
    index = gdf_rora_join.index[i]
    gdf_rora['joined'].loc[index] = gdf_rora_join.iloc[i]
    
# print counting variables for examination
count_delete = count_delete - len(gdf_rora)
print(' ')
print('Untersuchung der Zusammenfassung von Potentialflächen im Bereich Roads and Railways')
print('Anzahl kleiner Flächen: '+str(count_small))
print('Anzahl der durchgeführten Prozedur des Zusammenfassens: '+str(count_join))
print('zusammengefasste Flächen am Ende: '+str(len(gdf_rora_join)))
print('gelöschte & hinzugefügte Flächen: '+str(count_delete))
print(' ')
   
gdf_agri_join = gpd.GeoSeries()     
gdf_agri['area'] = gdf_agri.area  
for index, row in gdf_agri.iterrows():
    if row['area'] < 10000:
        buffer = row['geom'].buffer(join_buffer)
        for index2, row2 in gdf_agri.iterrows():
            if ((row2['area'] > 10000) and (buffer.intersects(row2['geom']))): 
                #row2['geom']=gpd.GeoSeries([row2['geom'],row['geom']]).unary_union
                gdf_agri_join.loc[index2] = gpd.GeoSeries([row2['geom'],row['geom']]).unary_union
                break
        gdf_agri.drop(index,inplace=True)
gdf_agri['joined'] = gdf_agri['geom'].copy()
for i in range(len(gdf_agri_join)):
    index = gdf_agri_join.index[i]
    gdf_agri['joined'].loc[index] = gdf_agri_join.iloc[i]    
    
# files for depiction in QGis        
gdf_rora['joined'].to_file("potential_rora_joined.geojson", driver='GeoJSON',index=True)
gdf_agri['joined'].to_file("potential_agri_joined.geojson", driver='GeoJSON',index=True)

##############################################################################
# select potential areas with existing pv plants
# (potential areas intersect buffer around existing plants)

### test data: sollte TRUE ergeben
print(' ')
print('Testdaten für intersect-Funktion an PV-Koordinaten und Buffers je mit Potentialflächen - sollte TRUE liefern:')
print('POINT INTERSECTS: '+str(gdf_rora['geom'].loc[2136].intersects(gdf_loc['geometry'].loc[798803])))
print('BUFFER INTERSECTS: '+str(gdf_rora['geom'].loc[2136].intersects(gdf_loc['buffer'].loc[798803])))
print(' ')

rora_sel = pd.Series()
agri_sel = pd.Series()
for index1, loc in gdf_loc.iterrows():
    for index2, rora in gdf_rora.iterrows():
            if rora['geom'].intersects(loc['buffer']):
                rora_sel.loc[index2] = True
    for index2, agri in gdf_agri.iterrows():
            if loc['buffer'].intersects(agri['geom']):
                agri_sel.loc[index2] = True  
gdf_rora['selected'] = rora_sel                
gdf_agri['selected'] = agri_sel               
gdf_rora_sel = gdf_rora.loc[gdf_rora['selected'] == True]
gdf_agri_sel = gdf_agri.loc[gdf_agri['selected'] == True]

# files for depiction in QGis
gdf_rora_sel['geom'].to_file("potential_rora_selected.geojson", driver='GeoJSON')
gdf_agri_sel['geom'].to_file("potential_agri_selected.geojson", driver='GeoJSON')

##############################################################################
# build pv farms in selected areas

# calculation of centroids
gdf_rora_sel['centroid'] = gdf_rora_sel.centroid
gdf_agri_sel['centroid'] = gdf_agri_sel.centroid
# files for depiction in QGis
gdf_rora_sel['centroid'].to_file("PVs_rora_new.geojson", driver='GeoJSON')
gdf_agri_sel['centroid'].to_file("PVs_agri_new.geojson", driver='GeoJSON')

# calculation of power 
# TODO: remark this value from above (pow_per_area) in issue and set it as parameter for function in the end
df_pow_rora = pd.Series()
df_pow_agri = pd.Series()
gdf_rora_sel['area'] = gdf_rora_sel.area  
gdf_agri_sel['area'] = gdf_agri_sel.area  
for index, rora in gdf_rora_sel.iterrows():
    df_pow_rora.loc[index] = rora['area']*pow_per_area
for index, agri in gdf_agri_sel.iterrows():
    df_pow_agri.loc[index] = rora['area']*pow_per_area


##############################################################################
# TODO: start orange box










