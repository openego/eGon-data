

from egon.data import db
import psycopg2
import geopandas as gpd
from shapely import wkb
import pandas as pd
import numpy as np

###
import datetime

# TODO: 
### delete examination stuff / add it as parameter, eg plot=True

def regio_of_pv_ground_mounted():
    
    
    def mastr_existing_pv(path, pow_per_area):
        
        ### mastr = gpd.read_file('mastr.csv')

        # import MaStR data: locations, grid levels and installed capacities

        # get relevant pv plants: ground mounted
        df = pd.read_csv(path+'bnetza_mastr_solar_cleaned.csv',usecols = ['Lage','Laengengrad','Breitengrad','Nettonennleistung','EinheitMastrNummer'])
        df = df[df['Lage']=='Freiflaeche']

        # examine data concerning geographical locations and drop NaNs
        x1 = df['Laengengrad'].isnull().sum()
        x2 = df['Breitengrad'].isnull().sum()
        print(' ')
        print('Untersuchung des MaStR-Datensatzes:')
        print('originale Anzahl der Zeilen im Datensatz: '+str(len(df)))
        print('NaNs für Längen- und Breitengrad: '+str(x1)+' & '+str(x2))
        df.dropna(inplace=True)
        print('Anzahl der Zeilen im Datensatz nach Dropping der NaNs: '+str(len(df)))
        print(' ')

        # derive dataframe for locations
        mastr = gpd.GeoDataFrame(index=df.index,geometry=gpd.points_from_xy(df['Laengengrad'], df['Breitengrad']), crs={'init' :'epsg:4326'})
        mastr = mastr.to_crs(3035)

        # derive installed capacities
        mastr['installed capacity in kW'] = df['Nettonennleistung']

        # create buffer around locations

        # calculate bufferarea and -radius considering installed capacity
        df_radius = mastr['installed capacity in kW'].div(pow_per_area*np.pi)**0.5 # in m

        # create buffer
        mastr['buffer'] = mastr['geometry'].buffer(df_radius)
        mastr['buffer'].crs=3035

        # derive MaStR-Nummer
        mastr['mastr_nummer'] = df['EinheitMastrNummer']

        # derive voltage level

        mastr['voltage_level'] = pd.Series(dtype=int)
        lvl = pd.read_csv(path+'location_elec_generation_raw.csv',usecols = ['Spannungsebene','MaStRNummer'])

        # assign voltage_level to MaStR-unit:
        v_l = pd.Series()
        for index, row in mastr.iterrows():
            nr = row['mastr_nummer']
            l = lvl[lvl['MaStRNummer']=="['"+nr+"']"]['Spannungsebene']
            if len(l)>0:
                if l.iloc[0] == 'Mittelspannung':
                    v_l.loc[index] = 5
                if l.iloc[0] == 'UmspannungZurMittelspannung':
                    v_l.loc[index] = 4
                elif l.iloc[0] == 'Hochspannung':
                    v_l.loc[index] = 3
                elif l.iloc[0] == 'UmspannungZurHochspannung':
                    v_l.loc[index] = 1
                elif l.iloc[0] == 'Höchstspannung':
                    v_l.loc[index] = 1
                elif l.iloc[0] == 'UmspannungZurNiederspannung':
                    v_l.loc[index] = l.iloc[0]
                elif l.iloc[0] == 'Niederspannung':
                    v_l.loc[index] = l.iloc[0]
            else:
               v_l.loc[index] = np.NaN
        mastr['voltage_level'] = v_l

        # examine data concerning voltage level
        x1 = mastr['voltage_level'].isnull().sum()
        print(' ')
        print('Untersuchung des MaStR-Datensatzes für Spannungsebenen:')
        print('Anzahl der Zeilen im MaStR-Datensatz vorher: '+str(len(mastr)))
        print('NaNs in Spannungsebene aufgrund a) keine Zuordnung zur Nummer oder b) fehlender Daten: '+str(x1))
        # drop PVs with missing values due to a) no assignemtn of MaStR-numbers or b) missing data in row
        mastr.dropna(inplace=True)
        print('Anzahl der Zeilen im Datensatz nach Dropping der NaNs: '+str(len(mastr)))


        # drop PVs in low voltage level
        index_names = mastr[ mastr['voltage_level'] == 'Niederspannung' ].index
        x2 = len(index_names)
        mastr.drop(index_names,inplace=True)
        index_names = mastr[ mastr['voltage_level'] == 'UmspannungZurNiederspannung' ].index
        x3 = len(index_names)
        mastr.drop(index_names,inplace=True)

        # further examination
        print('Anzahl der PVs in der Niederspannungsebene: '+str(x2))
        print('Anzahl der PVs in der NSMS-Ebene: '+str(x3))
        print('Anzahl der Zeilen im Datensatz nach Dropping dieser Ebenen: '+str(len(mastr)))
        print(' ')

        return mastr


    def potential_areas(con, join_buffer):

        # import potential areas: railways and roads & agriculture

        # roads and railway
        sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_road_railway"
        potentials_rora = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_rora = potentials_rora.set_index("id")

        # agriculture
        sql = "SELECT id, geom FROM supply.egon_re_potential_area_pv_agriculture"
        potentials_agri = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_agri = potentials_agri.set_index("id")

        # add areas < 1 ha to bigger areas if they are very close, otherwise exclude areas < 1 ha
        
        # calculate area
        potentials_rora['area'] = potentials_rora.area
        potentials_agri['area'] = potentials_agri.area
        
        # roads and railways
        
        ### counting variables for examination
        before = len(potentials_rora)
        
        # get small areas and create buffer for joining around them
        small_areas = potentials_rora[potentials_rora['area'] < 10000]
        small_buffers = small_areas.copy()
        small_buffers['geom'] = small_areas['geom'].buffer(join_buffer)
        
        # drop small areas in potential areas
        index_names = potentials_rora[potentials_rora['area'] < 10000 ].index
        potentials_rora.drop(index_names,inplace=True)
        
        # check intersection of small areas with other potential areas
        overlay = gpd.sjoin(potentials_rora, small_buffers)
        o = overlay['index_right']
        o.drop_duplicates(inplace=True)
        
        # add small areas to big ones if buffer intersects
        for i in range(0,len(o)):
            index_potentials = o.index[i]
            index_small = o.iloc[i]
            x = potentials_rora['geom'].loc[index_potentials]
            y = small_areas['geom'].loc[index_small]
            join = gpd.GeoSeries(data=[x,y])
            potentials_rora['geom'].loc[index_potentials] = join.unary_union
            
        ### print counting variables for examination
        count_small = len(small_buffers)
        count_join = len(o)
        count_delete = count_small - count_join
        print(' ')
        print('Untersuchung der Zusammenfassung von Potentialflächen im Bereich Roads and Railways')
        print('Länge des Dataframes der Flächen vorher: '+str(before))
        print('Anzahl kleiner Flächen: '+str(count_small))
        print('Anzahl der durchgeführten Prozedur des Zusammenfassens: '+str(count_join))
        print('gelöschte Flächen (not joined): '+str(count_delete))
        print('Länge des Dataframes der Flächen danach: '+str(len(potentials_rora)))
        print(' ')
            
        # agriculture
        
        ### counting variables for examination
        before = len(potentials_agri)
        
        # get small areas and create buffer for joining around them
        small_areas = potentials_agri[potentials_agri['area'] < 10000]
        small_buffers = small_areas.copy()
        small_buffers['geom'] = small_areas['geom'].buffer(join_buffer)
        
        # drop small areas in potential areas
        index_names = potentials_agri[potentials_agri['area'] < 10000 ].index
        potentials_agri.drop(index_names,inplace=True)
        
        # check intersection of small areas with other potential areas
        overlay = gpd.sjoin(potentials_agri, small_buffers)
        o = overlay['index_right']
        o.drop_duplicates(inplace=True)
        
        # add small areas to big ones if buffer intersects
        for i in range(0,len(o)):
            index_potentials = o.index[i]
            index_small = o.iloc[i]
            x = potentials_agri['geom'].loc[index_potentials]
            y = small_areas['geom'].loc[index_small]
            join = gpd.GeoSeries(data=[x,y])
            potentials_agri['geom'].loc[index_potentials] = join.unary_union
            
        ### print counting variables for examination
        count_small = len(small_buffers)
        count_join = len(o)
        count_delete = count_small - count_join
        print(' ')
        print('Untersuchung der Zusammenfassung von Potentialflächen im Bereich Agriculture')
        print('Länge des Dataframes der Flächen vorher: '+str(before))
        print('Anzahl kleiner Flächen: '+str(count_small))
        print('Anzahl der durchgeführten Prozedur des Zusammenfassens: '+str(count_join))
        print('gelöschte Flächen (not joined): '+str(count_delete))
        print('Länge des Dataframes der Flächen danach: '+str(len(potentials_agri)))
        print(' ')
            
        # calculate new areas
        potentials_rora['area'] = potentials_rora.area
        potentials_agri['area'] = potentials_agri.area

        # check intersection of potential areas

        ### counting variables
        agri_vorher = len(potentials_agri)

        # if areas intersect, keep road & railway potential areas and drop agricultural ones
        overlay = gpd.sjoin(potentials_rora, potentials_agri)
        o = overlay['index_right']
        o.drop_duplicates(inplace=True)
        for i in range(0,len(o)):
            index=o.iloc[i]
            potentials_agri.drop([index], inplace=True)

        ### examination
        print(' ')
        print('Überprüfung der Funktion zur Meidung der Intersection von Potentialflächen:')
        print('Länge potentials_agri vorher: '+str(agri_vorher))
        print('Anzahl der auftretenden Fälle: '+str(len(o)))
        print('Länge potentials_agri nachher: '+str(len(potentials_agri)))
        print(' ')

        return potentials_rora, potentials_agri


    def select_pot_areas(mastr, potentials_pot):

        # select potential areas with existing pv plants
        # (potential areas intersect buffer around existing plants)
        
        # prepare dataframes to check intersection
        pvs = gpd.GeoDataFrame()
        pvs['geom'] = mastr['buffer'].copy()
        pvs.crs=3035
        pvs = pvs.set_geometry('geom')
        potentials = gpd.GeoDataFrame()
        potentials['geom'] = potentials_pot['geom'].copy()
        potentials.crs=3035
        potentials = potentials.set_geometry('geom')
        
        # check intersection of potential areas with exisiting PVs (MaStR)
        overlay = gpd.sjoin(pvs, potentials)
        o = overlay['index_right']
        o.drop_duplicates(inplace=True)
               
        # define selected potentials areas
        pot_sel = potentials_pot.copy()
        pot_sel['selected'] = pd.Series()
        pot_sel['voltage_level'] = pd.Series(dtype=int)
        for i in range(0,len(o)):
            index_pot = o.iloc[i]
            pot_sel['selected'].loc[index_pot] = True
            # get voltage level of existing PVs
            index_pv = o.index[i]
            pot_sel['voltage_level'] = mastr['voltage_level'].loc[index_pv]
        pot_sel = pot_sel[pot_sel['selected']==True]
        pot_sel.drop('selected', axis=1, inplace=True)

        return pot_sel


    def build_pv(pv_pot, pow_per_area):

        # build pv farms in selected areas

        # calculation of centroids
        pv_pot['centroid'] = pv_pot['geom'].centroid

        # calculation of power in kW
        pv_pot['installed capacity in kW'] = pd.Series()
        pv_pot['installed capacity in kW'] = pv_pot['area']*pow_per_area

        # check for maximal capacity for PV ground mounted
        limit_cap = 120000 # in kW
        pv_pot['installed capacity in kW'] = pv_pot['installed capacity in kW'].apply(
            lambda x: x if x < limit_cap else limit_cap)

        return pv_pot


    def adapt_grid_level(pv_pot, max_dist_hv, con):

        # divide dataframe in MV and HV
        pv_pot_mv = pv_pot[pv_pot['voltage_level'] == 5]
        pv_pot_hv = pv_pot[pv_pot['voltage_level'] == 4]

        # check installed capacity in MV

        max_cap_mv = 5500 # in kW

        # find PVs which need to be HV or to have reduced capacity
        pv_pot_mv_to_hv = pv_pot_mv[pv_pot_mv['installed capacity in kW'] > max_cap_mv]

        if len(pv_pot_mv_to_hv) > 0:

            # import data for HV substations

            sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
            hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col= "point")
            hvmv_substation = hvmv_substation.to_crs(3035)
            hvmv_substation['voltage'] = hvmv_substation['voltage'].apply(
                lambda x: int(x.split(';')[0]))
            hv_substations = hvmv_substation[hvmv_substation['voltage'] >= 110000]
            hv_substations = hv_substations.unary_union # join all the hv_substations

            # check distance to HV substations of PVs with too high installed capacity for MV

            # calculate distance to substations
            pv_pot_mv_to_hv['dist_to_HV'] = pv_pot_mv_to_hv['geom'].to_crs(3035).distance(hv_substations)

            # adjust grid level and keep capacity if transmission lines are close
            pv_pot_mv_to_hv = pv_pot_mv_to_hv[pv_pot_mv_to_hv['dist_to_HV'] <= max_dist_hv]
            pv_pot_mv_to_hv = pv_pot_mv_to_hv.drop(columns=['dist_to_HV'])
            pv_pot_hv = pv_pot_hv.append(pv_pot_mv_to_hv)

            # delete PVs which are now HV from MV dataframe
            for index, pot in pv_pot_mv_to_hv.iterrows():
                pv_pot_mv = pv_pot_mv.drop([index])
            pv_pot_hv['voltage_level'] = 4

            # keep grid level adjust capacity if transmission lines are too far
            pv_pot_mv['installed capacity in kW'] = pv_pot_mv['installed capacity in kW'].apply(
                lambda x: x if x < max_cap_mv else max_cap_mv)
            pv_pot_mv['voltage_level'] = 5

            pv_pot = pv_pot_mv.append(pv_pot_hv)

        return pv_pot


    def build_additional_pv(potentials, pv, pow_per_area, con):

        # get MV grid districts
        sql = "SELECT subst_id, geom FROM grid.mv_grid_districts"
        distr = gpd.GeoDataFrame.from_postgis(sql, con)
        distr = distr.set_index("subst_id")

        # identify potential areas where there are no PV parks yet
        for index, pv in pv.iterrows():
            potentials = potentials.drop([index])

        # aggregate potential area per MV grid district
        pv_per_distr=gpd.GeoDataFrame()
        pv_per_distr['geom'] = distr['geom'].copy()
        centroids = potentials.copy()
        centroids['geom'] = centroids['geom'].centroid

        overlay = gpd.sjoin(centroids, distr)
        
        ### examine potential area per grid district  
        anz = len(overlay)
        anz_distr = len(overlay['index_right'].unique())
        size = 137500 # m2 Fläche für > 5,5 MW: (5500 kW / (0,04 kW/m2))
        anz_big = len(overlay[overlay['area']>=size])
        anz_small = len(overlay[overlay['area']<size])
        
        print(' ')
        print('Untersuchung der (übrigen) Potentialflächen in den MV Grid Districts: ')
        print('Anzahl der Potentialflächen: '+str(anz))
        print(' -> verteilt über '+str(anz_distr)+' Districts')
        print('Anzahl der Flächen mit einem Potential >= 5,5 MW: '+str(anz_big))
        print('Anzahl der Flächen mit einem Potential < 5,5 MW: '+str(anz_small))
        print(' ')
        ###

        for index, dist in distr.iterrows():
            pots = overlay[overlay['index_right']==index]['geom'].index
            p = gpd.GeoSeries(index=pots)
            for i in pots:
                p.loc[i]=potentials['geom'].loc[i]
            pv_per_distr['geom'].loc[index] = p.unary_union

        # calculate area per MV grid district and linearly distribute needed capacity considering pow_per_area
        pv_per_distr['area'] = pv_per_distr['geom'].area
        pv_per_distr['installed capacity in kW'] = pv_per_distr['area']*pow_per_area

        # calculate centroid
        pv_per_distr['centroid'] = pv_per_distr['geom'].centroid
        
        return pv_per_distr


    def check_target(pv_rora_i, pv_agri_i, potentials_rora_i, potentials_agri_i, target_power, pow_per_area, con):

        # sum overall installed capacity for MV and HV

        total_pv_power = pv_rora_i['installed capacity in kW'].sum() + pv_agri_i['installed capacity in kW'].sum()

        pv_per_distr_i= gpd.GeoDataFrame()

        # check target value

        ###
        print(' ')
        print('Installierte Kapazität auf Flächen existierender PV-Parks (Bestandsflächen): '+str(total_pv_power/1000)+' MW')

        # linear scale farms to meet target if sum of installed capacity is too high
        if total_pv_power > target_power:

                scale_factor = target_power/total_pv_power
                pv_rora_i['installed capacity in kW'] = pv_rora_i['installed capacity in kW'] * scale_factor
                pv_agri_i['installed capacity in kW'] = pv_agri_i['installed capacity in kW'] * scale_factor

                ###
                print('Ausweitung existierender PV-Parks auf Potentialflächen zur Erreichung der Zielkapazität ist ausreichend.')
                print('Installierte Leistung ist größer als der Zielwert, es wird eine Skalierung vorgenommen:')
                print('Saklierungsfaktor: '+str(scale_factor))
                
        # build new pv parks if sum of installed capacity is below target value
        elif total_pv_power < target_power:

            rest_cap = target_power - total_pv_power

            ###
            print('Ausweitung existierender PV-Parks auf Potentialflächen zur Erreichung der Zielkapazität NICHT ausreichend:')
            print('Restkapazität: '+str(rest_cap/1000)+' MW')
            print('Restkapazität wird zunächst über übrige Potentialflächen Road & Railway verteilt.')
            print(datetime.datetime.now())

            # build pv parks in potential areas road & railway
            pv_per_distr_i = build_additional_pv(potentials_rora_i, pv_rora_i, pow_per_area, con)
            # change index to add different Dataframes in the end
            pv_per_distr_i['grid_district']=pv_per_distr_i.index.copy()
            pv_per_distr_i.index = range(0,len(pv_per_distr_i))
            # delete empty grid districts
            index_names = pv_per_distr_i[pv_per_distr_i['installed capacity in kW'] == 0.0 ].index
            pv_per_distr_i.drop(index_names,inplace=True)

            if pv_per_distr_i['installed capacity in kW'].sum() > rest_cap:
                scale_factor = rest_cap/pv_per_distr_i['installed capacity in kW'].sum()
                pv_per_distr_i['installed capacity in kW'] = pv_per_distr_i['installed capacity in kW'] * scale_factor

                ###
                print('Restkapazität ist mit dem Skalierungsfaktor '+str(scale_factor)+' über übrige Potentialflächen Road & Railway verteilt.')

            # build pv parks on potential areas ariculture if still necessary
            elif pv_per_distr_i['installed capacity in kW'].sum() < rest_cap:

                rest_cap = target_power - total_pv_power

                ###
                print('Verteilung über Potentialflächen Road & Railway zur Erreichung der Zielkapazität NICHT ausreichend:')
                print('Restkapazität: '+str(rest_cap/1000)+' MW')
                print('Restkapazität wird über übrige Potentialflächen Agriculture verteilt.')
                print(datetime.datetime.now())

                pv_per_distr_i_2 = build_additional_pv(potentials_agri_i, pv_agri_i, pow_per_area, con)
                # change index to add different Dataframes in the end
                pv_per_distr_i_2['grid_district']=pv_per_distr_i_2.index
                pv_per_distr_i_2.index = range(len(pv_per_distr_i),2*len(pv_per_distr_i))
                # delete empty grid districts
                index_names = pv_per_distr_i_2[pv_per_distr_i_2['installed capacity in kW'] == 0.0 ].index
                pv_per_distr_i_2.drop(index_names,inplace=True)
                pv_per_distr_i.append(pv_per_distr_i_2)

                if pv_per_distr_i['installed capacity in kW'].sum() > rest_cap:
                    scale_factor = rest_cap/pv_per_distr_i['installed capacity in kW'].sum()
                    pv_per_distr_i['installed capacity in kW'] = pv_per_distr_i['installed capacity in kW'] * scale_factor

                    ###
                    print('Restkapazität ist mit dem Skalierungsfaktor '+str(scale_factor)+' über übrige Potentialflächen Road & Railway und Agriculture verteilt.')
    
            # assign grid level to pv_per_distr
            v_lvl = pd.Series(dtype=int, index=pv_per_distr_i.index)
            for index, distr in pv_per_distr_i.iterrows():
                if distr['installed capacity in kW'] > 5500: # > 5 MW
                    v_lvl[index] = 4
                else:
                    v_lvl[index] = 5
            pv_per_distr_i['voltage_level'] = v_lvl
    
            # new overall installed capacity
            total_pv_power = pv_rora_i['installed capacity in kW'].sum() + \
            pv_agri_i['installed capacity in kW'].sum() + \
            pv_per_distr_i['installed capacity in kW'].sum()
            
            ###
            print('Installierte Leistung der PV-Parks insgesamt: '+str(total_pv_power/1000)+' MW')
            print(' ')
    
        return pv_rora_i, pv_agri_i, pv_per_distr_i


    def run_methodology():


        ### PARAMETERS ###

        # TODO: change to parameters, add explanation

        con = db.engine()

        path = ''

        # assumption for areas of existing pv farms and power of new built pv farms
        pow_per_area = 0.04 # kW per m²

        # maximum distance for joining of potential areas (only small ones to big ones)
        join_buffer = 10 # m

        # assumption for maximum distance of park with hv-power to next substation
        max_dist_hv = 20000 # m

        ### PARAMETERS ###
        
        ###
        print(' ')
        print('MaStR-Data')
        print(datetime.datetime.now())
        print(' ')

        # MaStR-data: existing PV farms
        mastr = mastr_existing_pv(path, pow_per_area)        
        
        # files for depiction in QGis
        mastr['geometry'].to_file("MaStR_PVs.geojson", driver='GeoJSON',index=True)
        
        ###
        print(' ')
        print('potential area')
        print(datetime.datetime.now())
        print(' ')
        
        # database-data: potential areas for new PV farms
        potentials_rora, potentials_agri = potential_areas(con, join_buffer)               
        
        # files for depiction in QGis
        potentials_rora['geom'].to_file("potentials_rora.geojson", driver='GeoJSON',index=True)
        potentials_agri['geom'].to_file("potentials_agri.geojson", driver='GeoJSON',index=True)

        ###
        print(' ')
        print('select potentials area')
        print(datetime.datetime.now())
        print(' ')
        
        # select potential areas with existing PV farms to build new PV farms
        pv_rora = select_pot_areas(mastr, potentials_rora)
        pv_agri = select_pot_areas(mastr, potentials_agri)

        ###
        print(' ')
        print('build PV parks where there is PV ground mounted already (-> MaStR) on potential area')
        print(datetime.datetime.now())
        print(' ')
        
        # build new PV farms
        pv_rora = build_pv(pv_rora, pow_per_area)
        pv_agri = build_pv(pv_agri, pow_per_area)
        
        ### 
        print(' ')
        print('adapt grid level of PV parks')
        print(datetime.datetime.now())
        print(' ')
        
        # adapt grid level to new farms
        rora = adapt_grid_level(pv_rora, max_dist_hv, con)
        agri = adapt_grid_level(pv_agri, max_dist_hv, con)

        ###
        print(' ')
        print('check target value and build more PV parks on potential area if necessary')
        print(datetime.datetime.now())
        print(' ')
        
        # 1) scenario: eGon2035
        
        ###
        print(' ')
        print('scenario: eGon2035')
        print(' ')
        
        # German states
        sql = "SELECT geometry as geom, nuts FROM boundaries.vg250_lan"
        states = gpd.GeoDataFrame.from_postgis(sql, con)
        
        # assumption for target value of installed capacity 
        sql = "SELECT capacity,scenario_name,nuts FROM supply.egon_scenario_capacities WHERE carrier='solar'"
        target = pd.read_sql(sql,con)
        target = target[target['scenario_name']=='eGon2035']
        nuts = np.unique(target['nuts'])
        
        # initialize final dataframe
        pv_rora = gpd.GeoDataFrame()
        pv_agri = gpd.GeoDataFrame()
        pv_per_distr = gpd.GeoDataFrame()
        
        # prepare selection per state
        rora = rora.set_geometry('centroid')
        agri = agri.set_geometry('centroid')
        potentials_rora = potentials_rora.set_geometry('geom')
        potentials_agri = potentials_agri.set_geometry('geom')
        
        # check target value per state
        for i in nuts:
            target_power = target[target['nuts']==i]['capacity'].iloc[0] * 1000
            
            ###
            land =  target[target['nuts']==i]['nuts'].iloc[0] 
            print(' ')
            print('Bundesland (NUTS): '+land)
            print('target power: '+str(target_power/1000)+' MW')
            
            # select state
            state = states[states['nuts']==i]
            state = state.to_crs(3035)
            
            # select PVs in state
            rora_i = gpd.sjoin(rora, state)
            agri_i = gpd.sjoin(agri, state)
            rora_i.drop('index_right', axis=1, inplace=True)
            agri_i.drop('index_right', axis=1, inplace=True)
            rora_i.drop_duplicates(inplace=True)
            agri_i.drop_duplicates(inplace=True)
            
            # select potential area in state
            potentials_rora_i = gpd.sjoin(potentials_rora, state)
            potentials_agri_i = gpd.sjoin(potentials_agri, state)
            potentials_rora_i.drop('index_right', axis=1, inplace=True)
            potentials_agri_i.drop('index_right', axis=1, inplace=True)
            potentials_rora_i.drop_duplicates(inplace=True)
            potentials_agri_i.drop_duplicates(inplace=True)
            
            # check target value and adapt installed capacity if necessary
            rora_i, agri_i, distr_i = check_target(rora_i, agri_i, potentials_rora_i, potentials_agri_i, target_power, pow_per_area, con)
            if len(distr_i) > 0 :
                distr_i['nuts'] = target[target['nuts']==i]['nuts'].iloc[0] 
            
            ###
            rora_i_mv = rora_i[rora_i['voltage_level']==5]
            rora_i_hv = rora_i[rora_i['voltage_level']==4]
            agri_i_mv = agri_i[agri_i['voltage_level']==5]
            agri_i_hv = agri_i[agri_i['voltage_level']==4]
            print('Untersuchung der Spannungslevel pro Bundesland:')
            print('a) PVs auf Potentialflächen Road & Railway: ')
            print('Insgesamt installierte Leistung: '+str(rora_i['installed capacity in kW'].sum()/1000)+' MW')
            print('Anzahl der PV-Parks: '+str(len(rora_i)))
            print(' - davon Mittelspannung: '+str(len(rora_i_mv)))
            print(' - davon Hochspannung: '+str(len(rora_i_hv)))
            print('b) PVs auf Potentialflächen Agriculture: ')
            print('Insgesamt installierte Leistung: '+str(agri_i['installed capacity in kW'].sum()/1000)+' MW')
            print('Anzahl der PV-Parks: '+str(len(agri_i)))
            print(' - davon Mittelspannung: '+str(len(agri_i_mv)))
            print(' - davon Hochspannung: '+str(len(agri_i_hv)))
            print('c) PVs auf zusätzlichen Potentialflächen pro MV-District: ')
            if len(distr_i) > 0:
                distr_i_mv = distr_i[distr_i['voltage_level']==5]
                distr_i_hv = distr_i[distr_i['voltage_level']==4]
                print('Insgesamt installierte Leistung: '+str(distr_i['installed capacity in kW'].sum()/1000)+' MW')
                print('Anzahl der PV-Parks: '+str(len(distr_i)))
                print(' - davon Mittelspannung: '+str(len(distr_i_mv)))
                print(' - davon Hochspannung: '+str(len(distr_i_hv)))
            else: 
                print(' -> zusätzlicher Ausbau nicht notwendig')
            print(' ')

            pv_rora = pv_rora.append(rora_i)
            pv_agri = pv_agri.append(agri_i)
            if len(distr_i) > 0:
                pv_per_distr = pv_per_distr.append(distr_i)
                
        ### create map to show distribution of installed capacity 
        
        # get MV grid districts
        sql = "SELECT subst_id, geom FROM grid.mv_grid_districts"
        distr = gpd.GeoDataFrame.from_postgis(sql, con)
        distr = distr.set_index("subst_id")
        
        # assign pv_per_distr-power to districts
        distr['capacity'] = pd.Series()
        for index, row in distr.iterrows():
            if index in np.unique(pv_per_distr['grid_district']):
                pv = pv_per_distr[pv_per_distr['grid_district']==index]
                x = pv['installed capacity in kW'].iloc[0]
                distr['capacity'].loc[index] = x
            else:
                distr['capacity'].loc[index] = 0
        distr['capacity'] = distr['capacity'] / 1000
        
        # add pv_rora- and pv_agri-power to district
        pv_rora = pv_rora.set_geometry('centroid')
        pv_agri = pv_agri.set_geometry('centroid')
        overlay_rora = gpd.sjoin(pv_rora,distr)
        overlay_agri = gpd.sjoin(pv_agri,distr)

        for index, row in distr.iterrows():
            o_rora = overlay_rora[overlay_rora['index_right']==index]
            o_agri = overlay_agri[overlay_agri['index_right']==index]
            cap_rora = o_rora['installed capacity in kW'].sum()/1000
            cap_agri = o_agri['installed capacity in kW'].sum()/1000
        distr['capacity'].loc[index] = distr['capacity'].loc[index] + cap_rora + cap_agri
           
        from matplotlib import pyplot as plt
        fig, ax = plt.subplots(1,1)
        distr.boundary.plot(linewidth=0.2,ax=ax, color='black')
        distr.plot(
            ax=ax,
            column = 'capacity',
            cmap='magma_r',
            legend=True,
            legend_kwds={'label': f"Installed capacity in MW",
                                 'orientation': "vertical"})
        plt.savefig('pv_per_distr_map.png', dpi=300)
        
        ###

        # 2) scenario: eGon100RE

        # TODO: eGon100RE-scenario
        
        '''
        # assumption for target value of installed capacity in Germany per scenario
        sql = "SELECT capacity,scenario_name FROM supply.egon_scenario_capacities WHERE carrier='solar'"
        target_power = (pd.read_sql(sql,con))
        target_power = target_power[target_power['scenario_name']=='eGon100RE']
        target_power = target_power['capacity'].sum() * 1000
        
        ###
        print(' ')
        print('scenario: eGon100RE')
        print('target power: '+str(target_power)+' kW')
        print(' ')
        
        # check target value and adapt installed capacity if necessary
        pv_rora_100RE, pv_agri_100RE, pv_per_distr_100RE = check_target(rora, agri, potentials_rora, potentials_agri, target_power, pow_per_area, con)
        '''

        return pv_rora, pv_agri, pv_per_distr #, pv_rora_100RE, pv_agri_100RE, pv_per_distr_100RE

    def pv_parks(pv_rora, pv_agri, pv_per_distr, scenario_name):

            # prepare dataframe for integration in supply.egon_power_plants

            # change indices to sum up Dataframes in the end
            pv_rora['pot_idx'] = pv_rora.index
            pv_rora.index = range(0,len(pv_rora))
            pv_agri['pot_idx'] = pv_agri.index
            l1 = len(pv_rora)+len(pv_agri)
            pv_agri.index = range(len(pv_rora), l1)
            l2 = l1 + len(pv_per_distr)
            pv_per_distr.index = range(l1,l2)

            pv_parks = gpd.GeoDataFrame(index=range(0,l2))

            # electrical capacity in MW
            cap = pv_rora['installed capacity in kW'].append(pv_agri['installed capacity in kW'])
            cap = cap.append(pv_per_distr['installed capacity in kW'])
            cap = cap/1000
            pv_parks['el_capacity'] = cap

            # voltage level
            lvl = pv_rora['voltage_level'].append(pv_agri['voltage_level'])
            lvl = lvl.append(pv_per_distr['voltage_level'])
            pv_parks['voltage_level'] = lvl

            # centroids
            cen = pv_rora['centroid'].append(pv_agri['centroid'])
            cen = cen.append(pv_per_distr['centroid'])
            pv_parks = pv_parks.set_geometry(cen)
            #to_crs(4326)

            # integration in supply.egon_power_plants

            con = db.engine()

            # maximum ID in egon_power_plants
            sql = "SELECT MAX(id) FROM supply.egon_power_plants"
            max_id = pd.read_sql(sql,con)
            max_id = max_id['max'].iat[0]
            if max_id == None: 
                max_id = 1
            
            pv_park_id = max_id+1

            # Copy relevant columns from pv_parks
            insert_pv_parks = pv_parks[
                ['el_capacity', 'voltage_level', 'geometry']]

            # Set static column values
            insert_pv_parks['carrier'] = 'solar'
            insert_pv_parks['chp'] = False
            insert_pv_parks['th_capacity'] = 0
            insert_pv_parks['scenario'] = scenario_name

            # Change name and crs of geometry column
            insert_pv_parks = insert_pv_parks.rename(
                {'geometry':'geom'}, axis=1).set_geometry('geom').to_crs(4326)

            # Reset index
            insert_pv_parks.index = pd.RangeIndex(
                start=pv_park_id,
                stop=pv_park_id+len(insert_pv_parks),
                name='id')

            # Insert into database
            insert_pv_parks.reset_index().to_postgis('egon_power_plants',
                                       schema='supply',
                                       con=db.engine(),
                                       if_exists='append')


            return pv_parks

    pv_rora, pv_agri, pv_per_distr = run_methodology() 
    
    # pv_rora_100RE, pv_agri_100RE, pv_per_distr_100RE
    
    ###
    pv_rora.to_csv('pv_rora.csv',index=True)
    pv_agri.to_csv('pv_agri.csv',index=True)
    pv_rora['centroid'].to_file("PVs_rora.geojson", driver='GeoJSON',index=True)
    pv_agri['centroid'].to_file("PVs_agri.geojson", driver='GeoJSON',index=True)
    if len(pv_per_distr) > 0:
        pv_per_distr.to_csv('pv_per_distr.csv',index=True)
        pv_per_distr['centroid'].to_file("PVs_per_distr.geojson", driver='GeoJSON',index=True)
        pv_per_distr_mv = pv_per_distr[pv_per_distr['voltage_level']==5]
        pv_per_distr_hv = pv_per_distr[pv_per_distr['voltage_level']==4]
    pv_rora_mv = pv_rora[pv_rora['voltage_level']==5]
    pv_rora_hv = pv_rora[pv_rora['voltage_level']==4]
    pv_agri_mv = pv_agri[pv_agri['voltage_level']==5]
    pv_agri_hv = pv_agri[pv_agri['voltage_level']==4]

    print(' ')
    print('Untersuchung der Spannungslevel (gesamt):')
    print('a) PVs auf Potentialflächen Road & Railway: ')
    print('Insgesamt installierte Leistung: '+str(pv_rora['installed capacity in kW'].sum()/1000)+' MW')
    print('Anzahl der PV-Parks: '+str(len(pv_rora)))
    print(' - davon Mittelspannung: '+str(len(pv_rora_mv)))
    print(' - davon Hochspannung: '+str(len(pv_rora_hv)))
    print('b) PVs auf Potentialflächen Agriculture: ')
    print('Insgesamt installierte Leistung: '+str(pv_agri['installed capacity in kW'].sum()/1000)+' MW')
    print('Anzahl der PV-Parks: '+str(len(pv_agri)))
    print(' - davon Mittelspannung: '+str(len(pv_agri_mv)))
    print(' - davon Hochspannung: '+str(len(pv_agri_hv)))
    print('c) PVs auf zusätzlichen Potentialflächen pro MV-District: ')
    if len(pv_per_distr) > 0: 
        print('Insgesamt installierte Leistung: '+str(pv_per_distr['installed capacity in kW'].sum()/1000)+' MW')
        print('Anzahl der PV-Parks: '+str(len(pv_per_distr)))
        print(' - davon Mittelspannung: '+str(len(pv_per_distr_mv)))
        print(' - davon Hochspannung: '+str(len(pv_per_distr_hv)))
    else: 
        print(' -> zusätzlicher Ausbau nicht notwendig')
    print(' ')
   
    '''   
    pv_rora_100RE.to_csv('pv_rora_100RE.csv',index=True)
    pv_agri_100RE.to_csv('pv_agri_100RE.csv',index=True)
    if len(pv_per_distr_100RE) > 0:
        pv_per_distr_100RE.to_csv('pv_per_distr_100RE.csv',index=True)
    '''
    
    pv_parks = pv_parks(pv_rora, pv_agri, pv_per_distr, 'eGon2035')
    
    # pv_parks_100RE = pv_parks(pv_rora_100RE, pv_agri_100RE, pv_per_distr_100RE, 'eGon100RE')
    
    # TODO: add dataframes for scenario eGon100RE

    return pv_parks #, pv_parks_100RE












