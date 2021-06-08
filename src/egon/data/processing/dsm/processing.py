
from egon.data import db
import psycopg2
import pandas as pd
import numpy as np

def dsm_cts_processing():
    
    
    def data_import(con):

        # import load data
        
        sql = "SELECT scn_name, load_id, bus, carrier FROM grid.egon_pf_hv_load"
        l = pd.read_sql_query(sql, con)
        
        sql = "SELECT scn_name, load_id, p_set FROM grid.egon_pf_hv_load_timeseries"
        t = pd.read_sql_query(sql, con)
        
        # select CTS load 
    
        l = l[l['carrier']=='AC-cts']
        load_ids = list(l['load_id'])
        idx = pd.Series(t.index)
        for index, row in t.iterrows():
            if row['load_id'] in load_ids:
               idx.drop(index,inplace=True) 
        t.drop(index=idx, axis=1, inplace=True)
        
        # TODO: Selektion der CTS-Daten sowie Nutzung der Indizes korrekt? einfacher möglich?
        
        # get relevant data
        
        dsm = pd.DataFrame(index=t.index)
        
        # TODO: eindeutige Identifikation mitschleppen, zB original Bus?
        
        dsm['load_id'] = t['load_id'].copy()
        dsm['scn_name'] = t['scn_name'].copy()
        dsm['load_p_set'] = t['p_set'].copy()
        # q_set is empty
        
        dsm['bus'] = pd.Series(dtype=int)
        for index, row in dsm.iterrows():
            load_id = row['load_id']
            scn_name = row['scn_name']
            x = l[l['load_id']==load_id]
            dsm['bus'].loc[index] = x[x['scn_name']==scn_name]['bus'].iloc[0]
            
        return dsm
    
    def calculate_potentials(cts_share, s_flex, s_util, s_inc, s_dec, dsm):
    
        # timeseries for air conditioning, cooling and ventilation out of CTS-data
    
        # calculate share of timeseries
        timeseries = dsm['load_p_set']
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item)*cts_share)
            timeseries.loc[index] = share
            
        # calculate scheduled load L(t)
        
        scheduled_load = timeseries.copy()
        
        for index, liste in scheduled_load.iteritems():
            share = []
            for item in liste:
                share.append(item*s_flex)
            scheduled_load.loc[index] = share
            
        # calculate maximum capacity Lambda
        
        # calculate energy annual requirement
        energy_annual = pd.Series(index=timeseries.index, dtype=float)
        for index, liste in timeseries.iteritems():
            energy_annual.loc[index] = sum(liste)
        
        # calculate Lambda
        lam = ((energy_annual * s_flex) / (8760 * s_util))  
        
        # calculation of P_max and P_min
        
        # P_max 
        p_max = scheduled_load.copy()
        for index, liste in scheduled_load.iteritems():
            lamb = lam.loc[index]
            p = []
            for item in liste:
                p.append(lamb*s_inc-item)
            p_max.loc[index] = p
            
        # P_min
        p_min = scheduled_load.copy()
        for index, liste in scheduled_load.iteritems():
            lamb = lam.loc[index]
            p = []
            for item in liste:
                p.append(-(item-lamb*s_dec))
            p_min.loc[index] = p
            
        # calculation of E_max and E_min
        
        # E_max
        e_max = scheduled_load.copy()
        
        # E_min
        e_min = scheduled_load.copy()
        for index, liste in scheduled_load.iteritems():
            e = []
            for i in range(0,len(liste)):
                if i == 0:
                    e.append(liste[len(liste)-1])
                else:
                    e.append(liste[i-1])
            e_min.loc[index] = e
            
        # TODO: Alternative zu Arbeit mit Timeseries als Listen?
        
        # TODO: Überprüfung der Warnungen...
            
        return p_max, p_min, e_max, e_min
    
    def create_dsm_components(con, p_max, p_min, e_max, e_min, dsm):
        
        # add DSM-buses to "original" buses
        
        dsm_buses = pd.DataFrame(index=dsm.index)
        dsm_buses['original_bus'] = dsm['bus'].copy()
        dsm_buses['scn_name'] = dsm['scn_name'].copy()
        
        # get original buses and add copy relevant information
        sql = "SELECT bus_id, v_nom, scn_name, x, y, geom FROM grid.egon_pf_hv_bus"
        original_buses = pd.read_sql_query(sql, con)
        
        # copy v_nom, x, y and geom 
        v_nom = pd.Series(index=dsm_buses.index)
        x = pd.Series(index=dsm_buses.index)
        y = pd.Series(index=dsm_buses.index)
        geom = pd.Series(index=dsm_buses.index)
        
        originals = dsm_buses['original_bus'].unique()
        for i in originals:
            o_bus = original_buses[original_buses['bus_id']==i]
            dsm_bus = dsm_buses[dsm_buses['original_bus']==i]
            v_nom[dsm_bus.index[0]] = o_bus.iloc[0]['v_nom']
            x[dsm_bus.index[0]] = o_bus.iloc[0]['x']
            y[dsm_bus.index[0]] = o_bus.iloc[0]['y']
            geom[dsm_bus.index[0]] = o_bus.iloc[0]['geom']
            v_nom[dsm_bus.index[1]] = o_bus.iloc[0]['v_nom']
            x[dsm_bus.index[1]] = o_bus.iloc[0]['x']
            y[dsm_bus.index[1]] = o_bus.iloc[0]['y']
            geom[dsm_bus.index[1]] = o_bus.iloc[0]['geom']
            
        dsm_buses['v_nom'] = v_nom
        dsm_buses['x'] = x
        dsm_buses['y'] = y
        dsm_buses['geom'] = geom
        
        # new bus_ids for DSM-buses
        max_id = original_buses['bus_id'].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1 
        bus_id = pd.Series(index=dsm_buses.index)
        bus_id.iloc[0:int((len(bus_id)/2))] = range(dsm_id,int((dsm_id+len(dsm_buses)/2)))
        bus_id.iloc[int((len(bus_id)/2)):len(bus_id)] = range(dsm_id,int((dsm_id+len(dsm_buses)/2)))
        dsm_buses['bus_id'] = bus_id            
                      
        # TODO: in Tabellen schreiben
        
        # add links from "orignal" buses to DSM-buses
        
        dsm_links = pd.DataFrame(index=dsm_buses.index)
        dsm_links['original_bus'] = dsm_buses['original_bus'].copy()
        dsm_links['dsm_bus'] = dsm_buses['bus_id'].copy()
        dsm_links['scn_name'] = dsm_buses['scn_name'].copy()
        
        # set link_id 
        sql = "SELECT link_id FROM grid.egon_pf_hv_link"
        max_id = pd.read_sql_query(sql, con)
        max_id = max_id['link_id'].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1 
        link_id = pd.Series(index=dsm_buses.index)
        link_id.iloc[0:int((len(link_id)/2))] = range(dsm_id,int((dsm_id+len(dsm_links)/2)))
        link_id.iloc[int((len(link_id)/2)):len(link_id)] = range(dsm_id,int((dsm_id+len(dsm_links)/2)))
        dsm_links['link_id'] = link_id   
        
        # timeseries
        dsm_links['p_nom'] = 1
        dsm_links['p_min'] = p_min
        dsm_links['p_max'] = p_max
        
        # TODO: in Tabellen schreiben
        
        # add stores
        
        dsm_stores = pd.DataFrame(index=dsm_buses.index)
        dsm_stores['bus'] = dsm_buses['bus_id'].copy()
        dsm_stores['scn_name'] = dsm_buses['scn_name'].copy()
        
        # set store_id 
        sql = "SELECT store_id FROM grid.egon_pf_hv_store"
        max_id = pd.read_sql_query(sql, con)
        max_id = max_id['store_id'].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1 
        store_id = pd.Series(index=dsm_buses.index)
        store_id.iloc[0:int((len(store_id)/2))] = range(dsm_id,int((dsm_id+len(dsm_stores)/2)))
        store_id.iloc[int((len(store_id)/2)):len(store_id)] = range(dsm_id,int((dsm_id+len(dsm_stores)/2)))
        dsm_stores['store_id'] = store_id  
        
        # timeseries
        dsm_stores['e_nom'] = 1
        dsm_stores['e_min'] = e_min
        dsm_stores['e_max'] = e_max
        
        return dsm_buses, dsm_links, dsm_stores
    
    def data_export(con, dsm_buses, dsm_links, dsm_stores):

        # dsm_buses
        
        insert_buses = pd.DataFrame(index=dsm_buses.index)
        insert_buses['version'] = '0.0.0'
        insert_buses['scn_name'] = dsm_buses['scn_name']
        insert_buses['bus_id'] = dsm_buses['bus_id']
        insert_buses['v_nom'] = dsm_buses['v_nom']
        insert_buses['carrier'] = 'dsm-cts'
        insert_buses['x'] = dsm_buses['x']
        insert_buses['y'] = dsm_buses['y']
        insert_buses['geom'] = dsm_buses['geom']
        #insert_buses = insert_buses.set_geometry('geom').to_crs(4326)
        
        # insert into database
        insert_buses.to_sql('egon_pf_hv_bus',
                                          con=db.engine(), 
                                          schema='grid',
                                          if_exists='append', 
                                          index=False)
        
        # dsm_links
        
        insert_links = pd.DataFrame(index=dsm_links.index)
        insert_links['version'] = '0.0.0'
        insert_links['scn_name'] = dsm_links['scn_name']
        insert_links['link_id'] = dsm_links['link_id']
        insert_links['bus0'] = dsm_links['original_bus']
        insert_links['bus1'] = dsm_links['dsm_bus']
        insert_links['carrier'] = 'dsm-cts'
        insert_links['p_nom'] = dsm_links['p_nom']
        
        # insert into database
        insert_links.to_sql('egon_pf_hv_link',
                                          con=db.engine(), 
                                          schema='grid',
                                          if_exists='append',
                                          index=False)
        
        insert_links_timeseries = pd.DataFrame(index=dsm_links.index)
        insert_links_timeseries['version'] = '0.0.0'
        insert_links_timeseries['scn_name'] = dsm_links['scn_name']
        insert_links_timeseries['link_id'] = dsm_links['link_id']
        insert_links_timeseries['p_min_pu'] = dsm_links['p_min']
        insert_links_timeseries['p_max_pu'] = dsm_links['p_max']
        
        # insert into database
        insert_links_timeseries.to_sql('egon_pf_hv_link_timeseries',
                                          con=db.engine(), 
                                          schema='grid',
                                          if_exists='append',
                                          index=False)
        
        # dsm_stores
        
        insert_stores = pd.DataFrame(index=dsm_stores.index)
        insert_stores['version'] = '0.0.0'
        insert_stores['scn_name'] = dsm_stores['scn_name']
        insert_stores['store_id'] = dsm_stores['store_id']
        insert_stores['bus'] = dsm_stores['bus']
        insert_stores['carrier'] = 'dsm-cts'
        insert_stores['e_nom'] = dsm_stores['e_nom']
        
        # insert into database
        insert_stores.to_sql('egon_pf_hv_store',
                                          con=db.engine(), 
                                          schema='grid',
                                          if_exists='append', 
                                          index=False)
        
        insert_stores_timeseries = pd.DataFrame(index=dsm_stores.index)
        insert_stores_timeseries['version'] = '0.0.0'
        insert_stores_timeseries['scn_name'] = dsm_stores['scn_name']
        insert_stores_timeseries['store_id'] = dsm_stores['store_id']
        insert_stores_timeseries['e_min_pu'] = dsm_stores['e_min']
        insert_stores_timeseries['e_max_pu'] = dsm_stores['e_max']
        
        # insert into database
        insert_stores_timeseries.to_sql('egon_pf_hv_store_timeseries',
                                          con=db.engine(), 
                                          schema='grid',
                                          if_exists='append',
                                          index=False)
    
    ### PARAMETERS ###
    
    #con = db.engine()
    
    '''l = pd.read_csv('egon.-Daten/grid_egon_pf_hv_load.csv')
    
    t = pd.read_csv('egon.-Daten/grid_egon_pf_hv_load_timeseries.csv')'''
    
    con = psycopg2.connect(host = "127.0.0.1",
                                   database = "SH",
                                   user = "egon",
                                   password = "data",
                                   port= 59734)
    
    # share of air conditioning, cooling and ventilation in CTS
    cts_share = 0.22
    
    # feasability factor (socio technical)
    s_flex = 0.5
    
    # average annual utilisation rate 
    s_util = 0.67
    
    # shiftable share of installed capacity (technial)
    s_inc = 1
    s_dec = 0
    
    # maximum shift duration
    # delta_t = 1
    # TODO: allgemein programmieren, sodass Parameter einfach änderbar
    
    ### PARAMETERS ###
    
    dsm = data_import(con)
    
    p_max, p_min, e_max, e_min = calculate_potentials(cts_share, s_flex, s_util, s_inc, s_dec, dsm)
    
    dsm_buses, dsm_links, dsm_stores = create_dsm_components(con, p_max, p_min, e_max, e_min, dsm)
    
    data_export(con, dsm_buses, dsm_links, dsm_stores)
    

