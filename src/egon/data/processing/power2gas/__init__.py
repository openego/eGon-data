# -*- coding: utf-8 -*-
"""
Definition of the power-to-gas installations
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.ops as sp_ops

from shapely import geometry
from geoalchemy2.types import Geometry
from scipy.spatial import cKDTree
from pyproj import Transformer
from egon.data import db
from egon.data.importing.gas_grid import next_id


def insert_power2gas():
    sql_AC = "SELECT bus_id, geom FROM grid.egon_pf_hv_bus WHERE carrier = 'AC';"
    sql_gas = "SELECT bus_id, version, scn_name, geom FROM grid.egon_pf_hv_bus WHERE carrier = 'gas';" 
    
    # Connect to local database
    engine = db.engine()
    
    # Select next id value
    new_id = next_id('link')
        
    gdf_AC = gpd.read_postgis(sql_AC, engine, crs=4326)
    gdf_gas = gpd.read_postgis(sql_gas, engine, crs=4326)

    
    if gdf_AC.size == 0:
        print("WARNING: No data returned in grid.egon_pf_hv_bus WHERE carrier = 'AC'")
    elif gdf_gas.size == 0: 
        print("WARNING: No data returned in grid.egon_pf_hv_bus WHERE carrier = 'gas'")
    
    print(gdf_AC)
    print(gdf_gas)
       
    n_gas = np.array(list(gdf_gas.geometry.apply(lambda x: (x.x, x.y))))
    n_AC = np.array(list(gdf_AC.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(n_AC)
    dist, idx = btree.query(n_gas, k=1)
    gd_AC_nearest = gdf_AC.iloc[idx].rename(columns={'bus_id': 'bus1','geom': 'geom_AC'}).reset_index(drop=True)
    gdf = pd.concat(
        [
            gdf_gas.reset_index(drop=True),
            gd_AC_nearest,
            pd.Series(dist, name='dist')
        ], 
        axis=1)
    
    gdf = gdf.rename(columns={'bus_id': 'bus0','geom': 'geom_gas'})
    my_transformer = Transformer.from_crs('EPSG:4326', 'EPSG:3857', always_xy=True)
        
    geom = []
    topo = []
    length = []
    lines = []
    p_nom_max = []
    for index, row in gdf.iterrows():
        line = geometry.LineString([row['geom_gas'], row['geom_AC']])
        topo.append(line)
        lines.append(line)
        geom.append(geometry.MultiLineString(lines))
        lines.pop()
        l = (sp_ops.transform(my_transformer.transform, line).length)/1000
        length.append(l)
        if l > 0.5:
            p_nom_max.append(1)
        else:
            p_nom_max.append(float('Inf'))
        
    gdf['geom'] = geom
    gdf = gdf.set_geometry('geom', crs=4326)
    
    gdf['p_nom_max'] = p_nom_max
    gdf['topo'] = topo
    gdf['length'] = length
    gdf['link_id'] = range(new_id, new_id + len(n_gas)) 
    gdf['carrier'] = 'power-to-gas'
    gdf['efficiency_fixed'] = 0.8   # H2 electrolysis - Brown et al. 2018 "SynergSynergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4 
    gdf['p_nom'] = 0
    gdf['p_nom_extendable'] = True
    gdf['capital_cost'] = 350000  # H2 electrolysis [€/MW] Brown et al. 2018 "SynergSynergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    

######################################## Do we also consider methanisation? ########################################

    # capital_cost_methanation_DAC = 1000000    # [€/MW] Brown et al. 2018 "SynergSynergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    # ptch4_efficiency = 0.6 * 0.8              # [€/MW] Brown et al. 2018 "SynergSynergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    
    gdf = gdf.drop(columns=['geom_gas', 'geom_AC', 'dist']) # 
    print(gdf)
    gdf.to_csv('NN.csv')
    
    # Insert data to db
    gdf.to_postgis('egon_pf_hv_gas_link',
                          engine,
                          schema = 'grid',
                          index = False,
                          if_exists = 'replace',
                          dtype = { 'geom': Geometry(), 'topo': Geometry()})
    
    db.execute_sql(
        """
    DELETE FROM grid.egon_pf_hv_link WHERE "carrier" = 'power-to-gas';
    
    select UpdateGeometrySRID('grid', 'egon_pf_hv_gas_link', 'topo', 4326) ;
    
    INSERT INTO grid.egon_pf_hv_link (version, scn_name, link_id, bus0, 
                                              bus1, p_nom, p_nom_extendable, capital_cost,length,
                                              geom, topo, efficiency_fixed, carrier, p_nom_max)
    SELECT
    version, scn_name, link_id, bus0, 
        bus1, p_nom, p_nom_extendable, capital_cost, length, 
        geom, topo, efficiency_fixed, carrier, p_nom_max
    FROM grid.egon_pf_hv_gas_link;
        
    DROP TABLE grid.egon_pf_hv_gas_link;
        """)