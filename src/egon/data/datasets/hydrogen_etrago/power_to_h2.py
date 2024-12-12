# -*- coding: utf-8 -*-
"""
Module containing the definition of the AC grid to H2 links

In this module the functions used to define and insert into the database
the links between H2 and AC buses are to be found.
These links are modelling:
  * Electrolysis (carrier name: 'power_to_H2'): technology to produce H2
    from AC
  * Fuel cells (carrier name: 'H2_to_power'): techonology to produce
    power from H2
  * Waste_heat usage (carrier name: 'power_to_Heat'): Components to use 
    waste heat as by-product from electrolysis
  * Oxygen usage (carrier name: 'power_to_O2'): Components to use 
    oxygen as by-product from elctrolysis
    
 
"""
import pandas as pd
import math
import geopandas as gpd
from itertools import count
from sqlalchemy import text
from shapely.geometry import MultiLineString, LineString, Point
from shapely.wkb import dumps
from egon.data import db, config
from egon.data.datasets.scenario_parameters import get_sector_parameters
from pathlib import Path
import numpy as np
from shapely.strtree import STRtree



def insert_power_to_h2_to_power():
    """
    Insert electrolysis and fuel cells capacities into the database.
    For electrolysis potential waste_heat- and oxygen-utilisation is 
    implemented if district_heating-/oxygen-demand is nearby electrolysis
    location

    The potentials for power-to-H2 in electrolysis and H2-to-power in
    fuel cells are created between each HVMV Substaion (or each AC_BUS related 
    to setting SUBSTATION) and closest H2-Bus (H2 and H2_saltcaverns) inside 
    buffer-range of 30km. 
    For oxygen-usage all WWTP within MV-district and buffer-range of 10km 
    is connected to relevant HVMV Substation
    For heat-usage closest central-heat-bus inner an dynamic buffer is connected 
    to relevant HVMV-Substation.
    
    All links are extendable. 

    This function inserts data into the database and has no return.


    """
<<<<<<< HEAD
    scenarios = config.settings()["egon-data"]["--scenarios"]  
    
    # General Constant Parameters  
    DATA_CRS = 4326  # default CRS
    METRIC_CRS = 32632  # demanded CRS
    DISCOUNT_RATE = 0.05  # to calculate annualized  cost
 
    # Power to H2 (Electricity & Electrolyser)
    ELEC_COST = 60  # [EUR/MWh]
    AC_TRANS = 17_500  # [EUR/MVA]
    AC_LIFETIME_CABLE = 25  # [year]
    AC_COST_CABLE = 800_000  # [EUR/km/MVA]
    ELZ_SEC = 50  # [kWh/kgH2] Electrolyzer Specific Energy Consumption
    ELZ_EFF = 33.33 / ELZ_SEC  # [%] H2 energy kWh/kgH2 / electricity input kWh/kgH2
    ELZ_FLH = 8760  # [hour] full load hours 		5217
    ELZ_LIFETIME_H = 85_000 / ELZ_FLH  # [Year] lifetime of stack [15 years]
    ELZ_LIFETIME_Y = 25  # [Year] lifetiem of ELZ system in [year]
    ELZ_CAPEX_SYSTEM = 504_000  # [EUR/MW]
    ELZ_CAPEX_STACK = 180_000 * 2  # [EUR/MW] to equivalent it with system lifetime
    ELZ_OPEX = (
        ELZ_CAPEX_SYSTEM + ELZ_CAPEX_STACK
    ) * 0.03  # [EUR/MW]	3% of total CAPEX per year
    H2_TO_POWER_EFF = 0.5  # as per existing postgres database
    H2_PRESSURE_ELZ = 30  # [bar]
    O2_PRESSURE_ELZ = 13  # [bar]
 
    # Power to Heat
    HEAT_RATIO = 0.2  # % heat ratio to hydrogen production
    HEAT_LIFETIME = 25  # [YEAR]
    HEAT_EFFICIENCY = 0.8805  # efficiency of transferring heat
    HEAT_COST_EXCHANGER = 25_000  # [EUR/MW/YEAR]  equipments except pipeline
    HEAT_COST_PIPELINE = 400_000  # [EUR/MWH/KM]
    HEAT_SELLING_PRICE = 21.6  # [EUR/MWh]
 
 
    # Power to O2 (Wastewater Treatment Plants)
    WWTP_SEC = {
        "c5": 29.6,
        "c4": 31.3,
        "c3": 39.8,
        "c2": 42.1,
    }  # [kWh/year] Specific Energy Consumption
    O2_O3_RATIO = 1.7  # [-] conversion of O2 to O3
    O2_H2_RATIO = 7.7  # [-] ratio of O2 to H2
    O2_PURE_RATIO = 20.95 / 100  # [-] ratio of pure oxygen to ambient air
    FACTOR_AERATION_EC = 0.6  # [%] aeration EC from total capacity of WWTP (PE)
    FACTOR_O2_EC = 0.8  # [%] Oxygen EC from total aeration EC
    O2_LIFETIME_PIPELINE = 25  # [Year]
    O2_EFFICIENCY = 0.9
    O2_PRESSURE_MIN = 2  # [bar]
    O2_COST_EQUIPMENT = 5000  # [Euro] equipments except pipeline
    O2_LIEFTIME_EQUIPMENT = 25  # [Year]
    MOLAR_MASS_O2 = 0.0319988  # [kg/mol]
 
    # H2 to Power (Hydrogen Pipeline)
    H2_PRESSURE_MIN = 29  # [bar]
    H2_LIFETIME_PIPELINE = 25  # [YEAR]
    H2_COST_PIPELINE = 25_000  # [EUR/MW]
    FUEL_CELL_EFF = 0.5  # %
    FUEL_CELL_COST = 1_084_000  # [EUR/MWe]
    FUEL_CELL_LIFETIME = 10  # [Year]
    PIPELINE_DIAMETER_RANGE = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]  # [m]
    TEMPERATURE = 15 + 273.15  # [Kelvin] degree + 273.15
    UNIVERSAL_GAS_CONSTANT = 8.3145  # [J/(mol·K)]
    MOLAR_MASS_H2 = 0.002016  # [kg/mol]
 
    H2 = "h2"
    WWTP = "wwtp"
    AC = "ac"
    H2GRID = "h2_grid"
    ACZONE_HVMV = "ac_zone_hvmv"
    ACZONE_EHV = "ac_zone_ehv"
    ACSUB_HVMV = "ac_sub_hvmv"
    ACSUB_EHV = "ac_sub_ehv"
    HEAT_BUS = "heat_point"
    HEAT_LOAD = "heat_load"
    HEAT_TIMESERIES = "heat_timeseries"
    H2_BUSES_CH4 = 'h2_buses_ch4' 
    AC_LOAD = 'ac_load'
    HEAT_AREA = 'heat_area'
 
    buffer_heat_factor= 1500  #625/3125 for worstcase/bestcase-Szeanrio
    max_buffer_heat= 12000 #5000/30000 for worstcase/bestcase-Szenario 
    Buffer = {
        "O2": 5000,  # m to define the radii between O2 to AC
        "H2_HVMV": 5000,  # m define the distance between H2 and reference points (AC/O2)
        "H2_EHV": 20000,
        "HVMV": 10000,
        "EHV": 20000,
        "HEAT": 5000,
    }  # m define the distance betweeen Heat and reference points (AC/O2)
    
    # connet to PostgreSQL database (to localhost)
    engine = db.engine()
    
    data_config = config.datasets()
    sources = data_config["PtH2_waste_heat_O2"]["sources"]
    targets = data_config["PtH2_waste_heat_O2"]["targets"]
    
    for SCENARIO_NAME in scenarios:
        scn_params_gas = get_sector_parameters("gas", SCENARIO_NAME)
        scn_params_elec = get_sector_parameters("electricity", SCENARIO_NAME)
        
        AC_TRANS = scn_params_elec["capital_cost"]["transformer_220_110"]  # [EUR/MW/YEAR]
        AC_COST_CABLE = scn_params_elec["capital_cost"]["ac_hv_cable"]   #[EUR/MW/km/YEAR]
        ELZ_CAPEX_SYSTEM = scn_params_gas["capital_cost"]["power_to_H2_system"]   # [EUR/MW/YEAR]
        ELZ_CAPEX_STACK = scn_params_gas["capital_cost"]["power_to_H2_stack"]  # [EUR/MW/YEAR]
        ELZ_OPEX = scn_params_gas["capital_cost"]["power_to_H2_OPEX"]  # [EUR/MW/YEAR]
        H2_COST_PIPELINE = scn_params_gas["capital_cost"]["H2_pipeline"]  #[EUR/MW/km/YEAR] 
        ELZ_EFF = scn_params_gas["efficiency"]["power_to_H2"] 
        
        HEAT_COST_EXCHANGER = scn_params_gas["capital_cost"]["Heat_exchanger"]  # [EUR/MW/YEAR]
        HEAT_COST_PIPELINE = scn_params_gas["capital_cost"]["Heat_pipeline"] # [EUR/MW/YEAR]  
      
        O2_PIPELINE_COSTS = scn_params_gas["O2_capital_cost"]   #[EUR/km/YEAR]
        O2_COST_EQUIPMENT = scn_params_gas["capital_cost"]["O2_components"]  #[EUR/MW/YEAR]
         
        FUEL_CELL_COST = scn_params_gas["capital_cost"]["H2_to_power"]   #[EUR/MW/YEAR]
        FUEL_CELL_EFF = scn_params_gas["efficiency"]["H2_to_power"] 
        FUEL_CELL_LIFETIME = scn_params_gas["lifetime"]["H2_to_power"]
                               
        
        
        def export_o2_buses_to_db(df):
            max_bus_id = db.next_etrago_id("bus")
            next_bus_id = count(start=max_bus_id, step=1)
            schema = targets['buses']['schema']
            table_name = targets['buses']['table']

            db.execute_sql(
                        f"DELETE FROM {schema}.{table_name} WHERE carrier = 'O2' AND scn_name='{SCENARIO_NAME}'"
                )
            df = df.copy(deep=True)
            result = []
            for _, row in df.iterrows():
                bus_id = next(next_bus_id)
                result.append(
                    {
                        "scn_name": SCENARIO_NAME,
                        "bus_id": bus_id,
                        "v_nom": "110",
                        "type": row["KA_ID"],
                        "carrier": "O2",
                        "x": row["longitude Kläranlage_rw"],
                        "y": row["latitdue Kläranlage_hw"],
                        "geom": dumps(
                            Point(
                                row["longitude Kläranlage_rw"], row["latitdue Kläranlage_hw"]
                            ),
                            srid=DATA_CRS,
                        ),
                        "country": "DE",
                    }
                )
            result_df = pd.DataFrame(result)
            result_df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        
        
        wwtp_spec = pd.read_csv(Path(".")/"WWTP_spec.csv")
        export_o2_buses_to_db(wwtp_spec)  # Call the function with the dataframe
        
        # dictionary of SQL queries
        queries = {
            WWTP: f"""
                    SELECT bus_id AS id, geom, type AS ka_id
                    FROM {sources["buses"]["schema"]}.{sources["buses"]["table"]}
                    WHERE carrier in ('O2') AND scn_name = '{SCENARIO_NAME}'
                    """,
            H2: f"""
                    SELECT bus_id AS id, geom 
                    FROM {sources["buses"]["schema"]}.{sources["buses"]["table"]}
                    WHERE carrier in ('H2_grid', 'H2')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND country = 'DE'
                    """,
            H2GRID: f"""
                    SELECT link_id, geom, bus0, bus1
                    FROM {sources["links"]["schema"]}.{sources["links"]["table"]}
                    WHERE carrier in ('H2_grid') AND scn_name  = '{SCENARIO_NAME}'
                    """,
            AC: f"""
                    SELECT bus_id AS id, geom
                    FROM {sources["buses"]["schema"]}.{sources["buses"]["table"]}
                    WHERE carrier in ('AC')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND v_nom = '110'
                    """,
            ACSUB_HVMV: f"""
                    SELECT bus_id AS id, point AS geom
                    FROM {sources["hvmv_substation"]["schema"]}.{sources["hvmv_substation"]["table"]}
                    """,
            ACSUB_EHV:f"""
                    SELECT bus_id AS id, point AS geom
                    FROM {sources["ehv_substation"]["schema"]}.{sources["ehv_substation"]["table"]}
                    """,
            ACZONE_HVMV: f"""
                    SELECT bus_id AS id, ST_Transform(geom, 4326) as geom
                    FROM {sources["mv_districts"]["schema"]}.{sources["mv_districts"]["table"]}
                    """,
            ACZONE_EHV: f"""
                    SELECT bus_id AS id, ST_Transform(geom, 4326) as geom
                    FROM {sources["ehv_voronoi"]["schema"]}.{sources["ehv_voronoi"]["table"]}
                    """,
            HEAT_BUS: f"""
        			SELECT bus_id AS id, geom
        			FROM {sources["buses"]["schema"]}.{sources["buses"]["table"]}
        			WHERE carrier in ('central_heat')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND country = 'DE'
                    """,
        }

        dfs = {
            key: gpd.read_postgis(queries[key], engine, crs=DATA_CRS).to_crs(METRIC_CRS)
            for key in queries.keys()
            }
        
        with engine.connect() as conn:
            conn.execute(
                        text(
                            f"""DELETE FROM {targets["links"]["schema"]}.{targets["links"]["table"]}
                            WHERE carrier IN ('power_to_H2', 'H2_to_power', 'power_to_O2', 'power_to_Heat') 
                            AND scn_name = '{SCENARIO_NAME}'
                            """
                        )
                    )   
            
        def prepare_dataframes_for_spartial_queries():
               
            #filter_out_potential_methanisation_buses
            h2_grid_bus_ids=tuple(dfs[H2GRID]['bus1']) + tuple(dfs[H2GRID]['bus0'])
            dfs[H2_BUSES_CH4] = dfs[H2][~dfs[H2]['id'].isin(h2_grid_bus_ids)]
            
            #prepare h2_links for filtering:
            # extract geometric data for bus0
            merged_link_with_bus0_geom = pd.merge(dfs[H2GRID], dfs[H2], left_on='bus0', right_on='id', how='left')
            merged_link_with_bus0_geom = merged_link_with_bus0_geom.rename(columns={'geom_y': 'geom_bus0'}).rename(columns={'geom_x': 'geom_link'})
            
            # extract geometric data for bus1
            merged_link_with_bus1_geom = pd.merge(merged_link_with_bus0_geom, dfs[H2], left_on='bus1', right_on='id', how='left')
            merged_link_with_bus1_geom = merged_link_with_bus1_geom.rename(columns={'geom': 'geom_bus1'})
            merged_link_with_bus1_geom = merged_link_with_bus1_geom[merged_link_with_bus1_geom['geom_bus1'] != None] #delete all abroad_links
            
            #prepare heat_buses for filtering
            queries[HEAT_AREA]=f"""
                     SELECT area_id, geom_polygon as geom
                     FROM  {sources["district_heating_area"]["schema"]}.{sources["district_heating_area"]["table"]}  
                     WHERE scenario = '{SCENARIO_NAME}'
                     """
            dfs[HEAT_AREA] = gpd.read_postgis(queries[HEAT_AREA], engine).to_crs(METRIC_CRS)    
            
            heat_bus_geoms = dfs[HEAT_BUS]['geom'].tolist()
            heat_bus_index = STRtree(heat_bus_geoms)
            

            for _, heat_area_row in dfs[HEAT_AREA].iterrows():
                heat_area_geom = heat_area_row['geom']
                area_id = heat_area_row['area_id']
                
                potential_matches = heat_bus_index.query(heat_area_geom)
                
                nearest_bus_idx = None
                nearest_distance = float('inf')

                for bus_idx in potential_matches:
                    bus_geom = dfs[HEAT_BUS].at[bus_idx, 'geom']
                    
                    distance = heat_area_geom.centroid.distance(bus_geom)
                    if distance < nearest_distance:
                        nearest_distance = distance
                        nearest_bus_idx = bus_idx
            
                if nearest_bus_idx is not None:
                    dfs[HEAT_BUS].at[nearest_bus_idx, 'area_id'] = area_id
                    dfs[HEAT_BUS].at[nearest_bus_idx, 'area_geom'] = heat_area_geom
            
                    
            
            dfs[HEAT_BUS]['area_geom'] = gpd.GeoSeries(dfs[HEAT_BUS]['area_geom'])
            
            queries[HEAT_LOAD] = f"""
                        SELECT bus, load_id 
            			FROM {sources["loads"]["schema"]}.{sources["loads"]["table"]}
            			WHERE carrier in ('central_heat')
                        AND scn_name = '{SCENARIO_NAME}'
                        """
            dfs[HEAT_LOAD] = pd.read_sql(queries[HEAT_LOAD], engine)
            load_ids=tuple(dfs[HEAT_LOAD]['load_id'])
            print(load_ids)
            print(dfs[HEAT_LOAD])
            queries[HEAT_TIMESERIES] = f"""
                SELECT load_id, p_set
                FROM {sources["load_timeseries"]["schema"]}.{sources["load_timeseries"]["table"]}
                WHERE load_id IN {load_ids}
                AND scn_name = '{SCENARIO_NAME}'
                """  
            dfs[HEAT_TIMESERIES] = pd.read_sql(queries[HEAT_TIMESERIES], engine)
            dfs[HEAT_TIMESERIES]['sum_of_p_set'] = dfs[HEAT_TIMESERIES]['p_set'].apply(sum)
            dfs[HEAT_TIMESERIES].drop('p_set', axis=1, inplace=True)
            dfs[HEAT_TIMESERIES].dropna(subset=['sum_of_p_set'], inplace=True)
            dfs[HEAT_LOAD] = pd.merge(dfs[HEAT_LOAD], dfs[HEAT_TIMESERIES], on='load_id')
            dfs[HEAT_BUS] = pd.merge(dfs[HEAT_BUS], dfs[HEAT_LOAD], left_on='id', right_on='bus', how='inner')
            dfs[HEAT_BUS]['p_mean'] = dfs[HEAT_BUS]['sum_of_p_set'].apply(lambda x: x / 8760)
            dfs[HEAT_BUS]['buffer'] = dfs[HEAT_BUS]['p_mean'].apply(lambda x: x*buffer_heat_factor)
            dfs[HEAT_BUS]['buffer'] = dfs[HEAT_BUS]['buffer'].apply(lambda x: x if x < max_buffer_heat else max_buffer_heat)  
            
            return merged_link_with_bus1_geom, dfs[HEAT_BUS], dfs[H2_BUSES_CH4]



        def find_h2_grid_connection(df_AC, df_h2, buffer_h2, buffer_AC, sub_type):
            df_h2['buffer'] = df_h2['geom_link'].buffer(buffer_h2)
            df_AC['buffer'] = df_AC['geom'].buffer(buffer_AC)
            
            h2_index = STRtree(df_h2['buffer'].tolist())
            
            results = []
            
            for idx, row in df_AC.iterrows():
                buffered_AC = row['buffer']
                
                possible_matches_idx = h2_index.query(buffered_AC)
                
                nearest_match = None
                nearest_distance = float('inf')
                
                for match_idx in possible_matches_idx:
                    h2_row = df_h2.iloc[match_idx] 
                    
                    if buffered_AC.intersects(h2_row['buffer']):
                        intersection = buffered_AC.intersection(h2_row['buffer'])
                        
                        if not intersection.is_empty:
                            distance = row['geom'].distance(h2_row['geom_link'])
                            distance_to_0 = row['geom'].distance(h2_row['geom_bus0'])
                            distance_to_1 = row['geom'].distance(h2_row['geom_bus1'])
                            
                            if distance_to_0 < distance_to_1:
                                bus_H2 = h2_row['bus0']
                                point_H2 = h2_row['geom_bus0']
                            else:
                                bus_H2 = h2_row['bus1']
                                point_H2 = h2_row['geom_bus1']
                            
                            if distance < nearest_distance:
                                nearest_distance = distance
                                nearest_match = {
                                    'bus_h2': bus_H2,
                                    'bus_AC': row['id'],
                                    'geom_h2': point_H2,
                                    'geom_AC': row['geom'],
                                    'distance_h2': distance,
                                    'intersection': intersection,
                                    'sub_type': sub_type,
                                }
                
                if nearest_match:
                    results.append(nearest_match)
            
            if not results:
                return pd.DataFrame(columns=['bus_h2', 'bus_AC', 'geom_h2', 'geom_AC', 'distance_h2', 'distance_ac', 'intersection', 'sub_type'])
            else: 
                return pd.DataFrame(results)


        def find_h2_bus_connection(df_H2, df_AC, buffer_h2, buffer_AC, sub_type):
            
            df_H2['buffer'] = df_H2['geom'].buffer(buffer_h2)
            df_AC['buffer'] = df_AC['geom'].buffer(buffer_AC)
            
            h2_index = STRtree(df_H2['buffer'].tolist())
            
            results = []
            for _, row in df_AC.iterrows():
                possible_matches_idx = h2_index.query(row['buffer'])
                
                nearest_match = None
                nearest_distance = float('inf')
                
                for match_idx in possible_matches_idx:
                    h2_row = df_H2.iloc[match_idx]  
                    
                    if row['buffer'].intersects(h2_row['buffer']):
                        intersection = row['buffer'].intersection(h2_row['buffer'])
                        distance_AC = row['geom'].distance(intersection.centroid)
                        distance_H2 = h2_row['geom'].distance(intersection.centroid)
                        
                        if (distance_AC + distance_H2) < nearest_distance:
                            nearest_distance = distance_AC + distance_H2
                            nearest_match = {
                                'bus_h2': h2_row['id'],
                                'bus_AC': row['id'],
                                'geom_h2': h2_row['geom'],
                                'geom_AC': row['geom'],
                                'distance_h2': distance_H2,
                                'distance_ac': distance_AC,
                                'intersection': intersection,
                                'sub_type': sub_type,
                            }
                
                if nearest_match:
                    results.append(nearest_match)
            
            if not results:
                return pd.DataFrame(columns=['bus_h2', 'bus_AC', 'geom_h2', 'geom_AC', 'distance_h2', 'distance_ac', 'intersection', 'sub_type'])
            else: 
                return pd.DataFrame(results)


        def find_h2_connection(df_h2):
            ####find H2-HVMV connection:
            potential_location_grid = find_h2_grid_connection(dfs[ACSUB_HVMV], df_h2, Buffer['H2_HVMV'], Buffer['HVMV'],'HVMV')
            potential_location_grid = potential_location_grid.loc[potential_location_grid.groupby(['bus_h2', 'bus_AC'])['distance_h2'].idxmin()]
            
            filtered_df_hvmv = dfs[ACSUB_HVMV][~dfs[ACSUB_HVMV]['id'].isin(potential_location_grid['bus_AC'])].copy()
            potential_location_buses = find_h2_bus_connection(dfs[H2_BUSES_CH4], filtered_df_hvmv,  Buffer['H2_HVMV'], Buffer['HVMV'], 'HVMV')
            potential_location_buses = potential_location_buses.loc[potential_location_buses.groupby(['bus_h2', 'bus_AC'])['distance_h2'].idxmin()]
            
            potential_location_hvmv = pd.concat([potential_location_grid, potential_location_buses], ignore_index = True)
            
            ####find H2-EHV connection:
            potential_location_grid = find_h2_grid_connection(dfs[ACSUB_EHV], df_h2, Buffer['H2_EHV'], Buffer['EHV'],'EHV')
            potential_location_grid = potential_location_grid.loc[potential_location_grid.groupby(['bus_h2', 'bus_AC'])['distance_h2'].idxmin()]
            
            filtered_df_ehv = dfs[ACSUB_EHV][~dfs[ACSUB_EHV]['id'].isin(potential_location_grid['bus_AC'])].copy()
            potential_location_buses = find_h2_bus_connection(dfs[H2_BUSES_CH4], filtered_df_ehv,  Buffer['H2_EHV'], Buffer['EHV'], 'EHV')
            potential_location_buses = potential_location_buses.loc[potential_location_buses.groupby(['bus_h2', 'bus_AC'])['distance_h2'].idxmin()]
            
            potential_location_ehv = pd.concat([potential_location_grid, potential_location_buses], ignore_index = True)

            ### combined potential ehv- and hvmv-connections:
            return pd.concat([potential_location_hvmv, potential_location_ehv], ignore_index = True)



        def find_heat_connection(potential_locations):

            dfs[HEAT_BUS]['buffered_geom'] = dfs[HEAT_BUS]['area_geom'].buffer(dfs[HEAT_BUS]['buffer'])
            intersection_index = STRtree(potential_locations['intersection'].tolist())

            potential_locations['bus_heat'] = None
            potential_locations['geom_heat'] = None
            potential_locations['distance_heat'] = None
            
            results= []

            for _, heat_row in dfs[HEAT_BUS].iterrows():
                buffered_geom = heat_row['buffered_geom']

                potential_matches = intersection_index.query(buffered_geom)
                
                if len(potential_matches) > 0:
                    nearest_distance = float('inf')
                    nearest_ac_index = None

                    for match_idx in potential_matches:
                        ac_row = potential_locations.iloc[match_idx]  # Hole die entsprechende Zeile
                        
                        if buffered_geom.intersects(ac_row['intersection']):
                            distance = buffered_geom.centroid.distance(ac_row['intersection'].centroid)
                            
                            if distance < nearest_distance:
                                nearest_distance = distance
                                nearest_ac_index = match_idx

                    if nearest_ac_index is not None:
                        results.append({
                            'bus_AC': potential_locations.at[nearest_ac_index, 'bus_AC'],
                            'bus_heat': heat_row['id'],
                            'geom_AC': potential_locations.at[nearest_ac_index, 'geom_AC'],
                            'geom_heat': heat_row['geom'],
                            'distance_heat': distance
                        })
                        potential_locations.at[nearest_ac_index, 'bus_heat'] = heat_row['id']
                        potential_locations.at[nearest_ac_index, 'geom_heat'] = heat_row['geom']
                        potential_locations.at[nearest_ac_index, 'distance_heat'] = nearest_distance

            return pd.DataFrame(results)




        def find_o2_connections(df_o2, potential_locations, sub_id):
            
            df_o2['hvmv_id'] = None
            for _, district_row in dfs[ACZONE_HVMV].iterrows():
                district_geom = district_row['geom']
                district_id = district_row['id']
                
                mask = df_o2['geom'].apply(lambda x: x.within(district_geom))
                df_o2.loc[mask, 'hvmv_id'] = district_id
                
            df_o2['ehv_id'] = None
            for _, district_row in dfs[ACZONE_EHV].iterrows():
                district_geom = district_row['geom']
                district_id = district_row['id']
                
                mask = df_o2['geom'].apply(lambda x: x.within(district_geom))
                df_o2.loc[mask, 'ehv_id'] = district_id
                
            intersection_geometries = potential_locations['intersection'].tolist()
            intersection_tree = STRtree(intersection_geometries)
            
            results = []

            for _, o2_row in df_o2.iterrows():
                o2_buffer = o2_row['geom'].buffer(Buffer['O2'])  
                o2_id = o2_row['id']  
                o2_district_id = o2_row[sub_id] 

                possible_matches = intersection_tree.query(o2_buffer)

                for match_idx in possible_matches:
                    ac_row = potential_locations.iloc[match_idx]
                    intersection_centroid = ac_row['intersection'].centroid


         
                    if ac_row['bus_AC'] == o2_district_id and o2_buffer.intersects(ac_row['intersection']):           
                        distance = intersection_centroid.distance(o2_buffer.centroid)
                        results.append({
                            'bus_AC': ac_row['bus_AC'],
                            'bus_O2': o2_id,
                            'geom_AC':  ac_row['geom_AC'],
                            'geom_O2': o2_row['geom'],
                            'distance_O2': distance,
                            'KA_ID': o2_row['ka_id']
                        })

            return pd.DataFrame(results)



        def find_spec_for_ka_id(ka_id):
            found_spec = wwtp_spec[wwtp_spec["KA_ID"] == ka_id]
            if len(found_spec) > 1:
                raise Exception("multiple spec for a ka_id")
            found_spec = found_spec.iloc[0]
            return {
                "pe": found_spec["WWTP_PE"],
                "demand_o2": found_spec["O2 Demand 2035 [tonne/year]"],
                "demand_o3": found_spec["O3 Demand 2035 [tonne/year]"],
            }


        def calculate_wwtp_capacity(pe):  # [MWh/year]
            c = "c2"
            if pe > 100_000:
                c = "c5"
            elif pe > 10_000 and pe <= 100_000:
                c = "c4"
            elif pe > 2000 and pe <= 10_000:
                c = "c3"
            return pe * WWTP_SEC[c] / 1000

        def gas_pipeline_size(gas_volume_y, distance, input_pressure, molar_mass, min_pressure):
            """
                Parameters
                ----------
                gas_valume : kg/year
                distance : km
                input pressure : bar
                min pressure : bar
                molar mas : kg/mol
                Returns
            -------
            Final pressure drop [bar] & pipeline diameter [m]
            """

            def _calculate_final_pressure(pipeline_diameter):
                flow_rate = (
                    (gas_volume_y / (8760 * molar_mass))
                    * UNIVERSAL_GAS_CONSTANT
                    * TEMPERATURE
                    / (input_pressure * 100_000)
                )  # m3/hour
                flow_rate_s = flow_rate / 3600  # m3/second
                pipeline_area = math.pi * (pipeline_diameter / 2) ** 2  # m2
                gas_velocity = flow_rate_s / pipeline_area  # m/s
                gas_density = (input_pressure * 1e5 * molar_mass) / (
                    UNIVERSAL_GAS_CONSTANT * TEMPERATURE
                )  # kg/m3
                reynolds_number = (
                    gas_density * gas_velocity * pipeline_diameter
                ) / UNIVERSAL_GAS_CONSTANT
                # Estimate Darcy friction factor using Moody's approximation
                darcy_friction_factor = 0.0055 * (
                    1 + (2 * 1e4 * (2.51 / reynolds_number)) ** (1 / 3)
                )
                # Darcy-Weisbach equation
                pressure_drop = (
                    (4 * darcy_friction_factor * distance * 1000 * gas_velocity**2)
                    / (2 * pipeline_diameter)
                ) / 1e5  # bar
                return input_pressure - pressure_drop  # bar

            for diameter in PIPELINE_DIAMETER_RANGE:
                final_pressure = _calculate_final_pressure(diameter)
                if final_pressure > min_pressure:
                    return (round(final_pressure, 4), round(diameter, 4))
            raise Exception("couldn't find a final pressure < min_pressure")
            
            
        # O2 pipeline diameter cost range
        def get_o2_pipeline_cost(o2_pipeline_diameter):
           for diameter in sorted(O2_PIPELINE_COSTS.keys(), reverse=True):
               if o2_pipeline_diameter >= float(diameter):
                   return O2_PIPELINE_COSTS[diameter]
      


        def create_link_dataframes(links_h2, links_heat, links_O2):

            etrago_columns = [
                "scn_name",
                "link_id",
                "bus0",
                "bus1",
                "carrier",
                "efficiency",
                "lifetime",
                "p_nom",
                "p_nom_max",
                "p_nom_extendable",
                "capital_cost",
                "length",
                "geom",
                "topo",
            ]

            power_to_H2 = pd.DataFrame(columns=etrago_columns)
            H2_to_power = pd.DataFrame(columns=etrago_columns)
            power_to_Heat = pd.DataFrame(columns=etrago_columns)
            power_to_O2 = pd.DataFrame(columns=etrago_columns)
            
            max_link_id =  db.next_etrago_id("link")
            next_max_link_id = count(start=max_link_id, step=1)
            

            ####poower_to_H2
            for idx, row in links_h2.iterrows():
                capital_cost_H2 = H2_COST_PIPELINE + ELZ_CAPEX_STACK + ELZ_CAPEX_SYSTEM + ELZ_OPEX  # [EUR/MW/YEAR]
                capital_cost_AC = AC_COST_CABLE * row['distance_ac']/1000 + AC_TRANS # [EUR/MW/YEAR]
                capital_cost_PtH2 = capital_cost_AC + capital_cost_H2
                
                power_to_H2_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": next(next_max_link_id),
                    "bus0": row["bus_AC"],
                    "bus1": row["bus_h2"],
                    "carrier": "power_to_H2",
                    "efficiency": ELZ_EFF,    
                    "lifetime": ELZ_LIFETIME_Y,  
                    "p_nom": 0,  
                    "p_nom_max": 120 if row['sub_type'] == 'HVMV' else 5000, 
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost_PtH2,   
                    "geom": MultiLineString(
                        [LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_h2'].x, row['geom_h2'].y)])]
                    ),  
                    "topo": LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_h2'].x, row['geom_h2'].y)]
                    ),  
                }
                power_to_H2 = pd.concat([power_to_H2, pd.DataFrame([power_to_H2_entry])], ignore_index=True)
                
                ####H2_to_power
                capital_cost_H2 = H2_COST_PIPELINE + FUEL_CELL_COST # [EUR/MW/YEAR]
                capital_cost_AC = AC_COST_CABLE * row['distance_ac']/1000 + AC_TRANS # [EUR/MW/YEAR]
                capital_cost_H2tP = capital_cost_AC + capital_cost_H2
                H2_to_power_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": next(next_max_link_id),
                    "bus0": row["bus_h2"],
                    "bus1": row["bus_AC"],
                    "carrier": "H2_to_power",
                    "efficiency": FUEL_CELL_EFF,    
                    "lifetime": FUEL_CELL_LIFETIME,  
                    "p_nom": 0,  
                    "p_nom_max": 120 if row['sub_type'] == 'HVMV' else 5000, 
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost_H2tP,   
                    "geom": MultiLineString(
                        [LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_h2'].x, row['geom_h2'].y)])]
                    ),  
                    "topo": LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_h2'].x, row['geom_h2'].y)]
                    ),  
                }
                H2_to_power = pd.concat([H2_to_power, pd.DataFrame([H2_to_power_entry])], ignore_index=True)

            ###power_to_Heat
            for idx, row in links_heat.iterrows():
                capital_cost = HEAT_COST_EXCHANGER + HEAT_COST_PIPELINE*row['distance_heat']/1000 #EUR/MW/YEAR
                
                power_to_heat_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": next(next_max_link_id),
                    "bus0": row["bus_AC"],
                    "bus1": row["bus_heat"],
                    "carrier": "power_to_Heat",
                    "efficiency": 1,  
                    "lifetime": 25,  
                    "p_nom": 0,  
                    "p_nom_max": float('inf'),  
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost,  
                    "geom": MultiLineString(
                        [LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_heat'].x, row['geom_heat'].y)])]
                    ),  
                    "topo": LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_heat'].x, row['geom_heat'].y)]
                    ), 
                }
                power_to_Heat = pd.concat([power_to_Heat, pd.DataFrame([power_to_heat_entry])], ignore_index=True)
                
                
            ####power_to_O2   
            for idx, row in links_O2.iterrows():
                distance = row['distance_O2']/1000 #km
                ka_id = row["KA_ID"]
                spec = find_spec_for_ka_id(ka_id)
                wwtp_ec = calculate_wwtp_capacity(spec["pe"])  # [MWh/year]
                aeration_ec = wwtp_ec * FACTOR_AERATION_EC  # [MWh/year]
                o2_ec = aeration_ec * FACTOR_O2_EC  # [MWh/year]
                o2_ec_h = o2_ec / 8760  # [MWh/hour]
                total_o2_demand = (
                    spec["demand_o3"] + spec["demand_o2"]
                ) * 1000  # kgO2/year pure O2 tonne* 1000
                _, o2_pipeline_diameter = gas_pipeline_size(
                    total_o2_demand,
                    distance,
                    O2_PRESSURE_ELZ,
                    MOLAR_MASS_O2,
                    O2_PRESSURE_MIN,
                )
                annualized_cost_o2_pipeline = get_o2_pipeline_cost(o2_pipeline_diameter) # [EUR/KM/YEAR]
                annualized_cost_o2_component = O2_COST_EQUIPMENT #EUR/MW/YEAR
                capital_costs=annualized_cost_o2_pipeline * distance/o2_ec_h + annualized_cost_o2_component
                
                power_to_o2_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": next(next_max_link_id),
                    "bus0": row["bus_AC"],
                    "bus1": row["bus_O2"],
                    "carrier": "power_to_O2",
                    "efficiency": 1,  
                    "lifetime": 25,  
                    "p_nom": o2_ec_h,  
                    "p_nom_max": float('inf'),  
                    "p_nom_extendable": True,
                    "capital_cost": capital_costs,  
                    "geom": MultiLineString(
                        [LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_O2'].x, row['geom_O2'].y)])]
                    ),  
                    "topo": LineString([(row['geom_AC'].x, row['geom_AC'].y), (row['geom_O2'].x, row['geom_O2'].y)]),  
                }
                power_to_O2 = pd.concat([power_to_O2, pd.DataFrame([power_to_o2_entry])], ignore_index=True)

            return power_to_H2, power_to_Heat, power_to_O2



        def export_links_to_db(df, carrier):
            schema=targets["links"]["schema"]
            table_name=targets["links"]["table"]

            with engine.connect() as conn:
                conn.execute(
                    text(
                        f"DELETE FROM {schema}.{table_name} WHERE carrier IN ('{carrier}')"
                    )
                )
            gdf = gpd.GeoDataFrame(df, geometry="geom").set_crs(METRIC_CRS)
            gdf = gdf.to_crs(epsg=DATA_CRS)
            gdf.p_nom = 0
            
            try:
                gdf.to_postgis(
                    name=table_name,  
                    con=engine,  
                    schema=schema,  
                    if_exists="append",  
                    index=False,  
                )
                print(f"Links have been exported to {schema}.{table_name}")
            except Exception as e:
                print(f"Error while exporting link data: {e}")
                


        def insert_o2_load_points(df):
            new_id = db.next_etrago_id('load')
            next_load_id = count(start=new_id, step=1)
            schema =  targets["loads"]["schema"]
            table_name = targets["loads"]["table"]
            with engine.connect() as conn:
                conn.execute(
                    f"DELETE FROM {schema}.{table_name} WHERE carrier = 'O2' AND scn_name = '{SCENARIO_NAME}'"
                )
            df = df.copy(deep=True)
            df = df[df["carrier"] == "power_to_O2"]
            result = []
            for _, row in df.iterrows():
                load_id = next(next_load_id)
                result.append(
                    {
                        "scn_name": SCENARIO_NAME,
                        "load_id": load_id,
                        "bus": row["bus1"],
                        "carrier": "O2",
                        "o2_load_el": row["p_nom"],
                    }
                )
            df = pd.DataFrame(result)
            df[['scn_name', 'load_id', 'bus', 'carrier']].to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
            print(f"O2 load data exported to: {table_name}")
            return df
            
        def insert_o2_load_timeseries(df):
            query_o2_timeseries = f"""
                        SELECT load_curve
            			FROM {sources["o2_load_profile"]["schema"]}.{sources["o2_load_profile"]["table"]}
            			WHERE slp = 'G3' AND wz = 3
                        """
                        
            base_load_profile = pd.read_sql(query_o2_timeseries, engine)['load_curve'].values
            base_load_profile = np.array(base_load_profile[0])

            timeseries_list = []

            for index, row in df.iterrows():
                load_id = row['load_id']  # ID aus der aktuellen Zeile
                o2_load_el = row['o2_load_el']  # Nennleistung aus der aktuellen Zeile
                
                modified_profile = base_load_profile * o2_load_el
                
                timeseries_list.append({
                    'scn_name': 'eGon2035',
                    'load_id': load_id,
                    'temp_id': 1,
                    'p_set': modified_profile,
                    'bus': row['bus']
                })

            timeseries_df = pd.DataFrame(timeseries_list)
            timeseries_df['p_set'] = timeseries_df['p_set'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
            timeseries_df[['scn_name', 'load_id', 'temp_id', 'p_set']].to_sql(
                targets["load_timeseries"]["table"], 
                engine,
                schema=targets["load_timeseries"]["schema"], 
                if_exists="append", 
                index=False)

            return timeseries_df




        def insert_o2_generators(df):
            new_id = db.next_etrago_id("generator")
            next_generator_id = count(start=new_id, step=1)
            
            grid = targets["generators"]["schema"]
            table_name = targets["generators"]["table"]
            with engine.connect() as conn:
                conn.execute(
                    f"DELETE FROM {grid}.{table_name} WHERE carrier = 'O2' AND scn_name = '{SCENARIO_NAME}'"
                )
            df = df.copy(deep=True)
            df = df[df["carrier"] == "power_to_O2"]
            result = []
            for _, row in df.iterrows():
                generator_id = next(next_generator_id)
                result.append(
                    {
                        "scn_name": SCENARIO_NAME,
                        "generator_id": generator_id,
                        "bus": row["bus1"],
                        "carrier": "O2",
                        "p_nom_extendable": "true",
                        "marginal_cost": ELEC_COST, 
                    }
                )
            df = pd.DataFrame(result)
            df.to_sql(table_name, engine, schema=grid, if_exists="append", index=False)

            print(f"generator data exported to: {table_name}")



        def adjust_ac_load_timeseries(df, o2_timeseries):
            #filter out affected ac_loads
            queries[AC_LOAD] = f"""
                                SELECT bus, load_id 
                    			FROM {sources["loads"]["schema"]}.{sources["loads"]["table"]}
                                WHERE scn_name = '{SCENARIO_NAME}'
                                """
            dfs[AC_LOAD] = pd.read_sql(queries[AC_LOAD], engine)
            ac_loads = pd.merge(df, dfs[AC_LOAD], left_on='bus0', right_on='bus')
            
            #reduce each affected ac_load with o2_timeseries
            for _, row in ac_loads.iterrows():
                with engine.connect() as conn:

                    select_query = text(f"""
                        SELECT p_set 
                        FROM {sources["load_timeseries"]["schema"]}.{sources["load_timeseries"]["table"]}
                        WHERE load_id = :load_id and scn_name= :SCENARIO_NAME
                        """)
                    result = conn.execute(select_query, {"load_id": row["load_id"], "SCENARIO_NAME": SCENARIO_NAME}).fetchone()
                    
                    if result:
                         original_p_set = result["p_set"]                         
                         o2_timeseries_row = o2_timeseries.loc[o2_timeseries['bus'] == row['bus1']]
                         
                         if not o2_timeseries_row.empty:
                             o2_p_set = o2_timeseries_row.iloc[0]['p_set']
                             
                             if len(original_p_set) == len(o2_p_set):
                                 # reduce ac_load with o2_load_timeseries
                                 adjusted_p_set = (np.array(original_p_set) - np.array(o2_p_set)).tolist()
                                 update_query = text(f"""
                                     UPDATE {targets["load_timeseries"]["schema"]}.{targets["load_timeseries"]["table"]}
                                     SET p_set = :adjusted_p_set
                                     WHERE load_id = :load_id AND scn_name = :SCENARIO_NAME
                                 """)
                                 conn.execute(update_query, {"adjusted_p_set": adjusted_p_set, "load_id": row["load_id"], "SCENARIO_NAME": SCENARIO_NAME})
                             else:
                                 print(f"Length mismatch for load_id {row['load_id']}: original={len(original_p_set)}, o2={len(o2_p_set)}")
                         else:
                             print(f"No matching o2_timeseries entry for load_id {row['load_id']}")
                             

        def execute_PtH2_method():
            
            h2_grid_geom_df, dfs[HEAT_BUS], dfs[H2_BUSES_CH4] = prepare_dataframes_for_spartial_queries()
            potential_locations=find_h2_connection(h2_grid_geom_df)
            heat_links = find_heat_connection(potential_locations)
            o2_links_hvmv = find_o2_connections(dfs[WWTP], potential_locations[potential_locations.sub_type=='HVMV'], 'hvmv_id')
            o2_links_ehv = find_o2_connections(dfs[WWTP], potential_locations[potential_locations.sub_type=='EHV'], 'ehv_id')
            o2_links=pd.concat([o2_links_hvmv, o2_links_ehv], ignore_index=True)
            power_to_H2, power_to_Heat, power_to_O2 = create_link_dataframes(potential_locations, heat_links, o2_links)
            export_links_to_db(power_to_H2,'power_to_H2')
            export_links_to_db(power_to_Heat, 'power_to_Heat')
            export_links_to_db(power_to_O2, 'power_to_O2')
            o2_loads_df = insert_o2_load_points(power_to_O2)
            o2_timeseries = insert_o2_load_timeseries(o2_loads_df)
            insert_o2_generators(power_to_O2)
            adjust_ac_load_timeseries(power_to_O2, o2_timeseries) 
          
        execute_PtH2_method()
                         

