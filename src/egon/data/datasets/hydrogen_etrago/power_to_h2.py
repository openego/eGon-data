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
from rtree import index
from geopandas.tools import sjoin
from shapely.ops import nearest_points
from sqlalchemy import text
from scipy.optimize import minimize
from shapely.io import from_wkt, to_wkt
from shapely.geometry import MultiLineString, LineString, Point
from shapely.wkb import dumps
from geoalchemy2.types import Geometry
from egon.data import db, config
from egon.data.datasets.scenario_parameters import get_sector_parameters
from pathlib import Path
import numpy as np




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
    For heat-usage closest central-heat-bus inner an dynamic buffer (related to Heat-demand)
    is connected to relevant HVMV-Substation.
    
    All links are extendable. 

    This function inserts data into the database and has no return.


    """
    # General Constant Parameters for method
    SCENARIO_NO = 2  # 1 = WWTP-based location, 2 = AC-based location; default setting: 2
    OPTIMIZATION = "no"  # "yes" or "no" to activate optimization for the optimal location, default setting: "no"
    SUBSTATION = "yes"  # "yes" or "no" will switch between AC points and Substation points.
    DATA_CRS = 4326  # default CRS
    METRIC_CRS = 3857  # demanded CRS
    
    scenarios = config.settings()["egon-data"]["--scenarios"]
    
    # constant Parameters for Location_Optimization (Calculating LCOH)
    # Power to H2 (Electricity & Electrolyser)
    ELEC_COST = 60  # [EUR/MWh]
    ELZ_SEC = 50  # [kWh/kgH2] Electrolyzer Specific Energy Consumption
    ELZ_FLH = 8760  # [hour] full load hours 		5217
    H2_PRESSURE_ELZ = 30  # [bar]
    O2_PRESSURE_ELZ = 13  # [bar]
    
    # Power to Heat  
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
    O2_PRESSURE_MIN = 2  # [bar]
    MOLAR_MASS_O2 = 0.0319988  # [kg/mol]
    
    # H2 to Power (Hydrogen Pipeline)
    H2_PRESSURE_MIN = 29  # [bar]
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
    O2 = "o2"
    HEAT_BUS = "heat_point"
    HEAT_LOAD = "heat_load"
    HEAT_TIMESERIES = "heat_timeseries"
    H2_BUSES_CH4 = 'h2_buses_ch4' 
    AC_LOAD = 'ac_load'
    
    buffer_heat_factor= 1500  #625/3125 for worstcase/bestcase-Szeanrio , source : L.Zimmermann, MODELLIERUNG DER ABWÄRMENUTZUNG VON ELEKTROLYSEUREN IN DEUTSCHLAND FÜR EINE TECHNO-ÖKONOMISCHE OPTIMIERUNG EINES SEKTOR - GEKOPPELTEN ENERGIESYSTEMS, 2024
    max_buffer_heat= 12000 #5000/30000 for worstcase/bestcase-Szenario 
    MAXIMUM_DISTANCE = {
        O2: 10,  # km to define the radii between O2 to AC
        H2: 30000,  # m define the distance between H2 and reference points (AC/O2)
    } 
    
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
        ELZ_LIFETIME = scn_params_gas["lifetime"]["power_to_H2_system"] 
        
        HEAT_RATIO = scn_params_gas["efficiency"]["power_to_Heat"] - scn_params_gas["efficiency"]["power_to_H2"] # % heat ratio to hydrogen production
        HEAT_COST_EXCHANGER = scn_params_gas["capital_cost"]["Heat_exchanger"]  # [EUR/MW/YEAR]
        HEAT_COST_PIPELINE = scn_params_gas["capital_cost"]["Heat_pipeline"] # [EUR/MW/YEAR]
        HEAT_EFFICIENCY = scn_params_gas["efficiency"]["power_to_Heat"]    
        HEAT_LIFETIME = scn_params_gas["lifetime"]["Heat_exchanger"]
        
        O2_PIPELINE_COSTS = scn_params_gas["O2_capital_cost"]   #[EUR/km/YEAR]
        O2_COST_EQUIPMENT = scn_params_gas["capital_cost"]["O2_components"]  #[EUR/MW/YEAR]
        O2_EFFICIENCY = scn_params_gas["efficiency"]["power_to_O2"]    
        O2_LIFETIME_PIPELINE = 25  # [Year]
        
        FUEL_CELL_COST = scn_params_gas["capital_cost"]["H2_to_power"]   #[EUR/MW/YEAR]
        FUEL_CELL_EFF = scn_params_gas["efficiency"]["H2_to_power"] 
        FUEL_CELL_LIFETIME = scn_params_gas["lifetime"]["H2_to_power"]
        
        # read and reproject spatial data
        def read_query(engine, query):
            return gpd.read_postgis(query, engine, crs=DATA_CRS).to_crs(METRIC_CRS)
        
        
        def export_to_db(df):
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
                            srid=4326,
                        ),
                        "country": "DE",
                    }
                )
            result_df = pd.DataFrame(result)
            result_df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        
        
        wwtp_spec = pd.read_csv(Path(".")/"WWTP_spec.csv")
        export_to_db(wwtp_spec)  # Call the function with the dataframe
        print(f"Scenario No = {SCENARIO_NO} & Optimization = {OPTIMIZATION}")
        
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
                    WHERE carrier in ('H2_grid')
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
        # First Phase: Find intersection
        # Data management
        # read and convert the spatial CRS data to Metric CRS
        dfs = {
            key: gpd.read_postgis(queries[key], engine, crs=4326).to_crs(3857)
            for key in queries.keys()
            }
        
        #prepare h2_links for filtering
        h2_grid_bus_ids=tuple(dfs[H2GRID]['bus1']) + tuple(dfs[H2GRID]['bus0'])
        
        dfs[H2_BUSES_CH4] = dfs[H2][~dfs[H2]['id'].isin(h2_grid_bus_ids)]
        
        merged_df_bus0 = pd.merge(dfs[H2GRID], dfs[H2], left_on='bus0', right_on='id', how='left')
        merged_df_bus0 = merged_df_bus0.rename(columns={'geom_y': 'geom_bus0'}).rename(columns={'geom_x': 'geom_link'})   
        merged_df = pd.merge(merged_df_bus0, dfs[H2], left_on='bus1', right_on='id', how='left')
        merged_df = merged_df.rename(columns={'geom': 'geom_bus1'})

        
        
        #prepare heat_buses for filtering
        queries[HEAT_LOAD] = f"""
                    SELECT bus, load_id 
        			FROM {sources["loads"]["schema"]}.{sources["loads"]["table"]}
        			WHERE carrier in ('central_heat')
                    AND scn_name = '{SCENARIO_NAME}'
                    """
        dfs[HEAT_LOAD] = pd.read_sql(queries[HEAT_LOAD], engine)
        load_ids=tuple(dfs[HEAT_LOAD]['load_id'])
      
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

        # First Phase: Find intersection between points
        # Perform spatial join to find points within zones (substation zones)
        in_zone_hvmv = {
            WWTP: sjoin(dfs[WWTP], dfs[ACZONE_HVMV], how="inner", predicate="within"),
            AC: sjoin(
                dfs[AC if SUBSTATION == "no" else ACSUB_HVMV],
                dfs[ACZONE_HVMV],
                how="inner",
                predicate="within",
            ),
        }
        in_zone_ehv = {
            WWTP: sjoin(dfs[WWTP], dfs[ACZONE_EHV], how="inner", predicate="within"),
            AC: sjoin(
                dfs[AC if SUBSTATION == "no" else ACSUB_EHV],
                dfs[ACZONE_EHV],
                how="inner",
                predicate="within",
            ),
        }

        # Create R-tree index to speedup the process based on bounding box coordinates.
        rtree = {key: index.Index() for key in [H2, AC, ACSUB_HVMV, ACSUB_EHV, H2GRID, HEAT_BUS, H2_BUSES_CH4]}
        for key in rtree.keys():
            for i in range(len(dfs[key])):
                rtree[key].insert(i, dfs[key].iloc[i].geom.bounds)
                
        # Find the nearest intersection relation between AC points and WWTPs
        # 1. find ACs inside same network zone as wwtp
        # 2. calculate distances betweeen those ac and wwtp within a identical zone
        # 3. select the point which has the minimum distance among them
        # 4. distingush type of AC (point or substation)
        
        
        def find_closest_acs(sub_type, in_zone_type, keep_empty_acs=False):
            results = []
            # Iterate over the zones and calculate distances
            for zone_id in dfs[sub_type].index:
                wwtp_in_zones = in_zone_type[WWTP][in_zone_type[WWTP]["index_right"] == zone_id]
                ac_in_zones = in_zone_type[AC][in_zone_type[AC]["index_right"] == zone_id]
                for _, ac_row in ac_in_zones.iterrows():
                    if len(wwtp_in_zones) == 0 and keep_empty_acs == True:
                        results.append(
                            {
                                "WWTP_ID": "",
                                "KA_ID": "",
                                "AC_ID": ac_row["id_left"],
                                "distance_ac": 0,  # km
                                "point_wwtp": None,
                                "point_AC": ac_row.geom,
                            }
                        )
                    else:
                        for _, wwtp_row in wwtp_in_zones.iterrows():
                             distance = round(wwtp_row.geom.distance(ac_row.geom)) / 1000
                             if distance <= MAXIMUM_DISTANCE[O2]:
                                 results.append(
                                     {
                                         "WWTP_ID": wwtp_row["id_left"],
                                         "KA_ID": wwtp_row["ka_id"],
                                         "AC_ID": ac_row["id_left"],
                                         "distance_ac": distance,  # km
                                         "point_wwtp": wwtp_row.geom,
                                         "point_AC": ac_row.geom,
                                     }
                                )
                             else: 
                                 results.append(
                                     {
                                        "WWTP_ID": "",
                                        "KA_ID": "",
                                        "AC_ID": ac_row["id_left"],
                                        "distance_ac": 0,  # km
                                        "point_wwtp": None,
                                        "point_AC": ac_row.geom,
                                    })
                            
            results = pd.DataFrame(results).drop_duplicates()
            results = results.loc[results.groupby(["AC_ID", "WWTP_ID"])["distance_ac"].idxmin()]
            return results
        
        # Creating the initial main dataframes
        main_df_hvmv = find_closest_acs(ACSUB_HVMV, in_zone_hvmv, SCENARIO_NO == 2)
        main_df_ehv = find_closest_acs(ACSUB_EHV, in_zone_ehv, SCENARIO_NO == 2)
        
        def find_ac_type(sub_type, dataframe_with_ac):
            result = dataframe_with_ac.copy()
        
            def _find_ac_point(row):
                substations = dfs[sub_type].loc[dfs[sub_type]["id"] == row]
                points = dfs[AC].loc[dfs[AC]["id"] == row]
                is_sub = len(substations) > 0
                if is_sub:
                    return substations.iloc[0]["geom"]
                else:
                    return points.iloc[0]["geom"]
        
            def _find_ac_type(row):
                substations = dfs[sub_type].loc[dfs[sub_type]["id"] == row]
                is_sub = len(substations) > 0
                if is_sub:
                    return "substation"
                else:
                    return "ac_point"
        
            result["point_AC"] = result["AC_ID"].apply(_find_ac_point)
            result["AC_type"] = result["AC_ID"].apply(_find_ac_type)
            return result

        
        main_df_hvmv = find_ac_type(ACSUB_HVMV, main_df_hvmv)
        main_df_ehv = find_ac_type(ACSUB_EHV, main_df_ehv)
        main_df_hvmv['sub_type'] = 'HVMV'
        main_df_ehv['sub_type'] = 'EHV'
        main_df = pd.concat([main_df_hvmv, main_df_ehv], ignore_index = True)
        
        # The function find and assign the correct reference point for centrlizing as buffer for further steps
        def get_main_point():
            if SCENARIO_NO == 1:
                return "WWTP_ID", "point_wwtp"
            elif SCENARIO_NO == 2:
                return "AC_ID", "point_AC"
            else:
                raise Exception("Invalid scenario number")
            
        # Find nearest H2 points & grid pipeline to refernce points (AC or WWTP depend on scenario no)
        # below function support h2 points and h2_grid, by distingushing their types
        
        
        def find_h2_grid_intersections(rtree, df1, df2, buffer_factor, type):
            results = []
            col, point = get_main_point()
            for _, row in df2.iterrows():
                buffered = row[point].buffer(buffer_factor)
                for idx in rtree.intersection(buffered.bounds):
                    item = df1.iloc[idx]
                    if buffered.intersects(item.geom_link):
                        distance = round(row[point].distance(item.geom_link))
                        distance_to_0 = round(row[point].distance(item.geom_bus0))
                        distance_to_1 = round(row[point].distance(item.geom_bus1))
                        if distance_to_0 < distance_to_1:
                            bus_H2 = item.bus0
                            point_H2 = item.geom_bus0
                            
                        else:
                            bus_H2 = item.bus1
                            point_H2 = item.geom_bus1
                            
                        results.append(
                            {
                                col: row[col],
                                "H2_ID": bus_H2,
                                "distance_h2": distance / 1000,
                                "point_H2": point_H2,
                                "H2_type": type,
                                "sub_type": row["sub_type"]
                            }
                        )
            return pd.DataFrame(results)        
        
        def find_h2_intersections(rtree, df1, df2, buffer_factor, type):
            results = []
            col, point = get_main_point()
            for _, row in df2.iterrows():
                buffered = row[point].buffer(buffer_factor)
                for idx in rtree.intersection(buffered.bounds):
                    item = df1.iloc[idx]
                    if buffered.intersects(item.geom):
                        distance = round(row[point].distance(item.geom))
                        near_point = nearest_points(item.geom, row[point])[0]
                        results.append({
         					col: row[col],
         					"H2_ID": item.id,
         					"distance_h2": distance/1000,
         					"point_H2": near_point,
         					"H2_type": type,
                            "sub_type": row["sub_type"]
        				})
            return pd.DataFrame(results)
            
        def find_minimum_h2_intersections(df1, df2):
            col, _ = get_main_point()
            if not df1.empty:
                result_1 = df1.iloc[df1.groupby(col)["distance_h2"].idxmin()]
            else: 
                result_1 = pd.DataFrame(columns=['AC_ID', 'H2_ID', 'distance_h2', 'point_H2', 'H2_type'])
            result_2 = df2.iloc[df2.groupby(col)["distance_h2"].idxmin()]
            result = pd.concat([result_1, result_2], ignore_index=True)
            return result
        
        
        
        # Find nearest Heat Points to refernce points
        def find_heatpoint_intersections(rtree, df1, sub_type):
            col, point = get_main_point()
            results = []
            for idx, row in dfs[HEAT_BUS].iterrows():
                 buffered = row['geom'].buffer(row['buffer'])
                 for idx in rtree.intersection(buffered.bounds):
                    item = df1.iloc[idx]
                    if buffered.intersects(item.geom):
                        distance = round(row['geom'].distance(item.geom))
                        results.append(
                            {
                                col: item['id'],
                                "HEAT_ID": row['id'],
                                "distance_heat": distance / 1000,
                                "point_heat": row['geom'],
                                "sub_type": sub_type
                            }
                        )
            return pd.DataFrame(results)
        
        
        def find_minimum_heatpoint_intersections(df):
            col = "HEAT_ID"
            result = df.iloc[
                df.groupby(col)["distance_heat"].idxmin()
            ]
            return result
        
        def find_h2_heat_connection():
            # find h2_grid/h2_bus connection for hvmv-substations
            h2_grid_intersections = find_h2_grid_intersections(rtree[H2GRID], merged_df, main_df_hvmv, MAXIMUM_DISTANCE[H2], H2GRID)
            if h2_grid_intersections.empty:
                filtered_main_df_hvmv = main_df_hvmv
            else: 
                filtered_main_df_hvmv = main_df_hvmv[~main_df_hvmv['AC_ID'].isin(h2_grid_intersections['AC_ID'])]
           
            h2_intersections = find_h2_intersections(rtree[H2_BUSES_CH4], dfs[H2_BUSES_CH4], filtered_main_df_hvmv, MAXIMUM_DISTANCE[H2], H2)
            min_h2_intersections_hvmv = find_minimum_h2_intersections(h2_grid_intersections, h2_intersections)
            
            # find h2_grid/h2_bus connection for ehv-substations
            h2_grid_intersections = find_h2_grid_intersections(rtree[H2GRID], merged_df, main_df_ehv, MAXIMUM_DISTANCE[H2], H2GRID)
            if h2_grid_intersections.empty:
                filtered_main_df_ehv = main_df_ehv
            else: 
                filtered_main_df_ehv = main_df_ehv[~main_df_ehv['AC_ID'].isin(h2_grid_intersections['AC_ID'])]
           
            h2_intersections = find_h2_intersections(rtree[H2_BUSES_CH4], dfs[H2_BUSES_CH4], filtered_main_df_ehv, MAXIMUM_DISTANCE[H2], H2)
            min_h2_intersections_ehv = find_minimum_h2_intersections(h2_grid_intersections, h2_intersections)
            min_h2_intersections = pd.concat([min_h2_intersections_hvmv, min_h2_intersections_ehv], ignore_index = True)
        
        
            heatpoint_intersections = find_heatpoint_intersections(rtree[ACSUB_HVMV], dfs[ACSUB_HVMV], "HVMV")
            min_heatpoint_intersections_hvmv = find_minimum_heatpoint_intersections(heatpoint_intersections)
            heatpoint_intersections = find_heatpoint_intersections(rtree[ACSUB_EHV], dfs[ACSUB_EHV], "EHV")
            min_heatpoint_intersections_ehv = find_minimum_heatpoint_intersections(heatpoint_intersections)
            min_heatpoint_intersections = pd.concat([min_heatpoint_intersections_hvmv, min_heatpoint_intersections_ehv], ignore_index=True)
            
            return min_h2_intersections, min_heatpoint_intersections
        
        # Second Phase: Data management
        o2_ac = main_df
        ref_h2, ref_heat = find_h2_heat_connection()
        
        
        # Scenario nomination for the Model 1: wwtp as refernce point 2: ac as reference point
        def get_correct_ref_id_col():
            if OPTIMIZATION == "yes":
                return "OPTIMAL_ID"
            if SCENARIO_NO == 1:
                return "WWTP_ID"
            elif SCENARIO_NO == 2:
                return "AC_ID"
            else:
                raise Exception("invalid ref")
        
        
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
            
        
        def get_wwtps_for_ac(ac_id):
            acs = o2_ac[o2_ac["AC_ID"] == ac_id]
            res = []
            for _, ac in acs.iterrows():
                res.append(
                    {
                        "id": ac["WWTP_ID"],
                        "ka_id": ac["KA_ID"],
                        "point": ac["point_wwtp"],
                    }
                )
            return res
        
        
        def get_ac_for_wwtp(wwtp_id):
            wwtp = o2_ac[o2_ac["WWTP_ID"] == wwtp_id]
            if len(wwtp) > 1:
                raise Exception("found multiple ac for a wwtp_id")
            wwtp = wwtp.iloc[0]
            return {
                "id": wwtp["AC_ID"],
                "ka_id": wwtp["KA_ID"],
                "point": from_wkt(wwtp["point_AC"]),
            }
        
        
        def get_heat_for_ref(ref_id):
            heat = ref_heat[ref_heat[get_correct_ref_id_col()] == ref_id]
            if len(heat) > 1:
                raise Exception("found multiple heat for a ref_id")
            heat = heat.iloc[0]
            return {
                "id": heat["HEAT_ID"],
                "point": heat["point_heat"],
            }
        
        
        def get_h2_for_ref(ref_id):
            h2 = ref_h2[ref_h2[get_correct_ref_id_col()] == ref_id]
            if len(h2) > 1:
                raise Exception("found multiple h2 for a ref_id")
            h2 = h2.iloc[0]
            return {
                "id": h2["H2_ID"],
                "point": h2["point_H2"],
                "type": h2["H2_type"],
            }
            
            
        def get_wwtp_point(wwpt_id):
            row = o2_ac[o2_ac["WWTP_ID"] == wwpt_id].iloc[0]
            return row["point_wwtp"]
        
        
        def get_ac_point(ac_id):
            row = o2_ac[o2_ac["AC_ID"] == ac_id].iloc[0]
            return row["point_AC"]
        
        
        def get_ac_distance_for_ref(ref_id, o2_to_ac):
            if OPTIMIZATION == "yes":
                row = o2_to_ac[o2_to_ac["OPTIMAL_ID"] == ref_id]
                if len(row) < 1:
                    raise Exception("no wwtp found")
                row = row.iloc[0]
                return row["point_AC"].distance(row["point_optimal"]) / 1000
            if SCENARIO_NO == 1:
                row = o2_to_ac[o2_to_ac["WWTP_ID"] == ref_id]
                if len(row) != 1:
                    raise Exception("multiple wwtp found")
                row = row.iloc[0]
                return row["point_AC"].distance(row["point_wwtp"]) / 1000
            elif SCENARIO_NO == 2:
                return 0
            else:
                raise Exception("invalid scenario")
        
        
        print("Intersection Completed.")
        
        
        # Second Phase: Calculation Functions
        # Calculate gas pipeline diameter (O2 & H2) for further cost calculation:
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
        
               
        def get_o2_pipeline_cost(o2_pipeline_diameter):
            for diameter in sorted(O2_PIPELINE_COSTS.keys(), reverse=True):
                if o2_pipeline_diameter >= float(diameter):
                    return O2_PIPELINE_COSTS[diameter]

        
            
        # Calculate WWTPs capacity base on SEC depend on PE
        def calculate_wwtp_capacity(pe):  # [MWh/year]
            c = "c2"
            if pe > 100_000:
                c = "c5"
            elif pe > 10_000 and pe <= 100_000:
                c = "c4"
            elif pe > 2000 and pe <= 10_000:
                c = "c3"
            return pe * WWTP_SEC[c] / 1000
            
        
        # Second Phase: Links values Calculations
        # add ref_id and ref_point to o2_ac, ref_heat, ref_h2
        ID_OPTIMAL_START = db.next_etrago_id("bus")
        ids = o2_ac["WWTP_ID" if SCENARIO_NO == 1 else "AC_ID"].unique()
        ref_ids = {id: ID_OPTIMAL_START + i for i, id in enumerate(ids)}
        
        
        def add_ref_col(df):
            starting_col_id = "WWTP_ID" if SCENARIO_NO == 1 else "AC_ID"
            find_point = get_wwtp_point if SCENARIO_NO == 1 else get_ac_point
            return df.assign(
                **{
                    "OPTIMAL_ID": df[starting_col_id].map(ref_ids),
                    "point_optimal": df[starting_col_id].apply(find_point),
                }
            )

        o2_ac = add_ref_col(o2_ac)
        ref_heat = add_ref_col(ref_heat)
        ref_h2 = add_ref_col(ref_h2)
        
        # Calculate variables for Links: power_to_O2, power_to_H2, power_to_Heat, H2_to_power
        # optimized bus { "id": generated, "point": optimized }
        def find_links(o2_ac, ref_heat, ref_h2):
            links = []
            total_h2_production_y = {}
            total_lcoh = 0
            found_ac = {}
        
            # data calculation for power_to_O2
            for _, row in o2_ac.iterrows():
                if SCENARIO_NO == 2:
                    if row["AC_ID"] in found_ac:
                        continue
                    else:
                        found_ac[row["AC_ID"]] = 1
                carrier = "power_to_O2"
                if OPTIMIZATION == "yes":
                    bus0 = row["OPTIMAL_ID"]
                    bus0_point = row["point_optimal"]
                    if SCENARIO_NO == 1:
                        bus1s = [{"id": row["WWTP_ID"], "point": row["point_wwtp"]}]
                    elif SCENARIO_NO == 2:
                        bus1s = get_wwtps_for_ac(row["AC_ID"])
                else:
                    if SCENARIO_NO == 1:
                        bus0 = row["WWTP_ID"]
                        bus0_point = row["point_wwtp"]
                        bus1s = [{"id": bus0, "point": bus0_point}]
                    elif SCENARIO_NO == 2:
                        bus0 = row["AC_ID"]
                        bus0_point = row["point_AC"]
                        bus1s = get_wwtps_for_ac(bus0)
                for bus1 in bus1s:
                    if SCENARIO_NO == 1:
                        ka_id = row["KA_ID"]
                    else:
                        ka_id = bus1["ka_id"]
            
                    if bus1["id"] == "":
                        continue
            
                    geom = MultiLineString(
                        [LineString([(bus0_point.x, bus0_point.y), (bus1["point"].x, bus1["point"].y)])]
                    )
                    distance = bus0_point.distance(bus1["point"]) / 1000  # km
                    spec = find_spec_for_ka_id(ka_id)
                    wwtp_ec = calculate_wwtp_capacity(spec["pe"])  # [MWh/year]
                    aeration_ec = wwtp_ec * FACTOR_AERATION_EC  # [MWh/year]
                    o2_ec = aeration_ec * FACTOR_O2_EC  # [MWh/year]
                    o2_ec_h = o2_ec / 8760  # [MWh/hour]
                    total_o2_demand = (
                        O2_O3_RATIO * spec["demand_o3"] + spec["demand_o2"] * O2_PURE_RATIO
                    ) * 1000  # kgO2/year pure O2 tonne* 1000
                    h2_production_y = total_o2_demand / (O2_H2_RATIO)  # [kgH2/year]
                    h2_production_h = h2_production_y / 8760
                    elz_capacity = (h2_production_y * ELZ_SEC / ELZ_FLH) / 1000  # [MW]
                    o2_power_ratio = (
                        o2_ec_h / elz_capacity
                    )  # will be use as constraint for the etrago model
                    _, o2_pipeline_diameter = gas_pipeline_size(
                        total_o2_demand,
                        distance,
                        O2_PRESSURE_ELZ,
                        MOLAR_MASS_O2,
                        O2_PRESSURE_MIN,
                    )
            
                    # In below function MW is not considered since the diameter size already calcuated and km is enough
                    annualized_cost_o2_pipeline = get_o2_pipeline_cost(o2_pipeline_diameter) # [EUR/KM/YEAR]
                    annualized_cost_o2_component = O2_COST_EQUIPMENT #[EUR/MW/YEAR]
                    if SCENARIO_NO == 1:
                        annualized_cost_elz = ELZ_CAPEX_STACK + ELZ_CAPEX_SYSTEM + ELZ_OPEX # [EUR/MW/YEAR]
                        annualized_cost_ac_trans = AC_TRANS  # [EUR/MW/YEAR]
                    else:
                        annualized_cost_elz = 0
                        annualized_cost_ac_trans = 0
                    capital_cost_power_to_o2_pipeline = (
                        annualized_cost_o2_pipeline * distance
                    )  # [EUR/YEAR]
                    capital_cost_power_to_o2_component = (
                        annualized_cost_o2_component * o2_ec_h
                    )  # [EUR/YEAR]
                    capital_cost_elz_trans = (
                        annualized_cost_elz + annualized_cost_ac_trans
                    )  # [EUR/YEAR]
                    capital_cost_power_to_o2 = (
                        capital_cost_power_to_o2_pipeline
                        + capital_cost_power_to_o2_component
                        + (capital_cost_elz_trans * elz_capacity)
                    )  # [EUR/YEAR]
                    o2_selling_price = o2_ec * ELEC_COST / total_o2_demand  # EUR/kgO2
                    sellable_o2 = o2_selling_price * O2_H2_RATIO  # EUR/kgH2
            
                    lcoh_o2 = (
                        capital_cost_power_to_o2 / h2_production_y
                    )  # [EUR/Year]/[kgh2/Year]   [EUR/kgH2]
                    total_lcoh += lcoh_o2
                    etrago_cost_power_to_o2 = (
                        (annualized_cost_o2_pipeline * distance / o2_ec_h)
                        + annualized_cost_o2_component
                        + capital_cost_elz_trans
                    )  # [EUR/MW/YEAR]
            
                    links.append(
                        {
                            "bus0": bus0,
                            "bus1": bus1["id"],
                            "carrier": carrier,
                            "efficiency": O2_EFFICIENCY,
                            "power_ratio": o2_power_ratio,
                            "length": distance,
                            "capital_cost": etrago_cost_power_to_o2,
                            "lcoh_capital_cost": capital_cost_power_to_o2,
                            "p_nom": o2_ec_h,
                            "sellable_cost": sellable_o2,
                            "LCOH": lcoh_o2,
                            "elz_capacity": elz_capacity,
                            "diameter": o2_pipeline_diameter,
                            "ka_id": ka_id,
                            "type": ka_id,
                            "lifetime": O2_LIFETIME_PIPELINE,
                            "geom": geom,
                        }
                    )
                    # to accomulate H2 production demand as per O2 for the shared bus of AC
                    if total_h2_production_y.get(f"{bus0}") is None:
                        total_h2_production_y[f"{bus0}"] = h2_production_y
                    else:
                        total_h2_production_y[f"{bus0}"] += h2_production_y
            
            # data calculation for power_to_Heat
            for _, row in ref_heat.iterrows():
                carrier = "power_to_Heat"
                if OPTIMIZATION == "yes":
                    bus0 = row["OPTIMAL_ID"]
                    bus0_point = row["point_optimal"]
                else:
                    if SCENARIO_NO == 1:
                        bus0 = row["WWTP_ID"]
                        bus0_point = get_wwtp_point(bus0)
                    elif SCENARIO_NO == 2:
                        bus0 = row["AC_ID"]
                        bus0_point = get_ac_point(bus0)
            
                bus1 = row["HEAT_ID"]
                distance = bus0_point.distance(row['point_heat']) / 1000
                geom = MultiLineString(
                    [LineString([(bus0_point.x, bus0_point.y), (row['point_heat'].x, row['point_heat'].y)])]
                )
                if f"{bus0}" not in total_h2_production_y:
                    h2_production_y = 10 * 1000 * ELZ_FLH / ELZ_SEC
                else:
                    h2_production_y = total_h2_production_y[f"{bus0}"]  # [kgH2/year]
                h2_production_h = h2_production_y / 8760  # [kgH2/hour]
                elz_capacity = (h2_production_y * ELZ_SEC / ELZ_FLH) / 1000  # [MW]
                heat_production_h = elz_capacity * HEAT_RATIO  # [MWh/hour]
                annualized_capex_heat = HEAT_COST_EXCHANGER  # EUR/MW/year
                annualized_capex_heat_pipeline = HEAT_COST_PIPELINE  # [EUR/MW/KM/YEAR]
                capital_cost_power_to_heat = (
                    annualized_capex_heat + (annualized_capex_heat_pipeline * distance)
                ) * heat_production_h  # [EUR/YEAR]
                sellable_heat = (
                    elz_capacity * HEAT_RATIO * HEAT_SELLING_PRICE / h2_production_h
                )  # [EUR/kgH2]
                lcoh_heat = capital_cost_power_to_heat / h2_production_y  # [EUR/kgH2]
                total_lcoh += lcoh_heat
            
                etrago_cost_power_to_heat = annualized_capex_heat + (
                    annualized_capex_heat_pipeline * distance
                )  # [EUR/MW/YEAR]
            
                links.append(
                    {
                        "bus0": bus0,
                        "bus1": bus1["id"],
                        "carrier": carrier,
                        "efficiency": HEAT_EFFICIENCY,
                        "power_ratio": HEAT_RATIO,
                        "length": distance,
                        "capital_cost": etrago_cost_power_to_heat,
                        "lcoh_capital_cost": capital_cost_power_to_heat,
                        "p_nom": 0,
                        "sellable_cost": sellable_heat,
                        "LCOH": lcoh_heat,
                        "elz_capacity": elz_capacity,
                        "diameter": "",
                        "ka_id": HEAT_RATIO,
                        "type": HEAT_RATIO,
                        "lifetime": HEAT_LIFETIME,
                        "geom": geom,
                    }
                )
            
            # data calculation for power_to_H2
            for _, row in ref_h2.iterrows():
                carrier = "power_to_H2"
                if OPTIMIZATION == "yes":
                    bus0 = row["OPTIMAL_ID"]
                    bus0_point = row["point_optimal"]
                else:
                    if SCENARIO_NO == 1:
                        bus0 = row["WWTP_ID"]
                        bus0_point = get_wwtp_point(bus0)
                    elif SCENARIO_NO == 2:
                        bus0 = row["AC_ID"]
                        bus0_point = get_ac_point(bus0)
            
                bus1 = get_h2_for_ref(bus0)
                distance = bus0_point.distance(bus1["point"]) / 1000
            
                if SCENARIO_NO == 1:
                    ac = o2_ac[o2_ac["WWTP_ID"] == row["WWTP_ID"]].iloc[0]["point_AC"]
                elif SCENARIO_NO == 2:
                    ac = get_ac_point(row["AC_ID"])
                # 		geom = MultiLineString([[[bus0_point.x, bus0_point.y], [ac.x, ac.y]]])
                geom = MultiLineString(
                    [LineString([(bus0_point.x, bus0_point.y), (bus1["point"].x, bus1["point"].y)])]
                )
            
                # Electrolyzer Calculation
            
                if f"{bus0}" not in total_h2_production_y:
                    h2_production_y = 10 * 1000 * ELZ_FLH / ELZ_SEC
                else:
                    h2_production_y = total_h2_production_y[f"{bus0}"]  # [kgH2/year]
                h2_production_h = h2_production_y / 8760  # [kgH2/hour]
                elz_capacity = (h2_production_y * ELZ_SEC / ELZ_FLH) / 1000  # [MW]
                h2_production_energy_h = (
                    h2_production_y * 33.33 / 8760 / 1000
                )  # [MWh/HOUR] or ELZ_capacity * ELZ_EFF
                _, h2_pipeline_diameter = gas_pipeline_size(
                    h2_production_y, distance, H2_PRESSURE_ELZ, MOLAR_MASS_H2, H2_PRESSURE_MIN
                )
                ac_distance = get_ac_distance_for_ref(bus0, o2_ac)  # is this in m or km?
            
                # annualized cost calculation
                annualized_cost_ac_cable = AC_COST_CABLE # [EUR/MVA/km/YEAR]
                if SCENARIO_NO == 2:
                    annualized_cost_elz = ELZ_CAPEX_STACK + ELZ_CAPEX_SYSTEM + ELZ_OPEX  # [EUR/MW/YEAR]
                    annualized_cost_ac_trans = AC_TRANS # [EUR/MW/YEAR]
                else:
                    annualized_cost_elz = 0
                    annualized_cost_ac_trans = 0
                # below calcualtion aimed to find the capital cost of power to H2 for LCOH calculation for stand alone model.
                total_ac_cost = (
                    annualized_cost_ac_cable + annualized_cost_ac_trans + annualized_cost_elz
                ) * elz_capacity  # [EUR/YEAR]
                lcoh_h2_elz = (
                    total_ac_cost + (h2_production_y * ELZ_SEC * ELEC_COST / 1000)
                ) / h2_production_y  # [EUR/kgH2]
                total_lcoh += lcoh_h2_elz
            
                # Since Capital Cost in eTraGO rquires EUR/MW/YEAR not EUR/YEAR. in addition, the power to H2 in etrago relay on cost related to produce hdyrogen and transfering the cost of H2 pipeline will be excluded and will be considered in H2 to Power link.
                etrago_annualized_cost_h2_pipeline = H2_COST_PIPELINE * distance   # [EUR/MW/YEAR]
                etrago_cost_power_to_h2 = (
                    annualized_cost_ac_cable
                    + annualized_cost_ac_trans
                    + annualized_cost_elz
                    + etrago_annualized_cost_h2_pipeline
                )  # [EUR/MW/YEAR]
            
                links.append(
                    {
                        "bus0": bus0,
                        "bus1": bus1["id"],
                        "carrier": carrier,
                        "efficiency": ELZ_EFF,
                        "power_ratio": ac_distance,
                        "length": distance,
                        "capital_cost": etrago_cost_power_to_h2,
                        "lcoh_capital_cost": total_ac_cost,
                        "p_nom": 0,
                        "sellable_cost": "",
                        "LCOH": lcoh_h2_elz,
                        "elz_capacity": elz_capacity,
                        "diameter": h2_pipeline_diameter,
                        "ka_id": "",
                        "type": bus1["type"],
                        "lifetime": HEAT_LIFETIME,
                        "geom": geom,
                        "p_nom_max": 120 if row['sub_type'] == 'HVMV' else 5000,   #source: L. Hülk et al., Allocation of annual electricity consumption and power generation capacities across multi voltage levels in a high spatial resolution, 2017
                    }
                )
            
            # data calculation for H2_to_power
            for _, row in ref_h2.iterrows():
                carrier = "H2_to_power"
                bus0 = row["H2_ID"]
                bus0_point = row["point_H2"]
                type = row["H2_type"]
                if OPTIMIZATION == "yes":
                    bus1 = row["OPTIMAL_ID"]
                    bus1_point = row["point_optimal"]
                else:
                    if SCENARIO_NO == 1:
                        bus1 = row["WWTP_ID"]
                        bus1_point = get_wwtp_point(bus1)
                    elif SCENARIO_NO == 2:
                        bus1 = row["AC_ID"]
                        bus1_point = get_ac_point(bus1)
                distance = bus1_point.distance(bus0_point) / 1000
                geom = MultiLineString(
                    [LineString([(bus1_point.x, bus1_point.y), (bus0_point.x, bus0_point.y)])]
                )
                if f"{bus1}" not in total_h2_production_y:
                    h2_production_y = 10 * 1000 * ELZ_FLH / ELZ_SEC
                else:
                    h2_production_y = total_h2_production_y[f"{bus1}"]  # [kgH2/year]
                h2_production_h = h2_production_y / 8760  # [kgH2/hour]
                elz_capacity = (h2_production_y * ELZ_SEC / ELZ_FLH) / 1000  # [MW]
                h2_production_energy_h = (
                    h2_production_y * 33.33 / 8760 / 1000
                )  # [MWh/HOUR] or ELZ_capacity * ELZ_EFF
                _, h2_pipeline_diameter = gas_pipeline_size(
                    h2_production_y, distance, H2_PRESSURE_ELZ, MOLAR_MASS_H2, H2_PRESSURE_MIN
                )
                # calculating the cost of power to H2 for eTraGO since it is rquired EUR/MW/YEAR not EUR/YEAR
                annualized_cost_h2_pipeline = H2_COST_PIPELINE  # [EUR/KM/YEAR]    
                total_pipeline_cost = annualized_cost_h2_pipeline * distance  # [EUR/YEAR]
                lcoh_h2_pipeline = total_pipeline_cost / h2_production_y  # [EUR/kgH2]
                total_lcoh += lcoh_h2_pipeline
            
                etrago_annualized_cost_h2_pipeline = H2_COST_PIPELINE  # [EUR/KM/YEAR]     #toDO: ask sayed why different calculation to annualized_cost_h2_pipeline
                etrago_annualized_cost_fuel_cell = FUEL_CELL_COST # [EUR/MW/YEAR]
                etrago_cost_h2_to_power = (
                    etrago_annualized_cost_h2_pipeline * distance
                    + etrago_annualized_cost_fuel_cell
                )  # [EUR/MW/YEAR]
            
                links.append(
                    {
                        "bus0": bus0,
                        "bus1": bus1,
                        "carrier": carrier,
                        "efficiency": FUEL_CELL_EFF,
                        "power_ratio": 0,
                        "length": distance,
                        "capital_cost": etrago_cost_h2_to_power,
                        "lcoh_capital_cost": total_pipeline_cost,
                        "p_nom": 0,
                        "sellable_cost": "",
                        "LCOH": lcoh_h2_pipeline,
                        "elz_capacity": elz_capacity,
                        "diameter": h2_pipeline_diameter,
                        "ka_id": "",
                        "type": type,
                        "lifetime": FUEL_CELL_LIFETIME,
                        "geom": geom,
                        "p_nom_max": 120 if row['sub_type'] == 'HVMV' else 5000,
                    }
                )
            
            return gpd.GeoDataFrame(links, geometry="geom"), total_lcoh
        
        
        # Second Phase: Optimization function Method Nelder-Mead
        unoptimized_total = 0
               
        def find_optimal_loc(o2_ac, ref_heat, ref_h2):
            global unoptimized_total
            
            local_o2_ac = o2_ac.copy()
            local_ref_heat = ref_heat.copy()
            local_ref_h2 = ref_h2.copy()
            links_df, unoptimized_total = find_links(local_o2_ac, local_ref_heat, local_ref_h2)
            
            # filter H2_to_power links
            filtered = links_df[links_df["carrier"] != "H2_to_power"]
            unique_optimal_ids = filtered["bus0"].unique()
            
            for id in unique_optimal_ids:
                filtered_o2_ac = local_o2_ac[local_o2_ac["OPTIMAL_ID"] == id]
                filtered_ref_heat = local_ref_heat[local_ref_heat["OPTIMAL_ID"] == id]
                filtered_ref_h2 = local_ref_h2[local_ref_h2["OPTIMAL_ID"] == id]
            
                def _total_cost(center):
                    filtered_o2_ac.loc[filtered_o2_ac["OPTIMAL_ID"] == id, "point_optimal"] = (
                        Point(center)
                    )
                    filtered_ref_h2.loc[
                        filtered_ref_h2["OPTIMAL_ID"] == id, "point_optimal"
                    ] = Point(center)
                    filtered_ref_heat.loc[
                        filtered_ref_heat["OPTIMAL_ID"] == id, "point_optimal"
                    ] = Point(center)
                    try:
                        _, lcoh = find_links(filtered_o2_ac, filtered_ref_heat, filtered_ref_h2)
                    except:
                        return math.inf
            
                    return lcoh
            
                x = filtered_o2_ac["point_optimal"].iloc[0].x
                y = filtered_o2_ac["point_optimal"].iloc[0].y
                optimal_point = minimize(_total_cost, [x, y], method="Nelder-Mead")
                local_o2_ac.loc[local_o2_ac["OPTIMAL_ID"] == id, "point_optimal"] = Point(
                    optimal_point.x
                )
                local_ref_heat.loc[local_ref_heat["OPTIMAL_ID"] == id, "point_optimal"] = Point(
                    optimal_point.x
                )
                local_ref_h2.loc[local_ref_h2["OPTIMAL_ID"] == id, "point_optimal"] = Point(
                    optimal_point.x
                )
            return local_o2_ac, local_ref_heat, local_ref_h2
        
        
        # Second Phase: running the optimization
        if OPTIMIZATION == "yes":
            a, b, c = find_optimal_loc(o2_ac, ref_heat, ref_h2)
            links_df, optimized_total = find_links(a, b, c)
            print("optimized total LCOH: ", optimized_total)
            print("diff: ", unoptimized_total - optimized_total)
            links_df.to_csv(f"SCN-{SCENARIO_NO} Optimized.csv", index=False)
        else:
            links_df, _ = find_links(o2_ac, ref_heat, ref_h2)
            links_df.to_csv(f"SCN-{SCENARIO_NO} Original.csv", index=False)
            links_gdf = links_df.set_crs("EPSG:3857", allow_override=True)
            links_gdf = links_gdf.to_crs("EPSG:4326")
        
        
        # Filter out power_to_O2/power_to_Heat links, which has no connection to H2-sector 
        power_to_H2_bus0 = links_gdf[links_gdf['carrier'] == 'power_to_H2']['bus0'].unique()
        H2_links = links_gdf[links_gdf['carrier'].isin(['power_to_H2', 'H2_to_power'])]
        O2_Heat_Links = links_gdf[
        (links_gdf['carrier'].isin(['power_to_O2', 'power_to_Heat'])) &
        (links_gdf['bus0'].isin(power_to_H2_bus0))]
        filtered_links_df =  pd.concat([H2_links, O2_Heat_Links], ignore_index=True)
        
        #Filter out unused O2-buses
        o2_links = filtered_links_df[filtered_links_df['carrier'] == 'power_to_O2']
        o2_buses = tuple(o2_links['bus1'])
        
        db.execute_sql(
                        f"""DELETE FROM {targets['buses']['schema']}.{targets['buses']['table']} WHERE bus_id NOT IN {o2_buses} AND carrier = 'O2' AND scn_name = '{SCENARIO_NAME}'"""
                    )
        
        # Third Phase: Export to PostgreSQL
        # export links data to PostgreSQL database
        def export_to_db(df):
             df = df.copy(deep=True)
             etrago_columns = [
                 "scn_name",
                 "link_id",
                 "bus0",
                 "bus1",
                 "carrier",
                 "efficiency",
                 "build_year",
                 "lifetime",
                 "p_nom",
                 "p_nom_max",
                 "p_nom_extendable",
                 "capital_cost",
                 "length",
                 "terrain_factor",
                 "type",
                 "geom",
             ]
             max_link_id = db.next_etrago_id("link")
             next_max_link_id = count(start=max_link_id, step=1)
     
             df["scn_name"] = SCENARIO_NAME
             df["p_nom_extendable"] = True
             df["length"] = 0
             df["link_id"] = df["bus0"].apply(lambda _: next(next_max_link_id))
            # df["geom"] = df["geom"].apply(lambda x: wkb.dumps(x, hex=True) if x.is_valid else None)
             #df["geom"] = df["geom"].apply(lambda x: to_wkt(x))
             df = df.filter(items=etrago_columns, axis=1)
             #with engine.connect() as conn:
             db.execute_sql(
                        f"""DELETE FROM {targets['links']['schema']}.{targets['links']['table']} 
                        WHERE carrier IN ('power_to_H2', 'power_to_O2', 'power_to_Heat' , 'H2_to_power') 
                        AND scn_name = '{SCENARIO_NAME}'"""
                )
             df.to_postgis(
                 targets["links"]["table"], 
                 engine, 
                 schema="grid", 
                 if_exists="append", 
                 index=False,
                 dtype={"geom": Geometry()}
             )
     
        print("link data exported to: egon_etrago_link")
        export_to_db(filtered_links_df)
        
        
        
        #Export O2 load to PostgreSQL
        max_load_id = db.next_etrago_id("load")
        next_load_id = count(start=max_load_id, step=1)
        
        
        def insert_o2_load_points(df):
            schema = targets['loads']['schema']
            table_name = targets['loads']['table']
            #with engine.connect() as conn:
            db.execute_sql(
                    f"DELETE FROM {schema}.{table_name} WHERE carrier IN ('O2') AND scn_name = '{SCENARIO_NAME}'"
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
                        "type": "O2",
                        "p_set": row["p_nom"],
                    }
                )
            df = pd.DataFrame(result)
            df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        
        print("O2 load data exported to: egon_etrago_load")
        insert_o2_load_points(links_df)
       
        
        #Reduce affected AC-loads cause of seperate implementation of O2-loads  
        in_zone_filtered = in_zone_hvmv['wwtp'][in_zone_hvmv['wwtp']['id_left'].isin(o2_buses)]
        df_o2 = filtered_links_df[filtered_links_df["carrier"] == "power_to_O2"]
        o2_loads = pd.merge(in_zone_filtered, df_o2[['bus1','p_nom']], left_on='id_left', right_on='bus1')
        o2_loads_unique = o2_loads.drop_duplicates(subset='id_left')
        
        queries[AC_LOAD] = f"""
                            SELECT bus, load_id 
                			FROM {targets['loads']['schema']}.{targets['loads']['table']}
                            WHERE scn_name = '{SCENARIO_NAME}'
                            """
        dfs[AC_LOAD] = pd.read_sql(queries[AC_LOAD], engine)
        negative_loads = pd.merge(o2_loads_unique, dfs[AC_LOAD], left_on='id_right', right_on='bus')
        
        def insert_negative_loads(df):
            for _, row in df.iterrows():
                with engine.connect() as conn:
                    select_query = text(
                        f"""
                        SELECT p_set FROM grid.egon_etrago_load_timeseries
                        WHERE load_id = {row["load_id"]} and scn_name= '{SCENARIO_NAME}'
                    """)
                
                    result = conn.execute(select_query).fetchone()
                    
                    if result:
                        original_p_set = result["p_set"]  
                        p_nom_array = np.full(len(original_p_set), row["p_nom"])
                        adjusted_p_set = (np.array(original_p_set) - p_nom_array).tolist()

                        update_query = text("""
                            UPDATE grid.egon_etrago_load_timeseries
                            SET p_set = :adjusted_p_set
                            WHERE load_id = :load_id and scn_name= :SCENARIO_NAME
                        """)
                        conn.execute(update_query, {"adjusted_p_set": adjusted_p_set, "load_id": row["load_id"], "SCENARIO_NAME": SCENARIO_NAME})
        insert_negative_loads(negative_loads) 
        print("Negative O2 load data exported to: egon_etrago_load")            
        
        
        #Export O2 generator to O2 bus points in to the PostgreSQL database       
        def insert_generator_points(df):
            max_generator_id = db.next_etrago_id("generator")
            next_generator_id = count(start=max_generator_id, step=1)
            schema = targets['generators']['schema']
            table_name = targets['generators']['table']
            #with engine.connect() as conn:
            db.execute_sql(
                f"DELETE FROM {schema}.{table_name} WHERE carrier IN ('O2') AND scn_name = '{SCENARIO_NAME}'"
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
                        "type": "O2",
                        "marginal_cost": ELEC_COST,  # ELEC_COST, # row["O2 sellable [Euro/kgH2]"],
                    }
                )
            df = pd.DataFrame(result)
        
            df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        
        print("generator data exported to: egon_etrago_generator")
        insert_generator_points(links_df)
       
