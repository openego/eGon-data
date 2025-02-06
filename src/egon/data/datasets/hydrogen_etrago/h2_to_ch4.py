# -*- coding: utf-8 -*-
"""
Module containing the definition of the links between H2 and CH4 buses

In this module the functions used to define and insert into the database
the links between H2 and CH4 buses are to be found.
These links are modelling:
  * Methanisation (carrier name: 'H2_to_CH4'): technology to produce CH4
    from H2
  * H2_feedin: Injection of H2 into the CH4 grid
  * Steam Methane Reaction (SMR, carrier name: 'CH4_to_H2'): techonology
    to produce CH4 from H2

"""

from geoalchemy2.types import Geometry
import geopandas as gpd
import numpy as np
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiLineString

from egon.data import db, config
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_h2_to_ch4_to_h2():
    """
    Method for implementing Methanisation as optional usage of H2-Production;
    For H2_Buses and CH4_Buses with distance < 10 km Methanisation/SMR Link will be implemented
    
    Define the potentials for methanisation and Steam Methane Reaction
    (SMR) modelled as extendable links
    
    Returns
    -------
    None
        
    """
     
    scenarios = config.settings()["egon-data"]["--scenarios"]
    con=db.engine()
    target_links = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_links"]
    target_buses = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_buses"]
    

    if "status2019" in scenarios:
        scenarios.remove("status2019")

    for scn_name in scenarios:
        
        db.execute_sql(f"""
           DELETE FROM {target_links["schema"]}.{target_links["table"]} WHERE "carrier" in ('H2_to_CH4', 'CH4_to_H2')
           AND scn_name = '{scn_name}' AND bus0 IN (
             SELECT bus_id
             FROM {target_buses["schema"]}.{target_buses["table"]}
             WHERE country = 'DE'
             )
           """)
         
        sql_CH4_buses = f"""
                SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom
                FROM {target_buses["schema"]}.{target_buses["table"]} 
                WHERE carrier = 'CH4'
                AND scn_name = '{scn_name}' AND country = 'DE'
                """            
        sql_H2_buses = f"""
                SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom
                FROM {target_buses["schema"]}.{target_buses["table"]} 
                WHERE carrier in ('H2')
                AND scn_name = '{scn_name}' AND country = 'DE'
                """    
        CH4_buses = gpd.read_postgis(sql_CH4_buses, con)
        H2_buses = gpd.read_postgis(sql_H2_buses, con)   
        
        CH4_to_H2_links = []
        H2_to_CH4_links = []
        
        CH4_coords = np.array([(point.x, point.y) for point in CH4_buses.geometry])
        CH4_tree = cKDTree(CH4_coords)

        for idx, h2_bus in H2_buses.iterrows():
            h2_coords = [h2_bus['geom'].x, h2_bus['geom'].y]
            
            #Filter nearest CH4_bus
            dist, nearest_idx = CH4_tree.query(h2_coords, k=1)
            nearest_ch4_bus = CH4_buses.iloc[nearest_idx]

            if dist < 10000:
                CH4_to_H2_links.append({
                    'scn_name': scn_name,
                    'link_id': None,
                    'bus0': nearest_ch4_bus['bus_id'],
                    'bus1': h2_bus['bus_id'],
                    'geom': MultiLineString([LineString([(h2_bus['x'], h2_bus['y']), (nearest_ch4_bus['x'], nearest_ch4_bus['y'])])])
                    })
            
        H2_to_CH4_links = [
        {
            'scn_name': link['scn_name'],
            'link_id': link['link_id'],
            'bus0': link['bus1'],  # Swap bus0 and bus1
            'bus1': link['bus0'],
            'geom': link['geom']
        }
        for link in CH4_to_H2_links
        ]
        
        #set crs for geoDataFrame
        CH4_to_H2_links = gpd.GeoDataFrame(CH4_to_H2_links, geometry='geom', crs=4326)
        H2_to_CH4_links = gpd.GeoDataFrame(H2_to_CH4_links, geometry='geom', crs=4326)

        
        scn_params = get_sector_parameters("gas", scn_name)
        technology = [CH4_to_H2_links, H2_to_CH4_links]
        links_carriers = ["CH4_to_H2", "H2_to_CH4"]
        
        # Write new entries
        for table, carrier in zip(technology, links_carriers):
            # set parameters according to carrier name
            table["carrier"] = carrier
            table["efficiency"] = scn_params["efficiency"][carrier]   
            table["p_nom_extendable"] = True
            table["capital_cost"] = scn_params["capital_cost"][carrier]
            table["lifetime"] = scn_params["lifetime"][carrier]
            new_id = db.next_etrago_id("link")
            table["link_id"] = range(new_id, new_id + len(table))


            table.to_postgis(
                target_links["table"],
                con,
                schema=target_links["schema"],
                index=False,
                if_exists="append",
                dtype={"geom": Geometry()},
            )

        

def H2_CH4_mix_energy_fractions(x, T=25, p=50):
    """
    Calculate the fraction of H2 with respect to energy in a H2 CH4 mixture.

    Given the volumetric fraction of H2 in a H2 and CH4 mixture, the fraction
    of H2 with respect to energy is calculated with the ideal gas mixture law.
    Beware, that changing the fraction of H2 changes the overall energy within
    a specific volume of the mixture. If H2 is fed into CH4, the pipeline
    capacity (based on energy) therefore decreases if the volumetric flow
    does not change. This effect is neglected in eGon. At 15 vol% H2 the
    decrease in capacity equals about 10 % if volumetric flow does not change.

    Parameters
    ----------
    x : float
        Volumetric fraction of H2 in the mixture
    T : int, optional
        Temperature of the mixture in °C, by default 25
    p : int, optional
        Pressure of the mixture in bar, by default 50

    Returns
    -------
    float
        Fraction of H2 in mixture with respect to energy (LHV)

    """

    # molar masses
    M_H2 = 0.00201588
    M_CH4 = 0.0160428

    # universal gas constant (fluid independent!)
    R_u = 8.31446261815324
    # individual gas constants
    R_H2 = R_u / M_H2
    R_CH4 = R_u / M_CH4

    # volume is fixed: 1m^3, use ideal gas law at 25 °C, 50 bar
    V = 1
    T += 273.15
    p *= 1e5
    # volumetric shares of gases (specify share of H2)
    V_H2 = x
    V_CH4 = 1 - x

    # calculate data of mixture
    M_mix = V_H2 * M_H2 + V_CH4 * M_CH4
    R_mix = R_u / M_mix
    m_mix = p * V / (R_mix * T)

    # calulate masses with volumetric shares at mixture pressure
    m_H2 = p * V_H2 / (R_H2 * T)
    m_CH4 = p * V_CH4 / (R_CH4 * T)

    msg = (
        "Consistency check faild, individual masses are not equal to sum of "
        "masses. Residual is: " + str(m_mix - m_H2 - m_CH4)
    )
    assert round(m_mix - m_H2 - m_CH4, 6) == 0.0, msg

    LHV = {"CH4": 50e6, "H2": 120e6}

    return m_H2 * LHV["H2"] / (m_H2 * LHV["H2"] + m_CH4 * LHV["CH4"])
