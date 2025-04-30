"""
The central module containing all code dealing with the hydrogen buses

In this module, the functions allowing to create the H2 buses in Germany
for eTraGo are to be found.
The H2 buses in the neighbouring countries (only present in eGon100RE)
are defined in :py:mod:`pypsaeursec <egon.data.datasets.pypsaeursec>`.
In both scenarios, there are two types of H2 buses in Germany:
  * H2 buses: defined in :py:func:`insert_H2_buses_from_CH4_grid`,
    these buses are located at the places than the CH4 buses.
  * H2_saltcavern buses: defined in :py:func:`insert_H2_buses_from_saltcavern`,
    these buses are located at the intersection of AC buses and
    potential for H2 saltcavern.

"""

from geoalchemy2 import Geometry
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.wkb import loads
import numpy as np
from scipy.spatial import cKDTree

from egon.data import config, db
from egon.data.datasets.etrago_helpers import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)


def insert_hydrogen_buses(scn_name):
    """
    Insert hydrogen buses into the database. 
    Location of H2_buses are based on grid nodes of Wasserstoffkernnetz published by FNB-GAS 
    and the location of saltcaverns in germany. 

    Parameters
    ----------
    scn_name: str
        Name of scenario

    """
    
    h2_input= pd.read_csv(Path(".")/"h2_grid_nodes.csv")   
    h2_input.geom = h2_input.geom.apply(lambda wkb_hex: loads(bytes.fromhex(wkb_hex)))
    
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    target_buses = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_buses"] 
    h2_buses = initialise_bus_insertion('H2_grid', target_buses, scenario = scn_name)
    
    db.execute_sql(
        f"""
        DELETE FROM {target_buses['schema']}.{target_buses['table']}
        WHERE scn_name = '{scn_name}'
        AND carrier = 'H2' AND country = 'DE'
        """
    )
       
    h2_buses.x = h2_input.x
    h2_buses.y = h2_input.y
    h2_buses.geom = h2_input.geom    
    h2_buses.carrier = 'H2_grid'
    h2_buses.scn_name = scn_name
    next_bus_id = db.next_etrago_id('bus')
    h2_buses.bus_id= range(next_bus_id, next_bus_id + len(h2_input))

    h2_buses.to_postgis(
        target_buses["table"],
        schema=target_buses["schema"],
        if_exists="append",
        con=db.engine(),
        dtype={"geom": Geometry()}
    )

    #insert additional_buses for potential Methanisation to CH4_buses nearby:   
    h2_buses = h2_buses.to_crs(epsg=32632)
    
    sql_CH4_buses = f"""
            SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom
            FROM {target_buses["schema"]}.{target_buses["table"]}
            WHERE carrier = 'CH4'
            AND scn_name = '{scn_name}' AND country = 'DE'
            """    
    CH4_buses = gpd.read_postgis(sql_CH4_buses, con=db.engine())
    
    additional_H2_buses = []
    H2_coords = np.array([(point.x, point.y) for point in h2_buses.geometry])
    H2_tree = cKDTree(H2_coords)
    for idx, ch4_bus in CH4_buses.iterrows():
        ch4_coords = [ch4_bus['geom'].x, ch4_bus['geom'].y]
        
        # filter nearest h2_bus
        dist, nearest_idx = H2_tree.query(ch4_coords, k=1)
        # critcical distance assumed with 10km based on former ammount of h2_buses
        if dist > 10000:   
             # Neuen H2-Bus hinzufügen
             additional_H2_buses.append({
                 'scn_name': scn_name, 
                 'bus_id': None,
                 'x': ch4_bus['x'],
                 'y': ch4_bus['y'],
                 'carrier': 'H2',
                 'geom': ch4_bus['geom']
             })

    if additional_H2_buses:
        additional_H2_buses = gpd.GeoDataFrame(additional_H2_buses, geometry='geom', crs=CH4_buses.crs)
    additional_H2_buses =additional_H2_buses.to_crs(epsg=4326)

    next_bus_id = db.next_etrago_id('bus')
    additional_H2_buses['bus_id'] = range(next_bus_id, next_bus_id + len(additional_H2_buses))
    # Insert data to db
    additional_H2_buses.to_postgis(
         target_buses["table"],
         schema= target_buses["schema"],
         con=db.engine(),
         if_exists="append",
         dtype={"geom": Geometry()}
     )
    
    #insert h2_buses_from_saltcaverns
    hydrogen_buses = initialise_bus_insertion( 
            "H2_saltcavern", target_buses, scenario=scn_name
        )
    insert_H2_buses_from_saltcavern(
            hydrogen_buses, "H2_saltcavern", sources, target_buses, scn_name
        )


def insert_H2_buses_from_saltcavern(gdf, carrier, sources, target, scn_name):
    """
    Insert the H2 buses based on saltcavern locations into the database.

    These buses are located at the intersection of AC buses and
    potential for H2 saltcavern.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    carrier : str
        Name of the carrier.
    sources : dict
        Sources schema and table information.
    target : dict
        Target schema and table information.
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """
    # electrical buses related to saltcavern storage
    el_buses = db.select_dataframe(
        f"""
        SELECT bus_id
        FROM  {sources['saltcavern_data']['schema']}.
        {sources['saltcavern_data']['table']}"""
    )["bus_id"]

    # locations of electrical buses (filtering not necessarily required)
    locations = db.select_geodataframe(
        f"""
        SELECT bus_id, geom
        FROM  {sources['buses']['schema']}.
        {sources['buses']['table']} WHERE scn_name = '{scn_name}'
        AND country = 'DE'""",
        index_col="bus_id",
    ).to_crs(epsg=4326)

    # filter by related electrical buses and drop duplicates
    locations = locations.loc[el_buses]
    locations = locations[~locations.index.duplicated(keep="first")]

    # AC bus ids and respective hydrogen bus ids are written to db for
    # later use (hydrogen storage mapping)
    AC_bus_ids = locations.index.copy()

    # create H2 bus data
    hydrogen_bus_ids = finalize_bus_insertion(
        locations, carrier, target, scenario=scn_name
    )

    gdf_H2_cavern = hydrogen_bus_ids[["bus_id"]].rename(
        columns={"bus_id": "bus_H2"}
    )
    gdf_H2_cavern["bus_AC"] = AC_bus_ids
    gdf_H2_cavern["scn_name"] = hydrogen_bus_ids["scn_name"]

    # Insert data to db
    gdf_H2_cavern.to_sql(
        "egon_etrago_ac_h2",
        db.engine(),
        schema="grid",
        index=False,
        if_exists="replace",
    )


