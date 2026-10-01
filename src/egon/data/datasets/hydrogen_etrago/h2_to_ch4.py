# -*- coding: utf-8 -*-
"""
Module containing the definition of the links between H2 and CH4 buses

In this module the functions used to define and insert the links between
H2 and CH4 buses into the database are to be found.
These links are modelling:

* Methanisation (carrier name: 'H2_to_CH4'): technology to produce CH4 from H2
* Steam Methane Reaction (SMR, carrier name: 'CH4_to_H2'): technology
  to produce H2 from CH4

"""

from geoalchemy2.types import Geometry
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiLineString
import geopandas as gpd
import numpy as np

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
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

    sources, targets = load_sources_and_targets("HydrogenMethaneLinkEtrago")
    scenarios = config.settings()["egon-data"]["--scenarios"]
    con = db.engine()
    target_links = targets.tables["hydrogen_links"]
    target_buses = sources.tables["buses"]

    for scn_name in scenarios:
        if "status" in scn_name:
            continue

        db.execute_sql(
            f"""
           DELETE FROM {target_links} WHERE "carrier" in ('H2_to_CH4', 'CH4_to_H2')
           AND scn_name = '{scn_name}' AND (bus0 IN (
             SELECT bus_id
             FROM {target_buses}
             WHERE country = 'DE' AND scn_name = '{scn_name}'
             ) OR bus1 IN (
             SELECT bus_id
             FROM {target_buses}
             WHERE country = 'DE' AND scn_name = '{scn_name}'
             ))
           """
        )

        sql_CH4_buses = f"""
                SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom
                FROM {target_buses}
                WHERE carrier = 'CH4'
                AND scn_name = '{scn_name}' AND country = 'DE'
                """
        # Both H2 carriers ('H2_grid' and 'H2') are coupled to the CH4 grid
        sql_H2_buses = f"""
                SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom
                FROM {target_buses}
                WHERE carrier in ('H2', 'H2_grid')
                AND scn_name = '{scn_name}' AND country = 'DE'
                """
        CH4_buses = gpd.read_postgis(sql_CH4_buses, con)
        H2_buses = gpd.read_postgis(sql_H2_buses, con)

        if CH4_buses.empty or H2_buses.empty:
            print(
                f"{scn_name}: no German CH4 or H2 buses, no methanisation "
                "and SMR links are inserted."
            )
            continue

        CH4_to_H2_links = []
        H2_to_CH4_links = []

        CH4_coords = np.array(
            [(point.x, point.y) for point in CH4_buses.geometry]
        )
        CH4_tree = cKDTree(CH4_coords)

        for idx, h2_bus in H2_buses.iterrows():
            h2_coords = [h2_bus["geom"].x, h2_bus["geom"].y]

            # Filter nearest CH4_bus
            dist, nearest_idx = CH4_tree.query(h2_coords, k=1)
            nearest_ch4_bus = CH4_buses.iloc[nearest_idx]

            if dist < 10000:
                CH4_to_H2_links.append(
                    {
                        "scn_name": scn_name,
                        "link_id": None,
                        "bus0": nearest_ch4_bus["bus_id"],
                        "bus1": h2_bus["bus_id"],
                        "geom": MultiLineString(
                            [
                                LineString(
                                    [
                                        (h2_bus["x"], h2_bus["y"]),
                                        (
                                            nearest_ch4_bus["x"],
                                            nearest_ch4_bus["y"],
                                        ),
                                    ]
                                )
                            ]
                        ),
                    }
                )

        H2_to_CH4_links = [
            {
                "scn_name": link["scn_name"],
                "link_id": link["link_id"],
                "bus0": link["bus1"],  # Swap bus0 and bus1
                "bus1": link["bus0"],
                "geom": link["geom"],
            }
            for link in CH4_to_H2_links
        ]

        if not CH4_to_H2_links:
            print(
                f"{scn_name}: no H2 bus within 10 km of a CH4 bus, no "
                "methanisation and SMR links are inserted."
            )
            continue

        # set crs for geoDataFrame
        CH4_to_H2_links = gpd.GeoDataFrame(
            CH4_to_H2_links, geometry="geom", crs=4326
        )
        H2_to_CH4_links = gpd.GeoDataFrame(
            H2_to_CH4_links, geometry="geom", crs=4326
        )

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
            table["link_id"] = db.next_etrago_id("link", len(table))

            table.to_postgis(
                targets.get_table_name("hydrogen_links"),
                con,
                schema=targets.get_table_schema("hydrogen_links"),
                index=False,
                if_exists="append",
                dtype={"geom": Geometry()},
            )
