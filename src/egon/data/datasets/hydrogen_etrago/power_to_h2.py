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

"""
from geoalchemy2.types import Geometry
from pyproj import Geod
from scipy.spatial import cKDTree
from shapely import geometry
import numpy as np
import pandas as pd

from egon.data import db, config
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_power_to_h2_to_power():
    """
    Insert electrolysis and fuel cells capacities into the database.

    The potentials for power-to-H2 in electrolysis and H2-to-power in
    fuel cells are created between each H2 bus (H2 and
    H2_saltcavern) and its closest HV power bus.
    These links are extendable. For the electrolysis, if the distance
    between the AC and the H2 bus is > 500m, the maximum capacity of
    the installation is limited to 1 MW.

    This function inserts data into the database and has no return.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    """
    scenarios = config.settings()["egon-data"]["--scenarios"]

    if "status2019" in scenarios:
        scenarios.remove("status2019")

    for scn_name in scenarios:

        # Connect to local database
        engine = db.engine()

        # create bus connections
        gdf = map_buses(scn_name)

        bus_ids = {
            "PtH2": gdf[["bus0", "bus1"]],
            "H2tP": gdf[["bus0", "bus1"]].rename(
                columns={"bus0": "bus1", "bus1": "bus0"}
            ),
        }

        geom = {"PtH2": [], "H2tP": []}
        topo = {"PtH2": [], "H2tP": []}
        p_nom_max = {"PtH2": [], "H2tP": []}
        length = []

        geod = Geod(ellps="WGS84")

        # Add missing columns
        for index, row in gdf.iterrows():
            # Connect AC to gas
            line = geometry.LineString([row["geom_AC"], row["geom_gas"]])
            geom["PtH2"].append(geometry.MultiLineString([line]))
            topo["PtH2"].append(line)

            # Connect gas to AC
            line = geometry.LineString([row["geom_gas"], row["geom_AC"]])
            geom["H2tP"].append(geometry.MultiLineString([line]))
            topo["H2tP"].append(line)

            lenght_km = (
                geod.geometry_length(line) / 1000
            )  # Calculate the distance between the power and the gas buses (lenght of the link)
            length.append(lenght_km)
            if (
                lenght_km > 0.5
            ):  # If the distance is>500m, the max capacity of the power-to-gas installation is limited to 1 MW
                p_nom_max["PtH2"].append(1)
                p_nom_max["H2tP"].append(1)
            else:
                p_nom_max["PtH2"].append(float("Inf"))
                p_nom_max["H2tP"].append(float("Inf"))

        # read carrier information from scnario parameter data
        scn_params = get_sector_parameters("gas", scn_name)

        carrier = {"PtH2": "power_to_H2", "H2tP": "H2_to_power"}
        efficiency = {
            "PtH2": scn_params["efficiency"]["power_to_H2"],
            "H2tP": scn_params["efficiency"]["H2_to_power"],
        }
        capital_cost = {
            "PtH2": scn_params["capital_cost"]["power_to_H2"],
            "H2tP": scn_params["capital_cost"]["H2_to_power"],
        }

        lifetime = {
            "PtH2": scn_params["lifetime"]["power_to_H2"],
            "H2tP": scn_params["lifetime"]["H2_to_power"],
        }

        # Drop unused columns
        gdf.drop(columns=["geom_gas", "geom_AC", "dist"], inplace=True)

        # Iterate over carriers
        for key in geom:

            gdf["geom"] = geom[key]
            gdf = gdf.set_geometry("geom", crs=4326)

            gdf["topo"] = topo[key]

            # Adjust bus id column naming
            gdf["bus0"] = bus_ids[key]["bus0"]
            gdf["bus1"] = bus_ids[key]["bus1"]

            gdf["p_nom_max"] = p_nom_max[key]
            gdf["carrier"] = carrier[key]
            gdf["efficiency"] = efficiency[key]

            gdf["capital_cost"] = capital_cost[key]
            gdf["lifetime"] = lifetime[key]

            gdf["length"] = 0

            gdf["p_nom"] = 0
            gdf["p_nom_extendable"] = True

            print("Minimal length (in km): " + str(gdf["length"].min()))

            # Select next id value
            new_id = db.next_etrago_id("link")
            gdf["link_id"] = range(new_id, new_id + len(gdf))

            # Insert data to db
            gdf.to_postgis(
                "egon_etrago_h2_link",
                engine,
                schema="grid",
                index=False,
                if_exists="replace",
                dtype={"geom": Geometry(), "topo": Geometry()},
            )

            db.execute_sql(
                f"""
            DELETE FROM grid.egon_etrago_link WHERE "carrier" = '{carrier[key]}'
            AND scn_name = '{scn_name}' AND bus0 NOT IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            ) AND bus1 NOT IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            );
    
            select UpdateGeometrySRID('grid', 'egon_etrago_h2_link', 'topo', 4326);
    
            INSERT INTO grid.egon_etrago_link (
                scn_name, link_id, bus0,
                bus1, p_nom, p_nom_extendable, capital_cost, lifetime, length,
                geom, topo, efficiency, carrier, p_nom_max
            )
            SELECT scn_name, link_id, bus0,
                bus1, p_nom, p_nom_extendable, capital_cost, lifetime, length,
                geom, topo, efficiency, carrier, p_nom_max
            FROM grid.egon_etrago_h2_link;
    
            DROP TABLE grid.egon_etrago_h2_link;
                """
            )


def map_buses(scn_name):
    """
    Map H2 buses to nearest HV AC bus.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with connected buses.

    """
    # Create dataframes containing all gas buses and all the HV power buses
    sql_AC = f"""SELECT bus_id, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'AC' AND scn_name = '{scn_name}'
                AND country = 'DE';
                """
    sql_gas = f"""SELECT bus_id, scn_name, geom
                FROM grid.egon_etrago_bus
                WHERE carrier LIKE 'H2%%' AND scn_name = '{scn_name}'
                AND country = 'DE';"""

    gdf_AC = db.select_geodataframe(sql_AC, epsg=4326)
    gdf_gas = db.select_geodataframe(sql_gas, epsg=4326)

    # Associate each gas bus to its nearest HV power bus
    n_gas = np.array(list(gdf_gas.geometry.apply(lambda x: (x.x, x.y))))
    n_AC = np.array(list(gdf_AC.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(n_AC)
    dist, idx = btree.query(n_gas, k=1)
    gd_AC_nearest = (
        gdf_AC.iloc[idx]
        .rename(columns={"bus_id": "bus0", "geom": "geom_AC"})
        .reset_index(drop=True)
    )
    gdf = pd.concat(
        [
            gdf_gas.reset_index(drop=True),
            gd_AC_nearest,
            pd.Series(dist, name="dist"),
        ],
        axis=1,
    )

    return gdf.rename(columns={"bus_id": "bus1", "geom": "geom_gas"})
