# -*- coding: utf-8 -*-
"""
Module containing the definition of the AC grid to H2 links
"""
from geoalchemy2.types import Geometry
from pyproj import Geod
from scipy.spatial import cKDTree
from shapely import geometry
import numpy as np
import pandas as pd

from egon.data import db


def insert_power_to_h2_to_power():
    """Define power-to-H2-to-power capacities and insert in etrago_link table.

    The potentials for power-to-h2 in electrolysis and h2-to-power in fuel
    cells are created between each H2 bus and its closest HV power bus.

    Returns
    -------
    None.
    """

    # Connect to local database
    engine = db.engine()

    # create bus connections
    gdf = map_buses()

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

    # "Synergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    carrier = {"PtH2": "power-to-H2", "H2tP": "H2-to-power"}
    efficiency = {
        "PtH2": 0.8,  # H2 electrolysis - Brown et al. 2018
        "H2tP": 0.58,  # H2 fuel cell - Brown et al. 2018
    }
    capital_cost = {
        "PtH2": 350000,  # H2 electrolysis - Brown et al. 2018
        "H2tP": 339000,  # H2 fuel cell - Brown et al. 2018
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
        gdf["efficiency_fixed"] = efficiency[key]

        gdf["capital_cost"] = capital_cost[key]

        gdf["length"] = length

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
        DELETE FROM grid.egon_etrago_link WHERE "carrier" = '{carrier[key]}';

        select UpdateGeometrySRID('grid', 'egon_etrago_h2_link', 'topo', 4326) ;

        INSERT INTO grid.egon_etrago_link (scn_name, link_id, bus0,
                                                  bus1, p_nom, p_nom_extendable, capital_cost,length,
                                                  geom, topo, efficiency_fixed, carrier, p_nom_max)
        SELECT scn_name, link_id, bus0,
            bus1, p_nom, p_nom_extendable, capital_cost, length,
            geom, topo, efficiency_fixed, carrier, p_nom_max
        FROM grid.egon_etrago_h2_link;

        DROP TABLE grid.egon_etrago_h2_link;
            """
        )


def map_buses():
    """Map H2 buses to nearest HV AC bus.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with connected buses.
    """
    # Create dataframes containing all gas buses and all the HV power buses
    sql_AC = """SELECT bus_id, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'AC';"""
    sql_gas = """SELECT bus_id, scn_name, geom
                FROM grid.egon_etrago_bus
                WHERE carrier LIKE 'H2%%';"""

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
