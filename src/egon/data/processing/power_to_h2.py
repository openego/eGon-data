# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with the definition of the power-to-H2 installations
"""
import numpy as np
from pyproj import Geod

import pandas as pd
from egon.data import db
from geoalchemy2.types import Geometry
from scipy.spatial import cKDTree
from shapely import geometry


def insert_power_to_h2():
    """Function defining the potential power-to-H2 capacities and inserting them in the etrago_link table.
    The power-to-H2 capacities potentials are created between each H2 bus and its closest HV power bus.
    Returns
    -------
    None.
    """

    # Connect to local database
    engine = db.engine()

    # Select next id value
    new_id = db.next_etrago_id("link")

    # Create dataframes containing all gas buses and all the HV power buses
    sql_AC = """SELECT bus_id, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'AC';"""
    sql_gas = """SELECT bus_id, scn_name, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'H2';"""

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

    gdf = gdf.rename(columns={"bus_id": "bus1", "geom": "geom_gas"})
    geod = Geod(ellps="WGS84")

    # Add missing columns
    geom = []
    topo = []
    length = []
    lines = []
    p_nom_max = []
    for index, row in gdf.iterrows():
        line = geometry.LineString(
            [row["geom_gas"], row["geom_AC"]]
        )  # , srid=4326)
        #        line.transform(4326)
        topo.append(line)
        lines.append(line)
        geom.append(geometry.MultiLineString(lines))
        lines.pop()
        lenght_km = (
            geod.geometry_length(line) / 1000
        )  # Calculate the distance between the power and the gas buses (lenght of the link)
        length.append(lenght_km)
        if (
            lenght_km > 0.5
        ):  # If the distance is>500m, the max capacity of the power-to-gas installation is limited to 1 MW
            p_nom_max.append(1)
        else:
            p_nom_max.append(float("Inf"))

    gdf["geom"] = geom
    gdf = gdf.set_geometry("geom", crs=4326)

    gdf["p_nom_max"] = p_nom_max
    gdf["topo"] = topo
    gdf["length"] = length
    gdf["link_id"] = range(new_id, new_id + len(n_gas))
    gdf["carrier"] = "power-to-H2"
    gdf["efficiency_fixed"] = 0.8  # H2 electrolysis - Brown et al. 2018
    # "Synergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    gdf["p_nom"] = 0
    gdf["p_nom_extendable"] = True
    gdf["capital_cost"] = 350000  # H2 electrolysis - Brown et al. 2018
    # "Synergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4

    ######################################## Do we also consider methanisation? ########################################

    # capital_cost_methanation_DAC = 1000000    # H2 electrolysis - Brown et al. 2018
    # ptch4_efficiency = 0.6 * 0.8              # "Synergies of sector coupling and ...

    gdf = gdf.drop(columns=["geom_gas", "geom_AC", "dist"])  #
    print("Minimal length (in km): " + str(gdf["length"].min()))

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
        """
    DELETE FROM grid.egon_etrago_link WHERE "carrier" = 'power-to-H2';

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
