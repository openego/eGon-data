# -*- coding: utf-8 -*-
"""
Module containing the definition of the open cycle gas turbine links
"""

from geoalchemy2.types import Geometry
from scipy.spatial import cKDTree
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_open_cycle_gas_turbines():
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        insert_open_cycle_gas_turbines_per_scenario(scenario)


def insert_open_cycle_gas_turbines_per_scenario(scn_name):
    """Insert gas turbine links in egon_etrago_link table.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    ----------
    None

    """
    sources, targets = load_sources_and_targets("OpenCycleGasTurbineEtrago")

    # Connect to local database
    engine = db.engine()

    # create bus connections
    gdf = map_buses(scn_name)

    if gdf is None:
        return

    # create topology column (linestring)
    gdf = link_geom_from_buses(gdf, scn_name)
    gdf["p_nom_extendable"] = False
    carrier = "OCGT"
    gdf["carrier"] = carrier

    buses = tuple(
        db.select_dataframe(
            f"""SELECT bus_id FROM {sources.tables["etrago_bus"]}
            WHERE scn_name = '{scn_name}' AND country = 'DE';
        """
        )["bus_id"]
    )

    # Delete old entries
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["etrago_link"]}
        WHERE "carrier" = '{carrier}'
        AND scn_name = '{scn_name}'
        AND bus1 IN {buses};
        """
    )

    # read carrier information from scnario parameter data
    scn_params = get_sector_parameters("gas", scn_name)
    missing = set(gdf.efficiency_key) - set(scn_params["efficiency"])
    if missing:
        raise KeyError(
            f"{scn_name}: no efficiency parameter {missing} for the gas "
            "turbines, see scenario_parameters.parameters.gas"
        )
    gdf["efficiency"] = gdf.efficiency_key.map(scn_params["efficiency"])
    # VOM per MWh of fuel = VOM per MWh_el x efficiency (as PyPSA-Eur)
    gdf["marginal_cost"] = (
        scn_params["marginal_cost"][carrier] * gdf["efficiency"]
    )
    gdf = gdf.drop(columns="efficiency_key")

    # Adjust p_nom
    gdf["p_nom"] = gdf["p_nom"] / gdf["efficiency"]

    # Select next id value
    gdf["link_id"] = db.next_etrago_id("link", len(gdf))

    # Insert data to db
    gdf.to_postgis(
        targets.get_table_name("etrago_link"),
        engine,
        schema=targets.get_table_schema("etrago_link"),
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )


def map_buses(scn_name):
    """
    Map the AC buses of the gas turbines to the nearest fuel bus.

    Gas plants ("gas") are connected to the nearest CH4 bus, hydrogen power
    plants ("hydrogen") to the nearest H2 or H2_grid bus. The column
    "efficiency_key" gives the key of the efficiency in the gas parameters.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with connected buses.

    """
    sources, _ = load_sources_and_targets("OpenCycleGasTurbineEtrago")

    links = []
    for plant_carrier, bus_carriers in [
        ("gas", "('CH4')"),
        ("hydrogen", "('H2', 'H2_grid')"),
    ]:
        sql_AC = f"""SELECT bus_id, el_capacity as p_nom, geom,
                    sources->>'siting' AS siting
                    FROM {sources.tables["power_plants"]}
                    WHERE carrier = '{plant_carrier}'
                    AND scenario = '{scn_name}';
                    """
        gdf_AC = db.select_geodataframe(sql_AC, epsg=4326)
        if gdf_AC.size == 0:
            continue

        sql_fuel = f"""SELECT bus_id, scn_name, geom
                    FROM {sources.tables["etrago_bus"]}
                    WHERE carrier IN {bus_carriers}
                    AND scn_name = '{scn_name}'
                    AND country = 'DE';"""
        gdf_fuel = db.select_geodataframe(sql_fuel, epsg=4326)
        if gdf_fuel.size == 0:
            print(
                f"Warning: {scn_name}: no {bus_carriers} bus for the "
                f"{plant_carrier} power plants, they are not connected."
            )
            continue

        # Associate each power plant AC bus to the nearest fuel bus
        n_fuel = np.array(list(gdf_fuel.geometry.apply(lambda x: (x.x, x.y))))
        n_AC = np.array(list(gdf_AC.geometry.apply(lambda x: (x.x, x.y))))
        btree = cKDTree(n_fuel)
        dist, idx = btree.query(n_AC, k=1)
        nearest = (
            gdf_fuel.iloc[idx]
            .rename(columns={"bus_id": "bus0", "geom": "geom_gas"})
            .reset_index(drop=True)
        )
        link = pd.concat([gdf_AC.reset_index(drop=True), nearest], axis=1)
        if plant_carrier == "gas":
            link["efficiency_key"] = "OCGT"
        else:
            link["efficiency_key"] = np.where(
                link.siting == "load-near", "OCGT_H2_load_near", "OCGT_H2"
            )
        links.append(link.drop(columns="siting"))

    if not links:
        return

    gdf = pd.concat(links, ignore_index=True)

    return gdf.rename(columns={"bus_id": "bus1"}).drop(
        columns=["geom", "geom_gas"]
    )
