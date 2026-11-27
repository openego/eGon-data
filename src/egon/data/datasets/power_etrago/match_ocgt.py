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
from egon.data.datasets.chp import nearest
from egon.data.datasets.chp.small_chp import assign_use_case
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.power_plants.hydrogen import heat_to_power_ratio
from egon.data.datasets.scenario_parameters import get_sector_parameters

# Link carriers of the gas turbines: natural gas plants and the hydrogen
# plants by type (conversion of a gas site, new build at a freed
# connection or an existing site, load-near plant)
OCGT_CARRIERS = [
    "OCGT",
    "H2_OCGT_conversion",
    "H2_OCGT_new",
    "H2_OCGT_load_near",
]


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

    buses = tuple(
        db.select_dataframe(
            f"""SELECT bus_id FROM {sources.tables["etrago_bus"]}
            WHERE scn_name = '{scn_name}' AND country = 'DE';
        """
        )["bus_id"]
    )

    # Delete old entries
    carriers = "', '".join(OCGT_CARRIERS)
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["etrago_link"]}
        WHERE "carrier" IN ('{carriers}')
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
        scn_params["marginal_cost"]["OCGT"] * gdf["efficiency"]
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
    "carrier" gives the link carrier (see :data:`OCGT_CARRIERS`), the column
    "efficiency_key" the key of the efficiency in the gas parameters.

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
                    sources->>'siting' AS siting, sources->>'site' AS site
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
            link["carrier"] = "OCGT"
            link["efficiency_key"] = "OCGT"
        else:
            load_near = link.siting == "load-near"
            link["carrier"] = np.select(
                [load_near, link.site == "conversion"],
                ["H2_OCGT_load_near", "H2_OCGT_conversion"],
                "H2_OCGT_new",
            )
            link["efficiency_key"] = np.where(
                load_near, "OCGT_H2_load_near", "OCGT_H2"
            )
        links.append(link.drop(columns=["siting", "site"]))

    if not links:
        return

    gdf = pd.concat(links, ignore_index=True)

    return gdf.rename(columns={"bus_id": "bus1"}).drop(
        columns=["geom", "geom_gas"]
    )


def insert_hydrogen_chp_heat():
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        insert_hydrogen_chp_heat_per_scenario(scenario)


def insert_hydrogen_chp_heat_per_scenario(scn_name):
    """
    Insert the heat links of the hydrogen power plants with CHP

    Conversions of natural gas sites keep the heat-to-power ratio of the
    replaced units (see :py:func:`heat_to_power_ratio
    <egon.data.datasets.power_plants.hydrogen.heat_to_power_ratio>`). As
    for the gas CHP (see :py:mod:`egon.data.datasets.chp` and
    :py:mod:`egon.data.datasets.chp_etrago`), plants used in district
    heating are linked from their fuel bus (as their gas turbine link) to
    the central heat bus of the nearest district heating area.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """

    sources, targets = load_sources_and_targets("OpenCycleGasTurbineEtrago")
    chp_sources, _ = load_sources_and_targets("Chp")

    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["etrago_link"]}
        WHERE carrier = 'central_H2_CHP_heat'
        AND scn_name = '{scn_name}';
        """
    )

    plants = db.select_geodataframe(
        f"""
        SELECT id, el_capacity, source_id->>'replaced_units' AS units, geom
        FROM {sources.tables["power_plants"]}
        WHERE carrier = 'hydrogen'
        AND scenario = '{scn_name}'
        AND sources->>'site' = 'conversion'
        """,
        index_col="id",
        epsg=4326,
    )
    if plants.empty:
        return
    plants["th_capacity"] = plants.el_capacity * heat_to_power_ratio(
        plants.units
    )
    plants = plants[plants.th_capacity > 0]
    if plants.empty:
        return

    plants = assign_use_case(plants, chp_sources, scn_name)
    plants = plants[plants.district_heating]
    if plants.empty:
        return

    # Nearest district heating area and its central heat bus
    areas = db.select_geodataframe(
        f"""
        SELECT area_id, ST_Centroid(geom_polygon) AS geom
        FROM {sources.tables["district_heating_areas"]}
        WHERE scenario = '{scn_name}'
        """,
        epsg=4326,
    )
    plants["area_id"] = plants.apply(
        nearest,
        df=areas,
        row_geom_col="geom",
        df_geom_col="geom",
        centroid=True,
        src_column="area_id",
        axis=1,
    )
    heat_buses = db.select_dataframe(
        f"""
        SELECT b.area_id, c.bus_id AS bus1
        FROM {sources.tables["district_heating_areas"]} b
        JOIN {sources.tables["etrago_bus"]} c
        ON ST_DWithin(
            ST_Transform(ST_Centroid(b.geom_polygon), 4326),
            c.geom,
            0.00000001)
        WHERE b.scenario = '{scn_name}'
        AND c.scn_name = '{scn_name}'
        AND c.carrier = 'central_heat'
        """,
        index_col="area_id",
    )
    plants = plants.join(heat_buses, on="area_id", how="inner")

    # Nearest fuel bus, as for the gas turbines in map_buses
    fuel = db.select_geodataframe(
        f"""
        SELECT bus_id, geom
        FROM {sources.tables["etrago_bus"]}
        WHERE carrier IN ('H2', 'H2_grid')
        AND scn_name = '{scn_name}'
        AND country = 'DE'
        """,
        epsg=4326,
    )
    tree = cKDTree(np.column_stack([fuel.geometry.x, fuel.geometry.y]))
    _, idx = tree.query(
        np.column_stack([plants.geometry.x, plants.geometry.y]), k=1
    )
    plants["bus0"] = fuel.bus_id.values[idx]

    efficiency = get_sector_parameters("gas", scn_name)["efficiency"][
        "central_H2_CHP_heat"
    ]
    links = plants.groupby(["bus0", "bus1"], as_index=False).th_capacity.sum()
    links = links.assign(
        scn_name=scn_name,
        carrier="central_H2_CHP_heat",
        efficiency=efficiency,
        p_nom=links.th_capacity / efficiency,
        p_nom_extendable=False,
    ).drop(columns="th_capacity")
    links = link_geom_from_buses(links, scn_name)
    links["link_id"] = db.next_etrago_id("link", len(links))

    links.to_postgis(
        targets.get_table_name("etrago_link"),
        db.engine(),
        schema=targets.get_table_schema("etrago_link"),
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )
