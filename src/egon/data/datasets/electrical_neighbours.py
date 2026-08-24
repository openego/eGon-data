"""The central module containing all code dealing with electrical neighbours"""

from os import path
from pathlib import Path
import datetime
import functools
import io
import logging
import os.path
import zipfile

from shapely.geometry import LineString
from sqlalchemy.orm import sessionmaker
import entsoe
import geopandas as gpd
import pandas as pd
import requests

from egon.data import config, db, logger
from egon.data.datasets import (
    Dataset,
    DatasetSources,
    DatasetTargets,
)
from egon.data.datasets.fill_etrago_gen import add_marginal_costs
from egon.data.datasets.fix_ehv_subnetworks import select_bus_id
from egon.data.datasets.pypsaeur import prepared_network
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)
from egon.data.db import session_scope
import egon.data.datasets.etrago_setup as etrago
import egon.data.datasets.scenario_parameters.parameters as scenario_parameters


# Latitude/longitude of TYNDP
# "Nodes - Dict" sheet of TYNDP-2020-Scenario-Datafile.xlsx
# (https://2020.entsos-tyndp-scenarios.eu), restricted to the node_id
# prefixes used in eGon-data
TYNDP_NODE_COORDINATES = {
    "AT00": (47.64, 14.84),
    "BE00": (50.8, 4.72),
    "CH00": (46.95, 8.09),
    "CZ00": (49.85, 15.43),
    "DK00": (56.113, 9.096),
    "DKE1": (55.51, 11.8),
    "DKKF": (54.76, 12.32),
    "DKW1": (55.99, 9.16),
    "FR00": (47.1, 2.4),
    "FR15": (42.12, 9.11),
    "LU00": (49.671, 6.113),
    "LUB1": (49.92, 5.87),
    "LUF1": (49.64, 5.97),
    "LUG1": (49.65, 6.27),
    "LUV1": (49.96, 6.13),
    "NL00": (52.23, 5.63),
    "NO00": (61.3701, 9.3031),
    "NOM1": (63.21, 10.26),
    "NON1": (68.82, 17.31),
    "NOS0": (60.16, 7.85),
    "NOS1": (58.72, 4.44),
    "PL00": (52.32, 19.17),
    "PLE0": (51.41, 15.88),
    "PLI0": (51.41, 15.88),
    "RU00": (64.736, 104.062),
    "SE00": (66.2188, 19),
    "SE01": (67.13, 20.2),
    "SE02": (63.13, 15.5),
    "SE03": (59.71, 14.8),
    "SE04": (56.13, 13.57),
    "UK00": (53.81, -1.75),
    "UKNI": (54.58, -6.63),
}


def get_cross_border_buses(scenario, sources):
    """Returns buses from osmTGmod which are outside of Germany.

    Parameters
    ----------
    sources : dict
        List of sources

    Returns
    -------
    geopandas.GeoDataFrame
        Electricity buses outside of Germany

    """
    return db.select_geodataframe(
        f"""
        SELECT *
        FROM {sources.tables['electricity_buses']}
        WHERE
        NOT ST_INTERSECTS(
            geom,
            (SELECT ST_Transform(ST_Buffer(geometry, 5), 4326)
              FROM {sources.tables['german_borders']}))
        AND (bus_id IN (
             SELECT bus0
              FROM {sources.tables['lines']})
             OR bus_id IN (
             SELECT bus1
             FROM {sources.tables['lines']}))
        AND scn_name = '{scenario}';
      """,
        epsg=4326,
    )


def get_cross_border_lines(scenario, sources):
    """Returns lines from osmTGmod which end or start outside of Germany.

    Parameters
    ----------
    sources : dict
        List of sources

    Returns
    -------
    geopandas.GeoDataFrame
        AC-lines outside of Germany

    """
    return db.select_geodataframe(
        f"""
    SELECT *
    FROM {sources.tables['lines']} a
    WHERE
    ST_INTERSECTS (
        a.topo,
        (SELECT ST_Transform(ST_boundary(geometry), 4326)
         FROM {sources.tables['german_borders']})
    )
    AND scn_name = '{scenario}';
    """,
        epsg=4326,
    )


def central_buses_pypsaeur(sources, scenario):
    """Returns buses in the middle of foreign countries based on prepared pypsa-eur network

    Parameters
    ----------
    sources : dict
        List of sources

    Returns
    -------
    pandas.DataFrame
        Buses in the center of foreign countries

    """

    wanted_countries = [
        "AT",
        "CH",
        "CZ",
        "PL",
        "SE",
        "NO",
        "DK",
        "GB",
        "NL",
        "BE",
        "FR",
        "LU",
    ]
    network = prepared_network()

    df = network.buses[
        (network.buses.carrier == "AC")
        & (network.buses.country.isin(wanted_countries))
    ]

    return df


def buses(scenario, sources, targets):
    """Insert central buses in foreign countries per scenario

    Parameters
    ----------
    sources : dict
        List of dataset sources
    targets : dict
        List of dataset targets

    Returns
    -------
    central_buses : geoapndas.GeoDataFrame
        Buses in the center of foreign countries

    """
    sql_delete = f"""
        DELETE FROM {sources.tables['electricity_buses']}
        WHERE country != 'DE' AND scn_name = '{scenario}'
        AND carrier = 'AC'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM {sources.tables['osmtgmod_bus']})
        """

    # Delete existing buses
    db.execute_sql(sql_delete)

    central_buses = central_buses_pypsaeur(sources, scenario)

    central_buses["bus_id"] = db.next_etrago_id("bus", len(central_buses))

    # if in test mode, add bus in center of Germany
    if config.settings()["egon-data"]["--dataset-boundary"] != "Everything":
        central_buses = pd.concat(
            [
                central_buses,
                pd.DataFrame(
                    index=[db.next_etrago_id("bus")],
                    data={
                        "scn_name": scenario,
                        "bus_id": db.next_etrago_id("bus"),
                        "x": 10.4234469,
                        "y": 51.0834196,
                        "country": "DE",
                        "carrier": "AC",
                        "v_nom": 380.0,
                    },
                ),
            ],
            ignore_index=True,
        )

    # Add buses for other voltage levels
    foreign_buses = get_cross_border_buses(scenario, sources)
    if config.settings()["egon-data"]["--dataset-boundary"] == "Everything":
        foreign_buses = foreign_buses[foreign_buses.country != "DE"]
    vnom_per_country = foreign_buses.groupby("country").v_nom.unique().copy()
    for cntr in vnom_per_country.index:
        print(cntr)
        if 110.0 in vnom_per_country[cntr]:
            central_buses = pd.concat(
                [
                    central_buses,
                    pd.DataFrame(
                        index=[db.next_etrago_id("bus")],
                        data={
                            "scn_name": scenario,
                            "bus_id": db.next_etrago_id("bus"),
                            "x": central_buses[
                                central_buses.country == cntr
                            ].x.unique()[0],
                            "y": central_buses[
                                central_buses.country == cntr
                            ].y.unique()[0],
                            "country": cntr,
                            "carrier": "AC",
                            "v_nom": 110.0,
                        },
                    ),
                ],
                ignore_index=True,
            )

        if 220.0 in vnom_per_country[cntr]:
            central_buses = pd.concat(
                [
                    central_buses,
                    pd.DataFrame(
                        index=[db.next_etrago_id("bus")],
                        data={
                            "scn_name": scenario,
                            "bus_id": db.next_etrago_id("bus"),
                            "x": central_buses[
                                central_buses.country == cntr
                            ].x.unique()[0],
                            "y": central_buses[
                                central_buses.country == cntr
                            ].y.unique()[0],
                            "country": cntr,
                            "carrier": "AC",
                            "v_nom": 220.0,
                        },
                    ),
                ],
                ignore_index=True,
            )

    # Add geometry column
    central_buses = gpd.GeoDataFrame(
        central_buses,
        geometry=gpd.points_from_xy(central_buses.x, central_buses.y),
        crs="EPSG:4326",
    )
    central_buses["geom"] = central_buses.geometry.copy()
    central_buses = central_buses.set_geometry("geom").drop(
        "geometry", axis="columns"
    )
    central_buses.scn_name = scenario

    central_buses.drop(
        [
            "control",
            "generator",
            "location",
            "unit",
            "sub_network",
            "substation_off",
            "substation_lv",
        ],
        axis="columns",
        inplace=True,
        errors="ignore",
    )

    # Insert all central buses for the scenario
    central_buses.to_postgis(
        targets.get_table_name("buses"),
        schema=targets.get_table_schema("buses"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )

    return central_buses


def lines_between_foreign_countries(scenario, sources, targets, central_buses):
    # import network from pypsa-eur
    network = prepared_network()

    gdf_buses = gpd.GeoDataFrame(
        network.buses,
        geometry=gpd.points_from_xy(network.buses.x, network.buses.y),
    )

    central_buses_pypsaeur = gpd.sjoin(
        gdf_buses[gdf_buses.carrier == "AC"], central_buses
    )

    central_buses_pypsaeur = central_buses_pypsaeur[
        central_buses_pypsaeur.v_nom_right == 380
    ]

    lines_to_add = network.lines[
        (network.lines.bus0.isin(central_buses_pypsaeur.index))
        & (network.lines.bus1.isin(central_buses_pypsaeur.index))
    ]

    lines_to_add.loc[:, "lifetime"] = get_sector_parameters(
        "electricity", scenario
    )["lifetime"]["ac_ehv_overhead_line"]
    lines_to_add.loc[:, "line_id"] = db.next_etrago_id(
        "line", len(lines_to_add.index)
    )

    links_to_add = network.links[
        (network.links.bus0.isin(central_buses_pypsaeur.index))
        & (network.links.bus1.isin(central_buses_pypsaeur.index))
    ]

    links_to_add.loc[:, "lifetime"] = get_sector_parameters(
        "electricity", scenario
    )["lifetime"]["dc_overhead_line"]
    links_to_add.loc[:, "link_id"] = db.next_etrago_id(
        "link", len(links_to_add.index)
    )

    for df in [lines_to_add, links_to_add]:
        df.loc[:, "scn_name"] = scenario
        gdf = gpd.GeoDataFrame(df)
        gdf["geom_bus0"] = gdf_buses.geometry[df.bus0].values
        gdf["geom_bus1"] = gdf_buses.geometry[df.bus1].values
        gdf["geometry"] = gdf.apply(
            lambda x: LineString([x["geom_bus0"], x["geom_bus1"]]),
            axis=1,
        )

        gdf = gdf.set_geometry("geometry")
        gdf = gdf.set_crs(4326)

        gdf = gdf.rename_geometry("topo")

        gdf.loc[:, "bus0"] = central_buses_pypsaeur.bus_id.loc[df.bus0].values
        gdf.loc[:, "bus1"] = central_buses_pypsaeur.bus_id.loc[df.bus1].values

        gdf.drop(["geom_bus0", "geom_bus1"], inplace=True, axis="columns")
        if "link_id" in df.columns:
            table_name = "link"
            gdf.drop(
                [
                    "tags",
                    "under_construction",
                    "underground",
                    "underwater_fraction",
                    "bus2",
                    "efficiency2",
                    "length_original",
                    "bus4",
                    "efficiency4",
                    "reversed",
                    "ramp_limit_up",
                    "ramp_limit_down",
                    "p_nom_opt",
                    "bus3",
                    "efficiency3",
                    "location",
                    "project_status",
                    "dc",
                    "voltage",
                ],
                axis="columns",
                inplace=True,
            )
        else:
            table_name = "line"
            gdf.drop(
                [
                    "i_nom",
                    "sub_network",
                    "x_pu",
                    "r_pu",
                    "g_pu",
                    "b_pu",
                    "x_pu_eff",
                    "r_pu_eff",
                    "s_nom_opt",
                    "dc",
                ],
                axis="columns",
                inplace=True,
            )

        gdf = gdf.set_index(f"{table_name}_id")
        gdf.to_postgis(
            targets.get_table_name(f"{table_name}s"),
            db.engine(),
            schema=targets.get_table_schema(f"{table_name}s"),
            if_exists="append",
            index=True,
            index_label=f"{table_name}_id",
        )


def cross_border_lines(scenario, sources, targets, central_buses):
    """Adds lines which connect border-crossing lines from osmtgmod
    to the central buses in the corresponding neigbouring country

    Parameters
    ----------
    sources : dict
        List of dataset sources
    targets : dict
        List of dataset targets
    central_buses : geopandas.GeoDataFrame
        Buses in the center of foreign countries

    Returns
    -------
    new_lines : geopandas.GeoDataFrame
        Lines that connect cross-border lines to central bus per country

    """
    # Delete existing data
    db.execute_sql(f"""
        DELETE FROM {targets.tables['lines']}
        WHERE scn_name = '{scenario}'
        AND line_id NOT IN (
            SELECT branch_id
            FROM {sources.tables['osmtgmod_branch']}
              WHERE result_id = 1 and (link_type = 'line' or
                                       link_type = 'cable'))
        AND bus0 IN (
            SELECT bus_i
            FROM {sources.tables['osmtgmod_bus']})
        AND bus1 NOT IN (
            SELECT bus_i
            FROM {sources.tables['osmtgmod_bus']})
        """)

    # Calculate cross-border busses and lines from osmtgmod
    foreign_buses = get_cross_border_buses(scenario, sources)
    foreign_buses.dropna(subset="country", inplace=True)

    if config.settings()["egon-data"]["--dataset-boundary"] == "Everything":
        foreign_buses = foreign_buses[foreign_buses.country != "DE"]
    lines = get_cross_border_lines(scenario, sources)

    # Select bus outside of Germany from border-crossing lines
    lines.loc[
        lines[lines.bus0.isin(foreign_buses.bus_id)].index, "foreign_bus"
    ] = lines.loc[lines[lines.bus0.isin(foreign_buses.bus_id)].index, "bus0"]
    lines.loc[
        lines[lines.bus1.isin(foreign_buses.bus_id)].index, "foreign_bus"
    ] = lines.loc[lines[lines.bus1.isin(foreign_buses.bus_id)].index, "bus1"]

    # Drop lines with start and endpoint in Germany
    lines = lines[lines.foreign_bus.notnull()]
    lines.loc[:, "foreign_bus"] = lines.loc[:, "foreign_bus"].astype(int)

    # Copy all parameters from border-crossing lines
    new_lines = lines.copy().set_crs(4326)

    # Set bus0 as foreign_bus from osmtgmod
    new_lines.bus0 = new_lines.foreign_bus.copy()
    new_lines.bus0 = new_lines.bus0.astype(int)

    # Add country tag and set index
    new_lines["country"] = (
        foreign_buses.set_index("bus_id")
        .loc[lines.foreign_bus, "country"]
        .values
    )

    if config.settings()["egon-data"]["--dataset-boundary"] == "Everything":
        new_lines = new_lines[~new_lines.country.isnull()]
    new_lines.line_id = db.next_etrago_id("line", len(new_lines.index))

    # Set bus in center of foreign countries as bus1
    for i, row in new_lines.iterrows():
        print(row)
        new_lines.loc[i, "bus1"] = central_buses.bus_id[
            (central_buses.country == row.country)
            & (central_buses.v_nom == row.v_nom)
        ].values[0]

    # Create geometry for new lines
    new_lines["geom_bus0"] = (
        foreign_buses.set_index("bus_id").geom[new_lines.bus0].values
    )
    new_lines["geom_bus1"] = (
        central_buses.set_index("bus_id").geom[new_lines.bus1].values
    )
    new_lines["topo"] = new_lines.apply(
        lambda x: LineString([x["geom_bus0"], x["geom_bus1"]]), axis=1
    )

    # Set topo as geometry column
    new_lines = new_lines.set_geometry("topo").set_crs(4326)
    # Calcultae length of lines based on topology
    old_length = new_lines["length"].copy()
    new_lines["length"] = new_lines.to_crs(3035).length / 1000

    if (new_lines["length"] == 0).any():
        print("WARNING! THERE ARE LINES WITH LENGTH = 0")
        condition = new_lines["length"] != 0
        new_lines["length"] = new_lines["length"].where(condition, 1)

    # Set electrical parameters based on lines from osmtgmod
    for parameter in ["x", "r"]:
        new_lines[parameter] = (
            new_lines[parameter] / old_length * new_lines["length"]
        )
    for parameter in ["b", "g"]:
        new_lines[parameter] = (
            new_lines[parameter] * old_length / new_lines["length"]
        )

    # Drop intermediate columns
    new_lines.drop(
        ["foreign_bus", "country", "geom_bus0", "geom_bus1", "geom"],
        axis="columns",
        inplace=True,
    )

    new_lines = new_lines[new_lines.bus0 != new_lines.bus1]

    new_lines["cables"] = new_lines["cables"].apply(int)

    # Insert lines to the database
    new_lines.to_postgis(
        targets.get_table_name("lines"),
        schema=targets.get_table_schema("lines"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )

    return new_lines


def choose_transformer(s_nom):
    """Select transformer and parameters from existing data in the grid model

    It is assumed that transformers in the foreign countries are not limiting
    the electricity flow, so the capacitiy s_nom is set to the minimum sum
    of attached AC-lines.
    The electrical parameters are set according to already inserted
    transformers in the grid model for Germany.

    Parameters
    ----------
    s_nom : float
        Minimal sum of nominal power of lines at one side

    Returns
    -------
    int
        Selected transformer nominal power
    float
        Selected transformer nominal impedance

    """

    if s_nom <= 600:
        return 600, 0.0002
    elif (s_nom > 600) & (s_nom <= 1200):
        return 1200, 0.0001
    elif (s_nom > 1200) & (s_nom <= 1600):
        return 1600, 0.000075
    elif (s_nom > 1600) & (s_nom <= 2100):
        return 2100, 0.00006667
    elif (s_nom > 2100) & (s_nom <= 2600):
        return 2600, 0.0000461538
    elif (s_nom > 2600) & (s_nom <= 4800):
        return 4800, 0.000025
    elif (s_nom > 4800) & (s_nom <= 6000):
        return 6000, 0.0000225
    elif (s_nom > 6000) & (s_nom <= 7200):
        return 7200, 0.0000194444
    elif (s_nom > 7200) & (s_nom <= 8000):
        return 8000, 0.000016875
    elif (s_nom > 8000) & (s_nom <= 9000):
        return 9000, 0.000015
    elif (s_nom > 9000) & (s_nom <= 13000):
        return 13000, 0.0000103846
    elif (s_nom > 13000) & (s_nom <= 20000):
        return 20000, 0.00000675
    elif (s_nom > 20000) & (s_nom <= 33000):
        return 33000, 0.00000409091


def central_transformer(scenario, sources, targets, central_buses, new_lines):
    """Connect central foreign buses with different voltage levels

    Parameters
    ----------
    sources : dict
        List of dataset sources
    targets : dict
        List of dataset targets
    central_buses : geopandas.GeoDataFrame
        Buses in the center of foreign countries
    new_lines : geopandas.GeoDataFrame
        Lines that connect cross-border lines to central bus per country

    Returns
    -------
    None.

    """
    # Delete existing transformers in foreign countries
    db.execute_sql(f"""
        DELETE FROM {targets.tables['transformers']}
        WHERE scn_name = '{scenario}'
        AND trafo_id NOT IN (
            SELECT branch_id
            FROM {sources.tables['osmtgmod_branch']}
              WHERE result_id = 1 and link_type = 'transformer')
        """)

    # Initalize the dataframe for transformers
    trafo = gpd.GeoDataFrame(
        columns=["trafo_id", "bus0", "bus1", "s_nom"], dtype=int
    )
    trafo_id = db.next_etrago_id("transformer")

    # Add one transformer per central foreign bus with v_nom != 380
    for i, row in central_buses[central_buses.v_nom != 380].iterrows():
        s_nom_0 = new_lines[new_lines.bus0 == row.bus_id].s_nom.sum()
        s_nom_1 = new_lines[new_lines.bus1 == row.bus_id].s_nom.sum()
        if s_nom_0 == 0.0:
            s_nom = s_nom_1
        elif s_nom_1 == 0.0:
            s_nom = s_nom_0
        else:
            s_nom = min([s_nom_0, s_nom_1])

        s_nom, x = choose_transformer(s_nom)

        trafo = pd.concat(
            [
                trafo,
                pd.DataFrame(
                    index=[trafo.index.max() + 1],
                    data={
                        "trafo_id": trafo_id,
                        "bus0": row.bus_id,
                        "bus1": central_buses[
                            (central_buses.v_nom == 380)
                            & (central_buses.country == row.country)
                        ].bus_id.values[0],
                        "s_nom": s_nom,
                        "x": x,
                    },
                ),
            ],
            ignore_index=True,
        )
        trafo_id += 1

    # Set data type
    trafo = trafo.astype({"trafo_id": "int", "bus0": "int", "bus1": "int"})
    trafo["scn_name"] = scenario

    # Insert transformers to the database
    trafo.to_sql(
        targets.get_table_name("transformers"),
        schema=targets.get_table_schema("transformers"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )


def foreign_dc_lines(scenario, sources, targets, central_buses):
    """Insert DC lines to foreign countries manually

    Parameters
    ----------
    sources : dict
        List of dataset sources
    targets : dict
        List of dataset targets
    central_buses : geopandas.GeoDataFrame
        Buses in the center of foreign countries

    Returns
    -------
    None.

    """
    # Delete existing dc lines to foreign countries
    db.execute_sql(f"""
        DELETE FROM {targets.tables['links']}
        WHERE scn_name = '{scenario}'
        AND carrier = 'DC'
        AND bus0 IN (
            SELECT bus_id
            FROM {sources.tables['electricity_buses']}
              WHERE scn_name = '{scenario}'
              AND carrier = 'AC'
              AND country = 'DE')
        AND bus1 IN (
            SELECT bus_id
            FROM {sources.tables['electricity_buses']}
              WHERE scn_name = '{scenario}'
              AND carrier = 'AC'
              AND country != 'DE')
        """)
    capital_cost = get_sector_parameters("electricity", scenario)[
        "capital_cost"
    ]

    # Add DC line from Lübeck to Sweden
    converter_luebeck = select_bus_id(
        10.802358024202768,
        53.897547401787,
        380,
        scenario,
        "AC",
        find_closest=True,
    )

    foreign_links = pd.DataFrame(
        index=[0],
        data={
            "link_id": db.next_etrago_id("link"),
            "bus0": converter_luebeck,
            "bus1": central_buses[
                (central_buses.country == "SE") & (central_buses.v_nom == 380)
            ]
            .iloc[0]
            .squeeze()
            .bus_id,
            "p_nom": 600,
            "length": 262,
        },
    )

    # When not in test-mode, add DC line from Bentwisch to Denmark
    if config.settings()["egon-data"]["--dataset-boundary"] == "Everything":
        converter_bentwisch = select_bus_id(
            12.213671694775988,
            54.09974494662279,
            380,
            scenario,
            "AC",
            find_closest=True,
        )

        foreign_links = pd.concat(
            [
                foreign_links,
                pd.DataFrame(
                    index=[1],
                    data={
                        "link_id": db.next_etrago_id("link"),
                        "bus0": converter_bentwisch,
                        "bus1": central_buses[
                            (central_buses.country == "DK")
                            & (central_buses.v_nom == 380)
                            & (central_buses.x > 10)
                        ]
                        .iloc[0]
                        .squeeze()
                        .bus_id,
                        "p_nom": 600,
                        "length": 170,
                    },
                ),
            ]
        )

    # Set parameters for all DC lines
    foreign_links["capital_cost"] = (
        capital_cost["dc_cable"] * foreign_links.length
        + 2 * capital_cost["dc_inverter"]
    )
    foreign_links["p_min_pu"] = -1
    foreign_links["p_nom_extendable"] = True
    foreign_links["p_nom_min"] = foreign_links["p_nom"]
    foreign_links["scn_name"] = scenario
    foreign_links["carrier"] = "DC"
    foreign_links["efficiency"] = 1

    # Add topology
    foreign_links = etrago.link_geom_from_buses(foreign_links, scenario)

    # Insert DC lines to the database
    foreign_links.to_postgis(
        targets.get_table_name("links"),
        schema=targets.get_table_schema("links"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )


def grid():
    """Insert electrical grid compoenents for neighbouring countries

    Returns
    -------
    None.

    """
    # Select sources and targets from dataset configuration
    sources = ElectricalNeighbours.sources
    targets = ElectricalNeighbours.targets

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        central_buses = buses(scenario, sources, targets)

        foreign_lines = cross_border_lines(
            scenario, sources, targets, central_buses
        )

        central_transformer(
            scenario, sources, targets, central_buses, foreign_lines
        )

        foreign_dc_lines(scenario, sources, targets, central_buses)

        lines_between_foreign_countries(
            scenario, sources, targets, central_buses
        )


def map_carriers_tyndp():
    """Map carriers from TYNDP-data to carriers used in eGon
    Returns
    -------
    dict
        Carrier from TYNDP and eGon
    """
    return {
        "Nuclear": "nuclear",
        # Lignite
        "Lignite old 1": "lignite",
        "Lignite old 2": "lignite",
        "Lignite new": "lignite",
        "Lignite CCS": "lignite",
        "Lignite biofuel": "biomass",
        # Hard coal
        "Hard coal old 1": "coal",
        "Hard coal old 2": "coal",
        "Hard coal new": "coal",
        "Hard coal CCS": "coal",
        "Hard Coal biofuel": "biomass",
        # Gas
        "Gas conventional old 1": "gas",
        "Gas conventional old 2": "gas",
        "Gas CCGT old 1": "gas",
        "Gas CCGT old 2": "gas",
        "Gas CCGT new": "gas",
        "Gas CCGT CCS": "gas",
        "Gas CCGT present 1": "gas",
        "Gas CCGT present 2": "gas",
        "Gas OCGT old": "gas",
        "Gas OCGT new": "gas",
        "Gas biofuel": "biogas",
        # Oil
        "Light oil": "oil",
        "Heavy oil old 1": "oil",
        "Heavy oil old 2": "oil",
        "Light oil biofuel": "biomass",
        "Heavy oil biofuel": "biomass",
        "Oil shale old": "oil",
        "Oil shale new": "oil",
        "Oil shale biofuel": "biomass",
        # Hydro
        "Run-of-River": "run_of_river",
        "Reservoir": "reservoir",
        "Pondage": "reservoir",
        "Pump Storage - Open Loop (turbine)": "reservoir",
        "Pump Storage - Open Loop (pump)": "reservoir",
        "Pump Storage - Closed Loop (turbine)": "pumped_hydro",
        "Pump Storage - Closed Loop (pump)": "pumped_hydro",
        # Wind / Solar
        "Wind Onshore": "wind_onshore",
        "Wind Offshore": "wind_offshore",
        "Solar (Photovoltaic)": "solar",
        "Solar (Thermal)": "others",
        # Other renewable/non-renewable
        "Others renewable": "others",
        "Others non-renewable": "others",
        # Storage
        "Battery Storage discharge (gen.)": "battery",
    }


def get_foreign_bus_id(scenario):
    """Calculte the etrago bus id from Nodes of TYNDP based on the geometry

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """
    sources = ElectricalNeighbours.sources
    bus_id = db.select_geodataframe(
        f"""SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM {sources.tables['electricity_buses']}
        WHERE scn_name = '{scenario}'
        AND carrier = 'AC'
        AND v_nom = 380.
        AND country != 'DE'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM {sources.tables['osmtgmod_bus']})
        """,
        epsg=3035,
    )

    # Select buses in neighbouring countries as geodataframe
    buses = pd.DataFrame(
        [
            {"node_id": node_id, "latitude": lat, "longitude": lon}
            for node_id, (lat, lon) in TYNDP_NODE_COORDINATES.items()
        ]
    )
    buses = gpd.GeoDataFrame(
        buses,
        crs=4326,
        geometry=gpd.points_from_xy(buses.longitude, buses.latitude),
    ).to_crs(3035)

    buses["bus_id"] = 0

    # Select bus_id from etrago with shortest distance to TYNDP node
    for i, row in buses.iterrows():
        distance = bus_id.set_index("bus_id").geom.distance(row.geometry)
        buses.loc[i, "bus_id"] = distance[
            distance == distance.min()
        ].index.values[0]

    return buses.set_index("node_id").bus_id


def _select_tyndp_capacity_member(zip_file):
    """Select the main electricity output file from a TYNDP 2024 zip

    Each TYNDP 2024 scenario/year zip contains several .xlsb workbooks:
    the main electricity results plus separate H2, offshore and
    Heat/SynthFuels sector files, whose exact names shift per year (e.g.
    "MMStandardOutputFile_DE2035_Plexos_CY2009_v11_SoS.xlsb"), plus
    macOS "__MACOSX/._..." resource-fork artifacts that also happen to
    end in ".xlsb". The main file is selected by excluding the other
    sectors' name fragments and those artifacts, rather than hardcoding
    the changing prefix.

    Parameters
    ----------
    zip_file : zipfile.ZipFile
        Open TYNDP scenario/year zip archive

    Returns
    -------
    str
        Name of the main electricity .xlsb member

    """
    excluded_tokens = ("_H2_", "_offshore_", "_Heat_SynthFuels_")
    candidates = [
        name
        for name in zip_file.namelist()
        if name.endswith(".xlsb")
        and not any(token in name for token in excluded_tokens)
        and "__MACOSX" not in name
        and not name.rsplit("/", 1)[-1].startswith("._")
    ]
    if len(candidates) != 1:
        raise ValueError(
            "Expected exactly one main TYNDP electricity .xlsb file in "
            f"{zip_file.filename}, found: {candidates}"
        )
    return candidates[0]


def _extract_output_block(raw, output_type, header_row=5, data_start_row=6):
    """Extract one "Output type" block from a TYNDP 2024 "Yearly Outputs" sheet

    The sheet is laid out with node/zone identifiers as columns and a
    block of technology rows per "Output type" (e.g. "Installed
    Capacities [MW]", "Annual generation [GWh]", ...). The "Output type"
    label is only populated on each block's first row, so it needs to be
    forward-filled before filtering to the requested block.

    Parameters
    ----------
    raw : pandas.DataFrame
        Sheet read with ``header=None`` (raw grid, no header inference)
    output_type : str
        "Output type" block to extract, e.g. "Installed Capacities [MW]"
    header_row : int
        Row index holding the node/zone column identifiers
    data_start_row : int
        First row index of the data (below the header row)

    Returns
    -------
    pandas.DataFrame
        Columns: Node/Line, Generator_ID, Value

    """
    header = raw.iloc[header_row]
    node_cols = [
        c
        for c in raw.columns[2:]
        if isinstance(header[c], str)
        and " " not in header[c]
        and not header[c].endswith("RETE")
    ]

    data = raw.iloc[data_start_row:].copy()
    data[0] = data[0].ffill()
    block = data[data[0] == output_type]

    long_df = block.melt(
        id_vars=[1],
        value_vars=node_cols,
        var_name="_col",
        value_name="Value",
    )
    long_df["Node/Line"] = long_df["_col"].map(header)
    long_df = long_df.rename(columns={1: "Generator_ID"})

    return long_df[["Node/Line", "Generator_ID", "Value"]].dropna(
        subset=["Value"]
    )


@functools.lru_cache(maxsize=None)
def read_tyndp_capacities(year):
    """Read installed capacities for one TYNDP 2024 anchor year

    Reads the "Installed Capacities [MW]" block from the "Distributed
    Energy" scenario's downloaded zip for the given anchor year (2035,
    2040 or 2050), climate year 2009.

    Parameters
    ----------
    year : int
        TYNDP 2024 anchor year (2035, 2040 or 2050)

    Returns
    -------
    pandas.DataFrame
        Columns: Node/Line, Generator_ID, Value

    """
    outer_zip = zipfile.ZipFile(
        ElectricalNeighbours.sources.files[f"tyndp_capacities_{year}"]
    )
    member = _select_tyndp_capacity_member(outer_zip)
    raw = pd.read_excel(
        io.BytesIO(outer_zip.read(member)),
        sheet_name="Yearly Outputs",
        header=None,
        engine="pyxlsb",
    )
    return _extract_output_block(raw, "Installed Capacities [MW]")


def _bracket_tyndp_years(year, anchors=(2035, 2040, 2050)):
    """Select the pair of TYNDP 2024 anchor years bracketing a scenario year

    Parameters
    ----------
    year : int
        Scenario's target year
    anchors : tuple
        TYNDP 2024 anchor years with published data

    Returns
    -------
    tuple
        (lower, upper) anchor years to interpolate/extrapolate between

    """
    lo, mid, hi = anchors
    if year < lo or year > hi:
        logger.warning(
            f"Scenario year {year} is outside TYNDP 2024's anchor range "
            f"[{lo}, {hi}]; extrapolating from the nearest bracket."
        )
    return (lo, mid) if year <= mid else (mid, hi)


def calc_capacities(scenario):
    """Calculates installed capacities from TYNDP data

    TYNDP-2024 provides data points for 2035, 2040 and 2050, so the
    capacities for the scenario's target year (from
    :py:func:`get_scenario_year`) are obtained by linearly interpolating
    (or, outside that range, extrapolating) between the two closest of
    those data points.

    Parameters
    ----------
    scenario : str
        Scenario for which the capacities are calculated

    Returns
    -------
    pandas.DataFrame
        Installed capacities per foreign node and energy carrier

    """

    countries = [
        "AT",
        "BE",
        "CH",
        "CZ",
        "DK",
        "FR",
        "NL",
        "NO",
        "SE",
        "PL",
        "UK",
    ]

    year = get_scenario_year(scenario)
    lo, hi = _bracket_tyndp_years(year)
    weight = (year - lo) / (hi - lo)

    df_lo = read_tyndp_capacities(lo).set_index(
        ["Node/Line", "Generator_ID"]
    )
    df_hi = read_tyndp_capacities(hi).set_index(
        ["Node/Line", "Generator_ID"]
    )

    df_capacities = pd.DataFrame(index=df_lo.index.union(df_hi.index))
    df_capacities["cap_lo"] = df_lo.Value
    df_capacities["cap_hi"] = df_hi.Value
    df_capacities.fillna(0.0, inplace=True)
    df_capacities["cap"] = (
        df_capacities["cap_lo"]
        + (df_capacities["cap_hi"] - df_capacities["cap_lo"]) * weight
    ).clip(lower=0)
    df_capacities = df_capacities.reset_index()
    df_capacities["carrier"] = df_capacities.Generator_ID.map(
        map_carriers_tyndp()
    )

    # group capacities by new carriers
    grouped_capacities = (
        df_capacities.groupby(["carrier", "Node/Line"])
        .cap.sum()
        .reset_index()
    )

    # choose capacities for considered countries
    grouped_capacities = grouped_capacities[
        grouped_capacities["Node/Line"].str[:2].isin(countries)
    ]

    # Drop zero-capacity rows. Unlike TYNDP 2020's sparse long-format
    # sheet, TYNDP 2024's wide-format "Yearly Outputs" sheet has a row
    # for every (technology, node) pair system-wide, including ones
    # that don't apply at a given node (e.g. "Wind Offshore" for
    # landlocked Austria) with value 0. Besides being pointless to
    # model, such rows can crash renewable_timeseries_pypsaeur(), which
    # assumes every inserted generator has a nearby match in the
    # PyPSA-Eur network of the same carrier.
    return grouped_capacities[grouped_capacities["cap"] > 0]


def insert_generators_tyndp(capacities, scenario):
    """Insert generators for foreign countries based on TYNDP-data

    Parameters
    ----------
    capacities : pandas.DataFrame
        Installed capacities per foreign node and energy carrier
    scenario : str
        Scenario for which the generators are inserted

    Returns
    -------
    None.

    """
    targets = ElectricalNeighbours.targets
    map_buses = get_map_buses()

    # Delete existing data
    db.execute_sql(f"""
        DELETE FROM {targets.tables['generators']}
        WHERE bus IN (
            SELECT bus_id
            FROM {targets.tables['buses']}
            WHERE country != 'DE'
            AND scn_name = '{scenario}')
        AND scn_name = '{scenario}'
        AND carrier != 'CH4'
        """)

    db.execute_sql(f"""
        DELETE FROM {targets.tables['generators_timeseries']}
        WHERE generator_id NOT IN (
            SELECT generator_id FROM {targets.tables['generators']}
        )
        AND scn_name = '{scenario}'
        """)

    # Select generators from TYNDP capacities
    gen = capacities[
        capacities.carrier.isin(
            [
                "others",
                "wind_offshore",
                "wind_onshore",
                "solar",
                "reservoir",
                "run_of_river",
                "lignite",
                "coal",
                "oil",
                "nuclear",
            ]
        )
    ]

    # Set bus_id
    gen.loc[
        gen[gen["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ] = gen.loc[
        gen[gen["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ].map(
        map_buses
    )

    gen.loc[:, "bus"] = (
        get_foreign_bus_id(scenario=scenario)
        .loc[gen.loc[:, "Node/Line"]]
        .values
    )

    # Add scenario column
    gen["scenario"] = scenario

    # Add marginal costs
    gen = add_marginal_costs(gen)

    # insert generators data
    session = sessionmaker(bind=db.engine())()
    for i, row in gen.iterrows():
        entry = etrago.EgonPfHvGenerator(
            scn_name=row.scenario,
            generator_id=int(db.next_etrago_id("generator")),
            bus=row.bus,
            carrier=row.carrier,
            p_nom=row.cap,
            marginal_cost=row.marginal_cost,
        )

        session.add(entry)
        session.commit()

    # assign generators time-series data

    renewable_timeseries_pypsaeur(scenario)


def insert_storage_tyndp(capacities, scenario):
    """Insert storage units for foreign countries based on TYNDP-data

    Parameters
    ----------
    capacities : pandas.DataFrame
        Installed capacities per foreign node and energy carrier
    scenario : str
        Scenario for which the storage units are inserted

    Returns
    -------
    None.

    """
    targets = ElectricalNeighbours.targets
    map_buses = get_map_buses()

    # Delete existing data
    db.execute_sql(f"""
        DELETE FROM {targets.tables['storage']}
        WHERE bus IN (
            SELECT bus_id FROM
            {targets.tables['buses']}
            WHERE country != 'DE'
            AND scn_name = '{scenario}')
        AND scn_name = '{scenario}'
        """)

    # Add missing information suitable for eTraGo selected from scenario_parameter table
    parameters_pumped_hydro = scenario_parameters.electricity(scenario)[
        "efficiency"
    ]["pumped_hydro"]

    parameters_battery = scenario_parameters.electricity(scenario)[
        "efficiency"
    ]["battery"]

    # Select storage capacities from TYNDP-data
    store = capacities[capacities.carrier.isin(["battery", "pumped_hydro"])]

    # Set bus_id
    store.loc[
        store[store["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ] = store.loc[
        store[store["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ].map(
        map_buses
    )

    store.loc[:, "bus"] = (
        get_foreign_bus_id(scenario=scenario)
        .loc[store.loc[:, "Node/Line"]]
        .values
    )

    # Add columns for additional parameters to df
    (
        store["dispatch"],
        store["store"],
        store["standing_loss"],
        store["max_hours"],
    ) = (None, None, None, None)

    # Insert carrier specific parameters

    parameters = ["dispatch", "store", "standing_loss", "max_hours"]

    for x in parameters:
        store.loc[store["carrier"] == "battery", x] = parameters_battery[x]
        store.loc[store["carrier"] == "pumped_hydro", x] = (
            parameters_pumped_hydro[x]
        )

    # insert data
    session = sessionmaker(bind=db.engine())()
    for i, row in store.iterrows():
        entry = etrago.EgonPfHvStorage(
            scn_name=scenario,
            storage_id=int(db.next_etrago_id("storage")),
            bus=row.bus,
            max_hours=row.max_hours,
            efficiency_store=row.store,
            efficiency_dispatch=row.dispatch,
            standing_loss=row.standing_loss,
            carrier=row.carrier,
            p_nom=row.cap,
        )

        session.add(entry)
        session.commit()


def get_map_buses():
    """Returns a dictonary of foreign regions which are aggregated to another

    Returns
    -------
    Combination of aggregated regions


    """
    return {
        "DK00": "DKW1",
        "DKKF": "DKE1",
        "FR15": "FR00",
        "NON1": "NOM1",
        "NOS0": "NOM1",
        "NOS1": "NOM1",
        "PLE0": "PL00",
        "PLI0": "PL00",
        "SE00": "SE02",
        "SE01": "SE02",
        "SE03": "SE02",
        "SE04": "SE02",
        "RU": "RU00",
    }


def tyndp_generation():
    """Insert data from TYNDP 2024 for all configured scenarios that are
    not status-quo scenarios (i.e. 'Distributed Energy', linearly
    interpolated/extrapolated between 2035, 2040 and 2050 to each
    scenario's year).

    Returns
    -------
    None.
    """
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scenario:
            continue

        capacities = calc_capacities(scenario)

        insert_generators_tyndp(capacities, scenario)

        insert_storage_tyndp(capacities, scenario)


def _select_tyndp_demand_member(zip_file, scenario, year):
    """Select the electricity market demand workbook for one TYNDP 2024
    scenario/year from the downloaded demand-profiles zip archive.

    Parameters
    ----------
    zip_file : zipfile.ZipFile
        Open TYNDP demand-profiles zip archive
    scenario : str
        TYNDP scenario folder, e.g. "DE" for Distributed Energy
    year : int
        TYNDP anchor year (2030, 2040 or 2050)

    Returns
    -------
    str
        Name of the electricity market demand .xlsx member

    """
    target = (
        f"Demand Profiles/{scenario}/{year}/"
        f"ELECTRICITY_MARKET {scenario} {year}.xlsx"
    )
    candidates = [name for name in zip_file.namelist() if name == target]
    if len(candidates) != 1:
        raise ValueError(
            f"Expected exactly one TYNDP electricity demand file at "
            f"'{target}' in {zip_file.filename}, found: {candidates}"
        )
    return candidates[0]


def _tyndp_demand_climate_year_column(df, node, year, climate_year=2009):
    """Select the climate-year column for one node's demand sheet

    A few TYNDP 2024 demand sheets have their climate-year header labels
    shifted by one column relative to the actual data (confirmed for
    node "UK00" in the 2040/2050 demand files: the column labelled 2009
    is entirely empty, while the real data sits one column over, under
    the label 2008). If the nominal column is completely empty, fall
    back to the nearest fully populated column instead of silently
    returning an all-empty series.

    Parameters
    ----------
    df : pandas.DataFrame
        One node's demand sheet, as read by :py:func:`read_tyndp_demand`
    node : str
        TYNDP node code (only used for the warning message)
    year : int
        TYNDP anchor year (only used for the warning message)
    climate_year : int
        Nominal climate-year column to select

    Returns
    -------
    pandas.Series
        8760 hourly MW values for the requested climate year

    """
    if not df[climate_year].isna().all():
        return df[climate_year]

    candidates = [
        c
        for c in range(climate_year - 2, climate_year + 3)
        if c in df.columns and c != climate_year and df[c].notna().all()
    ]
    fallback = (
        min(candidates, key=lambda c: abs(c - climate_year))
        if candidates
        else None
    )
    if fallback is None:
        raise ValueError(
            f"No populated climate-year column found near {climate_year} "
            f"for node {node!r}, year {year}"
        )
    logger.warning(
        f"TYNDP demand column {climate_year} is empty for node {node!r}, "
        f"year {year}; using column {fallback} instead (known ENTSO-E "
        "header/data misalignment)."
    )
    return df[fallback]


@functools.lru_cache(maxsize=None)
def read_tyndp_demand(year, nodes):
    """Read hourly electricity demand for one TYNDP 2024 anchor year

    Reads climate year 2009 demand timeseries from the "Distributed
    Energy" scenario's downloaded demand-profiles zip, for the given
    anchor year (2030, 2040 or 2050).

    Parameters
    ----------
    year : int
        TYNDP 2024 anchor year (2030, 2040 or 2050)
    nodes : tuple
        TYNDP node codes to read demand timeseries for

    Returns
    -------
    dict
        Mapping of node code to a pandas.Series of 8760 hourly MW values

    """
    outer_zip = zipfile.ZipFile(
        ElectricalNeighbours.sources.files["tyndp_demand"]
    )
    member = _select_tyndp_demand_member(outer_zip, "DE", year)
    sheets = pd.read_excel(
        io.BytesIO(outer_zip.read(member)),
        sheet_name=list(nodes),
        skiprows=11,
    )
    return {
        node: _tyndp_demand_climate_year_column(sheets[node], node, year)
        for node in nodes
    }


def tyndp_demand():
    """Copy load timeseries data from TYNDP 2024 for all configured
    scenarios that are not status-quo scenarios. The data for 2030, 2040
    and 2050 is interpolated (or, outside that range, extrapolated)
    linearly to each scenario's year.

    Returns
    -------
    None.

    """
    map_buses = get_map_buses()

    sources = ElectricalNeighbours.sources  # class attributes
    targets = ElectricalNeighbours.targets

    nodes = (
        "AT00",
        "BE00",
        "CH00",
        "CZ00",
        "DKE1",
        "DKW1",
        "FR00",
        "NL00",
        "LUB1",
        "LUF1",
        "LUG1",
        "NOM1",
        "NON1",
        "NOS0",
        "SE01",
        "SE02",
        "SE03",
        "SE04",
        "PL00",
        "UK00",
        "UKNI",
    )

    # Transform map_buses to pandas.Series and select only used values
    map_series = pd.Series(map_buses)
    map_series = map_series[map_series.index.isin(nodes)]

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scenario:
            continue

        year = get_scenario_year(scenario)
        lo, hi = _bracket_tyndp_years(year, anchors=(2030, 2040, 2050))
        weight = (year - lo) / (hi - lo)

        dataset_lo = read_tyndp_demand(lo, nodes)
        dataset_hi = read_tyndp_demand(hi, nodes)

        # Connect to database
        engine = db.engine()
        session = sessionmaker(bind=engine)()

        # Delete existing data
        db.execute_sql(f"""
            DELETE FROM {targets.tables['loads']}
            WHERE
            scn_name = '{scenario}'
            AND carrier = 'AC'
            AND bus NOT IN (
                SELECT bus_i
                FROM {sources.tables['osmtgmod_bus']})
            """)

        # Assign etrago bus_id to TYNDP nodes
        buses = pd.DataFrame({"nodes": nodes})
        buses.loc[
            buses[buses.nodes.isin(map_buses.keys())].index, "nodes"
        ] = buses[buses.nodes.isin(map_buses.keys())].nodes.map(map_buses)
        buses.loc[:, "bus"] = (
            get_foreign_bus_id(scenario=scenario)
            .loc[buses.loc[:, "nodes"]]
            .values
        )
        buses.set_index("nodes", inplace=True)
        buses = buses[~buses.index.duplicated(keep="first")]

        # Calculate and insert demand timeseries per etrago bus_id
        for bus in buses.index:
            bus_nodes = [bus]

            if bus in map_series.values:
                bus_nodes.extend(
                    list(map_series[map_series == bus].index.values)
                )

            load_id = db.next_etrago_id("load")

            # Some etrago bus_ids represent multiple TYNDP nodes,
            # in this cases the loads are summed
            data_lo = pd.Series(index=range(8760), data=0.0)
            for node in bus_nodes:
                data_lo = dataset_lo[node] + data_lo

            data_hi = pd.Series(index=range(8760), data=0.0)
            for node in bus_nodes:
                data_hi = dataset_hi[node] + data_hi

            # Interpolate/extrapolate linearly to the scenario's year
            data_target = (data_lo + (data_hi - data_lo) * weight).clip(
                lower=0
            )[:8760]

            entry = etrago.EgonPfHvLoad(
                scn_name=scenario,
                load_id=int(load_id),
                carrier="AC",
                bus=int(buses.bus[bus]),
            )

            entry_ts = etrago.EgonPfHvLoadTimeseries(
                scn_name=scenario,
                load_id=int(load_id),
                temp_id=1,
                p_set=list(data_target.values),
            )

            session.add(entry)
            session.add(entry_ts)
            session.commit()


def get_entsoe_token():
    """Check for token in home dir. If not exists, check in working dir"""
    token_path = path.join(path.expanduser("~"), ".entsoe-token")
    if not os.path.isfile(token_path):
        logger.info(
            f"Token file not found at {token_path}. Will check in working directory."
        )
        token_path = Path(".entsoe-token")
        if os.path.isfile(token_path):
            logger.info(f"Token found at {token_path}")
    entsoe_token = open(token_path, "r").read(36)
    if entsoe_token is None:
        raise FileNotFoundError("No entsoe-token found.")
    return entsoe_token


def entsoe_historic_generation_capacities(
    year_start="20190101", year_end="20200101"
):
    entsoe_token = get_entsoe_token()
    client = entsoe.EntsoePandasClient(api_key=entsoe_token)

    start = pd.Timestamp(year_start, tz="Europe/Brussels")
    end = pd.Timestamp(year_end, tz="Europe/Brussels")
    start_gb = pd.Timestamp(year_start, tz="Europe/London")
    end_gb = pd.Timestamp(year_end, tz="Europe/London")
    countries = [
        "LU",
        "AT",
        "FR",
        "NL",
        "CZ",
        "DK_1",
        "DK_2",
        "PL",
        "CH",
        "NO",
        "BE",
        "SE",
        "GB",
    ]
    # No GB data after Brexit
    if int(year_start[:4]) > 2021:
        logger.warning(
            "No GB data after Brexit. GB is dropped from entsoe query!"
        )
        countries = [c for c in countries if c != "GB"]
    # todo: define wanted countries

    not_retrieved = []
    dfs = []
    for country in countries:
        if country == "GB":
            kwargs = dict(start=start_gb, end=end_gb)
        else:
            kwargs = dict(start=start, end=end)
        try:
            country_data = client.query_installed_generation_capacity(
                country, **kwargs
            )
            dfs.append(country_data)
        except (entsoe.exceptions.NoMatchingDataError, requests.HTTPError):
            logger.warning(
                f"Data for country: {country} could not be retrieved."
            )
            not_retrieved.append(country)
            pass

    if dfs:
        df = pd.concat(dfs)
        df["country"] = [c for c in countries if c not in not_retrieved]
        df.set_index("country", inplace=True)
        if int(year_start[:4]) == 2023:
            # https://www.bmreports.com/bmrs/?q=foregeneration/capacityaggregated
            # could probably somehow be automised
            # https://www.elexonportal.co.uk/category/view/178
            # in MW
            installed_capacity_gb = pd.Series(
                {
                    "Biomass": 4438,
                    "Fossil Gas": 37047,
                    "Fossil Hard coal": 1491,
                    "Hydro Pumped Storage": 5603,
                    "Hydro Run-of-river and poundage": 2063,
                    "Nuclear": 4950,
                    "Other": 3313,
                    "Other renewable": 1462,
                    "Solar": 14518,
                    "Wind Offshore": 13038,
                    "Wind Onshore": 13907,
                },
                name="GB",
            )
            df = pd.concat([df.T, installed_capacity_gb], axis=1).T
            logger.info("Manually added generation capacities for GB 2023.")
            not_retrieved = [c for c in not_retrieved if c != "GB"]
        df.fillna(0, inplace=True)
    else:
        df = pd.DataFrame()
    return df, not_retrieved


def entsoe_historic_demand(year_start="20190101", year_end="20200101"):
    entsoe_token = get_entsoe_token()
    client = entsoe.EntsoePandasClient(api_key=entsoe_token)

    start = pd.Timestamp(year_start, tz="Europe/Brussels")
    end = pd.Timestamp(year_end, tz="Europe/Brussels")
    start_gb = start.tz_convert("Europe/London")
    end_gb = end.tz_convert("Europe/London")

    countries = [
        "LU",
        "AT",
        "FR",
        "NL",
        "CZ",
        "DK_1",
        "DK_2",
        "PL",
        "CH",
        "NO",
        "BE",
        "SE",
        "GB",
    ]

    # No GB data after Brexit
    if int(year_start[:4]) > 2021:
        logger.warning(
            "No GB data after Brexit. GB is dropped from entsoe query!"
        )
        countries = [c for c in countries if c != "GB"]

    # todo: define wanted countries

    not_retrieved = []
    dfs = []

    for country in countries:
        if country == "GB":
            kwargs = dict(start=start_gb, end=end_gb)
        else:
            kwargs = dict(start=start, end=end)
        try:
            country_data = (
                client.query_load(country, **kwargs)
                .resample("H")["Actual Load"]
                .mean()
            )
            if country == "GB":
                country_data.index = country_data.index.tz_convert(
                    "Europe/Brussels"
                )
            dfs.append(country_data)
        except (entsoe.exceptions.NoMatchingDataError, requests.HTTPError):
            not_retrieved.append(country)
            logger.warning(
                f"Data for country: {country} could not be retrieved."
            )
            pass

    if dfs:
        df = pd.concat(dfs, axis=1)
        df.columns = [c for c in countries if c not in not_retrieved]
        df.index = pd.date_range(year_start, periods=len(df), freq="H")
        # Drop the leap day to keep a consistent 8760-hour year, matching
        # the model's fixed temporal resolution (see etrago_setup.temp_resolution)
        df = df[~((df.index.month == 2) & (df.index.day == 29))]
    else:
        df = pd.DataFrame()
    return df, not_retrieved


def map_carriers_entsoe():
    """Map carriers from entsoe-data to carriers used in eGon
    Returns
    -------
    dict
        Carrier from entsoe to eGon
    """
    return {
        "Biomass": "biomass",
        "Fossil Brown coal/Lignite": "lignite",
        "Fossil Coal-derived gas": "coal",
        "Fossil Gas": "OCGT",
        "Fossil Hard coal": "coal",
        "Fossil Oil": "oil",
        "Fossil Oil shale": "oil",
        "Fossil Peat": "others",
        "Geothermal": "geo_thermal",
        "Hydro Pumped Storage": "Hydro Pumped Storage",
        "Hydro Run-of-river and poundage": "run_of_river",
        "Hydro Water Reservoir": "reservoir",
        "Marine": "others",
        "Nuclear": "nuclear",
        "Other": "others",
        "Other renewable": "others",
        "Solar": "solar",
        "Waste": "others",
        "Wind Offshore": "wind_offshore",
        "Wind Onshore": "wind_onshore",
    }


def entsoe_to_bus_etrago(scenario="status2024"):
    map_entsoe = pd.Series(
        {
            "LU": "LU00",
            "AT": "AT00",
            "FR": "FR00",
            "NL": "NL00",
            "DK_1": "DK00",
            "DK_2": "DKE1",
            "PL": "PL00",
            "CH": "CH00",
            "NO": "NO00",
            "BE": "BE00",
            "SE": "SE00",
            "GB": "UK00",
            "CZ": "CZ00",
        }
    )

    for_bus = get_foreign_bus_id(scenario=scenario)

    return map_entsoe.map(for_bus)


def save_entsoe_data(df: pd.DataFrame, file_path: Path):
    os.makedirs(file_path.parent, exist_ok=True)
    if not df.empty:
        df.to_csv(file_path, index_label="Index")
        logger.info(
            f"Saved entsoe data for {file_path.stem} "
            f"to {file_path.parent} for countries: {df.index}"
        )


def fill_by_backup_data_from_former_runs(df_sq, file_path, not_retrieved):
    """
    Fills missing data from former runs
    Parameters
    ----------
    df_sq: pd.DataFrame
    file_path: str, Path
    not_retrieved: list

    Returns
    -------
    df_sq, not_retrieved

    """
    sq_backup = pd.read_csv(file_path, index_col="Index")
    # check for missing columns in backup (former runs)
    c_backup = [c for c in sq_backup.columns if c in not_retrieved]
    # remove columns, if found in backup
    not_retrieved = [c for c in not_retrieved if c not in c_backup]
    if c_backup:
        df_sq = pd.concat([df_sq, sq_backup.loc[:, c_backup]], axis=1)
        logger.info(f"Appended data from former runs for {c_backup}")
    return df_sq, not_retrieved


def insert_storage_units_sq():
    """
    Insert storage_units for foreign countries based on ENTSO-E data,
    for every configured status-quo scenario.

    Returns
    -------
    None.

    """
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" not in scn_name:
            continue

        year = int(scn_name.split("status")[-1])
        year_start_end = {
            "year_start": f"{year}0101",
            "year_end": f"{year+1}0101",
        }

        df_gen_sq, not_retrieved = entsoe_historic_generation_capacities(
            **year_start_end
        )

        if not_retrieved:
            logger.warning(
                "Generation data from entsoe could not be retrieved."
            )
            # check for generation backup from former runs
            file_path = Path(
                "./",
                "data_bundle_egon_data",
                "entsoe",
                f"gen_entsoe_{scn_name}.csv",
            ).resolve()
            if os.path.isfile(file_path):
                df_gen_sq, not_retrieved = (
                    fill_by_backup_data_from_former_runs(
                        df_gen_sq, file_path, not_retrieved
                    )
                )
            save_entsoe_data(df_gen_sq, file_path=file_path)

        sto_sq = df_gen_sq.loc[:, df_gen_sq.columns == "Hydro Pumped Storage"]
        sto_sq.rename(
            columns={"Hydro Pumped Storage": "p_nom"}, inplace=True
        )

        targets = ElectricalNeighbours.targets

        # Delete existing data
        db.execute_sql(f"""
            DELETE FROM {targets.tables['storage']}
            WHERE bus IN (
                SELECT bus_id
                FROM {targets.tables['buses']}
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            """)

        # Add missing information suitable for eTraGo selected from scenario_parameter table
        parameters_pumped_hydro = get_sector_parameters(
            sector="electricity", scenario=scn_name
        )["efficiency"]["pumped_hydro"]

        # Set bus_id
        entsoe_to_bus = entsoe_to_bus_etrago(scenario=scn_name)
        sto_sq["bus"] = sto_sq.index.map(entsoe_to_bus)

        # Insert carrier specific parameters
        sto_sq["carrier"] = "pumped_hydro"
        sto_sq["scn_name"] = scn_name
        sto_sq["dispatch"] = parameters_pumped_hydro["dispatch"]
        sto_sq["store"] = parameters_pumped_hydro["store"]
        sto_sq["standing_loss"] = parameters_pumped_hydro["standing_loss"]
        sto_sq["max_hours"] = parameters_pumped_hydro["max_hours"]
        sto_sq["cyclic_state_of_charge"] = parameters_pumped_hydro[
            "cyclic_state_of_charge"
        ]

        sto_sq["storage_id"] = db.next_etrago_id("storage", len(sto_sq))

        # Delete entrances without any installed capacity
        sto_sq = sto_sq[sto_sq["p_nom"] > 0]

        # insert data pumped_hydro storage

        with session_scope() as session:
            for i, row in sto_sq.iterrows():
                entry = etrago.EgonPfHvStorage(
                    scn_name=scn_name,
                    storage_id=row.storage_id,
                    bus=row.bus,
                    max_hours=row.max_hours,
                    efficiency_store=row.store,
                    efficiency_dispatch=row.dispatch,
                    standing_loss=row.standing_loss,
                    carrier=row.carrier,
                    p_nom=row.p_nom,
                    cyclic_state_of_charge=row.cyclic_state_of_charge,
                )
                session.add(entry)
                session.commit()

        # big scale batteries
        # info based on EASE data. https://ease-storage.eu/publication/emmes-7-0-march-2023/
        # batteries smaller than 100MW are neglected

        # TODO: include capacities between 2020 and 2023
        bat_per_country = {
            "LU": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "AT": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "FR": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "NL": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "DK_1": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "DK_2": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "PL": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "CH": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "NO": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "BE": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "SE": [0, pd.NA, pd.NA, pd.NA, pd.NA],
            "GB": [723.8, 952.3, 1380.9, 2333.3, 3928.5],
            "CZ": [0, pd.NA, pd.NA, pd.NA, pd.NA],
        }
        bat_sq = pd.DataFrame(bat_per_country).T.set_axis(
            ["2019", "2020", "2021", "2022", "2023"], axis=1
        )

        # Select year of interest, falling back to the latest year
        # available in the (currently un-updated) data above
        bat_year = min(year, max(int(c) for c in bat_sq.columns))
        bat_sq = bat_sq[[str(bat_year)]]
        bat_sq.rename(columns={str(bat_year): "p_nom"}, inplace=True)

        # Add missing information suitable for eTraGo selected from scenario_parameter table
        parameters_batteries = get_sector_parameters(
            sector="electricity", scenario=scn_name
        )["efficiency"]["battery"]

        # Set bus_id
        entsoe_to_bus = entsoe_to_bus_etrago(scenario=scn_name)
        bat_sq["bus"] = bat_sq.index.map(entsoe_to_bus)

        # Insert carrier specific parameters
        bat_sq["carrier"] = "battery"
        bat_sq["scn_name"] = scn_name
        bat_sq["dispatch"] = parameters_batteries["dispatch"]
        bat_sq["store"] = parameters_batteries["store"]
        bat_sq["standing_loss"] = parameters_batteries["standing_loss"]
        bat_sq["max_hours"] = parameters_batteries["max_hours"]
        bat_sq["cyclic_state_of_charge"] = parameters_batteries[
            "cyclic_state_of_charge"
        ]

        bat_sq["storage_id"] = db.next_etrago_id("storage", len(bat_sq))

        # Delete entrances without any installed capacity
        bat_sq = bat_sq[bat_sq["p_nom"] > 0]

        # insert data pumped_hydro storage
        with db.session_scope() as session:
            for i, row in bat_sq.iterrows():
                entry = etrago.EgonPfHvStorage(
                    scn_name=scn_name,
                    storage_id=row.storage_id,
                    bus=row.bus,
                    max_hours=row.max_hours,
                    efficiency_store=row.store,
                    efficiency_dispatch=row.dispatch,
                    standing_loss=row.standing_loss,
                    carrier=row.carrier,
                    p_nom=row.p_nom,
                    cyclic_state_of_charge=row.cyclic_state_of_charge,
                )
                session.add(entry)
                session.commit()


def insert_generators_sq():
    """
    Insert generators for foreign countries based on ENTSO-E data,
    for every configured status-quo scenario.

    Returns
    -------
    None.

    """
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" not in scn_name:
            continue

        year = int(scn_name.split("status")[-1])
        year_start_end = {
            "year_start": f"{year}0101",
            "year_end": f"{year+1}0101",
        }

        df_gen_sq, not_retrieved = entsoe_historic_generation_capacities(
            **year_start_end
        )

        if not_retrieved:
            logger.warning(
                "Generation data from entsoe could not be retrieved."
            )
            # check for generation backup from former runs
            file_path = Path(
                "./",
                "data_bundle_egon_data",
                "entsoe",
                f"gen_entsoe_{scn_name}.csv",
            ).resolve()
            if os.path.isfile(file_path):
                df_gen_sq, not_retrieved = (
                    fill_by_backup_data_from_former_runs(
                        df_gen_sq, file_path, not_retrieved
                    )
                )
            save_entsoe_data(df_gen_sq, file_path=file_path)

        targets = ElectricalNeighbours.targets
        # Delete existing data
        db.execute_sql(f"""
            DELETE FROM {targets.tables['generators']}
            WHERE bus IN (
                SELECT bus_id
                FROM {targets.tables['buses']}
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            AND carrier != 'CH4'
            """)

        db.execute_sql(f"""
            DELETE FROM {targets.tables['generators_timeseries']}
            WHERE generator_id NOT IN (
                SELECT generator_id FROM {targets.tables['generators']}
            )
            AND scn_name = '{scn_name}'
            """)
        entsoe_to_bus = entsoe_to_bus_etrago(scn_name)
        carrier_entsoe = map_carriers_entsoe()
        df_gen_sq = df_gen_sq.groupby(axis=1, by=carrier_entsoe).sum()

        # Filter generators modeled as storage and geothermal
        df_gen_sq = df_gen_sq.loc[
            :,
            ~df_gen_sq.columns.isin(["Hydro Pumped Storage", "geo_thermal"]),
        ]

        list_gen_sq = pd.DataFrame(
            dtype=int, columns=["carrier", "country", "capacity"]
        )
        for carrier in df_gen_sq.columns:
            gen_carry = df_gen_sq[carrier]
            for country, cap in gen_carry.items():
                gen = pd.DataFrame(
                    {
                        "carrier": carrier,
                        "country": country,
                        "capacity": cap,
                    },
                    index=[1],
                )
                # print(gen)
                list_gen_sq = pd.concat(
                    [list_gen_sq, gen], ignore_index=True
                )

        list_gen_sq = list_gen_sq[list_gen_sq.capacity > 0]
        list_gen_sq["scenario"] = scn_name

        # Add marginal costs
        list_gen_sq = add_marginal_costs(list_gen_sq)

        # Find foreign bus to assign the generator
        list_gen_sq["bus"] = list_gen_sq.country.map(entsoe_to_bus)

        # insert generators data
        session = sessionmaker(bind=db.engine())()
        for i, row in list_gen_sq.iterrows():
            entry = etrago.EgonPfHvGenerator(
                scn_name=row.scenario,
                generator_id=int(db.next_etrago_id("generator")),
                bus=row.bus,
                carrier=row.carrier,
                p_nom=row.capacity,
                marginal_cost=row.marginal_cost,
            )

            session.add(entry)
            session.commit()

        renewable_timeseries_pypsaeur(scn_name)


def renewable_timeseries_pypsaeur(scn_name):
    # select generators from database to get index values
    targets = ElectricalNeighbours.targets
    foreign_re_generators = db.select_dataframe(f"""
        SELECT generator_id, a.carrier, country, x, y
        FROM {targets.tables['generators']} a
        JOIN {targets.tables['buses']} b
        ON a.bus = b.bus_id
        WHERE a.scn_name = '{scn_name}'
        AND  b.scn_name = '{scn_name}'
        AND b.carrier = 'AC'
        AND b.country != 'DE'
        AND a.carrier IN ('wind_onshore', 'wind_offshore', 'solar')
        """)

    # Import prepared network from pypsa-eur
    network = prepared_network()

    # Select fluctuating renewable generators
    generators_pypsa_eur = network.generators.loc[
        network.generators[
            network.generators.carrier.isin(["onwind", "offwind-ac", "solar"])
        ].index,
        ["bus", "carrier"],
    ]

    # Align carrier names for wind turbines
    generators_pypsa_eur.loc[
        generators_pypsa_eur[generators_pypsa_eur.carrier == "onwind"].index,
        "carrier",
    ] = "wind_onshore"
    generators_pypsa_eur.loc[
        generators_pypsa_eur[
            generators_pypsa_eur.carrier == "offwind-ac"
        ].index,
        "carrier",
    ] = "wind_offshore"

    # Set coordinates from bus table
    generators_pypsa_eur["x"] = network.buses.loc[
        generators_pypsa_eur.bus.values, "x"
    ].values
    generators_pypsa_eur["y"] = network.buses.loc[
        generators_pypsa_eur.bus.values, "y"
    ].values

    # Get p_max_pu time series from pypsa-eur
    generators_pypsa_eur["p_max_pu"] = network.generators_t.p_max_pu[
        generators_pypsa_eur.index
    ].T.values.tolist()

    session = sessionmaker(bind=db.engine())()

    # Insert p_max_pu timeseries based on geometry and carrier
    for gen in foreign_re_generators.index:
        entry = etrago.EgonPfHvGeneratorTimeseries(
            scn_name=scn_name,
            generator_id=foreign_re_generators.loc[gen, "generator_id"],
            temp_id=1,
            p_max_pu=generators_pypsa_eur[
                (
                    (
                        generators_pypsa_eur.x
                        - foreign_re_generators.loc[gen, "x"]
                    ).abs()
                    < 0.01
                )
                & (
                    (
                        generators_pypsa_eur.y
                        - foreign_re_generators.loc[gen, "y"]
                    ).abs()
                    < 0.01
                )
                & (
                    generators_pypsa_eur.carrier
                    == foreign_re_generators.loc[gen, "carrier"]
                )
            ].p_max_pu.iloc[0],
        )

        session.add(entry)
        session.commit()


def insert_loads_sq():
    """
    Copy load timeseries data from entso-e, for every configured
    status-quo scenario.

    Returns
    -------
    None.

    """
    sources = ElectricalNeighbours.sources
    targets = ElectricalNeighbours.targets

    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" not in scn_name:
            continue

        year = int(scn_name.split("status")[-1])
        year_start_end = {
            "year_start": f"{year}0101",
            "year_end": f"{year+1}0101",
        }

        df_load_sq, not_retrieved = entsoe_historic_demand(**year_start_end)

        if not_retrieved:
            logger.warning("Demand data from entsoe could not be retrieved.")
            # check for generation backup from former runs
            file_path = Path(
                "./",
                "data_bundle_egon_data",
                "entsoe",
                f"load_entsoe_{scn_name}.csv",
            ).resolve()
            if os.path.isfile(file_path):
                df_load_sq, not_retrieved = (
                    fill_by_backup_data_from_former_runs(
                        df_load_sq, file_path, not_retrieved
                    )
                )
            save_entsoe_data(df_load_sq, file_path=file_path)

        # Delete existing data
        db.execute_sql(f"""
            DELETE FROM {targets.tables['load_timeseries']}
            WHERE
            scn_name = '{scn_name}'
            AND load_id IN (
                SELECT load_id FROM {targets.tables['loads']}
                WHERE  scn_name = '{scn_name}'
                AND carrier = 'AC'
                AND bus NOT IN (
                    SELECT bus_i
                    FROM {sources.tables['osmtgmod_bus']}))
            """)

        db.execute_sql(f"""
            DELETE FROM {targets.tables['loads']}
            WHERE
            scn_name = '{scn_name}'
            AND carrier = 'AC'
            AND bus NOT IN (
                SELECT bus_i
                FROM {sources.tables['osmtgmod_bus']})
            """)

        # get the corresponding bus per foreign country
        entsoe_to_bus = entsoe_to_bus_etrago(scn_name)

        # Calculate and insert demand timeseries per etrago bus_id
        with session_scope() as session:
            for country in df_load_sq.columns:
                load_id = db.next_etrago_id("load")

                entry = etrago.EgonPfHvLoad(
                    scn_name=scn_name,
                    load_id=int(load_id),
                    carrier="AC",
                    bus=int(entsoe_to_bus[country]),
                )

                entry_ts = etrago.EgonPfHvLoadTimeseries(
                    scn_name=scn_name,
                    load_id=int(load_id),
                    temp_id=1,
                    p_set=list(df_load_sq[country]),
                )

                session.add(entry)
                session.add(entry_ts)
                session.commit()


def no_neighbour_tasks_required():
    print("""
          None of the configured scenarios require additional
          electrical-neighbour tasks.
          """)
    return None


tasks = (
    grid,
    {
        tyndp_generation,
        tyndp_demand,
        insert_generators_sq,
        insert_loads_sq,
        insert_storage_units_sq,
    },
)


class ElectricalNeighbours(Dataset):
    """
    Add lines, loads, generation and storage for electrical neighbours

    This dataset creates data for modelling the considered foreign countries and writes
    that data into the database tables that can be read by the eTraGo tool.
    Neighbouring countries are modelled in a lower spatial resolution, in general one node per
    country is considered.
    Defined load timeseries as well as generatrion and storage capacities are connected to these nodes.
    The nodes are connected by AC and DC transmission lines with the German grid and other neighbouring countries
    considering the grid topology from ENTSO-E.


    *Dependencies*
      * :py:class:`Tyndp <egon.data.datasets.tyndp.Tyndp>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec.PypsaEurSec>`


    *Resulting tables*
      * :py:class:`grid.egon_etrago_bus <egon.data.datasets.etrago_setup.EgonPfHvBus>` is extended
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_line <egon.data.datasets.etrago_setup.EgonPfHvLine>` is extended
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_load_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended
      * :py:class:`grid.egon_etrago_storage <egon.data.datasets.etrago_setup.EgonPfHvStorageUnit>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
      * :py:class:`grid.egon_etrago_generator_timeseries <egon.data.datasets.etrago_setup.EgonPfHvGeneratorTimeseries>` is extended
      * :py:class:`grid.egon_etrago_transformer <egon.data.datasets.etrago_setup.EgonPfHvTransformer>` is extended

    """

    #:
    name: str = "ElectricalNeighbours"
    #:
    version: str = "0.0.18"

    sources = DatasetSources(
        tables={
            "electricity_buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "german_borders": "boundaries.vg250_sta_union",
            "osmtgmod_bus": "osmtgmod_results.bus_data",
            "osmtgmod_branch": "osmtgmod_results.branch_data",
        },
        files={
            "tyndp_capacities_2035": "tyndp/DE2035CY2009.zip",
            "tyndp_capacities_2040": "tyndp/DE2040CY2009.zip",
            "tyndp_capacities_2050": "tyndp/DE2050CY2009.zip",
            "tyndp_demand": "tyndp/Demand-Profiles.zip",
        },
    )

    targets = DatasetTargets(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "links": "grid.egon_etrago_link",
            "transformers": "grid.egon_etrago_transformer",
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "generators": "grid.egon_etrago_generator",
            "generators_timeseries": "grid.egon_etrago_generator_timeseries",
            "storage": "grid.egon_etrago_storage",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
        )
