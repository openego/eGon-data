"""The central module containing all code dealing with electrical neighbours
"""

import zipfile

import entsoe
import requests
import logging

import geopandas as gpd
import pandas as pd
import pypsa
from shapely.geometry import LineString
from sqlalchemy.orm import sessionmaker

import egon.data.datasets.etrago_setup as etrago
import egon.data.datasets.scenario_parameters.parameters as scenario_parameters
from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.fix_ehv_subnetworks import select_bus_id
from egon.data.datasets.fill_etrago_gen import add_marginal_costs
from egon.data.datasets.pypsaeur import prepared_network
from egon.data.datasets.scenario_parameters import get_sector_parameters
from os import path


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
        FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE
        NOT ST_INTERSECTS (
            geom,
            (SELECT ST_Transform(ST_Buffer(geometry, 5), 4326) FROM
             {sources['german_borders']['schema']}.
            {sources['german_borders']['table']}))
        AND (bus_id IN (
            SELECT bus0 FROM
            {sources['lines']['schema']}.{sources['lines']['table']})
            OR bus_id IN (
            SELECT bus1 FROM
            {sources['lines']['schema']}.{sources['lines']['table']}))
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
    FROM {sources['lines']['schema']}.{sources['lines']['table']} a
    WHERE
    ST_INTERSECTS (
        a.topo,
        (SELECT ST_Transform(ST_boundary(geometry), 4326)
         FROM {sources['german_borders']['schema']}.
        {sources['german_borders']['table']}))
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
        DELETE FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
        WHERE country != 'DE' AND scn_name = '{scenario}'
        AND carrier = 'AC'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """
    # Drop only buses with v_nom != 380 for eGon100RE
    # to keep buses from pypsa-eur-sec
    if scenario == "eGon100RE":
        sql_delete += "AND v_nom < 380"

    # Delete existing buses
    db.execute_sql(sql_delete)

    central_buses = central_buses_pypsaeur(sources, scenario)

    next_bus_id = db.next_etrago_id("bus") + 1

    central_buses["bus_id"] = central_buses.reset_index().index + next_bus_id

    next_bus_id += len(central_buses)

    # if in test mode, add bus in center of Germany
    if config.settings()["egon-data"]["--dataset-boundary"] != "Everything":
        central_buses = pd.concat(
            [
                central_buses,
                pd.DataFrame(
                    index=[central_buses.bus_id.max() + 1],
                    data={
                        "scn_name": scenario,
                        "bus_id": next_bus_id,
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
        next_bus_id += 1

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
                        index=[central_buses.index.max() + 1],
                        data={
                            "scn_name": scenario,
                            "bus_id": next_bus_id,
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
            next_bus_id += 1
        if 220.0 in vnom_per_country[cntr]:
            central_buses = pd.concat(
                [
                    central_buses,
                    pd.DataFrame(
                        index=[central_buses.index.max() + 1],
                        data={
                            "scn_name": scenario,
                            "bus_id": next_bus_id,
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
            next_bus_id += 1

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
        ["control", "generator", "location", "unit", "sub_network"],
        axis="columns",
        inplace=True,
        errors="ignore"
    )

    # Insert all central buses for eGon2035
    if scenario in ["eGon2035", "status2019"]:
        central_buses.to_postgis(
            targets["buses"]["table"],
            schema=targets["buses"]["schema"],
            if_exists="append",
            con=db.engine(),
            index=False,
        )
    # Insert only buses for eGon100RE that are not coming from pypsa-eur-sec
    # (buses with another voltage_level or inside Germany in test mode)
    else:
        central_buses[central_buses.carrier=="AC"].to_postgis(
            targets["buses"]["table"],
            schema=targets["buses"]["schema"],
            if_exists="append",
            con=db.engine(),
            index=False,
        )

    return central_buses


def lines_between_foreign_countries(scenario, sorces, targets, central_buses):
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
    lines_to_add.loc[:, "line_id"] = (
        lines_to_add.reset_index().index.astype(int)
        + db.next_etrago_id("line")
        + 1
    )

    links_to_add = network.links[
        (network.links.bus0.isin(central_buses_pypsaeur.index))
        & (network.links.bus1.isin(central_buses_pypsaeur.index))
    ]

    links_to_add.loc[:, "lifetime"] = get_sector_parameters(
        "electricity", scenario
    )["lifetime"]["dc_overhead_line"]
    links_to_add.loc[:, "link_id"] = (
        links_to_add.reset_index().index.astype(int)
        + db.next_etrago_id("link")
        + 1
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
                ],
                axis="columns",
                inplace=True,
            )

        gdf = gdf.set_index(f"{table_name}_id")
        gdf.to_postgis(
            f"egon_etrago_{table_name}",
            db.engine(),
            schema="grid",
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
    db.execute_sql(
        f"""
        DELETE FROM {targets['lines']['schema']}.
        {targets['lines']['table']}
        WHERE scn_name = '{scenario}'
        AND line_id NOT IN (
            SELECT branch_id
            FROM  {sources['osmtgmod_branch']['schema']}.
            {sources['osmtgmod_branch']['table']}
              WHERE result_id = 1 and (link_type = 'line' or
                                       link_type = 'cable'))
        AND bus0 IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        AND bus1 NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """
    )

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
    new_lines.line_id = range(
        db.next_etrago_id("line"), db.next_etrago_id("line") + len(new_lines)
    )

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


    # Insert lines to the database
    new_lines.to_postgis(
        targets["lines"]["table"],
        schema=targets["lines"]["schema"],
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
    db.execute_sql(
        f"""
        DELETE FROM {targets['transformers']['schema']}.
        {targets['transformers']['table']}
        WHERE scn_name = '{scenario}'
        AND trafo_id NOT IN (
            SELECT branch_id
            FROM {sources['osmtgmod_branch']['schema']}.
            {sources['osmtgmod_branch']['table']}
              WHERE result_id = 1 and link_type = 'transformer')
        """
    )

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
        targets["transformers"]["table"],
        schema=targets["transformers"]["schema"],
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
    db.execute_sql(
        f"""
        DELETE FROM {targets['links']['schema']}.
        {targets['links']['table']}
        WHERE scn_name = '{scenario}'
        AND carrier = 'DC'
        AND bus0 IN (
            SELECT bus_id
            FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
              WHERE scn_name = '{scenario}'
              AND carrier = 'AC'
              AND country = 'DE')
        AND bus1 IN (
            SELECT bus_id
            FROM {sources['electricity_buses']['schema']}.
            {sources['electricity_buses']['table']}
              WHERE scn_name = '{scenario}'
              AND carrier = 'AC'
              AND country != 'DE')
        """
    )
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
            ].iloc[0]
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
                        "link_id": db.next_etrago_id("link") + 1,
                        "bus0": converter_bentwisch,
                        "bus1": central_buses[
                            (central_buses.country == "DK")
                            & (central_buses.v_nom == 380)
                            & (central_buses.x > 10)
                        ].iloc[0]
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
        targets["links"]["table"],
        schema=targets["links"]["schema"],
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
    sources = config.datasets()["electrical_neighbours"]["sources"]
    targets = config.datasets()["electrical_neighbours"]["targets"]

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        central_buses = buses(scenario, sources, targets)

        foreign_lines = cross_border_lines(
            scenario, sources, targets, central_buses
        )

        central_transformer(
            scenario, sources, targets, central_buses, foreign_lines
        )

        foreign_dc_lines(scenario, sources, targets, central_buses)

        if scenario != "eGon100RE":
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
        "Battery": "battery",
        "DSR": "demand_side_response",
        "Gas CCGT new": "gas",
        "Gas CCGT old 2": "gas",
        "Gas CCGT present 1": "gas",
        "Gas CCGT present 2": "gas",
        "Gas conventional old 1": "gas",
        "Gas conventional old 2": "gas",
        "Gas OCGT new": "gas",
        "Gas OCGT old": "gas",
        "Gas CCGT old 1": "gas",
        "Gas CCGT old 2 Bio": "biogas",
        "Gas conventional old 2 Bio": "biogas",
        "Hard coal new": "coal",
        "Hard coal old 1": "coal",
        "Hard coal old 2": "coal",
        "Hard coal old 2 Bio": "coal",
        "Heavy oil old 1": "oil",
        "Heavy oil old 1 Bio": "oil",
        "Heavy oil old 2": "oil",
        "Light oil": "oil",
        "Lignite new": "lignite",
        "Lignite old 1": "lignite",
        "Lignite old 2": "lignite",
        "Lignite old 1 Bio": "lignite",
        "Lignite old 2 Bio": "lignite",
        "Nuclear": "nuclear",
        "Offshore Wind": "wind_offshore",
        "Onshore Wind": "wind_onshore",
        "Other non-RES": "others",
        "Other RES": "others",
        "P2G": "power_to_gas",
        "PS Closed": "pumped_hydro",
        "PS Open": "reservoir",
        "Reservoir": "reservoir",
        "Run-of-River": "run_of_river",
        "Solar PV": "solar",
        "Solar Thermal": "others",
        "Waste": "Other RES",
    }


def get_foreign_bus_id(scenario):
    """Calculte the etrago bus id from Nodes of TYNDP based on the geometry

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """

    sources = config.datasets()["electrical_neighbours"]["sources"]

    bus_id = db.select_geodataframe(
        f"""SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scenario}'
        AND carrier = 'AC'
        AND v_nom = 380.
        AND country != 'DE'
        AND bus_id NOT IN (
            SELECT bus_i
            FROM osmtgmod_results.bus_data)
        """,
        epsg=3035,
    )

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

    # Select buses in neighbouring countries as geodataframe
    buses = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Nodes - Dict",
    ).query("longitude==longitude")
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


def calc_capacities():
    """Calculates installed capacities from TYNDP data

    Returns
    -------
    pandas.DataFrame
        Installed capacities per foreign node and energy carrier

    """

    sources = config.datasets()["electrical_neighbours"]["sources"]

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

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
    df = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Capacity",
    )

    # differneces between different climate years are very small (<1MW)
    # choose 1984 because it is the mean value
    df_2030 = (
        df.rename({"Climate Year": "Climate_Year"}, axis="columns")
        .query(
            'Scenario == "Distributed Energy" & Year == 2030 & '
            "Climate_Year == 1984"
        )
        .set_index(["Node/Line", "Generator_ID"])
    )

    df_2040 = (
        df.rename({"Climate Year": "Climate_Year"}, axis="columns")
        .query(
            'Scenario == "Distributed Energy" & Year == 2040 & '
            "Climate_Year == 1984"
        )
        .set_index(["Node/Line", "Generator_ID"])
    )

    # interpolate linear between 2030 and 2040 for 2035 accordning to
    # scenario report of TSO's and the approval by BNetzA
    df_2035 = pd.DataFrame(index=df_2030.index)
    df_2035["cap_2030"] = df_2030.Value
    df_2035["cap_2040"] = df_2040.Value
    df_2035.fillna(0.0, inplace=True)
    df_2035["cap_2035"] = (
        df_2035["cap_2030"] + (df_2035["cap_2040"] - df_2035["cap_2030"]) / 2
    )
    df_2035 = df_2035.reset_index()
    df_2035["carrier"] = df_2035.Generator_ID.map(map_carriers_tyndp())

    # group capacities by new carriers
    grouped_capacities = (
        df_2035.groupby(["carrier", "Node/Line"]).cap_2035.sum().reset_index()
    )

    # choose capacities for considered countries
    return grouped_capacities[
        grouped_capacities["Node/Line"].str[:2].isin(countries)
    ]


def insert_generators(capacities):
    """Insert generators for foreign countries based on TYNDP-data

    Parameters
    ----------
    capacities : pandas.DataFrame
        Installed capacities per foreign node and energy carrier

    Returns
    -------
    None.

    """
    targets = config.datasets()["electrical_neighbours"]["targets"]
    map_buses = get_map_buses()

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {targets['buses']['schema']}.{targets['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        AND carrier != 'CH4'
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators_timeseries']['schema']}.
        {targets['generators_timeseries']['table']}
        WHERE generator_id NOT IN (
            SELECT generator_id FROM
            {targets['generators']['schema']}.{targets['generators']['table']}
        )
        AND scn_name = 'eGon2035'
        """
    )

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
        get_foreign_bus_id(scenario="eGon2035")
        .loc[gen.loc[:, "Node/Line"]]
        .values
    )

    # Add scenario column
    gen["scenario"] = "eGon2035"

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
            p_nom=row.cap_2035,
            marginal_cost=row.marginal_cost,
        )

        session.add(entry)
        session.commit()

    # assign generators time-series data

    renewable_timeseries_pypsaeur("eGon2035")


def insert_storage(capacities):
    """Insert storage units for foreign countries based on TYNDP-data

    Parameters
    ----------
    capacities : pandas.DataFrame
        Installed capacities per foreign node and energy carrier


    Returns
    -------
    None.

    """
    targets = config.datasets()["electrical_neighbours"]["targets"]
    map_buses = get_map_buses()

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {targets['buses']['schema']}.{targets['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        """
    )

    # Add missing information suitable for eTraGo selected from scenario_parameter table
    parameters_pumped_hydro = scenario_parameters.electricity("eGon2035")[
        "efficiency"
    ]["pumped_hydro"]

    parameters_battery = scenario_parameters.electricity("eGon2035")[
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
        get_foreign_bus_id(scenario="eGon2035")
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
            scn_name="eGon2035",
            storage_id=int(db.next_etrago_id("storage")),
            bus=row.bus,
            max_hours=row.max_hours,
            efficiency_store=row.store,
            efficiency_dispatch=row.dispatch,
            standing_loss=row.standing_loss,
            carrier=row.carrier,
            p_nom=row.cap_2035,
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
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """

    capacities = calc_capacities()

    insert_generators(capacities)

    insert_storage(capacities)


def tyndp_demand():
    """Copy load timeseries data from TYNDP 2020.
    According to NEP 2021, the data for 2030 and 2040 is interpolated linearly.

    Returns
    -------
    None.

    """
    map_buses = get_map_buses()

    sources = config.datasets()["electrical_neighbours"]["sources"]
    targets = config.datasets()["electrical_neighbours"]["targets"]

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM {targets['loads']['schema']}.
        {targets['loads']['table']}
        WHERE
        scn_name = 'eGon2035'
        AND carrier = 'AC'
        AND bus NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """
    )

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    nodes = [
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
    ]
    # Assign etrago bus_id to TYNDP nodes
    buses = pd.DataFrame({"nodes": nodes})
    buses.loc[buses[buses.nodes.isin(map_buses.keys())].index, "nodes"] = (
        buses[buses.nodes.isin(map_buses.keys())].nodes.map(map_buses)
    )
    buses.loc[:, "bus"] = (
        get_foreign_bus_id(scenario="eGon2035")
        .loc[buses.loc[:, "nodes"]]
        .values
    )
    buses.set_index("nodes", inplace=True)
    buses = buses[~buses.index.duplicated(keep="first")]

    # Read in data from TYNDP for 2030 and 2040
    dataset_2030 = pd.read_excel(
        f"tyndp/{sources['tyndp_demand_2030']}", sheet_name=nodes, skiprows=10
    )

    dataset_2040 = pd.read_excel(
        f"tyndp/{sources['tyndp_demand_2040']}", sheet_name=None, skiprows=10
    )

    # Transform map_buses to pandas.Series and select only used values
    map_series = pd.Series(map_buses)
    map_series = map_series[map_series.index.isin(nodes)]

    # Calculate and insert demand timeseries per etrago bus_id
    for bus in buses.index:
        nodes = [bus]

        if bus in map_series.values:
            nodes.extend(list(map_series[map_series == bus].index.values))

        load_id = db.next_etrago_id("load")

        # Some etrago bus_ids represent multiple TYNDP nodes,
        # in this cases the loads are summed
        data_2030 = pd.Series(index=range(8760), data=0.0)
        for node in nodes:
            data_2030 = dataset_2030[node][2011] + data_2030

        try:
            data_2040 = pd.Series(index=range(8760), data=0.0)

            for node in nodes:
                data_2040 = dataset_2040[node][2011] + data_2040
        except:
            data_2040 = data_2030

        # According to the NEP, data for 2030 and 2040 is linear interpolated
        data_2035 = ((data_2030 + data_2040) / 2)[:8760]

        entry = etrago.EgonPfHvLoad(
            scn_name="eGon2035",
            load_id=int(load_id),
            carrier="AC",
            bus=int(buses.bus[bus]),
        )

        entry_ts = etrago.EgonPfHvLoadTimeseries(
            scn_name="eGon2035",
            load_id=int(load_id),
            temp_id=1,
            p_set=list(data_2035.values),
        )

        session.add(entry)
        session.add(entry_ts)
        session.commit()


def entsoe_historic_generation_capacities(
    year_start="20190101", year_end="20200101"
):
    entsoe_token = open(
        path.join(path.expanduser("~"), ".entsoe-token"), "r"
    ).read(36)
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

    # todo: define wanted countries

    not_retrieved = []
    dfs = []
    for country in countries:
        if country == "GB":
            kwargs = dict(start=start_gb, end=end_gb)
        else:
            kwargs = dict(start=start, end=end)
        try:
            dfs.append(
                client.query_installed_generation_capacity(country, **kwargs)
            )

        except (entsoe.exceptions.NoMatchingDataError, requests.HTTPError):
            not_retrieved.append(country)
            pass

    if not_retrieved:
        logging.warning(
            f"Data for country (-ies) {', '.join(not_retrieved)} could not be retrieved."
        )
    df = pd.concat(dfs)
    df["country"] = countries
    df.set_index("country", inplace=True)
    df.fillna(0, inplace=True)
    return df


def entsoe_historic_demand(year_start="20190101", year_end="20200101"):
    entsoe_token = open(
        path.join(path.expanduser("~"), ".entsoe-token"), "r"
    ).read(36)
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
            pass
    if not_retrieved:
        logging.warning(
            f"Data for country (-ies) {', '.join(not_retrieved)} could not be retrieved."
        )

    df = pd.concat(dfs, axis=1)
    df.columns = countries
    df.index = pd.date_range(year_start, periods=8760, freq="H")

    return df


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


def entsoe_to_bus_etrago():
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

    for_bus = get_foreign_bus_id(scenario="status2019")

    return map_entsoe.map(for_bus)


def insert_generators_sq(scn_name="status2019"):
    """
    Insert generators for foreign countries based on ENTSO-E data

    Parameters
    ----------
    gen_sq : pandas dataframe
        df with all the foreign generators produced by the function
        entsoe_historic_generation_capacities
    scn_name : str
        The default is "status2019".

    Returns
    -------
    None.

    """
    try:
        gen_sq = entsoe_historic_generation_capacities()
    except:
        logging.warning(
            """Generation data from entsoe could not be retrieved.
                        Backup data is used instead"""
        )
        gen_sq = pd.read_csv(
            "data_bundle_egon_data/entsoe/gen_entsoe.csv", index_col="Index"
        )

    targets = config.datasets()["electrical_neighbours"]["targets"]
    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {targets['buses']['schema']}.{targets['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = '{scn_name}')
        AND scn_name = '{scn_name}'
        AND carrier != 'CH4'
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators_timeseries']['schema']}.
        {targets['generators_timeseries']['table']}
        WHERE generator_id NOT IN (
            SELECT generator_id FROM
            {targets['generators']['schema']}.{targets['generators']['table']}
        )
        AND scn_name = '{scn_name}'
        """
    )
    entsoe_to_bus = entsoe_to_bus_etrago()
    carrier_entsoe = map_carriers_entsoe()
    gen_sq = gen_sq.groupby(axis=1, by=carrier_entsoe).sum()

    # Filter generators modeled as storage and geothermal
    gen_sq = gen_sq.loc[
        :, ~gen_sq.columns.isin(["Hydro Pumped Storage", "geo_thermal"])
    ]

    list_gen_sq = pd.DataFrame(
        dtype=int, columns=["carrier", "country", "capacity"]
    )
    for carrier in gen_sq.columns:
        gen_carry = gen_sq[carrier]
        for country, cap in gen_carry.items():
            gen = pd.DataFrame(
                {"carrier": carrier, "country": country, "capacity": cap},
                index=[1],
            )
            # print(gen)
            list_gen_sq = pd.concat([list_gen_sq, gen], ignore_index=True)

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
    foreign_re_generators = db.select_dataframe(
        f"""
        SELECT generator_id, a.carrier, country, x, y
        FROM grid.egon_etrago_generator a
        JOIN  grid.egon_etrago_bus b
        ON a.bus = b.bus_id
        WHERE a.scn_name = '{scn_name}'
        AND  b.scn_name = '{scn_name}'
        AND b.carrier = 'AC'
        AND b.country != 'DE'
        AND a.carrier IN ('wind_onshore', 'wind_offshore', 'solar')
        """
    )

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


def insert_loads_sq(scn_name="status2019"):
    """
    Copy load timeseries data from entso-e.

    Returns
    -------
    None.

    """
    sources = config.datasets()["electrical_neighbours"]["sources"]
    targets = config.datasets()["electrical_neighbours"]["targets"]
    try:
        load_sq = entsoe_historic_demand()
    except:
        logging.warning(
            """Demand data from entsoe could not be retrieved.
                        Backup data is used instead"""
        )
        load_sq = pd.read_csv(
            "data_bundle_egon_data/entsoe/load_entsoe.csv", index_col="Index"
        )

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM {targets['load_timeseries']['schema']}.
        {targets['load_timeseries']['table']}
        WHERE
        scn_name = '{scn_name}'
        AND load_id IN (
        SELECT load_id FROM {targets['loads']['schema']}.
        {targets['loads']['table']}
        WHERE
        scn_name = '{scn_name}'
        AND carrier = 'AC'
        AND bus NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']}))
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM {targets['loads']['schema']}.
        {targets['loads']['table']}
        WHERE
        scn_name = '{scn_name}'
        AND carrier = 'AC'
        AND bus NOT IN (
            SELECT bus_i
            FROM  {sources['osmtgmod_bus']['schema']}.
            {sources['osmtgmod_bus']['table']})
        """
    )

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    # get the corresponding bus per foreign country
    entsoe_to_bus = entsoe_to_bus_etrago()

    # Calculate and insert demand timeseries per etrago bus_id
    for country in load_sq.columns:
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
            p_set=list(load_sq[country]),
        )

        session.add(entry)
        session.add(entry_ts)
        session.commit()


tasks = (grid,)

insert_per_scenario = set()

if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.update([tyndp_generation, tyndp_demand])

if "status2019" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.update([insert_generators_sq, insert_loads_sq])

tasks = tasks + (insert_per_scenario,)


class ElectricalNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ElectricalNeighbours",
            version="0.0.10",
            dependencies=dependencies,
            tasks=tasks,
        )
