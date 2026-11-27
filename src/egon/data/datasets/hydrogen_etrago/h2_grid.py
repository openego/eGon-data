"""
The central module containing all code dealing with the H2 grid.

"""

from pathlib import Path
from urllib.request import urlretrieve
import math
import os
import re

from fuzzywuzzy import process
from geoalchemy2.types import Geometry
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree
from shapely import wkb
from shapely.geometry import LineString, MultiLineString, Point
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.hydrogen_etrago.nep2025 import (
    NEP_H2_HHV_PER_LHV,
    NEP_H2_MEASURES_CRITERIA,
    NEP_SCENARIO_2045,
    kernnetz_commissioning_year,
    nep_h2_border_points,
    nep_h2_lng_terminals,
    nep_h2_measures_2037,
    nep_h2_measures_commissioning,
    nep_h2_measures_criterion,
    nep_h2_network_2045,
    nep_h2_other_import_projects,
    nep_h2_other_import_sites,
    nep_h2_other_imports,
    nep_h2_storage,
    nep_h2_storage_dunkelflaute,
    nep_h2_storage_projects,
    nep_kernnetz_dn_2045,
    nep_kernnetz_not_in_2045,
)
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)
from egon.data.datasets.scenario_parameters.parameters import (
    annualize_capital_costs,
)


def insert_h2_pipelines(scn_name):
    """Insert H2_grid based on input data from FNB-Gas."""
    sources, targets = load_sources_and_targets("HydrogenGridEtrago")

    (
        H2_grid_Neubau,
        H2_grid_Umstellung,
        H2_grid_Erweiterung,
    ) = read_h2_excel_sheets()
    h2_bus_location = read_h2_grid_nodes()
    con = db.engine()

    h2_buses_df = pd.read_sql(
        f"""
    SELECT bus_id, x, y FROM {sources.tables["buses"]}
    WHERE carrier in ('H2_grid')
    AND scn_name = '{scn_name}'   
    """,
        con,
    )

    # Delete all old entries
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["hydrogen_links"]}
        WHERE "carrier" = 'H2_grid'
        AND scn_name = '{scn_name}'
        """
    )

    for sheet, df in [
        ("Neubau", H2_grid_Neubau),
        ("Umstellung", H2_grid_Umstellung),
        ("Erweiterung", H2_grid_Erweiterung),
    ]:
        df = select_h2_pipelines(df, sheet, scn_name, h2_bus_location)

        # Manual adjustments based on Detailmaßnahmenkarte der FNB-Gas [https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/]
        df = fix_h2_grid_infrastructure(df)

        df_merged = pd.merge(
            df,
            h2_bus_location[["Ort", "geom", "x", "y"]],
            how="left",
            left_on="Anfangspunkt_matched",
            right_on="Ort",
        ).rename(
            columns={"geom": "geom_start", "x": "x_start", "y": "y_start"}
        )
        df_merged = pd.merge(
            df_merged,
            h2_bus_location[["Ort", "geom", "x", "y"]],
            how="left",
            left_on="Endpunkt_matched",
            right_on="Ort",
        ).rename(columns={"geom": "geom_end", "x": "x_end", "y": "y_end"})

        H2_grid_df = df_merged.dropna(subset=["geom_start", "geom_end"])

        # Pipelines starting and ending at the same node can not be placed
        # (57 km, mostly Gasnetz Hamburg), see the documentation.
        same_node = H2_grid_df["geom_start"] == H2_grid_df["geom_end"]
        H2_grid_df = H2_grid_df[~same_node]
        H2_grid_df = share_split_pipelines(H2_grid_df)
        H2_grid_df = pd.merge(
            H2_grid_df,
            h2_buses_df,
            how="left",
            left_on=["x_start", "y_start"],
            right_on=["x", "y"],
        ).rename(columns={"bus_id": "bus0"})
        H2_grid_df = pd.merge(
            H2_grid_df,
            h2_buses_df,
            how="left",
            left_on=["x_end", "y_end"],
            right_on=["x", "y"],
        ).rename(columns={"bus_id": "bus1"})
        H2_grid_df[["bus0", "bus1"]] = H2_grid_df[["bus0", "bus1"]].astype(
            "Int64"
        )

        H2_grid_df["geom_start"] = H2_grid_df["geom_start"].apply(
            lambda x: wkb.loads(bytes.fromhex(x))
        )
        H2_grid_df["geom_end"] = H2_grid_df["geom_end"].apply(
            lambda x: wkb.loads(bytes.fromhex(x))
        )
        H2_grid_df["topo"] = H2_grid_df.apply(
            lambda row: LineString([row["geom_start"], row["geom_end"]]),
            axis=1,
        )
        H2_grid_df["geom"] = H2_grid_df.apply(
            lambda row: MultiLineString(
                [LineString([row["geom_start"], row["geom_end"]])]
            ),
            axis=1,
        )
        H2_grid_gdf = gpd.GeoDataFrame(H2_grid_df, geometry="geom", crs=4326)

        scn_params = get_sector_parameters("gas", scn_name)
        interest_rate = get_sector_parameters("global", scn_name)[
            "interest_rate"
        ]

        H2_grid_gdf["link_id"] = db.next_etrago_id("link", len(H2_grid_gdf))
        H2_grid_gdf["scn_name"] = scn_name
        H2_grid_gdf["carrier"] = "H2_grid"
        H2_grid_gdf["Planerische Inbetriebnahme"] = (
            H2_grid_gdf["Planerische Inbetriebnahme"]
            .astype(str)
            .apply(
                lambda x: (
                    int(re.findall(r"\d{4}", x)[-1])
                    if re.findall(r"\d{4}", x)
                    else (
                        int(re.findall(r"\d{2}\.\d{2}\.(\d{4})", x)[-1])
                        if re.findall(r"\d{2}\.\d{2}\.(\d{4})", x)
                        else None
                    )
                )
            )
        )
        H2_grid_gdf["build_year"] = H2_grid_gdf[
            "Planerische Inbetriebnahme"
        ].astype("Int64")
        H2_grid_gdf["p_nom"] = H2_grid_gdf.apply(
            lambda row: calculate_H2_capacity(
                row["Druckstufe (DP)\n[mind. 30 barg]"],
                row["Nenndurchmesser \n(DN)"],
            ),
            axis=1,
        )
        H2_grid_gdf["p_nom_min"] = H2_grid_gdf["p_nom"]
        H2_grid_gdf["p_nom_max"] = float("Inf")
        H2_grid_gdf["p_nom_extendable"] = False
        H2_grid_gdf["lifetime"] = scn_params["lifetime"]["H2_pipeline"]
        H2_grid_gdf["capital_cost"] = H2_grid_gdf.apply(
            lambda row: annualize_capital_costs(
                (
                    (
                        float(row["Investitionskosten*\n(Mio. Euro)"])
                        * 10**6
                        / row["p_nom"]
                    )
                    if pd.notna(row["Investitionskosten*\n(Mio. Euro)"])
                    and str(row["Investitionskosten*\n(Mio. Euro)"])
                    .replace(",", "")
                    .replace(".", "")
                    .isdigit()
                    and float(row["Investitionskosten*\n(Mio. Euro)"]) != 0
                    else scn_params["overnight_cost"]["H2_pipeline"]
                    * row["Länge \n(km)"]
                ),
                row["lifetime"],
                interest_rate,
            ),
            axis=1,
        )
        H2_grid_gdf["p_min_pu"] = -1
        # Length of the pipeline in km (FNB-Gas list)
        H2_grid_gdf["length"] = H2_grid_gdf["Länge \n(km)"]

        selected_columns = [
            "scn_name",
            "link_id",
            "bus0",
            "bus1",
            "length",
            "build_year",
            "p_nom",
            "p_nom_min",
            "p_nom_extendable",
            "capital_cost",
            "geom",
            "topo",
            "carrier",
            "p_nom_max",
            "p_min_pu",
        ]

        H2_grid_final = H2_grid_gdf[selected_columns]

        # Insert data to db
        H2_grid_final.to_postgis(
            targets.get_table_name("hydrogen_links"),
            con,
            schema=targets.get_table_schema("hydrogen_links"),
            if_exists="append",
            dtype={"geom": Geometry()},
        )

    # Sections of the hydrogen network of the NEP beyond the core network
    # (measures up to 2037, additional sections 2045)
    insert_nep_h2_sections(scn_name, sources, targets)

    # connect saltcaverns to H2_grid
    connect_saltcavern_to_h2_grid(scn_name)

    # connect neighbour countries to H2_grid
    border_entry = connect_h2_grid_to_neighbour_countries(scn_name)
    remove_isolated_h2_buses_abroad(scn_name, targets)

    # supply of the hydrogen imports at the entry points of the NEP
    insert_h2_imports(scn_name, border_entry, sources, targets)

    # connect the parts of the core network that no pipeline of the lists
    # connects to the rest
    connect_h2_grid_islands(scn_name, sources, targets)


def read_h2_grid_nodes():
    """
    Nodes of the H2 grid (h2_grid_nodes.csv of the data bundle): the core
    network and the end points of the NEP sections

    Returns
    -------
    pandas.DataFrame
        Nodes with the columns Ort, geom (WKB as hex), x and y

    """
    nodes = pd.read_csv(
        Path(".")
        / "data_bundle_egon_data"
        / "hydrogen_network"
        / "h2_grid_nodes.csv"
    )
    nodes["Ort"] = nodes["Ort"].astype(str).str.strip()
    return nodes


def select_h2_pipelines(df, sheet, scn_name, h2_bus_location):
    """
    Select the pipelines of one FNB-Gas list that exist in a scenario

    Parameters
    ----------
    df : pandas.DataFrame
        One list of :py:func:`read_h2_excel_sheets`
    sheet : str
        "Neubau", "Umstellung" or "Erweiterung"
    scn_name : str
        Name of the scenario
    h2_bus_location : pandas.DataFrame
        Nodes of the H2 grid

    Returns
    -------
    pandas.DataFrame
        Pipelines with matched end points, not yet split

    """
    df = df.copy()

    if sheet == "Neubau":
        df.rename(
            columns={
                "Planerische \nInbetriebnahme": "Planerische Inbetriebnahme"
            },
            inplace=True,
        )
        df.loc[
            df["Endpunkt\n(Ort)"] == "AQD Anlandung", "Endpunkt\n(Ort)"
        ] = "Schillig"
        df.loc[
            df["Endpunkt\n(Ort)"] == "Hallendorf", "Endpunkt\n(Ort)"
        ] = "Salzgitter"

    if sheet == "Erweiterung":
        df.rename(
            columns={
                "Umstellungsdatum/ Planerische Inbetriebnahme": "Planerische Inbetriebnahme",
                "Nenndurchmesser (DN)": "Nenndurchmesser \n(DN)",
                "Investitionskosten\n(Mio. Euro),\nKostenschätzung": "Investitionskosten*\n(Mio. Euro)",
            },
            inplace=True,
        )
        df = df[
            df["Berücksichtigung im Kernnetz \n[ja/nein/zurückgezogen]"]
            .str.strip()
            .str.lower()
            == "ja"
        ]
        df.loc[
            df["Endpunkt\n(Ort)"] == "Osdorfer Straße", "Endpunkt\n(Ort)"
        ] = "Berlin- Lichterfelde"

    # Measures commissioned after the scenario year (year of the NEP where
    # known) are not inserted
    # The network 2045 builds on the modelling result 2037 of its scenario,
    # which drops or re-dimensions some measures
    if scn_name == "reGon2045":
        df = df[~df["Antrags-ID"].isin(nep_kernnetz_not_in_2045())]
        for measure, dn in nep_kernnetz_dn_2045().items():
            df.loc[df["Antrags-ID"] == measure, "Nenndurchmesser \n(DN)"] = dn
    df = df.assign(**{"Planerische Inbetriebnahme": commissioning_year(df)})
    df = df[~(df["Planerische Inbetriebnahme"] > get_scenario_year(scn_name))]

    h2_bus_location["Ort"] = h2_bus_location["Ort"].astype(str).str.strip()
    df["Anfangspunkt\n(Ort)"] = (
        df["Anfangspunkt\n(Ort)"].astype(str).str.strip()
    )
    df["Endpunkt\n(Ort)"] = df["Endpunkt\n(Ort)"].astype(str).str.strip()

    df = df[
        [
            "Anfangspunkt\n(Ort)",
            "Endpunkt\n(Ort)",
            "Nenndurchmesser \n(DN)",
            "Druckstufe (DP)\n[mind. 30 barg]",
            "Investitionskosten*\n(Mio. Euro)",
            "Planerische Inbetriebnahme",
            "Länge \n(km)",
        ]
    ]

    # matching start- and endpoint of each pipeline with georeferenced data
    df["Anfangspunkt_matched"] = fuzzy_match(
        df, h2_bus_location, "Anfangspunkt\n(Ort)"
    )
    df["Endpunkt_matched"] = fuzzy_match(
        df, h2_bus_location, "Endpunkt\n(Ort)"
    )

    return df


def active_h2_grid_nodes(scn_name):
    """
    Nodes of the H2 grid with at least one pipeline in the scenario

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    set
        Names ("Ort") of the active nodes

    """
    h2_bus_location = read_h2_grid_nodes()
    h2_bus_location["Ort"] = h2_bus_location["Ort"].astype(str).str.strip()
    Neubau, Umstellung, Erweiterung = read_h2_excel_sheets()

    nodes = set()
    for sheet, df in [
        ("Neubau", Neubau),
        ("Umstellung", Umstellung),
        ("Erweiterung", Erweiterung),
    ]:
        df = fix_h2_grid_infrastructure(
            select_h2_pipelines(df, sheet, scn_name, h2_bus_location)
        )
        df = df.dropna(subset=["Anfangspunkt_matched", "Endpunkt_matched"])
        df = df[df["Anfangspunkt_matched"] != df["Endpunkt_matched"]]
        nodes |= set(df["Anfangspunkt_matched"]) | set(df["Endpunkt_matched"])

    return nodes


def replace_pipeline(df, start, end, intermediate):
    """
    Method for adjusting pipelines manually by splittiing pipeline with an intermediate point.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        dataframe to be adjusted
    start: str
        startpoint of pipeline
    end: str
        endpoint of pipeline
    intermediate: str
        new intermediate point for splitting given pipeline

    Returns
    ---------
    df : <class 'pandas.core.frame.DataFrame'>
        adjusted dataframe


    """
    # Find rows where the start and end points match
    mask = (
        (df["Anfangspunkt_matched"] == start) & (df["Endpunkt_matched"] == end)
    ) | (
        (df["Anfangspunkt_matched"] == end) & (df["Endpunkt_matched"] == start)
    )

    # Separate the rows to replace
    if mask.any():
        df_replacement = df[~mask].copy()
        row_replaced = df[mask].iloc[0]

        # "split_id" identifies the pipeline of the list, length and
        # investment are shared in share_split_pipelines
        split_id = row_replaced.get("split_id")
        if pd.isna(split_id):
            split_id = f"{start} | {end}"

        # Add new rows for the split pipelines
        new_rows = pd.DataFrame(
            {
                "split_id": [split_id, split_id],
                "Anfangspunkt_matched": [start, intermediate],
                "Endpunkt_matched": [intermediate, end],
                "Nenndurchmesser \n(DN)": [
                    row_replaced["Nenndurchmesser \n(DN)"],
                    row_replaced["Nenndurchmesser \n(DN)"],
                ],
                "Druckstufe (DP)\n[mind. 30 barg]": [
                    row_replaced["Druckstufe (DP)\n[mind. 30 barg]"],
                    row_replaced["Druckstufe (DP)\n[mind. 30 barg]"],
                ],
                "Investitionskosten*\n(Mio. Euro)": [
                    row_replaced["Investitionskosten*\n(Mio. Euro)"],
                    row_replaced["Investitionskosten*\n(Mio. Euro)"],
                ],
                "Planerische Inbetriebnahme": [
                    row_replaced["Planerische Inbetriebnahme"],
                    row_replaced["Planerische Inbetriebnahme"],
                ],
                "Länge \n(km)": [
                    row_replaced["Länge \n(km)"],
                    row_replaced["Länge \n(km)"],
                ],
            }
        )

        df_replacement = pd.concat(
            [df_replacement, new_rows], ignore_index=True
        )
        return df_replacement
    else:
        return df


def share_split_pipelines(df):
    """
    Share length and investment of split pipelines by straight distance

    Parameters
    ----------
    df : pandas.DataFrame
        Pipelines with the columns x_start, y_start, x_end, y_end and
        split_id

    Returns
    -------
    pandas.DataFrame
        Pipelines with shared length and investment

    """
    df = df.copy()
    length = "Länge \n(km)"
    investment = "Investitionskosten*\n(Mio. Euro)"
    df[length] = pd.to_numeric(df[length], errors="coerce")

    if "split_id" not in df.columns or df["split_id"].isna().all():
        return df

    distance = (
        gpd.GeoSeries(
            [
                LineString([(x0, y0), (x1, y1)])
                for x0, y0, x1, y1 in zip(
                    df["x_start"], df["y_start"], df["x_end"], df["y_end"]
                )
            ],
            index=df.index,
            crs=4326,
        )
        .to_crs(3035)
        .length
    )
    split = df["split_id"].notna()
    share = distance[split] / distance[split].groupby(
        df.loc[split, "split_id"]
    ).transform("sum")

    df.loc[split, length] = df.loc[split, length] * share
    df.loc[split, investment] = (
        pd.to_numeric(df.loc[split, investment], errors="coerce") * share
    )

    return df


def fuzzy_match(df1, df2, column_to_match, threshold=80):
    """
    Method for matching input data of H2_grid with georeferenced data (even if the strings are not exact the same)

    Parameters
    ----------
    df1 : pandas.core.frame.DataFrame
        Input dataframe
    df2 : pandas.core.frame.DataFrame
        georeferenced dataframe with h2_buses
    column_to_match: str
        matching column
    treshhold: float
        matching percentage for succesfull comparison

    Returns
    ---------
    matched : list
        list with all matched location names

    """
    options = df2["Ort"].unique()
    matched = []

    # Compare every locationname in df1 with locationnames in df2
    for value in df1[column_to_match]:
        match, score = process.extractOne(value, options)
        if score >= threshold:
            matched.append(match)
        else:
            matched.append(None)

    return matched


# Design capacity of H2 pipelines [MW, LHV], European Hydrogen Backbone
# (April 2021, Appendix A, Tab. 3), see the documentation
H2_PIPE_REFERENCE = (1200, 80, 13000)  # DN [mm], pressure [bar], [MW]
H2_PIPE_SMALL_REFERENCE = (500, 50, 1200)
H2_PIPE_DN_EXPONENT = math.log(
    H2_PIPE_SMALL_REFERENCE[2]
    / H2_PIPE_REFERENCE[2]
    * H2_PIPE_REFERENCE[1]
    / H2_PIPE_SMALL_REFERENCE[1]
) / math.log(H2_PIPE_SMALL_REFERENCE[0] / H2_PIPE_REFERENCE[0])


def calculate_H2_capacity(pressure, diameter):
    """
    Transport capacity of a hydrogen pipeline, see :data:`H2_PIPE_REFERENCE`

    Parameters
    ----------
    pressure : float or str
        Design pressure of the pipeline [bar]
    diameter: float or str
        Nominal diameter of the pipeline [mm]

    Returns
    ---------
    float
        Transport capacity of the pipeline [MW, LHV]

    """

    pressure = str(pressure).replace(",", ".")
    diameter = str(diameter)

    def convert_to_float(value):
        try:
            return float(value)
        except ValueError:
            return 400  # average value from data-source cause capacities of some lines are not fixed yet

    # in case of given range for pipeline-capacity calculate average value
    if "-" in diameter:
        diameters = diameter.split("-")
        diameter = (
            convert_to_float(diameters[0]) + convert_to_float(diameters[1])
        ) / 2
    elif "/" in diameter:
        diameters = diameter.split("/")
        diameter = (
            convert_to_float(diameters[0]) + convert_to_float(diameters[1])
        ) / 2
    else:
        try:
            diameter = float(diameter)
        except ValueError:
            diameter = 400  # average value from data-source

    if "-" in pressure:
        pressures = pressure.split("-")
        pressure = (float(pressures[0]) + float(pressures[1])) / 2
    elif "/" in pressure:
        pressures = pressure.split("/")
        pressure = (float(pressures[0]) + float(pressures[1])) / 2
    else:
        try:
            pressure = float(pressure)
        except ValueError:
            pressure = 70  # averaqge value from data-source

    # Empty cells of the lists ("nan")
    if not math.isfinite(diameter):
        diameter = 400
    if not math.isfinite(pressure):
        pressure = 70

    dn_ref, pressure_ref, capacity_ref = H2_PIPE_REFERENCE
    return (
        capacity_ref
        * (diameter / dn_ref) ** H2_PIPE_DN_EXPONENT
        * pressure
        / pressure_ref
    )


def commissioning_year(df):
    """
    Commissioning year of the measures of a FNB-Gas list (NEP where known)

    Parameters
    ----------
    df : pandas.DataFrame
        FNB-Gas list with the columns "Antrags-ID" and
        "Planerische Inbetriebnahme"

    Returns
    -------
    pandas.Series
        Commissioning year (NaN if unknown)
    """
    years = df["Planerische Inbetriebnahme"].astype(str).str.findall(r"\d{4}")
    listed = pd.to_numeric(years.str[-1], errors="coerce")
    return df["Antrags-ID"].map(kernnetz_commissioning_year()).fillna(listed)


def _to_metric(geometries):
    geometries = gpd.GeoSeries(geometries.geometry)
    if geometries.crs is None:
        raise ValueError("The geometries need a crs.")
    return (
        geometries.to_crs(32632)
        if geometries.crs.is_geographic
        else (geometries)
    )


# Maximal distance [km] of a NEP end point to an H2_grid bus
H2_NODE_SNAP_KM = 5


def nep_h2_sections(scn_name):
    """
    Sections of the hydrogen network of the NEP beyond the core network

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    list of dict
        Sections with the keys label, start, end, length_km, dn, dp,
        build_year, start_xy and end_xy (lon, lat)
    """
    nodes = read_h2_grid_nodes()
    location = dict(zip(nodes["Ort"], zip(nodes["x"], nodes["y"])))
    year = get_scenario_year(scn_name)
    sections = []
    sections += [
        {
            "label": nep_id,
            "start": start,
            "end": end,
            "length_km": length_km,
            "dn": dn,
            "dp": dp,
            "build_year": nep_h2_measures_commissioning().get(
                nep_id, commissioning
            ),
            "start_xy": location[start],
            "end_xy": location[end],
        }
        for nep_id, start, end, length_km, dn, dp, commissioning in (
            nep_h2_measures_2037()
        )
        if nep_h2_measures_criterion().get(nep_id) in NEP_H2_MEASURES_CRITERIA
        and year >= nep_h2_measures_commissioning().get(nep_id, commissioning)
    ]
    if scn_name == "reGon2045":
        sections += [
            {
                "label": f"A6-{number}",
                "start": start,
                "end": end,
                "length_km": length_km,
                "dn": dn,
                "dp": dp,
                "build_year": year,
                "start_xy": location[start],
                "end_xy": location[end],
            }
            for number, start, end, kind, length_km, dn, dp, scenarios in (
                nep_h2_network_2045()
            )
            if NEP_SCENARIO_2045 in scenarios and None not in (start, end)
        ]
    return sections


def _section_points(sections):
    """End points of NEP sections: name -> (lon, lat), first occurrence"""
    points = {}
    for section in sections:
        points.setdefault(section["start"], section["start_xy"])
        points.setdefault(section["end"], section["end_xy"])
    return points


def nep_h2_nodes(existing_nodes, scn_name):
    """
    End points of the NEP sections of a scenario that need a new bus.

    Parameters
    ----------
    existing_nodes : pandas.DataFrame
        Existing nodes of the H2 grid with the coordinates x and y (lon, lat)
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.DataFrame
        Name (Ort), x, y and geom (shapely Point) of the new nodes
    """
    points = _section_points(nep_h2_sections(scn_name))
    names = sorted(points)
    if not names:
        return pd.DataFrame(columns=["Ort", "x", "y", "geom"])
    metric = gpd.GeoSeries(
        [Point(points[name]) for name in names], crs=4326
    ).to_crs(32632)
    existing = gpd.GeoSeries(
        gpd.points_from_xy(existing_nodes["x"], existing_nodes["y"]),
        crs=4326,
    ).to_crs(32632)
    kept = [(point.x, point.y) for point in existing]

    new = []
    for name, point in zip(names, metric):
        distance = (
            np.hypot(*(np.array(kept) - (point.x, point.y)).T).min()
            if kept
            else np.inf
        )
        if distance > H2_NODE_SNAP_KM * 1000:
            new.append(name)
            kept.append((point.x, point.y))

    # A new node needs a section to another node: both ends of a short
    # section can fall on the same node (e.g. H2-1201 Strohreit-Reitmehring)
    tree = cKDTree(np.array(kept))
    position = dict(zip(names, np.column_stack([metric.x, metric.y])))
    used = set()
    for section in nep_h2_sections(scn_name):
        _, nodes = tree.query(
            [position[section["start"]], position[section["end"]]]
        )
        if nodes[0] != nodes[1]:
            used.update(nodes)
    first_new = len(kept) - len(new)
    new = [name for k, name in enumerate(new) if first_new + k in used]

    xy = [points[name] for name in new]
    return pd.DataFrame(
        {
            "Ort": new,
            "x": [x for x, _ in xy],
            "y": [y for _, y in xy],
            "geom": [Point(x, y) for x, y in xy],
        }
    )


def insert_nep_h2_sections(scn_name, sources, targets):
    """
    Insert the sections of the hydrogen network of the NEP beyond the core
    network.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    sources : DatasetSources
        Sources of HydrogenGridEtrago
    targets : DatasetTargets
        Targets of HydrogenGridEtrago

    Returns
    -------
    None
    """
    sections = nep_h2_sections(scn_name)
    if not sections:
        return None
    buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'H2_grid'
        AND country = 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    if buses.empty:
        return None

    metric_buses = _to_metric(buses)
    tree = cKDTree(np.column_stack([metric_buses.x, metric_buses.y]))
    points = _section_points(sections)
    names = sorted(points)
    metric_points = gpd.GeoSeries(
        [Point(points[name]) for name in names], crs=4326
    ).to_crs(32632)
    _, closest = tree.query(
        np.column_stack([metric_points.x, metric_points.y])
    )
    bus_of = dict(zip(names, buses.index[closest]))

    scn_params = get_sector_parameters("gas", scn_name)
    interest_rate = get_sector_parameters("global", scn_name)["interest_rate"]

    links = []
    for section in sections:
        bus0, bus1 = bus_of[section["start"]], bus_of[section["end"]]
        if bus0 == bus1:
            continue
        bus_km = metric_buses[bus0].distance(metric_buses[bus1]) / 1000
        if bus_km > 2 * section["length_km"] + H2_NODE_SNAP_KM:
            continue
        p_nom = calculate_H2_capacity(section["dp"], section["dn"])
        lifetime = scn_params["lifetime"]["H2_pipeline"]
        line = LineString([buses.at[bus0, "geom"], buses.at[bus1, "geom"]])
        links.append(
            {
                "scn_name": scn_name,
                "bus0": bus0,
                "bus1": bus1,
                "carrier": "H2_grid",
                "build_year": section["build_year"],
                "p_nom": p_nom,
                "p_nom_min": p_nom,
                "p_nom_max": float("inf"),
                "p_nom_extendable": False,
                "p_min_pu": -1,
                "lifetime": lifetime,
                "capital_cost": annualize_capital_costs(
                    scn_params["overnight_cost"]["H2_pipeline"]
                    * section["length_km"],
                    lifetime,
                    interest_rate,
                ),
                "geom": MultiLineString([line]),
                "topo": line,
                "length": section["length_km"],
            }
        )

    if links:
        gdf = gpd.GeoDataFrame(links, geometry="geom", crs=4326)
        gdf["topo"] = gpd.GeoSeries(gdf["topo"], crs=4326)
        gdf["link_id"] = db.next_etrago_id("link", len(gdf))
        gdf.to_postgis(
            targets.get_table_name("hydrogen_links"),
            db.engine(),
            schema=targets.get_table_schema("hydrogen_links"),
            if_exists="append",
            index=False,
            dtype={"geom": Geometry(), "topo": Geometry()},
        )

    return None


def kernnetz_file(sources, key):
    """
    Path of a list of the H2 core network: the file of the data bundle, else
    the file downloaded into ./datasets/h2_data

    Parameters
    ----------
    sources : egon.data.datasets.DatasetSources
        Sources of HydrogenGridEtrago
    key : str
        Key of the list in the sources

    Returns
    -------
    pathlib.Path
        Path of the file

    """
    path = Path(sources.files[key])
    if path.is_file():
        return path
    return Path("datasets", "h2_data", path.name)


def download_h2_grid_data():
    """
    Download Input data for H2_grid from FNB-Gas (https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/)

    The following data for H2 are downloaded into the folder
    ./datasets/h2_data if they are not in the data bundle (folder
    hydrogen_network):
      * Links (Anlage 3: Neubau, Anlage 4: Umstellung, Anlage 2: weitere
        Leitungen), version of 2024-12-10

    Returns
    -------
    None

    """
    sources, _ = load_sources_and_targets("HydrogenGridEtrago")
    path = Path("datasets/h2_data")
    os.makedirs(path, exist_ok=True)

    target_file_Um = kernnetz_file(sources, "converted_ch4_pipes")
    target_file_Neu = kernnetz_file(sources, "new_constructed_pipes")
    target_file_Erw = kernnetz_file(
        sources, "pipes_of_further_h2_grid_operators"
    )

    for target_file in [target_file_Neu, target_file_Um, target_file_Erw]:
        if target_file is target_file_Um:
            url = sources.urls["converted_ch4_pipes"]
        elif target_file is target_file_Neu:
            url = sources.urls["new_constructed_pipes"]
        else:
            url = sources.urls["pipes_of_further_h2_grid_operators"]

        if not os.path.isfile(target_file):
            urlretrieve(url, target_file)


def read_h2_excel_sheets():
    """
    Read downloaded excel files with location names for future h2-pipelines

    Returns
    -------
    df_Neu : <class 'pandas.core.frame.DataFrame'>
    df_Um : <class 'pandas.core.frame.DataFrame'>
    df_Erw : <class 'pandas.core.frame.DataFrame'>


    """
    sources, _ = load_sources_and_targets("HydrogenGridEtrago")

    excel_file_Um = pd.ExcelFile(
        kernnetz_file(sources, "converted_ch4_pipes")
    )
    excel_file_Neu = pd.ExcelFile(
        kernnetz_file(sources, "new_constructed_pipes")
    )
    excel_file_Erw = pd.ExcelFile(
        kernnetz_file(sources, "pipes_of_further_h2_grid_operators")
    )

    df_Um = pd.read_excel(excel_file_Um, header=3)
    df_Neu = pd.read_excel(excel_file_Neu, header=3)
    df_Erw = pd.read_excel(excel_file_Erw, header=2)

    return df_Neu, df_Um, df_Erw


def fix_h2_grid_infrastructure(df):
    """
    Manuell adjustments for more accurate grid topology based on Detailmaßnahmenkarte der
    FNB-Gas [https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/]

    Returns
    -------
    df : <class 'pandas.core.frame.DataFrame'>

    """

    df = replace_pipeline(df, "Lubmin", "Uckermark", "Wrangelsburg")
    df = replace_pipeline(df, "Wrangelsburg", "Uckermark", "Schönermark")
    df = replace_pipeline(df, "Heidenau", "Elbe-Süd", "Weißenfelde")
    df = replace_pipeline(df, "Weißenfelde", "Elbe-Süd", "Stade")
    df = replace_pipeline(df, "Achim", "Folmhusen", "Wardenburg")
    df = replace_pipeline(df, "Achim", "Wardenburg", "Sandkrug")
    df = replace_pipeline(df, "Dykhausen", "Bunde", "Emden")
    df = replace_pipeline(df, "Emden", "Nüttermoor", "Jemgum")
    df = replace_pipeline(df, "Rostock", "Glasewitz", "Fliegerhorst Laage")
    df = replace_pipeline(df, "Wilhelmshaven", "Dykhausen", "Sande")
    df = replace_pipeline(
        df, "Wilhelmshaven Süd", "Wilhelmshaven Nord", "Wilhelmshaven"
    )
    df = replace_pipeline(df, "Sande", "Jemgum", "Westerstede")
    df = replace_pipeline(df, "Kalle", "Ochtrup", "Frensdorfer Bruchgraben")
    df = replace_pipeline(
        df, "Frensdorfer Bruchgraben", "Ochtrup", "Bad Bentheim"
    )
    df = replace_pipeline(df, "Bunde", "Wettringen", "Emsbüren")
    df = replace_pipeline(df, "Emsbüren", "Dorsten", "Ochtrup")
    df = replace_pipeline(df, "Ochtrup", "Dorsten", "Heek")
    df = replace_pipeline(df, "Lemförde", "Drohne", "Reiningen")
    df = replace_pipeline(df, "Edesbüttel", "Bobbau", "Uhrsleben")
    df = replace_pipeline(df, "Schkeuditz", "Plaußig", "Wiederitzsch")
    df = replace_pipeline(df, "Wiederitzsch", "Plaußig", "Mockau Nord")
    df = replace_pipeline(df, "Bobbau", "Rückersdorf", "Nempitz")
    df = replace_pipeline(df, "Räpitz", "Böhlen", "Kleindalzig")
    df = replace_pipeline(df, "Buchholz", "Friedersdorf", "Werben")
    df = replace_pipeline(df, "Radeland", "Uckermark", "Friedersdorf")
    df = replace_pipeline(df, "Friedersdorf", "Uckermark", "Herzfelde")
    df = replace_pipeline(df, "Blumberg", "Berlin-Mitte", "Berlin-Marzahn")
    df = replace_pipeline(df, "Radeland", "Zethau", "Coswig")
    df = replace_pipeline(df, "Leuna", "Böhlen", "Räpitz")
    df = replace_pipeline(df, "Dürrengleina", "Stadtroda", "Zöllnitz")
    df = replace_pipeline(df, "Mailing", "Kötz", "Wertingen")
    df = replace_pipeline(df, "Lampertheim", "Rüsselsheim", "Gernsheim-Nord")
    df = replace_pipeline(df, "Birlinghoven", "Rüsselsheim", "Wiesbaden")
    df = replace_pipeline(df, "Medelsheim", "Mittelbrunn", "Seyweiler")
    df = replace_pipeline(df, "Seyweiler", "Dillingen", "Fürstenhausen")
    df = replace_pipeline(df, "Reckrod", "Wolfsbehringen", "Eisenach")
    df = replace_pipeline(df, "Elten", "St. Hubert", "Hüthum")
    df = replace_pipeline(df, "St. Hubert", "Hüthum", "Uedener Bruch")
    df = replace_pipeline(df, "Wallach", "Möllen", "Spellen")
    df = replace_pipeline(df, "St. Hubert", "Glehn", "Krefeld")
    df = replace_pipeline(df, "Neumühl", "Werne", "Bottrop")
    df = replace_pipeline(df, "Bottrop", "Werne", "Recklinghausen")
    df = replace_pipeline(df, "Werne", "Eisenach", "Arnsberg-Bruchhausen")
    df = replace_pipeline(df, "Dorsten", "Gescher", "Gescher Süd")
    df = replace_pipeline(df, "Dorsten", "Hamborn", "Averbruch")
    df = replace_pipeline(df, "Neumühl", "Bruckhausen", "Hamborn")
    df = replace_pipeline(df, "Werne", "Paffrath", "Westhofen")
    df = replace_pipeline(df, "Glehn", "Voigtslach", "Dormagen")
    df = replace_pipeline(df, "Voigtslach", "Paffrath", "Leverkusen")
    df = replace_pipeline(df, "Glehn", "Ludwigshafen", "Wesseling")
    df = replace_pipeline(df, "Rothenstadt", "Rimpar", "Reutles")

    return df


def connect_h2_grid_islands(scn_name, sources, targets):
    """
    Connect the isolated parts of the H2 core network by straight pipelines

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    sources : DatasetSources
        Sources of HydrogenGridEtrago
    targets : DatasetTargets
        Targets of HydrogenGridEtrago

    Returns
    -------
    None

    """
    buses = db.select_geodataframe(
        f"""
        SELECT bus_id, country, geom FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}'
        AND carrier IN ('H2_grid', 'H2')
        """,
        index_col="bus_id",
        epsg=4326,
    )
    # Without export-only border links (p_min_pu 0): a part counts as
    # supplied from abroad only if hydrogen can flow into it
    links = db.select_dataframe(
        f"""
        SELECT bus0, bus1, p_nom, build_year FROM {targets.tables["hydrogen_links"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'H2_grid'
        AND p_min_pu < 0
        """
    )
    links = links[links.bus0.isin(buses.index) & links.bus1.isin(buses.index)]
    if links.empty:
        return

    # Connected parts of the network
    position = pd.Series(range(len(buses)), index=buses.index)
    graph = coo_matrix(
        (
            np.ones(len(links)),
            (position[links.bus0].values, position[links.bus1].values),
        ),
        shape=(len(buses), len(buses)),
    )
    _, label = connected_components(graph, directed=False)
    buses["part"] = label

    german = buses[buses.country == "DE"]
    linked = set(links.bus0) | set(links.bus1)
    main = german[german.index.isin(linked)].part.value_counts().idxmax()
    abroad = set(buses[buses.country != "DE"].part)

    metric = buses.to_crs(3035)
    main_buses = metric[(metric.part == main) & (metric.country == "DE")]

    scn_params = get_sector_parameters("gas", scn_name)
    lifetime = scn_params["lifetime"]["H2_pipeline"]
    interest_rate = get_sector_parameters("global", scn_name)["interest_rate"]

    new_links = []
    for part in set(german[german.index.isin(linked)].part) - {main}:
        if part in abroad:
            continue
        island = metric[metric.part == part]
        distance = island.geometry.apply(
            lambda point: main_buses.distance(point)
        )
        island_bus = distance.min(axis=1).idxmin()
        main_bus = distance.loc[island_bus].idxmin()
        length_km = distance.loc[island_bus, main_bus] / 1000

        island_links = links[
            links.bus0.isin(island.index) | links.bus1.isin(island.index)
        ]
        line = LineString(
            [buses.at[island_bus, "geom"], buses.at[main_bus, "geom"]]
        )
        new_links.append(
            {
                "scn_name": scn_name,
                "bus0": island_bus,
                "bus1": main_bus,
                "carrier": "H2_grid",
                "build_year": island_links.build_year.min(),
                "p_nom": island_links.p_nom.max(),
                "p_nom_min": island_links.p_nom.max(),
                "p_nom_max": float("inf"),
                "p_nom_extendable": False,
                "p_min_pu": -1,
                "lifetime": lifetime,
                "length": length_km,
                "capital_cost": annualize_capital_costs(
                    scn_params["overnight_cost"]["H2_pipeline"] * length_km,
                    lifetime,
                    interest_rate,
                ),
                "geom": MultiLineString([line]),
                "topo": line,
            }
        )

    if not new_links:
        return

    new_links = gpd.GeoDataFrame(new_links, geometry="geom", crs=4326)
    new_links["topo"] = gpd.GeoSeries(new_links["topo"], crs=4326)
    new_links["link_id"] = db.next_etrago_id("link", len(new_links))
    new_links.to_postgis(
        targets.get_table_name("hydrogen_links"),
        db.engine(),
        schema=targets.get_table_schema("hydrogen_links"),
        if_exists="append",
        index=False,
        dtype={"geom": Geometry(), "topo": Geometry()},
    )


# Federal states with salt structures and their group in the market survey
SALTCAVERN_SURVEY_STATES = {
    "Brandenburg": "Brandenburg/Berlin",
    "Berlin": "Brandenburg/Berlin",
    "Mecklenburg-Vorpommern": "Mecklenburg-Vorpommern",
    "Niedersachsen": "Niedersachsen/Bremen",
    "Bremen": "Niedersachsen/Bremen",
    "Nordrhein-Westfalen": "Nordrhein-Westfalen",
    "Sachsen-Anhalt": "Sachsen-Anhalt",
    "Schleswig-Holstein": "Schleswig-Holstein/Hamburg",
    "Hamburg": "Schleswig-Holstein/Hamburg",
    "Thüringen": "Thüringen",
}


def nep_h2_storage_rates(scn_name):
    """
    Return the hydrogen storage capacity of the NEP per federal state

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    rates : pandas.Series
        Withdrawal capacity [MW, LHV] per federal state of the survey
    injection_share : float
        Injection capacity as a share of the withdrawal capacity

    """
    nep_year = get_scenario_year(scn_name)
    withdrawal, injection = nep_h2_storage()[nep_year]
    projects = pd.Series(nep_h2_storage_projects()[nep_year], dtype=float)
    rates = withdrawal * projects / projects.sum()
    dunkelflaute = nep_h2_storage_dunkelflaute().get(nep_year)
    if dunkelflaute:
        extra_withdrawal, states = dunkelflaute
        extra = pd.Series(extra_withdrawal / len(states), index=states)
        rates = rates.add(extra, fill_value=0)
    total = rates.sum()

    with_caverns = rates.index.isin(set(SALTCAVERN_SURVEY_STATES.values()))
    rates = rates[with_caverns] * total / rates[with_caverns].sum()

    # GWh/h (Brennwert) -> MW (lower heating value)
    return rates * 1000 / NEP_H2_HHV_PER_LHV, injection / total


def connect_saltcavern_to_h2_grid(scn_name):
    """
    Connect each saltcavern with nearest H2-Bus of the H2-Grid and insert the links into the database

    Returns
    -------
    None

    """
    sources, targets = load_sources_and_targets("HydrogenGridEtrago")

    engine = db.engine()

    db.execute_sql(f"""
           DELETE FROM {targets.tables["hydrogen_links"]}
           WHERE "carrier" in ('H2_saltcavern')
           AND scn_name = '{scn_name}';    
           """)
    h2_buses_query = f"""SELECT bus_id, x, y,ST_Transform(geom, 32632) as geom 
                        FROM  {sources.tables["buses"]}
                        WHERE carrier = 'H2_grid' AND scn_name = '{scn_name}'
                    """
    h2_buses = gpd.read_postgis(h2_buses_query, engine)

    salt_caverns_query = f"""SELECT bus_id, x, y, ST_Transform(geom, 32632) as geom 
                            FROM  {sources.tables["buses"]}
                            WHERE carrier = 'H2_saltcavern'  AND scn_name = '{scn_name}'
                        """
    salt_caverns = gpd.read_postgis(salt_caverns_query, engine)

    # Storage capacity of the NEP per saltcavern, shared over the
    # saltcaverns of a federal state by their storage potential
    rates, injection_share = nep_h2_storage_rates(scn_name)
    potential = db.select_dataframe(
        f"""
        SELECT m."bus_H2" AS bus_id, s.gen AS state,
            SUM(s.potential * s.area_fraction) AS potential
        FROM {sources.tables["saltcavern_data"]} s
        JOIN {sources.tables["H2_AC_map"]} m ON m."bus_AC" = s.bus_id
        WHERE m.scn_name = '{scn_name}'
        GROUP BY m."bus_H2", s.gen
        """
    )
    potential["survey_state"] = potential.state.map(SALTCAVERN_SURVEY_STATES)
    potential["share"] = potential.potential / potential.groupby(
        "survey_state"
    ).potential.transform("sum")
    potential["p_nom_max"] = potential.share * potential.survey_state.map(
        rates
    ).fillna(0)
    p_nom_max = potential.groupby("bus_id").p_nom_max.sum()

    scn_params = get_sector_parameters("gas", scn_name)

    H2_coords = np.array([(point.x, point.y) for point in h2_buses.geometry])
    H2_tree = cKDTree(H2_coords)
    links = []
    for idx, bus_saltcavern in salt_caverns.iterrows():
        saltcavern_coords = [
            bus_saltcavern["geom"].x,
            bus_saltcavern["geom"].y,
        ]

        dist, nearest_idx = H2_tree.query(saltcavern_coords, k=1)
        nearest_h2_bus = h2_buses.iloc[nearest_idx]

        link = {
            "scn_name": scn_name,
            "bus0": nearest_h2_bus["bus_id"],
            "bus1": bus_saltcavern["bus_id"],
            "link_id": db.next_etrago_id("link"),
            "carrier": "H2_saltcavern",
            "lifetime": 25,
            "p_nom_extendable": True,
            "p_nom_max": p_nom_max.get(bus_saltcavern["bus_id"], 0),
            # bus0 is the grid: negative flow = withdrawal (p_nom)
            "p_min_pu": -1,
            "p_max_pu": injection_share,
            "capital_cost": scn_params["capital_cost"]["H2_pipeline"]
            * dist
            / 1000,
            "length": dist / 1000,
            "geom": MultiLineString(
                [
                    LineString(
                        [
                            (nearest_h2_bus["x"], nearest_h2_bus["y"]),
                            (bus_saltcavern["x"], bus_saltcavern["y"]),
                        ]
                    )
                ]
            ),
        }
        links.append(link)

    links_df = gpd.GeoDataFrame(links, geometry="geom", crs=4326)

    links_df.to_postgis(
        targets.get_table_name("hydrogen_links"),
        engine,
        schema=targets.get_table_schema("hydrogen_links"),
        index=False,
        if_exists="append",
        dtype={"geom": Geometry()},
    )


def connect_h2_grid_to_neighbour_countries(scn_name):
    """
    Connect the German H2 grid to the neighbouring countries at the NEP
    cross-border points.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.Series
        Entry capacity [MW] per H2 bus abroad (index: bus_id)

    """
    sources, targets = load_sources_and_targets("HydrogenGridEtrago")

    engine = db.engine()

    german_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {sources.tables["buses"]}
        WHERE carrier = 'H2_grid' AND scn_name = '{scn_name}'
        AND country = 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    abroad_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, country, geom FROM {sources.tables["buses"]}
        WHERE carrier = 'H2' AND scn_name = '{scn_name}'
        AND country != 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    if german_buses.empty or abroad_buses.empty:
        return pd.Series(dtype=float)

    nodes = read_h2_grid_nodes()
    location = {
        name: (x, y) for name, x, y in zip(nodes.Ort, nodes.x, nodes.y)
    }
    german_metric = _to_metric(german_buses)
    abroad_metric = _to_metric(abroad_buses)

    scn_params = get_sector_parameters("gas", scn_name)
    lifetime = scn_params["lifetime"]["H2_pipeline"]
    overnight_cost = scn_params["overnight_cost"]["H2_pipeline"]
    interest_rate = get_sector_parameters("global", scn_name)["interest_rate"]

    links = []
    for (
        name,
        country,
        share,
        node,
        entry_2037,
        exit_2037,
        entry_2045,
        exit_2045,
    ) in nep_h2_border_points():
        entry, exit_ = (
            (entry_2045, exit_2045)
            if scn_name == "reGon2045"
            else (entry_2037, exit_2037)
        )
        # GWh/h (Brennwert) -> MW (lower heating value)
        entry, exit_ = (
            entry * share * 1000 / NEP_H2_HHV_PER_LHV,
            exit_ * share * 1000 / NEP_H2_HHV_PER_LHV,
        )
        if max(entry, exit_) == 0:
            continue

        point = _to_metric(
            gpd.GeoSeries([Point(location[node])], crs=4326)
        ).iloc[0]
        distance = german_metric.distance(point)
        if distance.min() > H2_NODE_SNAP_KM * 1000:
            continue
        german_bus = distance.idxmin()

        candidates = abroad_metric[abroad_buses["country"] == country]
        if candidates.empty:
            continue
        distance_abroad = candidates.distance(german_metric[german_bus])
        abroad_bus = distance_abroad.idxmin()
        length_km = distance_abroad.min() / 1000

        p_nom = max(entry, exit_)
        line = LineString(
            [
                german_buses.at[german_bus, "geom"],
                abroad_buses.at[abroad_bus, "geom"],
            ]
        )
        links.append(
            {
                "scn_name": scn_name,
                "carrier": "H2_grid",
                "bus0": german_bus,
                "bus1": abroad_bus,
                "p_nom": p_nom,
                "p_nom_min": p_nom,
                "p_nom_max": float("inf"),
                "p_nom_extendable": False,
                # bus0 is in Germany: positive flow = exit (export)
                "p_max_pu": exit_ / p_nom,
                "p_min_pu": -entry / p_nom,
                "lifetime": lifetime,
                "capital_cost": annualize_capital_costs(
                    overnight_cost * length_km, lifetime, interest_rate
                ),
                "length": length_km,
                "geom": MultiLineString([line]),
                "topo": line,
                "name": name,
                "country": country,
                "entry": entry,
                "exit": exit_,
            }
        )

    if links:
        report = pd.DataFrame(links)
        gdf = gpd.GeoDataFrame(
            report.drop(columns=["name", "country", "entry", "exit"]),
            geometry="geom",
            crs=4326,
        )
        gdf["topo"] = gpd.GeoSeries(gdf["topo"], crs=4326)
        gdf["link_id"] = db.next_etrago_id("link", len(gdf))
        gdf.to_postgis(
            name=targets.get_table_name("hydrogen_links"),
            con=engine,
            schema=targets.get_table_schema("hydrogen_links"),
            if_exists="append",
            index=False,
            dtype={"geom": Geometry(), "topo": Geometry()},
        )
    if not links:
        return pd.Series(dtype=float)
    return pd.DataFrame(links).groupby("bus1")["entry"].sum()


def remove_isolated_h2_buses_abroad(scn_name, targets):
    """
    Delete the H2 buses abroad without a cross-border point of the NEP

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    targets : DatasetTargets
        Targets of HydrogenGridEtrago

    Returns
    -------
    None

    """
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["buses"]}
        WHERE carrier = 'H2'
        AND scn_name = '{scn_name}'
        AND country != 'DE'
        AND bus_id NOT IN
            (SELECT bus0 FROM {targets.tables["hydrogen_links"]}
            WHERE scn_name = '{scn_name}')
        AND bus_id NOT IN
            (SELECT bus1 FROM {targets.tables["hydrogen_links"]}
            WHERE scn_name = '{scn_name}');
        """
    )


def insert_h2_imports(scn_name, border_entry, sources, targets):
    """
    Insert the hydrogen imports at the entry points of the NEP.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    border_entry : pandas.Series
        Entry capacity [MW] per H2 bus abroad
    sources : DatasetSources
        Sources of HydrogenGridEtrago
    targets : DatasetTargets
        Targets of HydrogenGridEtrago

    Returns
    -------
    None
    """
    # Also the generators of buses rebuilt since the last run
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["generators"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'H2'
        AND (bus NOT IN (
            SELECT bus_id FROM {sources.tables["buses"]}
            WHERE scn_name = '{scn_name}'
        ) OR bus IN (
            SELECT bus_id FROM {sources.tables["buses"]}
            WHERE scn_name = '{scn_name}' AND carrier IN ('H2', 'H2_grid')
        ))
        """
    )

    imports = [
        {"bus": bus, "p_nom": p_nom}
        for bus, p_nom in border_entry.items()
        if p_nom > 0
    ]

    german_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'H2_grid'
        AND country = 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    nodes = read_h2_grid_nodes()
    location = {
        name: (x, y) for name, x, y in zip(nodes.Ort, nodes.x, nodes.y)
    }
    german_metric = _to_metric(german_buses)
    for terminal, node, entry_2037, entry_2045 in nep_h2_lng_terminals():
        # GWh/h (Brennwert) -> MW (lower heating value)
        entry = (
            (entry_2045 if scn_name == "reGon2045" else entry_2037)
            * 1000
            / NEP_H2_HHV_PER_LHV
        )
        if entry == 0:
            continue
        point = _to_metric(
            gpd.GeoSeries([Point(location[node])], crs=4326)
        ).iloc[0]
        distance = german_metric.distance(point)
        if german_buses.empty or distance.min() > H2_NODE_SNAP_KM * 1000:
            continue
        imports.append(
            {"bus": distance.idxmin(), "p_nom": entry}
        )

    # Other imports (LH2 and derivatives) at the sites of the market survey
    if not german_buses.empty:
        nep_year = get_scenario_year(scn_name)
        # States without a site (Baden-Württemberg, 0.1 MW) are left out
        projects = pd.Series(nep_h2_other_import_projects()[nep_year])
        sites_of = nep_h2_other_import_sites()
        projects = projects[projects.index.isin(sites_of)]
        other = (
            nep_h2_other_imports()[nep_year]
            * 1000
            / NEP_H2_HHV_PER_LHV
            * projects
            / projects.sum()
        )
        for state, p_nom in other.items():
            sites = sites_of[state]
            for site in sites:
                point = _to_metric(
                    gpd.GeoSeries([Point(location[site])], crs=4326)
                ).iloc[0]
                imports.append(
                    {
                        "bus": german_metric.distance(point).idxmin(),
                        "p_nom": p_nom / len(sites),
                    }
                )

    if not imports:
        return None

    scn_params = get_sector_parameters("gas", scn_name)
    marginal_cost = scn_params["marginal_cost"]["H2_import"]
    generators = pd.DataFrame(imports)
    generators = generators.groupby("bus", as_index=False)["p_nom"].sum()
    generators["scn_name"] = scn_name
    generators["carrier"] = "H2"
    generators["p_nom_extendable"] = False
    generators["marginal_cost"] = marginal_cost
    # Imports over the year, shared by the entry capacity
    if "max_H2_import_overtheyear" in scn_params:
        generators["e_nom_max"] = (
            scn_params["max_H2_import_overtheyear"]
            * generators["p_nom"]
            / generators["p_nom"].sum()
        )
    generators["generator_id"] = db.next_etrago_id(
        "generator", len(generators)
    )
    generators.to_sql(
        targets.get_table_name("generators"),
        db.engine(),
        schema=targets.get_table_schema("generators"),
        if_exists="append",
        index=False,
    )
    return None
