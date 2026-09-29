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
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree
from shapely import wkb
from shapely.geometry import LineString, MultiLineString, Point
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.hydrogen_etrago.nep2025 import (
    DROPPED_KERNNETZ_MEASURES,
    KERNNETZ_COMMISSIONING_YEAR,
    NEP_CH4_NETWORK_2045,
    NEP_CH4_NETWORK_2045_NODES,
    NEP_CH4_NETWORK_YEAR,
    NEP_H2_BORDER_POINTS,
    NEP_H2_HHV_PER_LHV,
    NEP_H2_LNG_TERMINALS,
    NEP_H2_MEASURES_2037,
    NEP_H2_MEASURES_2037_COMMISSIONING,
    NEP_H2_MEASURES_2037_NODES,
    NEP_H2_MEASURES_CRITERIA,
    NEP_H2_MEASURES_YEAR,
    NEP_H2_NETWORK_2045,
    NEP_H2_NETWORK_2045_NODES,
    NEP_H2_NETWORK_YEAR,
    NEP_H2_OTHER_IMPORT_PROJECTS,
    NEP_H2_OTHER_IMPORT_SITES,
    NEP_H2_OTHER_IMPORTS,
    NEP_H2_STORAGE,
    NEP_H2_STORAGE_DUNKELFLAUTE,
    NEP_H2_STORAGE_DUNKELFLAUTE_STATES,
    NEP_H2_STORAGE_PROJECTS,
    NEP_KERNNETZ_DN_2045,
    NEP_KERNNETZ_NOT_IN_2045,
    NEP_SCENARIO_2045,
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
        # The list of the pipes converted from CH4 to H2
        is_conversion = sheet == "Umstellung"

        df = select_h2_pipelines(df, sheet, scn_name, h2_bus_location)

        # Corridors of the converted pipes, before pipes are split
        if is_conversion:
            conversions = conversion_corridors(df, h2_bus_location)

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

        # Report the pipelines that can not be georeferenced
        unmatched = df_merged[
            df_merged["geom_start"].isna() | df_merged["geom_end"].isna()
        ]
        unmatched = unmatched[
            ~unmatched["Anfangspunkt\n(Ort)"].isin(["nan", "---"])
            & ~unmatched["Endpunkt\n(Ort)"].isin(["nan", "---"])
        ]
        if not unmatched.empty:
            print(
                f"{scn_name}: {len(unmatched)} pipelines of the FNB-Gas list "
                "are not inserted, because an endpoint is not a node of the "
                "H2 grid: "
                + "; ".join(
                    f"{row['Anfangspunkt' + chr(10) + '(Ort)']} -> "
                    f"{row['Endpunkt' + chr(10) + '(Ort)']} "
                    f"({row['Länge ' + chr(10) + '(km)']} km)"
                    for _, row in unmatched.iterrows()
                )
            )

        H2_grid_df = df_merged.dropna(subset=["geom_start", "geom_end"])

        # Pipelines starting and ending at the same node can not be placed
        # (57 km, mostly Gasnetz Hamburg), see the documentation.
        same_node = H2_grid_df["geom_start"] == H2_grid_df["geom_end"]
        if same_node.any():
            dropped = H2_grid_df[same_node]
            print(
                f"{scn_name}: {same_node.sum()} pipelines "
                f"({pd.to_numeric(dropped['Länge ' + chr(10) + '(km)'], errors='coerce').sum():.1f} km) "
                "start and end at the same node and are not inserted: "
                + ", ".join(sorted(set(dropped["Anfangspunkt_matched"])))
            )
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

        # Remove the CH4 pipelines converted to H2 (from 2045: reduce to the
        # NEP methane network instead)
        if (
            is_conversion
            and get_scenario_year(scn_name) < NEP_CH4_NETWORK_YEAR
        ):
            remove_ch4_pipes_in_conversion_corridors(
                scn_name, conversions, sources, targets
            )

    # Keep only the methane network of the NEP
    if get_scenario_year(scn_name) >= NEP_CH4_NETWORK_YEAR:
        reduce_ch4_network_to_nep(scn_name, sources, targets)

    # Sections of the hydrogen network of the NEP beyond the core network
    # (measures up to 2037, additional sections 2045)
    insert_nep_h2_sections(scn_name, sources, targets)

    # connect saltcaverns to H2_grid
    connect_saltcavern_to_h2_grid(scn_name)

    # connect neighbour countries to H2_grid
    border_entry = connect_h2_grid_to_neighbour_countries(scn_name)

    # supply of the hydrogen imports at the entry points of the NEP
    insert_h2_imports(scn_name, border_entry, sources, targets)

    # connect the parts of the core network that no pipeline of the lists
    # connects to the rest
    connect_h2_grid_islands(scn_name, sources, targets)


# Nodes of the Kernnetz lists missing in h2_grid_nodes.csv: name, x, y
# (coordinates of the municipality, assumption)
MISSING_H2_GRID_NODES = {
    # Rastede - Wiefelstede (Neubau, 8.3 km)
    "Wiefelstede": (8.1167, 53.2500),
    # Glehn - Niederaußem (Umstellung, 13.4 km), Bergheim-Niederaußem
    "Niederaußem": (6.6667, 50.9833),
    # Coesfeld - Kanal Kreuzung Nord (bei Amelsbüren) (Umstellung, 33.2 km)
    "Coesfeld": (7.1667, 51.9500),
}


def read_h2_grid_nodes():
    """
    Nodes of the H2 core network

    The nodes of h2_grid_nodes.csv of the data bundle plus
    :py:data:`MISSING_H2_GRID_NODES`.

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
    missing = pd.DataFrame(
        [
            {"Ort": name, "geom": Point(x, y).wkb_hex, "x": x, "y": y}
            for name, (x, y) in MISSING_H2_GRID_NODES.items()
            if name not in set(nodes["Ort"])
        ],
        columns=["Ort", "geom", "x", "y"],
    )
    return pd.concat([nodes, missing], ignore_index=True)


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

    # Measures dropped by the NEP and measures commissioned after the
    # scenario year (year of the NEP where known) are not inserted
    df = df[~df["Antrags-ID"].isin(DROPPED_KERNNETZ_MEASURES)]
    # The network 2045 builds on the modelling result 2037 of its scenario,
    # which drops or re-dimensions some measures
    if get_scenario_year(scn_name) >= NEP_H2_NETWORK_YEAR:
        df = df[~df["Antrags-ID"].isin(NEP_KERNNETZ_NOT_IN_2045)]
        for measure, dn in NEP_KERNNETZ_DN_2045.items():
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


# Parameters of the flagging of the CH4 pipelines that are converted to
# H2, see :py:func:`flag_ch4_pipes_per_conversion`
CORRIDOR_BUFFER_KM = 8
CORRIDOR_MIN_SHARE = 0.3
CORRIDOR_MAX_LENGTH_RATIO = 1.3


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
    return df["Antrags-ID"].map(KERNNETZ_COMMISSIONING_YEAR).fillna(listed)


def _to_metric(geometries):
    geometries = gpd.GeoSeries(geometries.geometry)
    if geometries.crs is None:
        raise ValueError("The geometries need a crs.")
    return (
        geometries.to_crs(32632)
        if geometries.crs.is_geographic
        else (geometries)
    )


def diameter_class(diameter):
    """
    Class (A to G) of a pipeline diameter in mm.

    The classes are the ones of
    :py:func:`define_gas_pipeline_list <egon.data.datasets.gas_grid.define_gas_pipeline_list>`.
    """
    if pd.isna(diameter):
        return None
    for limit, pipe_class in (
        (1000, "A"),
        (700, "B"),
        (500, "C"),
        (350, "D"),
        (200, "E"),
        (100, "F"),
    ):
        if diameter >= limit:
            return pipe_class
    return "G"


def conversion_corridors(df, h2_bus_location):
    """
    Corridors of the CH4 pipelines that are converted to H2.

    Parameters
    ----------
    df : pandas.DataFrame
        FNB-Gas list of the conversions with matched end points and the
        commissioning year in "Planerische Inbetriebnahme"
    h2_bus_location : pandas.DataFrame
        Coordinates (x, y) of the nodes of the H2 grid (column Ort)

    Returns
    -------
    geopandas.GeoDataFrame
        Straight line, length in km, DN and commissioning year of every
        converted pipeline
    """
    rows = df[
        df["Anfangspunkt_matched"].notna()
        & df["Endpunkt_matched"].notna()
        & (df["Anfangspunkt_matched"] != df["Endpunkt_matched"])
    ]
    xy = h2_bus_location.drop_duplicates("Ort").set_index("Ort")[["x", "y"]]
    diameter = (
        rows["Nenndurchmesser \n(DN)"].astype(str).str.extract(r"(\d{3,4})")[0]
    )
    return gpd.GeoDataFrame(
        {
            "length_km": pd.to_numeric(
                rows["Länge \n(km)"], errors="coerce"
            ).to_numpy(),
            "diameter": pd.to_numeric(diameter, errors="coerce").to_numpy(),
            "year": pd.to_numeric(
                rows["Planerische Inbetriebnahme"], errors="coerce"
            ).to_numpy(),
        },
        geometry=[
            LineString([tuple(xy.loc[a]), tuple(xy.loc[e])])
            for a, e in zip(
                rows["Anfangspunkt_matched"], rows["Endpunkt_matched"]
            )
        ],
        crs=4326,
    )


def flag_ch4_pipes_per_conversion(
    conversions,
    pipes,
    pipe_class,
    buffer_km=CORRIDOR_BUFFER_KM,
    min_share=CORRIDOR_MIN_SHARE,
    max_ratio=CORRIDOR_MAX_LENGTH_RATIO,
):
    """
    Flag the CH4 pipelines that are converted to H2, one conversion at a time.

    Parameters
    ----------
    conversions : geopandas.GeoDataFrame
        Converted pipelines, see :py:func:`conversion_corridors`
    pipes : geopandas.GeoDataFrame
        CH4 pipelines inside of Germany
    pipe_class : pandas.Series
        Diameter class of the CH4 pipelines (index of pipes)
    buffer_km, min_share, max_ratio : float, optional
        Corridor width, minimum share of a pipeline inside and maximum
        length ratio

    Returns
    -------
    flagged : set
        Index of the flagged CH4 pipelines
    matched_km : numpy.ndarray
        Length of the pipelines flagged for every conversion in km
    """
    corridors = _to_metric(conversions)
    metric_pipes = _to_metric(pipes)
    length_km = metric_pipes.length / 1000

    flagged, matched_km = set(), []
    for corridor, converted_km, diameter in zip(
        corridors, conversions["length_km"], conversions["diameter"]
    ):
        got = 0.0
        if converted_km > 0:
            zone = corridor.buffer(buffer_km * 1000)
            near = metric_pipes[metric_pipes.intersects(zone)]
            share = near.intersection(zone).length / near.length
            candidates = share[share >= min_share]
            same = pipe_class[candidates.index] == diameter_class(diameter)
            ordered = list(
                candidates[same].sort_values(ascending=False).index
            ) + list(candidates[~same].sort_values(ascending=False).index)
            for i in ordered:
                if got >= converted_km:
                    break
                if got > 0 and got + length_km[i] > max_ratio * converted_km:
                    continue
                flagged.add(i)
                got += length_km[i]
        matched_km.append(got)

    return flagged, np.array(matched_km)


def select_removable_ch4_pipes(links, countries, candidates):
    """
    Select the CH4 pipelines that can be removed without cutting off buses.

    Parameters
    ----------
    links : pandas.DataFrame
        All CH4 links of a scenario with the columns bus0 and bus1
    countries : pandas.Series
        Country of every CH4 bus (index: bus_id)
    candidates : list
        link_id of the pipelines to remove, in the order of the removal

    Returns
    -------
    removable : list
        link_id of the pipelines that can be removed
    kept : list
        link_id of the candidates that are kept
    """
    position = pd.Series(np.arange(len(countries)), index=countries.index)
    start = position[links["bus0"]].to_numpy()
    end = position[links["bus1"]].to_numpy()
    n_buses = len(countries)

    german = (countries == "DE").to_numpy()
    crossing = german[start] != german[end]

    def connectivity(active):
        """Buses connected to a border point and number of pipelines"""
        border = np.zeros(n_buses, dtype=bool)
        border[start[active & crossing & german[start]]] = True
        border[end[active & crossing & german[end]]] = True

        adjacency = coo_matrix(
            (np.ones(active.sum()), (start[active], end[active])),
            shape=(n_buses, n_buses),
        )
        labels = connected_components(adjacency, directed=False)[1]
        connected = np.isin(labels, np.unique(labels[border]))
        degree = np.bincount(
            np.concatenate([start[active], end[active]]), minlength=n_buses
        )
        return connected, degree

    row = pd.Series(np.arange(len(links)), index=links.index)
    active = np.ones(len(links), dtype=bool)
    connected_before, degree_before = connectivity(active)

    removable, kept = [], []
    for link_id in candidates:
        active[row[link_id]] = False
        connected, degree = connectivity(active)

        lost_connection = (german & connected_before & ~connected).any()
        lost_last_pipeline = (
            german & (degree_before > 0) & (degree == 0)
        ).any()

        if lost_connection or lost_last_pipeline:
            active[row[link_id]] = True
            kept.append(link_id)
        else:
            removable.append(link_id)

    return removable, kept


def remove_ch4_pipes_in_conversion_corridors(
    scn_name, conversions, sources, targets
):
    """
    Remove the CH4 pipelines that are converted to H2 by the scenario year.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    conversions : geopandas.GeoDataFrame
        Converted pipelines, see :py:func:`conversion_corridors`
    sources : DatasetSources
        Sources of HydrogenGridEtrago
    targets : DatasetTargets
        Targets of HydrogenGridEtrago

    Returns
    -------
    list or None
        link_id of the removed pipelines, None if there is nothing to remove
    """
    buses = db.select_dataframe(
        f"""
        SELECT bus_id, country FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
        """,
        index_col="bus_id",
    )
    links = db.select_geodataframe(
        f"""
        SELECT link_id, bus0, bus1, p_nom, geom FROM {sources.tables["links"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
        """,
        index_col="link_id",
        geom_col="geom",
        epsg=4326,
    )
    links = links[
        links["bus0"].isin(buses.index) & links["bus1"].isin(buses.index)
    ]

    # The pipelines inside of Germany. The links to the buses abroad are only
    # needed to know the border points.
    german = buses.index[buses["country"] == "DE"]
    pipes = links[links["bus0"].isin(german) & links["bus1"].isin(german)]

    # Conversions without a known year are always candidates
    scenario_year = get_scenario_year(scn_name)
    not_yet = (
        len(conversions)
        - (
            conversions["year"].isna() | (conversions["year"] <= scenario_year)
        ).sum()
    )
    conversions = conversions[
        conversions["year"].isna() | (conversions["year"] <= scenario_year)
    ]

    converted_km = conversions["length_km"].sum()
    if pipes.empty or not converted_km > 0:
        print(
            f"{scn_name}: no CH4 pipelines or no converted pipelines, no "
            "CH4 pipelines are removed."
        )
        return None

    # Diameter class of the CH4 pipelines from their capacity
    gas_sources, _ = load_sources_and_targets("GasNodesAndPipes")
    classification = pd.read_csv(
        gas_sources.files["pipeline_classification"]["path"],
        delimiter=",",
        usecols=["classification", "max_transport_capacity_Gwh/d"],
    )
    capacity = classification.set_index("classification")[
        "max_transport_capacity_Gwh/d"
    ] * (1000 / 24)
    pipe_class = pipes["p_nom"].apply(
        lambda p: next(
            (c for c, cap in capacity.items() if np.isclose(cap, p)), None
        )
    )

    # The pipelines of the NEP methane network 2045 are not candidates
    german_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'CH4' AND country = 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    methane_2045, _ = match_nep_ch4_network(
        pipes, german_buses, NEP_CH4_NETWORK_2045, NEP_CH4_NETWORK_2045_NODES
    )
    corridor_pipes = pipes.drop(index=list(methane_2045))

    flagged, matched_km = flag_ch4_pipes_per_conversion(
        conversions, corridor_pipes, pipe_class[corridor_pipes.index]
    )
    length_km = _to_metric(pipes).length / 1000

    # Remove the longest pipelines first
    candidates = length_km[list(flagged)].sort_values(ascending=False).index
    removable, kept = select_removable_ch4_pipes(
        links[["bus0", "bus1"]], buses["country"], list(candidates)
    )

    if removable:
        db.execute_sql(
            f"""
            DELETE FROM {targets.tables["hydrogen_links"]}
            WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
            AND link_id IN ({", ".join(str(int(i)) for i in removable)})
            """
        )

    flagged_km = length_km[candidates].sum()
    fit = np.abs(matched_km - conversions["length_km"].to_numpy()) <= (
        0.3 * conversions["length_km"].to_numpy()
    )
    print(
        f"{scn_name}: {len(candidates)} of {len(pipes)} CH4 pipelines "
        f"({flagged_km:.0f} of {length_km.sum():.0f} km) are flagged as "
        f"converted to H2 ({converted_km:.0f} km in {len(conversions)} "
        f"conversions of the FNB-Gas list, {fit.mean():.0%} of them matched "
        f"within 30 % of their length). {len(removable)} pipelines "
        f"({length_km[removable].sum():.0f} km) are removed. {len(kept)} "
        f"pipelines ({length_km[kept].sum():.0f} km) are kept, because "
        "their removal would cut off a bus. "
        f"{len(methane_2045)} pipelines "
        f"({length_km[list(methane_2045)].sum():.0f} km) of the methane "
        f"network {NEP_CH4_NETWORK_YEAR} of the NEP are not candidates. "
        f"{not_yet} conversions are not commissioned before {scenario_year} "
        "yet and are excluded for this scenario."
    )

    return removable


def match_nep_ch4_network(
    pipes, buses, sections, nodes, snap_km=15, max_ratio=2
):
    """
    Match the sections of the NEP methane network to CH4 pipelines.

    Each section is matched to the shortest path between candidate buses
    near its end points whose length fits the NEP length best.

    Parameters
    ----------
    pipes : geopandas.GeoDataFrame
        CH4 pipelines inside of Germany with the columns bus0, bus1 and
        p_nom (index: link_id)
    buses : geopandas.GeoDataFrame
        CH4 buses in Germany (index: bus_id)
    sections : list of tuple
        (number, start, end, length in km, DN)
    nodes : dict
        Coordinates (lon, lat) of the start and end points
    snap_km : float, optional
        Radius around the end points for candidate buses. The default is 15.
    max_ratio : float, optional
        Maximal ratio of the path length to the NEP length. The default
        is 2.

    Returns
    -------
    keep : set
        link_id of the matched pipelines
    report : pandas.DataFrame
        Distance of the end points, path length and match per section
    """
    pipes = pipes.copy()
    pipes["length_km"] = _to_metric(pipes).length / 1000

    # One edge per pair of buses: the pipeline with the largest capacity
    pipes["pair"] = [
        (min(a, b), max(a, b)) for a, b in zip(pipes["bus0"], pipes["bus1"])
    ]
    edges = pipes.sort_values("p_nom", ascending=False).drop_duplicates("pair")

    graph_buses = np.unique(edges[["bus0", "bus1"]].to_numpy())
    position = pd.Series(np.arange(len(graph_buses)), index=graph_buses)
    start = position[edges["bus0"]].to_numpy()
    end = position[edges["bus1"]].to_numpy()
    graph = coo_matrix(
        (edges["length_km"].to_numpy(), (start, end)),
        shape=(len(graph_buses), len(graph_buses)),
    ).tocsr()
    edge_of_pair = pd.Series(edges.index.to_numpy(), index=edges["pair"])

    # Candidate buses of the graph around every end point
    metric_buses = _to_metric(buses.loc[graph_buses])
    tree = cKDTree(np.column_stack([metric_buses.x, metric_buses.y]))
    points = _to_metric(
        gpd.GeoSeries(
            [Point(xy) for xy in nodes.values()],
            index=list(nodes.keys()),
            crs=4326,
        )
    )
    xy = np.column_stack([points.x, points.y])
    closest = tree.query(xy)[1]
    around = tree.query_ball_point(xy, snap_km * 1000)
    candidates = {
        name: sorted(set(around[k]) | {closest[k]})
        for k, name in enumerate(points.index)
    }

    def distance_km(name, i):
        offset = xy[points.index.get_loc(name)] - tree.data[i]
        return np.hypot(*offset) / 1000

    keep, rows = set(), []
    for number, name_start, name_end, length_km, _ in sections:
        # The pair of candidates whose path length fits the NEP length best
        starts, ends = candidates[name_start], candidates[name_end]
        lengths, predecessors = dijkstra(
            graph, directed=False, indices=starts, return_predecessors=True
        )
        best = None
        for k, s in enumerate(starts):
            for e in ends:
                if np.isinf(lengths[k, e]):
                    continue
                score = (
                    abs(lengths[k, e] - length_km)
                    + distance_km(name_start, s)
                    + distance_km(name_end, e)
                )
                if best is None or score < best[0]:
                    best = (score, k, s, e)

        found, links, s, e = False, [], closest[0], closest[0]
        if best is not None:
            _, k, s, e = best
            path_km = lengths[k, e]
            # Implausible paths (detours) are not kept
            found = path_km <= max_ratio * length_km + snap_km
            if found:
                i = e
                while i != s:
                    a_, b_ = graph_buses[predecessors[k, i]], graph_buses[i]
                    links.append(edge_of_pair[(min(a_, b_), max(a_, b_))])
                    i = predecessors[k, i]
        keep.update(links)
        rows.append(
            {
                "section": number,
                "name": f"{name_start}-{name_end}",
                "nep_km": length_km,
                "path_km": pipes.loc[links, "length_km"].sum(),
                "distance_start_km": (
                    distance_km(name_start, s) if best else np.nan
                ),
                "distance_end_km": (
                    distance_km(name_end, e) if best else np.nan
                ),
                "found": found,
            }
        )
    return keep, pd.DataFrame(rows).set_index("section")


def reduce_ch4_network_to_nep(scn_name, sources, targets):
    """
    Reduce the CH4 pipelines in Germany to the methane network of the NEP.

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
    list or None
        link_id of the removed pipelines, None if there is nothing to remove
    """
    buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {sources.tables["buses"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'CH4' AND country = 'DE'
        """,
        index_col="bus_id",
        epsg=4326,
    )
    links = db.select_geodataframe(
        f"""
        SELECT link_id, bus0, bus1, p_nom, geom FROM {sources.tables["links"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
        """,
        index_col="link_id",
        geom_col="geom",
        epsg=4326,
    )
    pipes = links[
        links["bus0"].isin(buses.index) & links["bus1"].isin(buses.index)
    ]
    if pipes.empty:
        print(f"{scn_name}: no CH4 pipelines, the network is not reduced.")
        return None

    keep, report = match_nep_ch4_network(
        pipes, buses, NEP_CH4_NETWORK_2045, NEP_CH4_NETWORK_2045_NODES
    )
    removable = [i for i in pipes.index if i not in keep]
    if removable:
        db.execute_sql(
            f"""
            DELETE FROM {targets.tables["hydrogen_links"]}
            WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
            AND link_id IN ({", ".join(str(int(i)) for i in removable)})
            """
        )

    # Sections far from the CH4 grid, without path or with an implausible
    # path length (the boundary of a test run cuts most sections)
    doubtful = report[
        ~report["found"]
        | (report[["distance_start_km", "distance_end_km"]].max(axis=1) > 20)
        | ~report["path_km"].between(
            0.5 * report["nep_km"], 2 * report["nep_km"]
        )
    ]
    length_km = _to_metric(pipes).length / 1000
    print(
        f"{scn_name}: CH4 network reduced to the NEP methane network "
        f"{NEP_CH4_NETWORK_YEAR} ({report['nep_km'].sum():.0f} km in "
        f"{len(report)} sections). {len(keep)} of {len(pipes)} pipelines "
        f"({length_km[list(keep)].sum():.0f} of {length_km.sum():.0f} km) "
        f"are kept, {len(removable)} are removed. Sections to check: "
        + (
            "; ".join(
                f"{i} {row['name']} (NEP {row['nep_km']:.0f} km, path "
                f"{row['path_km']:.0f} km, end points "
                f"{row['distance_start_km']:.0f}/"
                f"{row['distance_end_km']:.0f} km from the grid)"
                for i, row in doubtful.iterrows()
            )
            or "none"
        )
    )
    return removable


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
        Sections with the keys label, start, end, kind, length_km, dn, dp,
        build_year, start_xy and end_xy (lon, lat)
    """
    year = get_scenario_year(scn_name)
    sections = []
    sections += [
        {
            "label": nep_id,
            "start": start,
            "end": end,
            "kind": kind,
            "length_km": length_km,
            "dn": dn,
            "dp": dp,
            "build_year": NEP_H2_MEASURES_2037_COMMISSIONING.get(
                nep_id, NEP_H2_MEASURES_YEAR
            ),
            "start_xy": NEP_H2_MEASURES_2037_NODES[start],
            "end_xy": NEP_H2_MEASURES_2037_NODES[end],
        }
        for nep_id, start, end, kind, length_km, dn, dp, criterion in (
            NEP_H2_MEASURES_2037
        )
        if criterion in NEP_H2_MEASURES_CRITERIA
        and year
        >= NEP_H2_MEASURES_2037_COMMISSIONING.get(nep_id, NEP_H2_MEASURES_YEAR)
    ]
    if year >= NEP_H2_NETWORK_YEAR:
        sections += [
            {
                "label": f"A6-{number}",
                "start": start,
                "end": end,
                "kind": kind,
                "length_km": length_km,
                "dn": dn,
                "dp": dp,
                "build_year": NEP_H2_NETWORK_YEAR,
                "start_xy": NEP_H2_NETWORK_2045_NODES[start],
                "end_xy": NEP_H2_NETWORK_2045_NODES[end],
            }
            for number, start, end, kind, length_km, dn, dp, scenarios in (
                NEP_H2_NETWORK_2045
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
        print(f"{scn_name}: no H2_grid buses, no NEP sections inserted.")
        return None

    metric_buses = _to_metric(buses)
    tree = cKDTree(np.column_stack([metric_buses.x, metric_buses.y]))
    points = _section_points(sections)
    names = sorted(points)
    metric_points = gpd.GeoSeries(
        [Point(points[name]) for name in names], crs=4326
    ).to_crs(32632)
    distance, closest = tree.query(
        np.column_stack([metric_points.x, metric_points.y])
    )
    bus_of = dict(zip(names, buses.index[closest]))
    far = [
        name for name, d in zip(names, distance) if d > H2_NODE_SNAP_KM * 1000
    ]

    scn_params = get_sector_parameters("gas", scn_name)
    interest_rate = get_sector_parameters("global", scn_name)["interest_rate"]

    links, inside, implausible = [], [], []
    for section in sections:
        bus0, bus1 = bus_of[section["start"]], bus_of[section["end"]]
        name = f"{section['label']} {section['start']}-{section['end']}"
        if bus0 == bus1:
            inside.append(name)
            continue
        bus_km = metric_buses[bus0].distance(metric_buses[bus1]) / 1000
        if bus_km > 2 * section["length_km"] + H2_NODE_SNAP_KM:
            implausible.append(name)
            continue
        cost_key = (
            "H2_pipeline_retrofit"
            if section["kind"] == "conversion"
            and "H2_pipeline_retrofit" in scn_params["overnight_cost"]
            else "H2_pipeline"
        )
        p_nom = calculate_H2_capacity(section["dp"], section["dn"])
        lifetime = scn_params["lifetime"][cost_key]
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
                    scn_params["overnight_cost"][cost_key]
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

    for year in sorted({section["build_year"] for section in sections}):
        of_year = [s for s in sections if s["build_year"] == year]
        inserted = [link for link in links if link["build_year"] == year]
        print(
            f"{scn_name}: {len(inserted)} of {len(of_year)} NEP hydrogen "
            f"sections with commissioning {year} inserted "
            f"({sum(link['length'] for link in inserted):.0f} of "
            f"{sum(s['length_km'] for s in of_year):.0f} km, "
            f"{sum(link['p_nom'] for link in inserted) / 1000:.0f} GW)."
        )
    print(
        f"{scn_name}: NEP hydrogen sections inside a node: {len(inside)}"
        + (
            f"; not inserted, because the buses are too far apart for the "
            f"length: {'; '.join(implausible)}"
            if implausible
            else ""
        )
        + (
            f"; end points farther than {H2_NODE_SNAP_KM} km from an "
            f"H2_grid bus: {', '.join(far)}"
            if far
            else ""
        )
        + "."
    )
    return None


def download_h2_grid_data():
    """
    Download Input data for H2_grid from FNB-Gas (https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/)

    The following data for H2 are downloaded into the folder
    ./datasets/h2_data:
      * Links (Anlage 3: Neubau, Anlage 4: Umstellung, Anlage 2: weitere
        Leitungen), version of 2024-12-10

    Returns
    -------
    None

    """
    sources, _ = load_sources_and_targets("HydrogenGridEtrago")
    path = Path("datasets/h2_data")
    os.makedirs(path, exist_ok=True)

    target_file_Um = path / sources.files["converted_ch4_pipes"]
    target_file_Neu = path / sources.files["new_constructed_pipes"]
    target_file_Erw = (
        path / sources.files["pipes_of_further_h2_grid_operators"]
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
    path = Path(".") / "datasets" / "h2_data"

    excel_file_Um = pd.ExcelFile(path / sources.files["converted_ch4_pipes"])
    excel_file_Neu = pd.ExcelFile(
        path / sources.files["new_constructed_pipes"]
    )
    excel_file_Erw = pd.ExcelFile(
        path / sources.files["pipes_of_further_h2_grid_operators"]
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
    links = db.select_dataframe(
        f"""
        SELECT bus0, bus1, p_nom, build_year FROM {targets.tables["hydrogen_links"]}
        WHERE scn_name = '{scn_name}' AND carrier = 'H2_grid'
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
        print(
            f"{scn_name}: isolated part of the H2 core network with "
            f"{len(island)} buses connected by {length_km:.1f} km "
            f"({island_links.p_nom.max():.0f} MW) from bus {island_bus} "
            f"to bus {main_bus}"
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
    nep_year = (
        NEP_H2_NETWORK_YEAR
        if get_scenario_year(scn_name) >= NEP_H2_NETWORK_YEAR
        else 2037
    )
    withdrawal, injection = NEP_H2_STORAGE[nep_year]
    projects = pd.Series(NEP_H2_STORAGE_PROJECTS[nep_year], dtype=float)
    rates = withdrawal * projects / projects.sum()
    dunkelflaute = NEP_H2_STORAGE_DUNKELFLAUTE.get(nep_year, 0)
    if dunkelflaute:
        extra = pd.Series(
            dunkelflaute / len(NEP_H2_STORAGE_DUNKELFLAUTE_STATES),
            index=NEP_H2_STORAGE_DUNKELFLAUTE_STATES,
        )
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

    db.execute_sql(
        f"""
           DELETE FROM {targets.tables["hydrogen_links"]}
           WHERE "carrier" in ('H2_saltcavern')
           AND scn_name = '{scn_name}';
           """
    )
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
    placed = rates.index.isin(potential.survey_state)
    print(
        f"{scn_name}: H2 saltcavern links up to {p_nom_max.sum() / 1000:.1f}"
        f" GW withdrawal ({injection_share:.0%} for injection) of the NEP"
        f" {rates.sum() / 1000:.1f} GW"
        + (
            "; not in the area: "
            + ", ".join(
                f"{state} {rate / 1000:.1f} GW"
                for state, rate in rates[~placed].items()
            )
            if not placed.all()
            else ""
        )
        + "."
    )

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
        print(
            f"{scn_name}: no H2 buses in Germany or abroad, no border links."
        )
        return pd.Series(dtype=float)

    nodes = read_h2_grid_nodes()
    location = {
        **NEP_H2_NETWORK_2045_NODES,
        **{name: (x, y) for name, x, y in zip(nodes.Ort, nodes.x, nodes.y)},
    }
    german_metric = _to_metric(german_buses)
    abroad_metric = _to_metric(abroad_buses)

    scn_params = get_sector_parameters("gas", scn_name)
    lifetime = scn_params["lifetime"]["H2_pipeline"]
    overnight_cost = scn_params["overnight_cost"]["H2_pipeline"]
    interest_rate = get_sector_parameters("global", scn_name)["interest_rate"]
    year_2045 = get_scenario_year(scn_name) >= NEP_H2_NETWORK_YEAR

    links, missing = [], []
    for (
        name,
        country,
        share,
        node,
        entry_2037,
        exit_2037,
        entry_2045,
        exit_2045,
    ) in NEP_H2_BORDER_POINTS:
        entry, exit_ = (
            (entry_2045, exit_2045) if year_2045 else (entry_2037, exit_2037)
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
            missing.append(f"{name} ({country})")
            continue
        german_bus = distance.idxmin()

        candidates = abroad_metric[abroad_buses["country"] == country]
        if candidates.empty:
            missing.append(f"{name} ({country}, no bus abroad)")
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
        per_country = report.groupby("country")[["entry", "exit"]].sum()
        print(
            f"{scn_name}: {len(links)} H2 border links, entry "
            f"{report['entry'].sum() / 1000:.1f} GW, exit "
            f"{report['exit'].sum() / 1000:.1f} GW ("
            + ", ".join(
                f"{c} {row['entry'] / 1000:.1f}/{row['exit'] / 1000:.1f}"
                for c, row in per_country.iterrows()
            )
            + ")."
        )
    if missing:
        print(
            f"{scn_name}: H2 border points with capacity but without H2_grid "
            f"bus at the border node: {', '.join(missing)}."
        )
    if not links:
        return pd.Series(dtype=float)
    return pd.DataFrame(links).groupby("bus1")["entry"].sum()


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
        {"bus": bus, "p_nom": p_nom, "point": "abroad"}
        for bus, p_nom in border_entry.items()
        if p_nom > 0
    ]

    year_2045 = get_scenario_year(scn_name) >= NEP_H2_NETWORK_YEAR
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
    missing = []
    for terminal, node, entry_2037, entry_2045 in NEP_H2_LNG_TERMINALS:
        # GWh/h (Brennwert) -> MW (lower heating value)
        entry = (
            (entry_2045 if year_2045 else entry_2037)
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
            missing.append(terminal)
            continue
        imports.append(
            {"bus": distance.idxmin(), "p_nom": entry, "point": terminal}
        )

    # Other imports (LH2 and derivatives) at the sites of the market survey
    if not german_buses.empty:
        nep_year = NEP_H2_NETWORK_YEAR if year_2045 else 2037
        projects = pd.Series(NEP_H2_OTHER_IMPORT_PROJECTS[nep_year])
        other = (
            NEP_H2_OTHER_IMPORTS[nep_year]
            * 1000
            / NEP_H2_HHV_PER_LHV
            * projects
            / projects.sum()
        )
        for state, p_nom in other.items():
            sites = NEP_H2_OTHER_IMPORT_SITES[state]
            for site, (x, y) in sites.items():
                point = _to_metric(
                    gpd.GeoSeries([Point(x, y)], crs=4326)
                ).iloc[0]
                imports.append(
                    {
                        "bus": german_metric.distance(point).idxmin(),
                        "p_nom": p_nom / len(sites),
                        "point": f"other: {site}",
                    }
                )

    if not imports:
        return None

    marginal_cost = get_sector_parameters("gas", scn_name)["marginal_cost"][
        "H2_import"
    ]
    generators = pd.DataFrame(imports)
    report = generators.groupby("point")["p_nom"].sum()
    generators = generators.groupby("bus", as_index=False)["p_nom"].sum()
    generators["scn_name"] = scn_name
    generators["carrier"] = "H2"
    generators["p_nom_extendable"] = False
    generators["marginal_cost"] = marginal_cost
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
    other = report.index.str.startswith("other: ")
    print(
        f"{scn_name}: H2 imports {generators['p_nom'].sum() / 1000:.1f} GW at "
        f"{marginal_cost:.1f} EUR/MWh (abroad "
        f"{report.get('abroad', 0) / 1000:.1f} GW, LNG terminals "
        f"{report[~other].drop('abroad', errors='ignore').sum() / 1000:.1f} "
        f"GW, other imports {report[other].sum() / 1000:.1f} GW)"
        + (
            f"; LNG terminals without H2_grid bus: {', '.join(missing)}"
            if missing
            else ""
        )
        + "."
    )
    return None
