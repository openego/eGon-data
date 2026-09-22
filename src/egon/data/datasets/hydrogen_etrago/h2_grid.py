"""
The central module containing all code dealing with the H2 grid.

"""

from itertools import count
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
from shapely.ops import unary_union
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.scenario_parameters import get_sector_parameters
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
    h2_bus_location = pd.read_csv(
        Path(".")
        / "data_bundle_egon_data"
        / "hydrogen_network"
        / "h2_grid_nodes.csv"
    )
    con = db.engine()

    target = targets.tables["hydrogen_links"]

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

    for df in [H2_grid_Neubau, H2_grid_Umstellung, H2_grid_Erweiterung]:
        # The list of the pipes converted from CH4 to H2
        is_conversion = df is H2_grid_Umstellung

        if df is H2_grid_Neubau:
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

        if df is H2_grid_Erweiterung:
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

        # Length of the converted pipes that are georeferenced
        if is_conversion:
            georeferenced = (
                df["Anfangspunkt_matched"].notna()
                & df["Endpunkt_matched"].notna()
                & (df["Anfangspunkt_matched"] != df["Endpunkt_matched"])
            )
            converted_km = float(
                pd.to_numeric(
                    df.loc[georeferenced, "Länge \n(km)"], errors="coerce"
                ).sum()
            )

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
        H2_grid_df = H2_grid_df[
            H2_grid_df["geom_start"] != H2_grid_df["geom_end"]
        ]
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
                0.05,
            ),
            axis=1,
        )
        H2_grid_gdf["p_min_pu"] = -1

        selected_columns = [
            "scn_name",
            "link_id",
            "bus0",
            "bus1",
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

        # Remove the CH4 pipelines that are converted to H2
        if is_conversion:
            remove_ch4_pipes_in_conversion_corridors(
                scn_name,
                gpd.GeoSeries(H2_grid_df["topo"].tolist(), crs=4326),
                converted_km,
                sources,
                targets,
            )

    # connect saltcaverns to H2_grid
    connect_saltcavern_to_h2_grid(scn_name)

    # connect neighbour countries to H2_grid
    connect_h2_grid_to_neighbour_countries(scn_name)


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

        # Add new rows for the split pipelines
        new_rows = pd.DataFrame(
            {
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


def calculate_H2_capacity(pressure, diameter):
    """
    Method for calculagting capacity of pipelines based on data input from FNB Gas

    Parameters
    ----------
    pressure : float
        input for pressure of pipeline
    diameter: float
        input for diameter of pipeline
    column_to_match: str
        matching column
    treshhold: float
        matching percentage for succesfull comparison

    Returns
    ---------
    energy_flow: float
        transmission capacity of pipeline

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

    velocity = 40  # source: L.Koops (2023): GAS PIPELINE VERSUS LIQUID HYDROGEN TRANSPORT – PERSPECTIVES FOR TECHNOLOGIES, ENERGY DEMAND ANDv TRANSPORT CAPACITY, AND IMPLICATIONS FOR AVIATION
    temperature = (
        10 + 273.15
    )  # source: L.Koops (2023): GAS PIPELINE VERSUS LIQUID HYDROGEN TRANSPORT – PERSPECTIVES FOR TECHNOLOGIES, ENERGY DEMAND ANDv TRANSPORT CAPACITY, AND IMPLICATIONS FOR AVIATION
    density = (
        pressure * 10**5 / (4.1243 * 10**3 * temperature)
    )  # gasconstant H2 = 4.1243 [kJ/kgK]
    mass_flow = density * math.pi * ((diameter / 10**3) / 2) ** 2 * velocity
    energy_flow = mass_flow * 119.988  # low_heating_value H2 = 119.988 [MJ/kg]

    return energy_flow


#: Buffer (km) and minimal share of the length inside the buffer, used to
#: flag the CH4 pipelines in the corridors of the converted pipelines
CORRIDOR_BUFFER_KM = 5
CORRIDOR_SHARE = 0.8


def _to_metric(geometries):
    geometries = gpd.GeoSeries(geometries.geometry)
    if geometries.crs is None:
        raise ValueError("The geometries need a crs.")
    return (
        geometries.to_crs(32632)
        if geometries.crs.is_geographic
        else (geometries)
    )


def _share_inside_corridors(corridors, pipes, buffer_km):
    zone = unary_union(list(corridors.buffer(buffer_km * 1000)))
    return pipes.apply(
        lambda geom: (
            geom.intersection(zone).length / geom.length
            if geom.length > 0
            else 0.0
        )
    )


def flag_ch4_pipes_in_corridor(corridors, ch4_pipes, buffer_km, share):
    """
    Flag the CH4 pipelines that lie in the corridors of converted pipelines.

    A CH4 pipeline is flagged if at least the given share of its length is
    inside the buffer around the corridors. The corridors are the straight
    lines between the end points of the CH4 pipelines that are converted to
    H2 (see :py:func:`select_corridor_parameters` for the parameters).

    Parameters
    ----------
    corridors : geopandas.GeoSeries
        Straight lines between the end points of the converted pipelines
    ch4_pipes : geopandas.GeoDataFrame or geopandas.GeoSeries
        CH4 pipelines
    buffer_km : float
        Distance around the corridors in km
    share : float
        Minimal share (0 to 1) of the length of a pipeline inside the buffer

    Returns
    -------
    pandas.Series
        True for the flagged pipelines, with the index of ch4_pipes
    """
    corridors = _to_metric(corridors)
    pipes = _to_metric(ch4_pipes)

    return _share_inside_corridors(corridors, pipes, buffer_km) >= share


def select_corridor_parameters(
    corridors,
    ch4_pipes,
    target_km,
    buffers_km=range(1, 11),
    shares=(0.6, 0.7, 0.8, 0.9),
    tolerance=0.1,
    warn=True,
):
    """
    Select buffer and share to flag the CH4 pipelines that are converted.

    Offline calibration helper, not called from the pipeline: it produced
    the :data:`CORRIDOR_BUFFER_KM` / :data:`CORRIDOR_SHARE` constants used
    at runtime by :py:func:`remove_ch4_pipes_in_conversion_corridors`. Rerun
    it by hand (e.g. in a notebook/REPL) and update those constants if the
    FNB-Gas list or the converted length changes materially.

    The FNB-Gas list of the conversion (Anlage 4) has no identifier that
    matches the CH4 pipelines. Therefore the CH4 pipelines inside the
    corridors of the converted pipelines are flagged (see
    :py:func:`flag_ch4_pipes_in_corridor`). Buffer and share are selected
    automatically, so that the total length of the flagged pipelines is
    as close as possible to the length of the converted pipelines. If
    several pairs are equally close, the largest share and then the
    smallest buffer are preferred.

    A single total does not fix two parameters. Therefore the table with all
    pairs is returned as well, to check how sensitive the result is.

    Parameters
    ----------
    corridors : geopandas.GeoSeries
        Straight lines between the end points of the converted pipelines
    ch4_pipes : geopandas.GeoDataFrame or geopandas.GeoSeries
        CH4 pipelines
    target_km : float
        Length of the converted pipelines in km. Use the length of the list
        before pipelines are split (:py:func:`replace_pipeline` repeats the
        full length for both parts).
    buffers_km : iterable of float, optional
        Buffers to test in km. The default is 1 to 10 km.
    shares : iterable of float, optional
        Shares to test. The default is 0.6 to 0.9.
    tolerance : float, optional
        Relative deviation from the target above which a warning is printed.
        The default is 0.1.
    warn : bool, optional
        Whether to print the warning. The default is True.

    Returns
    -------
    best : dict
        buffer_km, share, flagged_km and the relative deviation from the target
    table : pandas.DataFrame
        The same values for all tested pairs
    """
    corridors = _to_metric(corridors)
    pipes = _to_metric(ch4_pipes)

    if target_km <= 0 or corridors.empty or pipes.empty:
        raise ValueError(
            "A target length, corridors and CH4 pipelines are needed."
        )

    # Only pipelines close to a corridor can be flagged
    near = pipes.distance(unary_union(list(corridors))) <= (
        max(buffers_km) * 1000
    )
    pipes = pipes[near]
    length_km = pipes.length / 1000

    rows = []
    for buffer_km in buffers_km:
        share_inside = _share_inside_corridors(corridors, pipes, buffer_km)
        for share in shares:
            flagged_km = length_km[share_inside >= share].sum()
            rows.append(
                {
                    "buffer_km": buffer_km,
                    "share": share,
                    "flagged_km": flagged_km,
                    "deviation": abs(flagged_km - target_km) / target_km,
                }
            )
    table = pd.DataFrame(rows)

    best = (
        table.assign(deviation_rounded=table["deviation"].round(3))
        .sort_values(
            ["deviation_rounded", "share", "buffer_km"],
            ascending=[True, False, True],
        )
        .iloc[0]
        .drop("deviation_rounded")
    )

    if warn and best["deviation"] > tolerance:
        print(
            f"Warning: the flagged length ({best['flagged_km']:.0f} km) "
            f"deviates by {best['deviation']:.0%} from the converted length "
            f"({target_km:.0f} km), even for the best pair."
        )

    return best.to_dict(), table


def select_removable_ch4_pipes(links, countries, candidates):
    """
    Select the CH4 pipelines that can be removed without cutting off buses.

    The candidates are removed one after the other, in the given order. A
    candidate is only removed if afterwards no German bus has lost its
    connection to a border point (a German bus with a link to a bus abroad)
    or its last pipeline. Otherwise it is kept.

    Parameters
    ----------
    links : pandas.DataFrame
        All CH4 links of a scenario with the columns bus0 and bus1
        (index: link_id), including the links to buses abroad
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
    scn_name, corridors, converted_km, sources, targets
):
    """
    Remove the CH4 pipelines that are converted to H2.

    The FNB-Gas list has no identifier of the CH4 pipelines. Therefore the
    CH4 pipelines in the corridors of the converted pipelines are flagged,
    with the buffer :data:`CORRIDOR_BUFFER_KM` and share
    :data:`CORRIDOR_SHARE` (calibrated offline with
    :py:func:`select_corridor_parameters` against the December 2024 FNB-Gas
    lists, see the module docstring / dev notes for that calibration). The
    flagged pipelines are removed with :py:func:`select_removable_ch4_pipes`,
    i.e. only as far as no bus is cut off from the border points or loses its
    last pipeline. The other flagged pipelines are kept. Only the links are
    removed, the CH4 buses are not changed.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    corridors : geopandas.GeoSeries
        Straight lines between the end points of the converted pipelines
    converted_km : float
        Length of the georeferenced converted pipelines in km
    sources : DatasetSources
        Sources of HydrogenGridEtrago with the tables of the buses and links
    targets : DatasetTargets
        Targets of HydrogenGridEtrago with the table of the links

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
        SELECT link_id, bus0, bus1, geom FROM {sources.tables["links"]}
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

    if pipes.empty or converted_km <= 0:
        print(
            f"{scn_name}: no CH4 pipelines or no converted pipelines, no "
            "CH4 pipelines are removed."
        )
        return None

    flagged = flag_ch4_pipes_in_corridor(
        corridors, pipes, CORRIDOR_BUFFER_KM, CORRIDOR_SHARE
    )
    length_km = _to_metric(pipes).length / 1000

    # Remove the longest pipelines first
    candidates = length_km[flagged].sort_values(ascending=False).index
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
    deviation = abs(flagged_km - converted_km) / converted_km
    print(
        f"{scn_name}: {len(candidates)} of {len(pipes)} CH4 pipelines "
        f"({flagged_km:.0f} of {length_km.sum():.0f} km) lie in the "
        f"corridors (buffer {CORRIDOR_BUFFER_KM:g} km, share "
        f"{CORRIDOR_SHARE:.0%}) of the pipelines converted to H2 "
        f"({converted_km:.0f} km in the FNB-Gas list, {deviation:.0%} "
        f"deviation). {len(removable)} pipelines "
        f"({length_km[removable].sum():.0f} km) are removed. {len(kept)} "
        f"pipelines ({length_km[kept].sum():.0f} km) are kept, because "
        "their removal would cut off a bus."
    )

    return removable


def download_h2_grid_data():
    """
    Download Input data for H2_grid from FNB-Gas (https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/)

    The following data for H2 are downloaded into the folder
    ./datasets/h2_data (the file names are defined in the sources of
    :py:class:`HydrogenGridEtrago <egon.data.datasets.hydrogen_etrago.HydrogenGridEtrago>`):
      * Links (Anlage 3: new construction, Anlage 4: conversion of CH4
        pipelines, Anlage 2: pipelines of further operators). The version of
        2024-12-10 is used, which is the revision according to the approval
        of the core network of 2024-10-22 (9_040 km).

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
    df = replace_pipeline(
        df, "Hemmingstedt", "Ascheberg (Holstein)", "Remmels Nord"
    )
    df = replace_pipeline(df, "Heidenau", "Elbe-Süd", "Weißenfelde")
    df = replace_pipeline(df, "Weißenfelde", "Elbe-Süd", "Stade")
    df = replace_pipeline(df, "Stade AOS", "KW Schilling", "Abzweig Stade")
    df = replace_pipeline(df, "Rosengarten (Sottorf)", "Moorburg", "Leversen")
    df = replace_pipeline(df, "Leversen", "Moorburg", "Hamburg Süd")
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
    df = replace_pipeline(df, "Sixdorf", "Wiederitzsch", "Cörmigk")
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
            "p_min_pu": -1,
            "capital_cost": scn_params["capital_cost"]["H2_pipeline"]
            * dist
            / 1000,
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
    Connect germand H2_grid with neighbour countries. All german H2-Buses wich were planned as connection
    points for Import/Export of Hydrogen to corresponding neighbours country, are based on Publication
    of FNB-GAS (https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/).

    Returns
    -------
    None

    """
    sources, targets = load_sources_and_targets("HydrogenGridEtrago")

    engine = db.engine()

    h2_buses_df = gpd.read_postgis(
        f"""
    SELECT bus_id, x, y, geom
    FROM {sources.tables["buses"]}
    WHERE carrier in ('H2_grid')
    AND scn_name = '{scn_name}'

    """,
        engine,
    )

    h2_links_df = pd.read_sql(
        f"""
    SELECT link_id, bus0, bus1, p_nom
    FROM {sources.tables["links"]}
    WHERE carrier in ('H2_grid')
    AND scn_name = '{scn_name}'

    """,
        engine,
    )

    abroad_buses_df = gpd.read_postgis(
        f"""
        SELECT bus_id, x, y, geom, country
        FROM {sources.tables["buses"]}
        WHERE carrier = 'H2' AND scn_name = '{scn_name}' AND country != 'DE'
        """,
        engine,
    )

    abroad_con_buses = [
        ("Greifenhagen", "PL"),
        ("Fürstenberg (PL)", "PL"),
        ("Eynatten", "BE"),
        ("Überackern", "AT"),
        ("Vlieghuis", "NL"),
        ("Oude", "NL"),
        ("Oude Statenzijl", "NL"),
        ("Vreden", "NL"),
        ("Elten", "NL"),
        ("Leidingen", "FR"),
        ("Carling", "FR"),
        ("Medelsheim", "FR"),
        ("Waidhaus", "CZ"),
        ("Deutschneudorf", "CZ"),
        ("Grenzach", "CH"),
        ("AWZ", "DK"),
        ("AWZ", "SE"),
        ("AQD Offshore SEN 1", "GB"),
        ("AQD Offshore SEN 1", "NO"),
        ("AQD Offshore SEN 1", "DK"),
        ("AQD Offshore SEN 1", "NL"),
        ("Fessenheim", "FR"),
        ("Ellund", "DK"),
    ]

    h2_bus_location = pd.read_csv(
        Path(".")
        / "data_bundle_egon_data"
        / "hydrogen_network"
        / "h2_grid_nodes.csv"
    )

    ### prepare data for connecting abroad_buses
    matched_locations = h2_bus_location[
        h2_bus_location["Ort"].isin([name for name, _ in abroad_con_buses])
    ]
    matched_buses = matched_locations.merge(
        h2_buses_df, left_on=["x", "y"], right_on=["x", "y"], how="inner"
    )

    final_matched_buses = matched_buses[
        ["bus_id", "Ort", "x", "y", "geom_y"]
    ].rename(columns={"geom_y": "geom"})

    abroad_links = h2_links_df[
        (h2_links_df["bus0"].isin(final_matched_buses["bus_id"]))
        | (h2_links_df["bus1"].isin(final_matched_buses["bus_id"]))
    ]
    abroad_links_bus0 = abroad_links.merge(
        final_matched_buses, left_on="bus0", right_on="bus_id", how="inner"
    )
    abroad_links_bus1 = abroad_links.merge(
        final_matched_buses, left_on="bus1", right_on="bus_id", how="inner"
    )
    abroad_con_df = pd.concat([abroad_links_bus1, abroad_links_bus0])

    scn_params = get_sector_parameters("gas", scn_name)
    lifetime = scn_params["lifetime"]["H2_pipeline"]
    overnight_cost = scn_params["overnight_cost"]["H2_pipeline"]

    abroad_con_df["geom_metric"] = gpd.GeoSeries(
        abroad_con_df["geom"].tolist(),
        index=abroad_con_df.index,
        crs=4326,
    ).to_crs(epsg=32632)
    abroad_buses_metric = gpd.GeoSeries(
        abroad_buses_df["geom"].tolist(),
        index=abroad_buses_df.index,
        crs=4326,
    ).to_crs(epsg=32632)

    connection_links = []

    for inland_name, country_code in abroad_con_buses:
        # filter out germand h2_buses for connecting neighbour-countries
        inland_bus = abroad_con_df[abroad_con_df["Ort"] == inland_name]
        if inland_bus.empty:
            print(f"Warning: No Inland-Bus found for {inland_name}.")
            continue

        # filter out corresponding abroad_bus for connecting neighbour countries
        abroad_bus = abroad_buses_df[
            abroad_buses_df["country"] == country_code
        ]
        if abroad_bus.empty:
            print(f"Warning: No Abroad-Bus found for {country_code}.")
            continue
        abroad_bus_metric = abroad_buses_metric.loc[abroad_bus.index]

        for _, i_bus in inland_bus.iterrows():
            distance_km = (
                abroad_bus_metric.distance(i_bus["geom_metric"]) / 1000
            )

            nearest_abroad_bus = abroad_bus.loc[distance_km.idxmin()]
            nearest_distance_km = distance_km.min()
            relevant_buses = inland_bus[
                inland_bus["bus_id"] == i_bus["bus_id"]
            ]
            p_nom_value = relevant_buses["p_nom"].sum()

        connection_links.append(
            {
                "scn_name": scn_name,
                "carrier": "H2_grid",
                "link_id": db.next_etrago_id("link"),
                "bus0": i_bus["bus_id"],
                "bus1": nearest_abroad_bus["bus_id"],
                "p_nom": p_nom_value,
                "p_nom_min": p_nom_value,
                "p_nom_max": float("inf"),
                "p_nom_extendable": False,
                "p_min_pu": -1,
                "lifetime": lifetime,
                "capital_cost": annualize_capital_costs(
                    overnight_cost * nearest_distance_km,
                    lifetime,
                    0.05,
                ),
                "geom": MultiLineString(
                    [
                        LineString(
                            [
                                (i_bus["geom"].x, i_bus["geom"].y),
                                (
                                    nearest_abroad_bus["geom"].x,
                                    nearest_abroad_bus["geom"].y,
                                ),
                            ]
                        )
                    ]
                ),
            }
        )
    connection_links_df = gpd.GeoDataFrame(
        connection_links, geometry="geom", crs="EPSG:4326"
    )

    connection_links_df.to_postgis(
        name=targets.get_table_name("hydrogen_links"),
        con=engine,
        schema=targets.get_table_schema("hydrogen_links"),
        if_exists="append",
        index=False,
    )
    print("Neighbour countries are succesfully connected to H2-grid")
