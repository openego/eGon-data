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
  * Waste_heat usage (carrier name: 'PtH2_waste_heat'): Components to use
    waste heat as by-product from electrolysis


"""

from shapely.geometry import LineString, MultiLineString, Point
from shapely.strtree import STRtree
from sqlalchemy import text
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.scenario_parameters import get_sector_parameters

#: Lower heating value of hydrogen [kWh/kg]
H2_LHV = 33.33


def bound_coproducts_by_electrolysis(
    power_to_H2, power_to_Heat, efficiency, waste_heat_share
):
    """
    Limit the waste heat links to what the electrolysers at the same AC bus
    produce at full load

    Parameters
    ----------
    power_to_H2 : pandas.DataFrame
        Electrolyser links with "bus0" (AC bus) and "p_nom_max" [MW_el]
    power_to_Heat : pandas.DataFrame
        Waste heat links with "bus0"
    efficiency : float
        Efficiency of the electrolysis (LHV) [p.u.]
    waste_heat_share : float
        Waste heat per electrical input [p.u.]

    Returns
    -------
    pandas.DataFrame
        The waste heat links with the new p_nom_max

    """
    electrolysis = power_to_H2.groupby("bus0")["p_nom_max"].sum()  # [MW_el]

    power_to_Heat = power_to_Heat.copy()
    if not power_to_Heat.empty:
        links_at_bus = power_to_Heat.groupby("bus0")["bus0"].transform("size")
        power_to_Heat["p_nom_max"] = (
            power_to_Heat["bus0"].map(electrolysis).fillna(0)
            * waste_heat_share
            / links_at_bus
        )

    return power_to_Heat


def scale_electrolysis_to_nep(
    power_to_H2, capacity, scn_name, sources, crs=4326
):
    """
    Limit the electrolysers to the capacity of the NEP per federal state,
    distributed by connection level

    Parameters
    ----------
    power_to_H2 : pandas.DataFrame
        Electrolyser links with the columns "topo" (line from the AC bus
        to the H2 bus) and "p_nom_max"
    capacity : dict or None
        Capacity of the electrolysers per federal state [MW], see
        "power_to_H2_capacity" in the scenario parameters
    scn_name : str
        Name of the scenario
    sources : DatasetSources
        Sources of HydrogenPowerLinkEtrago
    crs : int
        EPSG code of the column "topo"

    Returns
    -------
    pandas.DataFrame
        The electrolyser links with the new p_nom_max

    """
    if capacity is None:
        print(
            f"Warning: no NEP capacity of the electrolysers for {scn_name}, "
            "the links are only limited by their connection level."
        )
        return power_to_H2

    if power_to_H2.empty:
        return power_to_H2

    target = pd.Series(capacity, dtype=float)

    # Federal state of the AC bus (nearest, for the coast and the border)
    links = gpd.GeoDataFrame(
        index=power_to_H2.index,
        geometry=[Point(line.coords[0]) for line in power_to_H2["topo"]],
        crs=crs,
    ).to_crs(3035)
    states = db.select_geodataframe(
        f"""
        SELECT gen, geometry AS geom
        FROM {sources.tables['federal_states']}
        WHERE gf = 4
        """,
        geom_col="geom",
        epsg=3035,
    )
    located = gpd.sjoin_nearest(links, states[["gen", "geom"]], how="left")
    located = located[~located.index.duplicated(keep="first")]
    # A link without a state would silently keep its connection limit
    if located["gen"].isna().any():
        raise ValueError(
            f"{scn_name}: {located['gen'].isna().sum()} electrolysers could "
            "not be assigned to a federal state (check the CRS of 'topo')."
        )

    power_to_H2 = power_to_H2.copy()
    connection_limit = power_to_H2["p_nom_max"].astype(float)

    for state, index in located.groupby("gen").groups.items():
        capacity = target.get(state, 0.0)
        hostable = connection_limit[index].sum()
        factor = min(1.0, capacity / hostable) if hostable > 0 else 0.0

        power_to_H2.loc[index, "p_nom_max"] = connection_limit[index] * factor

        print(
            f"{scn_name}, {state}: electrolysers limited to "
            f"{min(capacity, hostable):.0f} of {capacity:.0f} MW (NEP) "
            f"over {len(index)} links"
            + (
                f" - the substations can only host {hostable:.0f} MW"
                if capacity > hostable
                else ""
            )
        )

    return power_to_H2


def insert_power_to_h2_to_power():
    """
    Insert electrolysis and fuel cells capacities into the database.
    For electrolysis potential waste_heat-utilisation is implemented if
    district_heating-demand is nearby electrolysis location

    The potentials for power-to-H2 in electrolysis and H2-to-power in
    fuel cells are created between each HVMV Substaion (or each AC_BUS related
    to setting SUBSTATION) and closest H2-Bus (H2 and H2_saltcaverns) inside
    buffer-range of 30km.
    For heat-usage closest central-heat-bus inner an dynamic buffer is connected
    to relevant HVMV-Substation.

    All links are extendable.

    This function inserts data into the database and has no return.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

    """
    sources, targets = load_sources_and_targets("HydrogenPowerLinkEtrago")

    scenarios = config.settings()["egon-data"]["--scenarios"]

    # General Constant Parameters
    DATA_CRS = 4326  # default CRS
    METRIC_CRS = 32632  # demanded CRS

    H2 = "h2"
    AC = "ac"
    H2GRID = "h2_grid"
    ACSUB_HVMV = "ac_sub_hvmv"
    ACSUB_EHV = "ac_sub_ehv"
    HEAT_BUS = "heat_point"
    HEAT_LOAD = "heat_load"
    HEAT_TIMESERIES = "heat_timeseries"
    H2_BUSES_CH4 = "h2_buses_ch4"
    HEAT_AREA = "heat_area"

    # buffer_range
    buffer_heat_factor = (
        625  # [m/MW_th] 625/3125 for worstcase/bestcase-Szeanrio
    )
    max_buffer_heat = 5000  # [m] 5000/30000 for worstcase/bestcase-Szenario
    Buffer = {
        "H2_HVMV": 5000,
        "H2_EHV": 20000,
        "HVMV": 10000,
        "EHV": 20000,
        "HEAT": 5000,
    }

    # connet to PostgreSQL database (to localhost)
    engine = db.engine()

    for SCENARIO_NAME in scenarios:

        # The status quo scenarios have no H2 system, see the concept of
        # the scenarios: they model the gas system with a single CH4 bus
        if "status" in SCENARIO_NAME:
            continue

        scn_params_gas = get_sector_parameters("gas", SCENARIO_NAME)
        scn_params_elec = get_sector_parameters("electricity", SCENARIO_NAME)

        AC_TRANS = scn_params_elec["capital_cost"][
            "transformer_220_110"
        ]  # [EUR/MW/YEAR]
        AC_COST_CABLE = scn_params_elec["capital_cost"][
            "ac_hv_cable"
        ]  # [EUR/MW/km/YEAR]
        ELZ_CAPEX_SYSTEM = scn_params_gas["capital_cost"][
            "power_to_H2_system"
        ]  # [EUR/MW/YEAR]
        ELZ_CAPEX_STACK = scn_params_gas["capital_cost"][
            "power_to_H2_stack"
        ]  # [EUR/MW/YEAR]
        ELZ_LIFETIME_Y = scn_params_gas["lifetime"][
            "power_to_H2_system"
        ]  # [Year]
        ELZ_OPEX = scn_params_gas["capital_cost"][
            "power_to_H2_OPEX"
        ]  # [EUR/MW/YEAR]
        H2_COST_PIPELINE = scn_params_gas["capital_cost"][
            "H2_pipeline"
        ]  # [EUR/MW/km/YEAR]
        ELZ_EFF = scn_params_gas["efficiency"]["power_to_H2"]

        HEAT_COST_EXCHANGER = scn_params_gas["capital_cost"][
            "Heat_exchanger"
        ]  # [EUR/MW/YEAR]
        HEAT_COST_PIPELINE = scn_params_gas["capital_cost"][
            "Heat_pipeline"
        ]  # [EUR/MW/YEAR]

        FUEL_CELL_COST = scn_params_gas["capital_cost"][
            "H2_to_power"
        ]  # [EUR/MW/YEAR]
        FUEL_CELL_EFF = scn_params_gas["efficiency"]["H2_to_power"]
        FUEL_CELL_LIFETIME = scn_params_gas["lifetime"]["H2_to_power"]

        HEAT_LIFETIME = scn_params_gas["lifetime"]["Heat_exchanger"]

        # dictionary of SQL queries
        queries = {
            H2: f"""
                    SELECT bus_id AS id, geom
                    FROM {sources.tables["buses"]}
                    WHERE carrier in ('H2_grid', 'H2')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND country = 'DE'
                    """,
            H2GRID: f"""
                    SELECT link_id, geom, bus0, bus1
                    FROM {sources.tables["links"]}
                    WHERE carrier in ('H2_grid') AND scn_name  = '{SCENARIO_NAME}'
                    """,
            AC: f"""
                    SELECT bus_id AS id, geom
                    FROM {sources.tables["buses"]}
                    WHERE carrier in ('AC')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND v_nom = '110'
                    """,
            ACSUB_HVMV: f"""
                    SELECT bus_id AS id, point AS geom
                    FROM {sources.tables["hvmv_substation"]}
                    """,
            ACSUB_EHV: f"""
                    SELECT bus_id AS id, point AS geom
                    FROM {sources.tables["ehv_substation"]}
                    """,
            HEAT_BUS: f"""
        			SELECT bus_id AS id, geom
        			FROM {sources.tables["buses"]}
        			WHERE carrier in ('central_heat')
                    AND scn_name = '{SCENARIO_NAME}'
                    AND country = 'DE'
                    """,
        }

        dfs = {
            key: gpd.read_postgis(queries[key], engine, crs=DATA_CRS).to_crs(
                METRIC_CRS
            )
            for key in queries.keys()
        }

        with engine.begin() as conn:
            # By either end: the H2 buses may have new ids since the last run
            conn.execute(
                text(
                    f"""DELETE FROM {sources.tables["links"]}
                            WHERE carrier IN ('power_to_H2', 'H2_to_power', 'PtH2_waste_heat')
                            AND scn_name = '{SCENARIO_NAME}' AND (bus0 IN (
                              SELECT bus_id
                              FROM {sources.tables["buses"]}
                              WHERE country = 'DE' AND scn_name = '{SCENARIO_NAME}'
                            ) OR bus1 IN (
                              SELECT bus_id
                              FROM {sources.tables["buses"]}
                              WHERE country = 'DE' AND scn_name = '{SCENARIO_NAME}'
                            ))
                            """
                )
            )

        def prepare_dataframes_for_spartial_queries():

            # filter_out_potential_methanisation_buses
            h2_grid_bus_ids = tuple(dfs[H2GRID]["bus1"]) + tuple(
                dfs[H2GRID]["bus0"]
            )
            dfs[H2_BUSES_CH4] = dfs[H2][~dfs[H2]["id"].isin(h2_grid_bus_ids)]

            # prepare h2_links for filtering:
            # extract geometric data for bus0
            merged_link_with_bus0_geom = pd.merge(
                dfs[H2GRID], dfs[H2], left_on="bus0", right_on="id", how="left"
            )
            merged_link_with_bus0_geom = merged_link_with_bus0_geom.rename(
                columns={"geom_y": "geom_bus0"}
            ).rename(columns={"geom_x": "geom_link"})

            # extract geometric data for bus1
            merged_link_with_bus1_geom = pd.merge(
                merged_link_with_bus0_geom,
                dfs[H2],
                left_on="bus1",
                right_on="id",
                how="left",
            )
            merged_link_with_bus1_geom = merged_link_with_bus1_geom.rename(
                columns={"geom": "geom_bus1"}
            )
            merged_link_with_bus1_geom = merged_link_with_bus1_geom[
                merged_link_with_bus1_geom["geom_bus1"] != None
            ]  # delete all abroad_links

            # prepare heat_buses for filtering
            queries[
                HEAT_AREA
            ] = f"""
                     SELECT area_id, geom_polygon as geom
                     FROM {sources.tables["district_heating_area"]}
                     WHERE scenario = '{SCENARIO_NAME}'
                     """
            dfs[HEAT_AREA] = gpd.read_postgis(
                queries[HEAT_AREA], engine
            ).to_crs(METRIC_CRS)

            heat_bus_geoms = dfs[HEAT_BUS]["geom"].tolist()
            heat_bus_index = STRtree(heat_bus_geoms)

            for _, heat_area_row in dfs[HEAT_AREA].iterrows():
                heat_area_geom = heat_area_row["geom"]
                area_id = heat_area_row["area_id"]

                potential_matches = heat_bus_index.query(heat_area_geom)

                nearest_bus_idx = None
                nearest_distance = float("inf")

                for bus_idx in potential_matches:
                    bus_geom = dfs[HEAT_BUS].at[bus_idx, "geom"]

                    distance = heat_area_geom.centroid.distance(bus_geom)
                    if distance < nearest_distance:
                        nearest_distance = distance
                        nearest_bus_idx = bus_idx

                if nearest_bus_idx is not None:
                    dfs[HEAT_BUS].at[nearest_bus_idx, "area_id"] = area_id
                    dfs[HEAT_BUS].at[
                        nearest_bus_idx, "area_geom"
                    ] = heat_area_geom

            dfs[HEAT_BUS]["area_geom"] = gpd.GeoSeries(
                dfs[HEAT_BUS]["area_geom"]
            )

            queries[
                HEAT_LOAD
            ] = f"""
                    SELECT bus, load_id
                    	FROM {sources.tables["loads"]}
                    WHERE carrier in ('central_heat')
                    AND scn_name = '{SCENARIO_NAME}'
                    """

            dfs[HEAT_LOAD] = pd.read_sql(queries[HEAT_LOAD], engine)
            load_ids = tuple(dfs[HEAT_LOAD]["load_id"])

            queries[
                HEAT_TIMESERIES
            ] = f"""
                SELECT load_id, p_set
                FROM {sources.tables["load_timeseries"]}
                WHERE load_id IN {load_ids}
                AND scn_name = '{SCENARIO_NAME}'
                """
            dfs[HEAT_TIMESERIES] = pd.read_sql(
                queries[HEAT_TIMESERIES], engine
            )
            dfs[HEAT_TIMESERIES]["sum_of_p_set"] = dfs[HEAT_TIMESERIES][
                "p_set"
            ].apply(sum)
            dfs[HEAT_TIMESERIES].drop("p_set", axis=1, inplace=True)
            dfs[HEAT_TIMESERIES].dropna(subset=["sum_of_p_set"], inplace=True)
            dfs[HEAT_LOAD] = pd.merge(
                dfs[HEAT_LOAD], dfs[HEAT_TIMESERIES], on="load_id"
            )
            dfs[HEAT_BUS] = pd.merge(
                dfs[HEAT_BUS],
                dfs[HEAT_LOAD],
                left_on="id",
                right_on="bus",
                how="inner",
            )
            dfs[HEAT_BUS]["p_mean"] = dfs[HEAT_BUS]["sum_of_p_set"].apply(
                lambda x: x / 8760
            )
            dfs[HEAT_BUS]["buffer"] = dfs[HEAT_BUS]["p_mean"].apply(
                lambda x: x * buffer_heat_factor
            )
            dfs[HEAT_BUS]["buffer"] = dfs[HEAT_BUS]["buffer"].apply(
                lambda x: x if x < max_buffer_heat else max_buffer_heat
            )

            return merged_link_with_bus1_geom, dfs[HEAT_BUS], dfs[H2_BUSES_CH4]

        def find_h2_grid_connection(
            df_AC, df_h2, buffer_h2, buffer_AC, sub_type
        ):
            df_h2["buffer"] = df_h2["geom_link"].buffer(buffer_h2)
            df_AC["buffer"] = df_AC["geom"].buffer(buffer_AC)

            h2_index = STRtree(df_h2["buffer"].tolist())

            results = []

            for idx, row in df_AC.iterrows():
                buffered_AC = row["buffer"]

                possible_matches_idx = h2_index.query(buffered_AC)

                nearest_match = None
                nearest_distance = float("inf")

                for match_idx in possible_matches_idx:
                    h2_row = df_h2.iloc[match_idx]

                    if buffered_AC.intersects(h2_row["buffer"]):
                        intersection = buffered_AC.intersection(
                            h2_row["buffer"]
                        )

                        if not intersection.is_empty:
                            distance_AC = row["geom"].distance(
                                intersection.centroid
                            )
                            distance_H2 = h2_row["geom_link"].distance(
                                intersection.centroid
                            )
                            distance_to_0 = row["geom"].distance(
                                h2_row["geom_bus0"]
                            )
                            distance_to_1 = row["geom"].distance(
                                h2_row["geom_bus1"]
                            )

                            if distance_to_0 < distance_to_1:
                                bus_H2 = h2_row["bus0"]
                                point_H2 = h2_row["geom_bus0"]
                            else:
                                bus_H2 = h2_row["bus1"]
                                point_H2 = h2_row["geom_bus1"]

                            if distance_H2 < nearest_distance:
                                nearest_distance = distance_H2
                                nearest_match = {
                                    "bus_h2": bus_H2,
                                    "bus_AC": row["id"],
                                    "geom_h2": point_H2,
                                    "geom_AC": row["geom"],
                                    "distance_h2": distance_H2,
                                    "distance_ac": distance_AC,
                                    "intersection": intersection,
                                    "sub_type": sub_type,
                                }

                if nearest_match:
                    results.append(nearest_match)

            if not results:
                return pd.DataFrame(
                    columns=[
                        "bus_h2",
                        "bus_AC",
                        "geom_h2",
                        "geom_AC",
                        "distance_h2",
                        "distance_ac",
                        "intersection",
                        "sub_type",
                    ]
                )
            else:
                return pd.DataFrame(results)

        def find_h2_bus_connection(
            df_H2, df_AC, buffer_h2, buffer_AC, sub_type
        ):

            df_H2["buffer"] = df_H2["geom"].buffer(buffer_h2)
            df_AC["buffer"] = df_AC["geom"].buffer(buffer_AC)

            h2_index = STRtree(df_H2["buffer"].tolist())

            results = []
            for _, row in df_AC.iterrows():
                possible_matches_idx = h2_index.query(row["buffer"])

                nearest_match = None
                nearest_distance = float("inf")

                for match_idx in possible_matches_idx:
                    h2_row = df_H2.iloc[match_idx]

                    if row["buffer"].intersects(h2_row["buffer"]):
                        intersection = row["buffer"].intersection(
                            h2_row["buffer"]
                        )
                        distance_AC = row["geom"].distance(
                            intersection.centroid
                        )
                        distance_H2 = h2_row["geom"].distance(
                            intersection.centroid
                        )

                        if (distance_AC + distance_H2) < nearest_distance:
                            nearest_distance = distance_AC + distance_H2
                            nearest_match = {
                                "bus_h2": h2_row["id"],
                                "bus_AC": row["id"],
                                "geom_h2": h2_row["geom"],
                                "geom_AC": row["geom"],
                                "distance_h2": distance_H2,
                                "distance_ac": distance_AC,
                                "intersection": intersection,
                                "sub_type": sub_type,
                            }

                if nearest_match:
                    results.append(nearest_match)

            if not results:
                return pd.DataFrame(
                    columns=[
                        "bus_h2",
                        "bus_AC",
                        "geom_h2",
                        "geom_AC",
                        "distance_h2",
                        "distance_ac",
                        "intersection",
                        "sub_type",
                    ]
                )
            else:
                return pd.DataFrame(results)

        def find_h2_connection(df_h2):
            ####find H2-HVMV connection:
            potential_location_grid = find_h2_grid_connection(
                dfs[ACSUB_HVMV],
                df_h2,
                Buffer["H2_HVMV"],
                Buffer["HVMV"],
                "HVMV",
            )
            potential_location_grid = potential_location_grid.loc[
                potential_location_grid.groupby(["bus_h2", "bus_AC"])[
                    "distance_h2"
                ].idxmin()
            ]

            filtered_df_hvmv = dfs[ACSUB_HVMV][
                ~dfs[ACSUB_HVMV]["id"].isin(potential_location_grid["bus_AC"])
            ].copy()
            potential_location_buses = find_h2_bus_connection(
                dfs[H2_BUSES_CH4],
                filtered_df_hvmv,
                Buffer["H2_HVMV"],
                Buffer["HVMV"],
                "HVMV",
            )
            potential_location_buses = potential_location_buses.loc[
                potential_location_buses.groupby(["bus_h2", "bus_AC"])[
                    "distance_h2"
                ].idxmin()
            ]

            potential_location_hvmv = pd.concat(
                [potential_location_grid, potential_location_buses],
                ignore_index=True,
            )

            ####find H2-EHV connection:
            potential_location_grid = find_h2_grid_connection(
                dfs[ACSUB_EHV], df_h2, Buffer["H2_EHV"], Buffer["EHV"], "EHV"
            )
            potential_location_grid = potential_location_grid.loc[
                potential_location_grid.groupby(["bus_h2", "bus_AC"])[
                    "distance_h2"
                ].idxmin()
            ]

            filtered_df_ehv = dfs[ACSUB_EHV][
                ~dfs[ACSUB_EHV]["id"].isin(potential_location_grid["bus_AC"])
            ].copy()
            potential_location_buses = find_h2_bus_connection(
                dfs[H2_BUSES_CH4],
                filtered_df_ehv,
                Buffer["H2_EHV"],
                Buffer["EHV"],
                "EHV",
            )
            potential_location_buses = potential_location_buses.loc[
                potential_location_buses.groupby(["bus_h2", "bus_AC"])[
                    "distance_h2"
                ].idxmin()
            ]

            potential_location_ehv = pd.concat(
                [potential_location_grid, potential_location_buses],
                ignore_index=True,
            )

            ### combined potential ehv- and hvmv-connections:
            return pd.concat(
                [potential_location_hvmv, potential_location_ehv],
                ignore_index=True,
            )

        def find_heat_connection(potential_locations):

            dfs[HEAT_BUS]["buffered_geom"] = dfs[HEAT_BUS]["area_geom"].buffer(
                dfs[HEAT_BUS]["buffer"]
            )
            intersection_index = STRtree(
                potential_locations["intersection"].tolist()
            )

            potential_locations["bus_heat"] = None
            potential_locations["geom_heat"] = None
            potential_locations["distance_heat"] = None

            results = []

            for _, heat_row in dfs[HEAT_BUS].iterrows():
                buffered_geom = heat_row["buffered_geom"]

                potential_matches = intersection_index.query(buffered_geom)

                if len(potential_matches) > 0:
                    nearest_distance = float("inf")
                    nearest_ac_index = None

                    for match_idx in potential_matches:
                        ac_row = potential_locations.iloc[
                            match_idx
                        ]  # Hole die entsprechende Zeile

                        if buffered_geom.intersects(ac_row["intersection"]):
                            distance = buffered_geom.centroid.distance(
                                ac_row["intersection"].centroid
                            )

                            if distance < nearest_distance:
                                nearest_distance = distance
                                nearest_ac_index = match_idx

                    if nearest_ac_index is not None:
                        results.append(
                            {
                                "bus_AC": potential_locations.at[
                                    nearest_ac_index, "bus_AC"
                                ],
                                "bus_heat": heat_row["id"],
                                "geom_AC": potential_locations.at[
                                    nearest_ac_index, "geom_AC"
                                ],
                                "geom_heat": heat_row["geom"],
                                "distance_heat": distance,
                            }
                        )
                        potential_locations.at[
                            nearest_ac_index, "bus_heat"
                        ] = heat_row["id"]
                        potential_locations.at[
                            nearest_ac_index, "geom_heat"
                        ] = heat_row["geom"]
                        potential_locations.at[
                            nearest_ac_index, "distance_heat"
                        ] = nearest_distance

            return pd.DataFrame(results)

        def create_link_dataframes(links_h2, links_heat):

            etrago_columns = [
                "scn_name",
                "link_id",
                "bus0",
                "bus1",
                "carrier",
                "efficiency",
                "lifetime",
                "p_nom",
                "p_nom_max",
                "p_nom_extendable",
                "capital_cost",
                "length",
                "geom",
                "topo",
            ]

            power_to_H2 = pd.DataFrame(columns=etrago_columns)
            H2_to_power = pd.DataFrame(columns=etrago_columns)
            power_to_Heat = pd.DataFrame(columns=etrago_columns)

            ####poower_to_H2
            for idx, row in links_h2.iterrows():
                capital_cost_H2 = (
                    H2_COST_PIPELINE * row["distance_h2"] / 1000
                    + ELZ_CAPEX_STACK
                    + ELZ_CAPEX_SYSTEM
                    + ELZ_OPEX
                )  # [EUR/MW/YEAR]
                capital_cost_AC = (
                    AC_COST_CABLE * row["distance_ac"] / 1000 + AC_TRANS
                )  # [EUR/MW/YEAR]
                capital_cost_PtH2 = capital_cost_AC + capital_cost_H2

                power_to_H2_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": db.next_etrago_id("link"),
                    "bus0": row["bus_AC"],
                    "bus1": row["bus_h2"],
                    "carrier": "power_to_H2",
                    "efficiency": ELZ_EFF,
                    "lifetime": ELZ_LIFETIME_Y,
                    "p_nom": 0,
                    "p_nom_max": 120 if row["sub_type"] == "HVMV" else 5000,
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost_PtH2,
                    "geom": MultiLineString(
                        [
                            LineString(
                                [
                                    (row["geom_AC"].x, row["geom_AC"].y),
                                    (row["geom_h2"].x, row["geom_h2"].y),
                                ]
                            )
                        ]
                    ),
                    "topo": LineString(
                        [
                            (row["geom_AC"].x, row["geom_AC"].y),
                            (row["geom_h2"].x, row["geom_h2"].y),
                        ]
                    ),
                }
                power_to_H2 = pd.concat(
                    [power_to_H2, pd.DataFrame([power_to_H2_entry])],
                    ignore_index=True,
                )

                ####H2_to_power
                capital_cost_H2 = (
                    H2_COST_PIPELINE * row["distance_h2"] / 1000
                    + FUEL_CELL_COST
                )  # [EUR/MW/YEAR]
                capital_cost_AC = (
                    AC_COST_CABLE * row["distance_ac"] / 1000 + AC_TRANS
                )  # [EUR/MW/YEAR]
                capital_cost_H2tP = capital_cost_AC + capital_cost_H2
                H2_to_power_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": db.next_etrago_id("link"),
                    "bus0": row["bus_h2"],
                    "bus1": row["bus_AC"],
                    "carrier": "H2_to_power",
                    "efficiency": FUEL_CELL_EFF,
                    "lifetime": FUEL_CELL_LIFETIME,
                    "p_nom": 0,
                    "p_nom_max": 120 if row["sub_type"] == "HVMV" else 5000,
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost_H2tP,
                    "geom": MultiLineString(
                        [
                            LineString(
                                [
                                    (row["geom_AC"].x, row["geom_AC"].y),
                                    (row["geom_h2"].x, row["geom_h2"].y),
                                ]
                            )
                        ]
                    ),
                    "topo": LineString(
                        [
                            (row["geom_AC"].x, row["geom_AC"].y),
                            (row["geom_h2"].x, row["geom_h2"].y),
                        ]
                    ),
                }
                H2_to_power = pd.concat(
                    [H2_to_power, pd.DataFrame([H2_to_power_entry])],
                    ignore_index=True,
                )

            ###power_to_Heat
            for idx, row in links_heat.iterrows():
                capital_cost = (
                    HEAT_COST_EXCHANGER
                    + HEAT_COST_PIPELINE * row["distance_heat"] / 1000
                )  # EUR/MW/YEAR

                power_to_heat_entry = {
                    "scn_name": SCENARIO_NAME,
                    "link_id": db.next_etrago_id("link"),
                    "bus0": row["bus_AC"],
                    "bus1": row["bus_heat"],
                    "carrier": "PtH2_waste_heat",
                    "efficiency": 1,
                    "lifetime": HEAT_LIFETIME,
                    "p_nom": 0,
                    "p_nom_max": float("inf"),
                    "p_nom_extendable": True,
                    "capital_cost": capital_cost,
                    "geom": MultiLineString(
                        [
                            LineString(
                                [
                                    (row["geom_AC"].x, row["geom_AC"].y),
                                    (row["geom_heat"].x, row["geom_heat"].y),
                                ]
                            )
                        ]
                    ),
                    "topo": LineString(
                        [
                            (row["geom_AC"].x, row["geom_AC"].y),
                            (row["geom_heat"].x, row["geom_heat"].y),
                        ]
                    ),
                }
                power_to_Heat = pd.concat(
                    [power_to_Heat, pd.DataFrame([power_to_heat_entry])],
                    ignore_index=True,
                )

            return power_to_H2, H2_to_power, power_to_Heat

        def export_links_to_db(df, carrier):

            gdf = gpd.GeoDataFrame(df, geometry="geom").set_crs(METRIC_CRS)
            gdf = gdf.to_crs(epsg=DATA_CRS)
            # "topo" is built in the metric CRS like "geom"
            gdf["topo"] = (
                gpd.GeoSeries(
                    df["topo"].values, index=gdf.index, crs=METRIC_CRS
                )
                .to_crs(epsg=DATA_CRS)
                .tolist()
            )
            gdf.p_nom = 0

            try:
                gdf.to_postgis(
                    name=targets.get_table_name("hydrogen_links"),
                    con=engine,
                    schema=targets.get_table_schema("hydrogen_links"),
                    if_exists="append",
                    index=False,
                )
                print(
                    f"Links have been exported to {targets.tables['hydrogen_links']}"
                )
            except Exception as e:
                print(f"Error while exporting the {carrier} links: {e}")
                raise

        def connect_off_grid_h2_demand(potential_locations):
            """
            Electrolysers at the nearest substation for the H2 demand at H2 buses
            without H2_grid link and without electrolyser candidate

            """
            demand_buses = set(
                pd.read_sql(
                    f"""
                    SELECT DISTINCT bus FROM {sources.tables["loads"]}
                    WHERE carrier = 'H2_for_industry'
                    AND scn_name = '{SCENARIO_NAME}'
                    """,
                    engine,
                )["bus"]
            )
            # Buses reached by the H2 grid
            grid_buses = set(dfs[H2GRID]["bus0"]) | set(dfs[H2GRID]["bus1"])
            candidates = set(potential_locations["bus_h2"])
            off_grid = dfs[H2][
                dfs[H2]["id"].isin(demand_buses)
                & ~dfs[H2]["id"].isin(grid_buses)
                & ~dfs[H2]["id"].isin(candidates)
            ]
            if off_grid.empty:
                return potential_locations

            substations = pd.concat(
                [
                    dfs[ACSUB_HVMV].assign(sub_type="HVMV"),
                    dfs[ACSUB_EHV].assign(sub_type="EHV"),
                ],
                ignore_index=True,
            )
            rows = []
            for _, h2_bus in off_grid.iterrows():
                distance = substations["geom"].distance(h2_bus["geom"])
                nearest = substations.loc[distance.idxmin()]
                rows.append(
                    {
                        "bus_h2": h2_bus["id"],
                        "bus_AC": nearest["id"],
                        "geom_h2": h2_bus["geom"],
                        "geom_AC": nearest["geom"],
                        "distance_h2": 0.0,
                        "distance_ac": distance.min(),
                        "intersection": h2_bus["geom"],
                        "sub_type": nearest["sub_type"],
                    }
                )
            print(
                f"{SCENARIO_NAME}: {len(rows)} H2 buses with industrial "
                "demand are not reached by the H2 grid and get an "
                "electrolyser at the nearest substation (mean distance "
                f"{np.mean([r['distance_ac'] for r in rows]) / 1000:.1f} km)"
            )
            return pd.concat(
                [potential_locations, pd.DataFrame(rows)], ignore_index=True
            )

        def execute_PtH2_method():

            (
                h2_grid_geom_df,
                dfs[HEAT_BUS],
                dfs[H2_BUSES_CH4],
            ) = prepare_dataframes_for_spartial_queries()
            potential_locations = find_h2_connection(h2_grid_geom_df)
            potential_locations = connect_off_grid_h2_demand(
                potential_locations
            )
            heat_links = find_heat_connection(potential_locations)
            power_to_H2, H2_to_power, power_to_Heat = create_link_dataframes(
                potential_locations, heat_links
            )
            # Electrolysers: capacity of the NEP per federal state, see
            # the function
            power_to_H2 = scale_electrolysis_to_nep(
                power_to_H2,
                scn_params_gas.get("power_to_H2_capacity"),
                SCENARIO_NAME,
                sources,
                crs=METRIC_CRS,
            )
            # Waste heat co-product: no more than the electrolysers produce
            power_to_Heat = bound_coproducts_by_electrolysis(
                power_to_H2,
                power_to_Heat,
                ELZ_EFF,
                scn_params_gas["efficiency"]["power_to_Heat"],
            )
            export_links_to_db(power_to_H2, "power_to_H2")
            export_links_to_db(power_to_Heat, "PtH2_waste_heat")
            export_links_to_db(H2_to_power, "H2_to_power")

        execute_PtH2_method()
