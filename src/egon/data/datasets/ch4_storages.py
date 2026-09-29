# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas stores

This module contains the functions to import the existing methane stores
in Germany and inserting them into the database. They are modelled as
PyPSA stores and are not extendable.

"""

import ast
import re
import unicodedata

from shapely.geometry import LineString
import geopandas
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.hydrogen_etrago.h2_grid import match_nep_ch4_network
from egon.data.datasets.hydrogen_etrago.nep2025 import (
    NEP_CH4_NETWORK_2045,
    NEP_CH4_NETWORK_2045_NODES,
    NEP_CH4_NETWORK_YEAR,
)
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)

# German cavern and pore storage sites (INES/DVGW/BVEG 2022, Anhang 1,
# p. 221-222); only caverns can be converted to H2
CAVERN_SITES = (
    "bad lauchstaedt",
    "bernburg",
    "lesum",
    "empelde",
    "epe",
    "etzel",
    "harsefeld",
    "huntorf",
    "jemgum",
    "jengum",
    "katharina",
    "kiel",
    "kraak",
    "krummhoern",
    "nuettermoor",
    "peckensen",
    "reckrod",
    "ruedersdorf",
    "stassfurt",
    "xanten",
)
PORE_SITES = (
    "allmenhausen",
    "berlin",
    "bierwang",
    "breitbrunn",
    "buchholz",
    "doetlingen",
    "eschenfelden",
    "frankenthal",
    "fronhofen",
    "haehnlein",
    "inzenham",
    "kalle",
    "kirchheilingen",
    "lehrte",
    "rehden",
    "reitbrook",
    "sandhausen",
    "schmidhausen",
    "stockstadt",
    "uelsen",
    "wolfersberg",
)

# Order in which the stores leave the methane system
STORE_CONVERSION_ORDER = ("cavern", "unknown", "pore")


def store_type(name):
    """
    Return the type of a gas store from its name

    Parameters
    ----------
    name : str
        Name of the store in the SciGRID_gas data

    Returns
    -------
    str
        "cavern", "pore" or "unknown"

    """
    name = (
        unicodedata.normalize("NFKD", str(name))
        .encode("ascii", "ignore")
        .decode()
        .lower()
    )
    name = re.sub(r"[^a-z0-9]+", " ", name).strip()

    if any(site in name for site in CAVERN_SITES):
        return "cavern"
    if any(site in name for site in PORE_SITES):
        return "pore"
    return "unknown"


def scale_to_scenario_capacity(Gas_storages_list, scn_name):
    """
    Reduce the gas stores to the storage capacity of the scenario, in the
    order of :data:`STORE_CONVERSION_ORDER`

    Parameters
    ----------
    Gas_storages_list : pandas.DataFrame
        Dataframe containing the gas stores with their "e_nom" and "name"
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.DataFrame
        The stores that remain in the scenario, with their reduced e_nom

    """
    target = get_sector_parameters("gas", scn_name)["CH4_storage_capacity"]

    Gas_storages_list = Gas_storages_list.assign(
        store_type=Gas_storages_list["name"].apply(store_type)
    )

    remaining = target
    for store_type_name in reversed(STORE_CONVERSION_ORDER):
        stores = Gas_storages_list["store_type"] == store_type_name
        capacity = Gas_storages_list.loc[stores, "e_nom"].sum()

        if capacity == 0:
            continue

        kept = min(capacity, remaining)
        remaining -= kept
        Gas_storages_list.loc[stores, "e_nom"] *= kept / capacity

        print(
            f"{scn_name}: {kept / 1e6:.1f} of {capacity / 1e6:.1f} TWh of the "
            f"{store_type_name} stores stay in the methane system."
        )

    return Gas_storages_list[Gas_storages_list["e_nom"] > 0].drop(
        columns=["store_type"]
    )


class CH4Storages(Dataset):
    """
    Inserts the gas stores in Germany

    Inserts the non extendable gas stores in Germany into the database
    for the scenarios with a CH4 grid, using the function
    :py:func:`insert_ch4_stores` once per scenario.

    *Dependencies*
      * :py:class:`GasAreas <egon.data.datasets.gas_areas.GasAreas>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended


    """

    #:
    name: str = "CH4Storages"
    #:
    version: str = "0.0.5"

    sources = DatasetSources(
        files={
            "scigrid_storages": "datasets/gas_data/data/IGGIELGN_Storages.csv",
            # Both only read to distribute the line pack over the nodes,
            # see :py:func:`german_ch4_line_pack_shares`
            "scigrid_nodes": "datasets/gas_data/data/IGGIELGN_Nodes.csv",
            "scigrid_pipes": (
                "datasets/gas_data/data/IGGIELGN_PipeSegments.csv"
            ),
        },
        tables={
            "gas_buses": "grid.egon_etrago_bus",
        },
    )
    targets = DatasetTargets(
        tables={
            "stores": "grid.egon_etrago_store",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_ch4_storages,),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            FROM_PARAMETERS,
            GasBusReferences,
            GasComponentTotal,
            for_each_scenario,
        )

        # Nationally: storage capacity of the scenario plus line pack
        return {
            "sanity": [
                # status2024 has no CH4 stores (see insert_ch4_storages)
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_CH4_STORES_DE_E_NOM",
                    table="grid.egon_etrago_store",
                    carriers=["CH4"],
                    measure=("sum", "e_nom"),
                    parameter=[
                        ("gas", "CH4_storage_capacity"),
                        ("gas", "CH4_grid_capacity"),
                    ],
                    expected={
                        "status2024": 0,
                        "eGon2035": {
                            "Schleswig-Holstein": 80358.729,
                            "Everything": FROM_PARAMETERS,
                        },
                        "reGon2037": {
                            "Schleswig-Holstein": 5375.525,
                            "Everything": FROM_PARAMETERS,
                        },
                        "reGon2045": {
                            "Schleswig-Holstein": 260.307,
                            "Everything": FROM_PARAMETERS,
                        },
                    },
                ),
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_CH4_STORES_BUSES",
                    table="grid.egon_etrago_store",
                    carriers=["CH4"],
                ),
            ],
        }


def import_installed_ch4_storages(scn_name):
    """
    Defines list of CH4 stores from the SciGRID_gas data

    This function reads from the SciGRID_gas dataset the existing CH4
    cavern stores in Germany, adjusts and returns them, reduced to the
    capacity of the scenario (:py:func:`scale_to_scenario_capacity`).
    Caverns reference: SciGRID_gas dataset (datasets/gas_data/data/IGGIELGN_Storages.csv
    downloaded in :func:`download_SciGRID_gas_data <egon.data.datasets.gas_grid.download_SciGRID_gas_data>`).
    For more information on these data, refer to the
    `SciGRID_gas IGGIELGN documentation <https://zenodo.org/record/4767098>`_.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the CH4 cavern store units in Germany

    """
    storage_file = CH4Storages.sources.files["scigrid_storages"]

    Gas_storages_list = pd.read_csv(
        storage_file,
        delimiter=";",
        decimal=".",
        usecols=["name", "lat", "long", "country_code", "param", "method"],
    )

    Gas_storages_list = Gas_storages_list[
        Gas_storages_list["country_code"].str.match("DE")
    ]

    # Define new columns
    max_workingGas_M_m3 = []
    NUTS1 = []
    end_year = []
    method_cap = []
    for index, row in Gas_storages_list.iterrows():
        param = ast.literal_eval(row["param"])
        NUTS1.append(param["nuts_id_1"])
        end_year.append(param["end_year"])
        max_workingGas_M_m3.append(param["max_workingGas_M_m3"])

        method = ast.literal_eval(row["method"])
        method_cap.append(method["max_workingGas_M_m3"])

    Gas_storages_list["method_cap"] = method_cap
    Gas_storages_list = Gas_storages_list.assign(NUTS1=NUTS1).drop_duplicates()

    # Calculate e_nom
    conv_factor = 10830  # gross calorific value = 39 MJ/m3 (eurogas.org)
    Gas_storages_list["e_nom"] = [conv_factor * i for i in max_workingGas_M_m3]

    end_year = [float("inf") if x == None else x for x in end_year]
    Gas_storages_list = Gas_storages_list.assign(end_year=end_year)

    # Adjust the storage capacities calculated by 'Median(max_workingGas_M_m3)'
    # to the German total (the scenario capacity is applied below)
    total_german_cap = 266424202  # MWh GIE https://www.gie.eu/transparency/databases/storage-database/
    ch4_estimated = Gas_storages_list[
        Gas_storages_list.method_cap == "Median(max_workingGas_M_m3)"
    ]
    german_cap_source = Gas_storages_list[
        Gas_storages_list.method_cap != "Median(max_workingGas_M_m3)"
    ].e_nom.sum()

    Gas_storages_list.loc[ch4_estimated.index, "e_nom"] = (
        total_german_cap - german_cap_source
    ) / len(ch4_estimated)

    # Reduce the fleet of 2021 to the storage capacity of the scenario,
    # converting the caverns first, see :py:func:`scale_to_scenario_capacity`
    Gas_storages_list = scale_to_scenario_capacity(Gas_storages_list, scn_name)

    # Cut data to federal state if in testmode
    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary != "Everything":
        map_states = {
            "Baden-Württemberg": "DE1",
            "Nordrhein-Westfalen": "DEA",
            "Hessen": "DE7",
            "Brandenburg": "DE4",
            "Bremen": "DE5",
            "Rheinland-Pfalz": "DEB",
            "Sachsen-Anhalt": "DEE",
            "Schleswig-Holstein": "DEF",
            "Mecklenburg-Vorpommern": "DE8",
            "Thüringen": "DEG",
            "Niedersachsen": "DE9",
            "Sachsen": "DED",
            "Hamburg": "DE6",
            "Saarland": "DEC",
            "Berlin": "DE3",
            "Bayern": "DE2",
        }

        Gas_storages_list = Gas_storages_list[
            Gas_storages_list["NUTS1"].isin([map_states[boundary], np.nan])
        ]

    # Remove unused storage units
    year = get_scenario_year(scn_name)
    Gas_storages_list = Gas_storages_list[
        Gas_storages_list["end_year"] >= year
    ]

    Gas_storages_list = Gas_storages_list.rename(
        columns={"lat": "y", "long": "x"}
    )
    Gas_storages_list = geopandas.GeoDataFrame(
        Gas_storages_list,
        geometry=geopandas.points_from_xy(
            Gas_storages_list["x"], Gas_storages_list["y"]
        ),
    )
    Gas_storages_list = Gas_storages_list.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    # Match to associated gas bus
    Gas_storages_list = Gas_storages_list.reset_index(drop=True)
    Gas_storages_list = db.assign_gas_bus_id(
        Gas_storages_list, scn_name, "CH4"
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": "CH4"}
    Gas_storages_list = Gas_storages_list.assign(**c)

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(
        columns=[
            "x",
            "y",
            "param",
            "country_code",
            "NUTS1",
            "end_year",
            "geom",
            "bus_id",
            "method_cap",
            "name",
        ]
    )

    return Gas_storages_list


def german_ch4_nodes():
    """
    Return the CH4 nodes of the SciGRID_gas data in Germany

    Returns
    -------
    pandas.DataFrame
        The German CH4 nodes with their coordinates, indexed by the
        SciGRID_gas id

    """
    gas_nodes_list = pd.read_csv(
        CH4Storages.sources.files["scigrid_nodes"],
        delimiter=";",
        decimal=".",
        usecols=["id", "lat", "long", "country_code"],
    )

    # Correct non valid neighbouring country nodes
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "INET_N_1182", "country_code"
    ] = "AT"
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "SEQ_10608_p", "country_code"
    ] = "NL"
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "N_88_NS_LMGN", "country_code"
    ] = "XX"

    gas_nodes_list = gas_nodes_list[
        gas_nodes_list["country_code"].str.match("DE")
    ]

    return gas_nodes_list.rename(columns={"lat": "y", "long": "x"}).set_index(
        "id"
    )


def german_ch4_pipes():
    """
    Return the CH4 pipelines of the SciGRID_gas data inside of Germany

    Returns
    -------
    geopandas.GeoDataFrame
        Pipelines with both ends in Germany, with the columns "bus0" and
        "bus1" (SciGRID_gas node ids), "diameter_m", "length_km" and
        "p_nom" (the diameter in mm, only used to rank parallel pipelines
        in :py:func:`match_nep_ch4_network
        <egon.data.datasets.hydrogen_etrago.h2_grid.match_nep_ch4_network>`)

    """
    nodes = german_ch4_nodes()

    pipes = pd.read_csv(
        CH4Storages.sources.files["scigrid_pipes"],
        delimiter=";",
        decimal=".",
        usecols=["node_id", "lat", "long", "param"],
    )

    rows = []
    for node_id, lat, lon, param in zip(
        pipes["node_id"], pipes["lat"], pipes["long"], pipes["param"]
    ):
        bus0, bus1 = [
            b.strip().strip("'") for b in node_id.strip("][").split(",")
        ]
        if bus0 not in nodes.index or bus1 not in nodes.index or bus0 == bus1:
            continue

        p = ast.literal_eval(param)
        if not p.get("diameter_mm") or not p.get("length_km"):
            continue

        lat, lon = ast.literal_eval(lat), ast.literal_eval(lon)
        rows.append(
            {
                "bus0": bus0,
                "bus1": bus1,
                "diameter_m": p["diameter_mm"] / 1000,
                "length_km": p["length_km"],
                "p_nom": p["diameter_mm"],
                "geometry": LineString(
                    list(
                        zip(
                            [lon[0]] + p.get("path_long", []) + [lon[1]],
                            [lat[0]] + p.get("path_lat", []) + [lat[1]],
                        )
                    )
                ),
            }
        )

    return geopandas.GeoDataFrame(rows, geometry="geometry", crs=4326)


def german_ch4_line_pack_shares(scn_name):
    """
    Return the share of the line pack of each German CH4 node, by the
    volume of the pipelines attached to it (whole German grid)

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.Series
        Share of the line pack per node, indexed by the rounded
        coordinates ("x", "y") of the node

    """
    nodes = german_ch4_nodes()
    pipes = german_ch4_pipes()

    if get_scenario_year(scn_name) >= NEP_CH4_NETWORK_YEAR:
        buses = geopandas.GeoDataFrame(
            index=nodes.index,
            geometry=geopandas.points_from_xy(nodes["x"], nodes["y"]),
            crs=4326,
        )
        keep, _ = match_nep_ch4_network(
            pipes, buses, NEP_CH4_NETWORK_2045, NEP_CH4_NETWORK_2045_NODES
        )
        pipes = pipes.loc[sorted(keep)]

    # The cross section without the constant pi/4, which cancels out in
    # the shares
    half_volume = pipes["length_km"] * pipes["diameter_m"] ** 2 / 2
    volume = (
        half_volume.groupby(pipes["bus0"])
        .sum()
        .add(half_volume.groupby(pipes["bus1"]).sum(), fill_value=0)
    ).reindex(nodes.index, fill_value=0)

    shares = volume / volume.sum()
    shares.index = pd.MultiIndex.from_arrays(
        [nodes["x"].round(6), nodes["y"].round(6)]
    )

    return shares


def import_ch4_grid_capacity(scn_name):
    """
    Defines the gas stores modelling the store capacity of the grid

    Define dataframe containing the modelling of the grid storage
    capacity. The line pack of the scenario ("CH4_grid_capacity") is
    distributed between the German gas nodes, see
    :py:func:`german_ch4_line_pack_shares`.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier : str
        Name of the carrier

    Returns
    -------
    Gas_storages_list :
        List of gas stores in Germany modelling the gas grid storage capacity

    """

    # Line pack of the CH4 grid of the scenario, see "CH4_grid_capacity" in
    # the scenario parameters for the value and its source
    Gas_grid_capacity = get_sector_parameters("gas", scn_name)[
        "CH4_grid_capacity"
    ]
    shares = german_ch4_line_pack_shares(scn_name)

    sql_gas = f"""SELECT bus_id, scn_name, carrier, x, y
                 FROM {CH4Storages.sources.tables['gas_buses']}
                 WHERE carrier = 'CH4' AND scn_name = '{scn_name}'
                 AND country = 'DE';"""
    Gas_storages_list = db.select_dataframe(sql_gas)

    # Add missing column
    Gas_storages_list["bus"] = Gas_storages_list["bus_id"]

    # Attribute the share of the line pack to each bus. The buses are
    # matched to the nodes of the SciGRID_gas data by their coordinates.
    Gas_storages_list["e_nom"] = (
        pd.MultiIndex.from_arrays(
            [
                Gas_storages_list["x"].round(6),
                Gas_storages_list["y"].round(6),
            ]
        )
        .map(shares)
        .to_numpy(dtype=float)
        * Gas_grid_capacity
    )

    unmatched = Gas_storages_list["e_nom"].isna()
    if unmatched.any():
        print(
            f"Warning: {int(unmatched.sum())} of {len(Gas_storages_list)} CH4 "
            f"buses of {scn_name} could not be matched to a node of the "
            "SciGRID_gas data, they get no line pack."
        )
        Gas_storages_list.loc[unmatched, "e_nom"] = 0

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=["bus_id", "x", "y"])

    return Gas_storages_list


def insert_ch4_stores(scn_name):
    """
    Inserts gas stores for specific scenario

    Insert non extendable gas stores for specific scenario in Germany
    by executing the following steps:

    * Clean the database.
    * For CH4 stores, call the functions
      :py:func:`import_installed_ch4_storages` to get the CH4
      cavern stores (only in the scenarios that still have a methane
      storage capacity) and :py:func:`import_ch4_grid_capacity` to
      get the CH4 stores modelling the storage capacity of the
      grid.
    * Aggregate the stores attached to the same bus.
    * Add the missing column store_id.
    * Insert the stores into the database.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """

    # Connect to local database
    engine = db.engine()

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM {CH4Storages.targets.tables['stores']}
        WHERE "carrier" = 'CH4'
        AND scn_name = '{scn_name}'
        AND bus IN (
            SELECT bus_id FROM {CH4Storages.sources.tables['gas_buses']}
            WHERE scn_name = '{scn_name}'
            AND country = 'DE'
            );
        """
    )

    # The line pack of the grid is always modelled, the stores only if the
    # scenario still has a methane storage capacity (reGon2045: none)
    stores = [import_ch4_grid_capacity(scn_name)]

    if get_sector_parameters("gas", scn_name)["CH4_storage_capacity"] > 0:
        stores.insert(0, import_installed_ch4_storages(scn_name))

    gas_storages_list = pd.concat(stores)

    # Aggregate ch4 stores with same properties at the same bus
    gas_storages_list = (
        gas_storages_list.groupby(["bus", "carrier", "scn_name"])
        .agg({"e_nom": "sum"})
        .reset_index(drop=False)
    )

    gas_storages_list["store_id"] = db.next_etrago_id(
        "store", len(gas_storages_list)
    )

    # Insert data to db
    gas_storages_list.to_sql(
        CH4Storages.targets.get_table_name("stores"),
        engine,
        schema=CH4Storages.targets.get_table_schema("stores"),
        index=False,
        if_exists="append",
    )


def insert_ch4_storages():
    """
    Overall function to import non extendable gas stores in Germany

    This function inserts the methane stores in Germany for the
    scenarios with a CH4 grid by using the function
    :py:func:`insert_ch4_stores` and has no return.

    """
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" not in scn_name:
            insert_ch4_stores(scn_name)
