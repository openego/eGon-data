"""Module containing code dealing with crossbording gas pipelines for eGon100RE

In this module the crossbordering pipelines for H2 and CH4, exclusively
between Germany and its neighbouring countries, in eGon100RE are
defined and inserted in the database.

Dependecies (pipeline)
======================
  * :dataset: PypsaEurSec, GasNodesandPipes, HydrogenBusEtrago,
    ElectricalNeighbours

Resulting tables
================
  * grid.egon_etrago_link is completed

"""

import pandas as pd

from egon.data import config, db
from egon.data.datasets.gas_neighbours.gas_abroad import (
    insert_gas_grid_capacities,
)
from egon.data.datasets.pypsaeur import read_network

countries = [
    "AT",
    "BE",
    "CH",
    "CZ",
    "DK",
    "FR",
    "LU",
    "NL",
    "NO",
    "PL",
]


def insert_gas_neigbours_eGon100RE():
    """Insert missing gas crossbordering grid capacities for eGon100RE

    This function insert the crossbordering pipelines for H2 and CH4,
    exclusively between Germany and its neighbouring countries,
    for eGon100RE in the database by executing the following steps:
      * call of the the function
        :py:func:`define_DE_crossbording_pipes_geom_eGon100RE`, that
        defines the crossbordering pipelines (H2 and CH4) between
        Germany and its neighbouring countries
      * call of the the function
        :py:func:`read_DE_crossbordering_cap_from_pes`, that calculates
        the crossbordering total exchange capactities for H2 and CH4
        between Germany and its neighbouring countries based on the
        pypsa-eur-sec results
      * call of the the function
        :py:func:`calculate_crossbordering_gas_grid_capacities_eGon100RE`,
        that attributes to each crossbordering pipeline (H2 and CH4)
        between Germany and its neighbouring countries its capacity
      * insertion of the H2 and CH4 pipelines between Germany and its
        neighbouring countries in the database with function
        :py:func:`insert_gas_grid_capacities`

    Returns
    -------
    None

    """

    DE_pipe_capacities_list = define_DE_crossbording_pipes_geom_eGon100RE()
    cap_DE = read_DE_crossbordering_cap_from_pes()

    Crossbordering_pipe_capacities_list = (
        calculate_crossbordering_gas_grid_capacities_eGon100RE(
            cap_DE, DE_pipe_capacities_list
        )
    )

    for i in ["link_id", "bus0", "bus1"]:
        Crossbordering_pipe_capacities_list[i] = (
            Crossbordering_pipe_capacities_list[i].astype(str).astype(int)
        )

    for i in ["p_nom", "length"]:
        Crossbordering_pipe_capacities_list[i] = (
            Crossbordering_pipe_capacities_list[i].astype(str).astype(float)
        )

    insert_gas_grid_capacities(
        Crossbordering_pipe_capacities_list, "eGon100RE"
    )


def define_DE_crossbording_pipes_geom_eGon100RE(scn_name="eGon100RE"):
    """Define the missing crossbordering gas pipelines in eGon100RE

    This function defines the crossbordering pipelines (for H2)
    between Germany and its neighbouring countries. These pipelines
    are defined as links and there are copied from the corresponding
    CH4 crossbering pipelines.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    gas_pipelines_list_DE : pandas.DataFrame
        List of the crossbordering H2 and CH4 pipelines between
        Germany and its neighbouring countries in eGon100RE, with
        geometry (geom and topo) but no capacity.

    """

    def find_equivalent_H2(df):
        equivalent = {}
        H2 = df[df["carrier"].isin(["H2"])].set_index("bus_id")
        CH4 = df[df["carrier"] == "CH4"].set_index("bus_id")

        for bus in CH4.index:
            bus_h2 = H2[
                (H2.x == CH4.at[bus, "x"]) & (H2.y == CH4.at[bus, "y"])
            ].index[0]
            equivalent[bus] = bus_h2
        return equivalent

    def set_foreign_country(link, foreign):
        if link["bus0"] in (foreign.index):
            country = foreign.at[link["bus0"], "country"]
        elif link["bus1"] in (foreign.index):
            country = foreign.at[link["bus1"], "country"]
        else:
            print(f"{link['link_id']} is has not foreign bus")

        return country

    sources = config.datasets()["gas_neighbours"]["sources"]

    gas_pipelines_list_CH4 = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND country != 'RU'
                        AND carrier = 'CH4'
                        AND scn_name = 'eGon100RE')
                    AND "bus1" IN (SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = 'CH4' 
                        AND scn_name = 'eGon100RE'))
                OR ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = 'CH4'
                        AND scn_name = 'eGon100RE')
                AND "bus1" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND country != 'RU'
                        AND carrier = 'CH4' 
                        AND scn_name = 'eGon100RE'))
        AND scn_name = 'eGon100RE'
        AND carrier = 'CH4'
        """,
        epsg=4326,
    )

    gas_nodes_list_100 = db.select_geodataframe(
        f"""
        SELECT * FROM {sources['buses']['schema']}.{sources['buses']['table']}
        WHERE scn_name = 'eGon100RE'
        AND carrier = 'CH4'
        AND country <> 'RU'
        """,
        epsg=4326,
    )

    foreign_bus = gas_nodes_list_100[
        gas_nodes_list_100.country != "DE"
    ].set_index("bus_id")
    gas_pipelines_list_CH4 = gas_pipelines_list_CH4[
        [
            "length",
            "geom",
            "topo",
            "bus0",
            "bus1",
            "p_min_pu",
            "carrier",
            "scn_name",
            "link_id",
        ]
    ]

    gas_pipelines_list_CH4["country"] = gas_pipelines_list_CH4.apply(
        lambda x: set_foreign_country(x, foreign=foreign_bus), axis=1
    )

    return gas_pipelines_list_CH4


def read_DE_crossbordering_cap_from_pes():
    """Read gas pipelines crossbordering capacities from pes run

    This function calculates the crossbordering total exchange
    capactities for H2 and CH4 between Germany and its neighbouring
    countries based on the pypsa-eur-sec results.

    Returns
    -------
    DE_pipe_capacities_list : pandas.DataFrame
        List of the H2 and CH4 exchange capacity for each neighbouring
        country of Germany.

    """
    n = read_network()

    DE_pipe_capacities_list_H2 = n.links[
        (n.links["carrier"] == "H2 pipeline retrofitted")
        & ((n.links["bus0"] == "DE0 0 H2") | (n.links["bus1"] == "DE0 0 H2"))
    ]

    DE_pipe_capacities_list_CH4 = n.links[
        (n.links["carrier"] == "gas pipeline")
        & ((n.links["bus0"] == "DE0 0 gas") | (n.links["bus1"] == "DE0 0 gas"))
    ]

    pipe_capacities_list = pd.DataFrame(
        columns=["p_nom", "carrier", "country_code"]
    )
    for DE_pipe_capacities_list in [
        DE_pipe_capacities_list_H2,
        DE_pipe_capacities_list_CH4,
    ]:

        DE_pipe_capacities_list = DE_pipe_capacities_list[
            ["bus0", "bus1", "p_nom_opt", "carrier"]
        ].rename(columns={"p_nom_opt": "p_nom"})

        DE_pipe_capacities_list["country_code"] = (
            DE_pipe_capacities_list.apply(
                lambda row: str(sorted([row.bus0[:2], row.bus1[:2]])), axis=1
            )
        )

        DE_pipe_capacities_list = DE_pipe_capacities_list.drop(
            columns=[
                "bus0",
                "bus1",
            ]
        )

        DE_pipe_capacities_list = DE_pipe_capacities_list.groupby(
            ["country_code"], as_index=False
        ).agg({"p_nom": "sum", "carrier": "first"})

        pipe_capacities_list = pd.concat(
            [pipe_capacities_list, DE_pipe_capacities_list]
        )

    map_countries = {
        "['AT', 'DE']": "AT",
        "['BE', 'DE']": "BE",
        "['CH', 'DE']": "CH",
        "['CZ', 'DE']": "CZ",
        "['DE', 'DK']": "DK",
        "['DE', 'FR']": "FR",
        "['DE', 'LU']": "LU",
        "['DE', 'NL']": "NL",
        "['DE', 'NO']": "NO",
        "['DE', 'PL']": "PL",
    }

    pipe_capacities_list["country_code"] = pipe_capacities_list[
        "country_code"
    ].replace(map_countries)
    pipe_capacities_list["carrier"] = pipe_capacities_list["carrier"].replace(
        {
            "H2 pipeline retrofitted": "H2_retrofit",
            "gas pipeline": "CH4",
        }
    )

    return pipe_capacities_list


def calculate_crossbordering_gas_grid_capacities_eGon100RE(
    cap_DE, DE_pipe_capacities_list
):
    """Attribute gas crossbordering grid capacities for eGon100RE

    This function attributes to each crossbordering pipeline (H2 and
    CH4) between Germany and its neighbouring countries its capacity.

    Parameters
    ----------
    cap_DE : pandas.DataFrame
        List of the H2 and CH4 exchange capacity for each neighbouring
        country of Germany.
    DE_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering for H2 and CH4 pipelines between
        Germany and its neighbouring countries in eGon100RE, with
        geometry (geom and topo) but no capacity.

    Returns
    -------
    Crossbordering_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering H2 and CH4 pipelines between
        Germany and its neighbouring countries in eGon100RE.

    """
    Crossbordering_pipe_capacities_list = pd.DataFrame(
        columns=[
            "length",
            "geom",
            "topo",
            "bus0",
            "bus1",
            "carrier",
            "scn_name",
            "link_id",
            "p_nom",
        ]
    )

    for carrier in ["CH4"]:
        p_nom = []
        cap = cap_DE[cap_DE["carrier"] == carrier].set_index("country_code")
        pipe_capacities_list = DE_pipe_capacities_list[
            DE_pipe_capacities_list["carrier"] == carrier
        ]

        for c in pipe_capacities_list["country"].to_list():
            n_links = len(
                pipe_capacities_list[
                    pipe_capacities_list["country"] == c
                ].index
            )
            p_nom.append(cap.at[c, "p_nom"] / n_links)

        pipe_capacities_list["p_nom"] = p_nom
        pipe_capacities_list = pipe_capacities_list.drop(columns={"country"})
        Crossbordering_pipe_capacities_list = pd.concat(
            [Crossbordering_pipe_capacities_list, pipe_capacities_list]
        )

    return Crossbordering_pipe_capacities_list
