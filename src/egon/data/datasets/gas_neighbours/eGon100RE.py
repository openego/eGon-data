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
from egon.data.datasets.pypsaeursec import read_network

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

    This function defines the crossbordering pipelines (for H2 and CH4)
    between Germany and its neighbouring countries. These pipelines
    are defined as links and there are copied from the corresponding
    CH4 crossbering pipelines from eGon2035.

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
    sources = config.datasets()["gas_neighbours"]["sources"]

    gas_pipelines_list = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = 'CH4'
                        AND scn_name = 'eGon2035')
                    AND "bus1" IN (SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = 'CH4' 
                        AND scn_name = 'eGon2035'))
                OR ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = 'CH4'
                        AND scn_name = 'eGon2035')
                AND "bus1" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = 'CH4' 
                        AND scn_name = 'eGon2035'))
        AND scn_name = 'eGon2035'
        AND carrier = 'CH4'
        """,
        epsg=4326,
    )

    # Insert bus0 and bus1
    gas_pipelines_list = gas_pipelines_list[
        ["bus0", "bus1", "length", "geom", "topo"]
    ].rename(columns={"bus0": "bus0_2035", "bus1": "bus1_2035"})

    gas_nodes_list_2035 = db.select_geodataframe(
        f"""
        SELECT * FROM {sources['buses']['schema']}.{sources['buses']['table']}
        WHERE scn_name = 'eGon2035'
        AND carrier = 'CH4'
        """,
        epsg=4326,
    )

    busID_table = gas_nodes_list_2035[["geom", "bus_id", "country"]].rename(
        columns={"bus_id": "bus_id_CH4_2035"}
    )
    gas_pipelines_list_DE = pd.DataFrame(
        columns=["length", "geom", "topo", "bus0", "bus1", "carrier"]
    )

    for carrier in ["H2", "CH4"]:
        if carrier == "CH4":
            carrier_bus_DE = carrier
        elif carrier == "H2":
            carrier_bus_DE = "H2_grid"

        busID_table_DE = db.assign_gas_bus_id(
            busID_table[busID_table["country"] == "DE"],
            scn_name,
            carrier_bus_DE,
        ).set_index("bus_id_CH4_2035")

        gas_nodes_abroad_100RE = db.select_geodataframe(
            f"""
            SELECT * FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE'
            AND carrier = '{carrier}'
            AND country != 'DE'
            """,
            epsg=4326,
        )

        buses = busID_table[busID_table["country"] != "DE"]
        buses["bus_id"] = 0

        # Select bus_id from db
        for i, row in buses.iterrows():
            distance = gas_nodes_abroad_100RE.set_index(
                "bus_id"
            ).geom.distance(row.geom)
            buses.loc[i, "bus_id"] = distance[
                distance == distance.min()
            ].index.values[0]

        buses = buses.set_index("bus_id_CH4_2035")

        bus0 = []
        bus1 = []
        country = []

        for b0 in gas_pipelines_list["bus0_2035"].to_list():
            if b0 in busID_table_DE.index.to_list():
                bus0.append(int(busID_table_DE.loc[b0, "bus_id"]))
            else:
                bus0.append(int(buses.loc[b0, "bus_id"]))
                country.append(buses.loc[b0, "country"])
        for b1 in gas_pipelines_list["bus1_2035"].to_list():
            if b1 in busID_table_DE.index.to_list():
                bus1.append(int(busID_table_DE.loc[b1, "bus_id"]))
            else:
                bus1.append(int(buses.loc[b1, "bus_id"]))
                country.append(buses.loc[b1, "country"])

        gas_pipelines_list["bus0"] = bus0
        gas_pipelines_list["bus1"] = bus1
        gas_pipelines_list["country"] = country

        # Insert carrier
        if carrier == "CH4":
            carrier_pipes = carrier
        elif carrier == "H2":
            carrier_pipes = "H2_retrofit"
        gas_pipelines_list["carrier"] = carrier_pipes

        gas_pipelines_list_DE = gas_pipelines_list_DE.append(
            gas_pipelines_list, ignore_index=True
        )

    gas_pipelines_list_DE["scn_name"] = scn_name

    # Select next id value
    new_id = db.next_etrago_id("link")
    gas_pipelines_list_DE["link_id"] = range(
        new_id, new_id + len(gas_pipelines_list_DE)
    )
    gas_pipelines_list_DE["link_id"] = gas_pipelines_list_DE["link_id"].astype(
        int
    )
    gas_pipelines_list_DE = gas_pipelines_list_DE.drop(
        columns={"bus0_2035", "bus1_2035"}
    )

    return gas_pipelines_list_DE


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

        DE_pipe_capacities_list[
            "country_code"
        ] = DE_pipe_capacities_list.apply(
            lambda row: str(sorted([row.bus0[:2], row.bus1[:2]])), axis=1
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

        pipe_capacities_list = pipe_capacities_list.append(
            DE_pipe_capacities_list, ignore_index=True
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

    for carrier in ["CH4", "H2_retrofit"]:
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
        Crossbordering_pipe_capacities_list = (
            Crossbordering_pipe_capacities_list.append(pipe_capacities_list)
        )

    return Crossbordering_pipe_capacities_list
