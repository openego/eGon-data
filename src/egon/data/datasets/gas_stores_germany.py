# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas stores

In this module, the non extendable H2 and CH4 stores in Germany are
defined and inserted to the database.

Dependecies (pipeline)
======================
* :dataset: GasAreaseGon2035, HydrogenBusEtrago, GasAreaseGon100RE

Resulting tables
================
* grid.egon_etrago_store is completed

"""
from pathlib import Path
from telnetlib import GA
import ast

import geopandas
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.gas_grid import (
    ch4_nodes_number_G,
    define_gas_nodes_list,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters


class GasStores(Dataset):
    "Insert the non extendable gas stores in Germany in the database"

    def __init__(self, dependencies):
        super().__init__(
            name="GasStores",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(insert_gas_stores_DE),
        )


def import_installed_ch4_storages(scn_name):
    """Define list of CH4 stores (caverns) from the SciGRID_gas data

    This function define the dataframe containing the CH4 cavern stores
    in Germany from the SciGRID_gas data for both scenarios.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the CH4 cavern stores units in Germany

    """
    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Storages.csv"
    )

    Gas_storages_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["lat", "long", "country_code", "param"],
    )

    Gas_storages_list = Gas_storages_list[
        Gas_storages_list["country_code"].str.match("DE")
    ]

    # Define new columns
    max_workingGas_M_m3 = []
    NUTS1 = []
    end_year = []
    for index, row in Gas_storages_list.iterrows():
        param = ast.literal_eval(row["param"])
        NUTS1.append(param["nuts_id_1"])
        end_year.append(param["end_year"])
        max_workingGas_M_m3.append(param["max_workingGas_M_m3"])

    Gas_storages_list = Gas_storages_list.assign(NUTS1=NUTS1)

    # Calculate e_nom
    conv_factor = 10830  # gross calorific value = 39 MJ/m3 (eurogas.org)
    Gas_storages_list["e_nom"] = [conv_factor * i for i in max_workingGas_M_m3]

    end_year = [float("inf") if x == None else x for x in end_year]
    Gas_storages_list = Gas_storages_list.assign(end_year=end_year)

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
    Gas_storages_list = Gas_storages_list[
        Gas_storages_list["end_year"] >= 2035
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
        ]
    )

    return Gas_storages_list


def import_gas_grid_capacity(scn_name, carrier):
    """Define the gas stores modelling the store capacity of the grid

    Define dataframe containing the modelling of the grid storage
    capacity. The whole storage capacity of the grid (130000 MWh,
    estimation of the Bundesnetzagentur) is split uniformly between
    all the german gas nodes of the grid.
    In eGon100RE, the storage capacity of the grid is slip between H2
    and CH4 stores, with the same share than the pipes capacity (value
    calculated in the p-e-s run).
    The capacities of the pipes are not considerated.

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
    scn_params = get_sector_parameters("gas", scn_name)
    # Select source from dataset configuration
    source = config.datasets()["gas_stores"]["source"]

    Gas_grid_capacity = 130000  # Storage capacity of the CH4 grid - G.Volk "Die Herauforderung an die Bundesnetzagentur die Energiewende zu meistern" Berlin, Dec 2012
    N_ch4_nodes_G = ch4_nodes_number_G(
        define_gas_nodes_list()
    )  # Number of nodes in Germany
    Store_capacity = (
        Gas_grid_capacity / N_ch4_nodes_G
    )  # Storage capacity associated to each CH4 node of the german grid

    sql_gas = f"""SELECT bus_id, geom
                FROM {source['buses']['schema']}.{source['buses']['table']}
                WHERE carrier = '{carrier}' AND scn_name = '{scn_name}'
                AND country = 'DE';"""
    Gas_storages_list = db.select_geodataframe(sql_gas, epsg=4326)

    # Add missing column
    Gas_storages_list["bus"] = Gas_storages_list["bus_id"]

    if scn_name == "eGon100RE" and carrier == "CH4":
        Gas_storages_list["e_nom"] = Store_capacity * (
            1
            - get_sector_parameters("gas", scn_name)[
                "retrofitted_CH4pipeline-to-H2pipeline_share"
            ]
        )
    elif scn_name == "eGon2035" and carrier == "CH4":
        Gas_storages_list["e_nom"] = Store_capacity
    elif scn_name == "eGon100RE" and carrier == "H2_grid":
        Gas_storages_list["e_nom"] = Store_capacity * (
            scn_params["retrofitted_CH4pipeline-to-H2pipeline_share"]
        )

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=["bus_id", "geom"])

    return Gas_storages_list


def insert_gas_stores_germany(scn_name, carrier):
    """Insert gas stores for specific scenario

    Insert non extendable gas stores for specific scenario in Germany
    by executing the following steps:
      * Clean the database
      * For CH4 stores, call the functions
        :py:func:`import_installed_ch4_storages` to receive the CH4
        cavern stores and :py:func:`import_gas_grid_capacity` to
        receive the CH4 stores modelling the storage capacity of the
        grid.
      * For H2 stores, call only the function
        :py:func:`import_gas_grid_capacity` to receive the H2 stores
        modelling the storage capacity of the grid.
      * Aggregate of the stores attached to the same bus
      * Add the missing columns: store_id, scn_name, carrier, e_cyclic
      * Insert the stores in the database

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier: str
        Name of the carrier

    Returns
    -------
    None

    """
    # Connect to local database
    engine = db.engine()

    # Select target from dataset configuration
    source = config.datasets()["gas_stores"]["source"]
    target = config.datasets()["gas_stores"]["target"]

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM {target['stores']['schema']}.{target['stores']['table']}  
        WHERE "carrier" = '{carrier}'
        AND scn_name = '{scn_name}'
        AND bus IN (
            SELECT bus_id FROM {source['buses']['schema']}.{source['buses']['table']}
            WHERE scn_name = '{scn_name}' 
            AND country = 'DE'
            );
        """
    )

    if carrier == "CH4":
        gas_storages_list = pd.concat(
            [
                import_installed_ch4_storages(scn_name),
                import_gas_grid_capacity(scn_name, carrier),
            ]
        )
    elif carrier == "H2":
        gas_storages_list = import_gas_grid_capacity(scn_name, "H2_grid")

    # Aggregate ch4 stores with same properties at the same bus
    gas_storages_list = (
        gas_storages_list.groupby(["bus"])
        .agg({"e_nom": "sum"})
        .reset_index(drop=False)
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": carrier, "e_cyclic": True}
    gas_storages_list = gas_storages_list.assign(**c)

    new_id = db.next_etrago_id("store")
    gas_storages_list["store_id"] = range(
        new_id, new_id + len(gas_storages_list)
    )

    # Insert data to db
    gas_storages_list.to_sql(
        target["stores"]["table"],
        engine,
        schema=target["stores"]["schema"],
        index=False,
        if_exists="append",
    )


def insert_gas_stores_DE():
    """Overall function to import non extendable gas stores in Germany

    This function calls :py:func:`insert_gas_stores_germany` three
    times to insert the corresponding non extendable gas stores in the
    database:
      * The CH4 stores for eGon2035: caverns from SciGRID_gas data and
        modelling of the storage grid capacity
      * The CH4 stores for eGon100RE: caverns from SciGRID_gas data and
        modelling of the storage grid capacity (split with H2)
      * The H2 stores for eGon100RE: caverns from SciGRID_gas data and
        modelling of the storage grid capacity (split with CH4)
    This function has no return.

    """
    insert_gas_stores_germany("eGon2035", "CH4")
    insert_gas_stores_germany("eGon100RE", "CH4")
    insert_gas_stores_germany("eGon100RE", "H2")
