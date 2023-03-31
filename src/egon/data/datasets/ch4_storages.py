# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas stores

This module contains the functions to import the existing methane stores
in Germany and to insert them into the database. They are modelled as
PyPSA stores and are not extendable.

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


class CH4Storages(Dataset):
    """
    Insert the non extendable gas stores in Germany into the database

    Insert the non extendable gas stores into the database in Germany
    for the scnenarios eGon2035 and eGon100RE using the function
    :py:func:`insert_ch4_storages`.

    *Dependencies*
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon100RE>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended


    """

    #:
    name: str = "CH4Storages"
    #:
    version: str = "0.0.3"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_ch4_storages),
        )


def import_installed_ch4_storages(scn_name):
    """
    Define list of CH4 stores from the SciGRID_gas data

    This function reads from the SciGRID_gas dataset the existing CH4
    cavern stores in Germany, adjusts and returns them.
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
        Dataframe containing the CH4 cavern stores units in Germany

    """
    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Storages.csv"
    )

    Gas_storages_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["lat", "long", "country_code", "param", "method"],
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
        ]
    )

    return Gas_storages_list


def import_ch4_grid_capacity(scn_name):
    """
    Define the gas stores modelling the store capacity of the grid

    Define dataframe containing the modelling of the grid storage
    capacity. The whole storage capacity of the grid (130000 MWh,
    estimation of the Bundesnetzagentur) is split uniformly between
    all the German gas nodes of the grid (without consideration of the
    capacities of the pipes).
    In eGon100RE, the storage capacity of the grid is split between H2
    and CH4 stores, with the same share than the pipes capacity (value
    calculated in the p-e-s run).

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
    # Select source from dataset configuration
    source = config.datasets()["gas_stores"]["source"]

    Gas_grid_capacity = 130000  # Storage capacity of the CH4 grid - G.Volk "Die Herauforderung an die Bundesnetzagentur die Energiewende zu meistern" Berlin, Dec 2012
    N_ch4_nodes_G = ch4_nodes_number_G(
        define_gas_nodes_list()
    )  # Number of nodes in Germany
    Store_capacity = (
        Gas_grid_capacity / N_ch4_nodes_G
    )  # Storage capacity associated to each CH4 node of the German grid

    sql_gas = f"""SELECT bus_id, scn_name, carrier, geom
                FROM {source['buses']['schema']}.{source['buses']['table']}
                WHERE carrier = 'CH4' AND scn_name = '{scn_name}'
                AND country = 'DE';"""
    Gas_storages_list = db.select_geodataframe(sql_gas, epsg=4326)

    # Add missing column
    Gas_storages_list["bus"] = Gas_storages_list["bus_id"]
    if scn_name == "eGon100RE":
        Gas_storages_list["e_nom"] = Store_capacity * (
            1
            - get_sector_parameters("gas", scn_name)[
                "retrofitted_CH4pipeline-to-H2pipeline_share"
            ]
        )
    else:
        Gas_storages_list["e_nom"] = Store_capacity

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=["bus_id", "geom"])

    return Gas_storages_list


def insert_ch4_stores(scn_name):
    """
    Insert gas stores for specific scenario

    Insert non extendable gas stores for specific scenario in Germany
    by executing the following steps:
      * Clean the database.
      * For CH4 stores, call the functions.
        :py:func:`import_installed_ch4_storages` to get the CH4
        cavern stores and :py:func:`import_ch4_grid_capacity` to
        get the CH4 stores modelling the storage capacity of the
        grid.
      * Aggregate of the stores attached to the same bus.
      * Add the missing columns: store_id, scn_name, carrier, e_cyclic.
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

    # Select target from dataset configuration
    source = config.datasets()["gas_stores"]["source"]
    target = config.datasets()["gas_stores"]["target"]

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM {target['stores']['schema']}.{target['stores']['table']}  
        WHERE "carrier" = 'CH4'
        AND scn_name = '{scn_name}'
        AND bus IN (
            SELECT bus_id FROM {source['buses']['schema']}.{source['buses']['table']}
            WHERE scn_name = '{scn_name}' 
            AND country = 'DE'
            );
        """
    )

    gas_storages_list = pd.concat(
        [
            import_installed_ch4_storages(scn_name),
            import_ch4_grid_capacity(scn_name),
        ]
    )

    # Aggregate ch4 stores with same properties at the same bus
    gas_storages_list = (
        gas_storages_list.groupby(["bus", "carrier", "scn_name"])
        .agg({"e_nom": "sum"})
        .reset_index(drop=False)
    )

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


def insert_ch4_storages():
    """
    Overall function to import non extendable gas stores in Germany

    This function inserts the methane stores in Germany for the
    scenarios eGon2035 and eGon100RE by using the function
    :py:func:`insert_ch4_stores` and has no return.

    """
    insert_ch4_stores("eGon2035")
    insert_ch4_stores("eGon100RE")
