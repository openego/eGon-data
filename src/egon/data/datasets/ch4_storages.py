# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas storages data
"""
from pathlib import Path
import ast
from telnetlib import GA

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
from egon.data.datasets.gas_prod import assign_bus_id


class CH4Storages(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CH4Storages",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(import_ch4_storages),
        )


def import_installed_ch4_storages(scn_name):
    """Define dataframe containing the ch4 storage units in Germany from the SciGRID_gas data

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the gas storages units in Germany

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
    Gas_storages_list = assign_bus_id(Gas_storages_list, scn_name, "CH4")

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
        ]
    )

    return Gas_storages_list


def import_ch4_grid_capacity(scn_name):
    """Define dataframe containing the modelling of the CH4 grid storage
    capacity. The whole storage capacity of the grid (130000 MWh, estimation of
    the Bundesnetzagentur) is split uniformly between all the german CH4 nodes
    of the grid. The capacities of the pipes are not considerated.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    Gas_storages_list :
        Dataframe containing the gas stores in Germany modelling the gas grid storage capacity

    """
    # Select source from dataset configuration
    source = config.datasets()["gas_stores"]["source"]

    Gas_grid_capacity = 130000  # Storage capacity of the CH4 grid - G.Volk "Die Herauforderung an die Bundesnetzagentur die Energiewende zu meistern" Berlin, Dec 2012
    N_ch4_nodes_G = ch4_nodes_number_G(
        define_gas_nodes_list()
    )  # Number of nodes in Germany
    Store_capacity = (
        Gas_grid_capacity / N_ch4_nodes_G
    )  # Storage capacity associated to each CH4 node of the german grid

    sql_gas = f"""SELECT bus_id, scn_name, carrier, geom
                FROM {source['buses']['schema']}.{source['buses']['table']}
                WHERE carrier = 'CH4' AND scn_name = '{scn_name}'
                AND country = 'DE';"""
    Gas_storages_list = db.select_geodataframe(sql_gas, epsg=4326)

    # Add missing column
    Gas_storages_list["e_nom"] = Store_capacity
    Gas_storages_list["bus"] = Gas_storages_list["bus_id"]

    # Remove useless columns
    Gas_storages_list = Gas_storages_list.drop(columns=["bus_id", "geom"])

    return Gas_storages_list


def import_ch4_storages():
    """Insert list of gas storages units in database"""
    # Connect to local database
    engine = db.engine()

    # Select target from dataset configuration
    source = config.datasets()["gas_stores"]["source"]
    target = config.datasets()["gas_stores"]["target"]

    # TODO move this to function call, how to do it is directly called in task list?
    scn_name = "eGon2035"

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

    # Select next id value
    new_id = db.next_etrago_id("store")

    gas_storages_list = pd.concat(
        [
            import_installed_ch4_storages(scn_name),
            import_ch4_grid_capacity(scn_name),
        ]
    )
    gas_storages_list["store_id"] = range(
        new_id, new_id + len(gas_storages_list)
    )

    gas_storages_list = gas_storages_list.reset_index(drop=True)

    # Insert data to db
    gas_storages_list.to_sql(
        target["stores"]["table"],
        engine,
        schema=target["stores"]["schema"],
        index=False,
        if_exists="append",
    )
