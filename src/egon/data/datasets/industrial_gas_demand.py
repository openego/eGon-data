# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas industrial demand
"""
from pathlib import Path
import os

from geoalchemy2.types import Geometry
from shapely import wkt
import numpy as np
import pandas as pd
import requests

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.etrago_helpers import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.pypsaeursec import read_network
from egon.data.datasets.scenario_parameters import get_sector_parameters


class IndustrialGasDemand(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemand",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(download_industrial_gas_demand),
        )


class IndustrialGasDemandeGon2035(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemandeGon2035",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand_egon2035),
        )


class IndustrialGasDemandeGon100RE(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemandeGon100RE",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand_egon100RE),
        )


def read_industrial_demand(scn_name, carrier):
    """Read the gas demand data

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier : str
        Name of the gas carrier

    Returns
    -------
    df : pandas.core.frame.DataFrame
        Dataframe containing the industrial demand

    """
    target_file = Path(".") / "datasets/gas_data/demand/region_corr.json"
    df_corr = pd.read_json(target_file)
    df_corr = df_corr.loc[:, ["id_region", "name_short"]]
    df_corr.set_index("id_region", inplace=True)

    target_file = (
        Path(".")
        / "datasets/gas_data/demand"
        / (carrier + "_" + scn_name + ".json")
    )
    industrial_loads = pd.read_json(target_file)
    industrial_loads = industrial_loads.loc[:, ["id_region", "values"]]
    industrial_loads.set_index("id_region", inplace=True)

    # Match the id_region to obtain the NUT3 region names
    industrial_loads_list = pd.concat(
        [industrial_loads, df_corr], axis=1, join="inner"
    )
    industrial_loads_list["NUTS0"] = (industrial_loads_list["name_short"].str)[
        0:2
    ]
    industrial_loads_list["NUTS1"] = (industrial_loads_list["name_short"].str)[
        0:3
    ]
    industrial_loads_list = industrial_loads_list[
        industrial_loads_list["NUTS0"].str.match("DE")
    ]

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

        industrial_loads_list = industrial_loads_list[
            industrial_loads_list["NUTS1"].isin([map_states[boundary], np.nan])
        ]

    industrial_loads_list = industrial_loads_list.rename(
        columns={"name_short": "nuts3", "values": "p_set"}
    )
    industrial_loads_list = industrial_loads_list.set_index("nuts3")

    # Add the centroid point to each NUTS3 area
    sql_vg250 = """SELECT nuts as nuts3, geometry as geom
                    FROM boundaries.vg250_krs
                    WHERE gf = 4 ;"""
    gdf_vg250 = db.select_geodataframe(sql_vg250, epsg=4326)

    point = []
    for index, row in gdf_vg250.iterrows():
        point.append(wkt.loads(str(row["geom"])).centroid)
    gdf_vg250["point"] = point
    gdf_vg250 = gdf_vg250.set_index("nuts3")
    gdf_vg250 = gdf_vg250.drop(columns=["geom"])

    # Match the load to the NUTS3 points
    industrial_loads_list = pd.concat(
        [industrial_loads_list, gdf_vg250], axis=1, join="inner"
    )
    return industrial_loads_list.rename(
        columns={"point": "geom"}
    ).set_geometry("geom", crs=4326)


def read_and_process_demand(scn_name="eGon2035", carrier=None, grid_carrier=None):
    """Assign the industrial demand in Germany to buses

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    carrier : str
        Name of the carrier, the demand should hold

    grid_carrier : str
        Carrier name of the buses, the demand should be assigned to

    Returns
    -------
    industrial_demand :
        Dataframe containing the industrial demand in Germany

    """
    if grid_carrier is None:
        grid_carrier = carrier
    industrial_loads_list = read_industrial_demand(scn_name, carrier)
    number_loads = len(industrial_loads_list)

    # Match to associated gas bus
    industrial_loads_list = db.assign_gas_bus_id(
        industrial_loads_list, scn_name, grid_carrier
    )

    # Add carrier
    industrial_loads_list["carrier"] = carrier

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"], errors="ignore"
    )

    msg = (
        "The number of load changed when assigning to the respective buses."
        f"It should be {number_loads} loads, but only"
        f"{len(industrial_loads_list)} got assigned to buses."
        f"scn_name: {scn_name}, load carrier: {carrier}, carrier of buses to"
        f"connect loads to: {grid_carrier}"
        )
    assert len(industrial_loads_list) == number_loads, msg

    return industrial_loads_list


def delete_old_entries(scn_name):
    """
    Delete loads and load timeseries.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.
    """
    # Clean tables
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load_timeseries
        WHERE "load_id" IN (
            SELECT load_id FROM grid.egon_etrago_load
            WHERE "carrier" IN ('CH4', 'H2') AND
            scn_name = '{scn_name}' AND bus not IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            )
        );
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load
        WHERE "load_id" IN (
            SELECT load_id FROM grid.egon_etrago_load
            WHERE "carrier" IN ('CH4', 'H2') AND
            scn_name = '{scn_name}' AND bus not IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            )
        );
        """
    )


def insert_new_entries(industrial_gas_demand, scn_name):
    """
    Insert loads.

    Parameters
    ----------
    industrial_gas_demand : pandas.DataFrame
        Load data to insert.
    scn_name : str
        Name of the scenario.
    """

    new_id = db.next_etrago_id("load")
    industrial_gas_demand["load_id"] = range(
        new_id, new_id + len(industrial_gas_demand)
    )

    # Add missing columns
    c = {"scn_name": scn_name, "sign": -1}
    industrial_gas_demand = industrial_gas_demand.assign(**c)

    industrial_gas_demand = industrial_gas_demand.reset_index(drop=True)

    # Remove useless columns
    egon_etrago_load_gas = industrial_gas_demand.drop(columns=["p_set"])

    engine = db.engine()
    # Insert data to db
    egon_etrago_load_gas.to_sql(
        "egon_etrago_load",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )

    return industrial_gas_demand


def insert_industrial_gas_demand_egon2035():
    """Insert list of industrial gas demand (one per NUTS3) in database

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
        industrial_gas_demand : Dataframe containing the industrial gas demand
        in Germany
    """
    scn_name = "eGon2035"
    delete_old_entries(scn_name)

    industrial_gas_demand = pd.concat(
        [
            read_and_process_demand(scn_name=scn_name, carrier="CH4"),
            read_and_process_demand(
                scn_name=scn_name, carrier="H2", grid_carrier="H2_grid"
            ),
        ]
    )

    industrial_gas_demand = (
        industrial_gas_demand.groupby(["bus", "carrier"])["p_set"]
        .apply(lambda x: [sum(y) for y in zip(*x)])
        .reset_index(drop=False)
    )

    industrial_gas_demand = insert_new_entries(industrial_gas_demand, scn_name)
    insert_industrial_gas_demand_time_series(industrial_gas_demand)


def insert_industrial_gas_demand_egon100RE():
    """Insert list of industrial gas demand (one per NUTS3) in database

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
        industrial_gas_demand : Dataframe containing the industrial gas demand
        in Germany
    """
    scn_name = "eGon100RE"
    delete_old_entries(scn_name)

    # read demands
    industrial_gas_demand_CH4 = read_and_process_demand(
        scn_name=scn_name, carrier="CH4"
    )
    industrial_gas_demand_H2 = read_and_process_demand(
        scn_name=scn_name, carrier="H2", grid_carrier="H2_grid"
    )

    # adjust H2 and CH4 total demands (values from PES)
    # CH4 demand = 0 in 100RE, therefore scale H2 ts
    # fallback values see https://github.com/openego/eGon-data/issues/626
    n = read_network()

    try:
        H2_total_PES = (
            n.loads[n.loads["carrier"] == "H2 for industry"].loc[
                "DE0 0 H2 for industry", "p_set"
            ]
            * 8760
        )
    except KeyError:
        H2_total_PES = 42090000
        print("Could not find data from PES-run, assigning fallback number.")

    try:
        CH4_total_PES = (
            n.loads[n.loads["carrier"] == "gas for industry"].loc[
                "DE0 0 gas for industry", "p_set"
            ]
            * 8760
        )
    except KeyError:
        CH4_total_PES = 105490000
        print("Could not find data from PES-run, assigning fallback number.")

    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary != "Everything":
        # modify values for test mode
        # the values are obtained by evaluating the share of H2 demand in
        # test region (NUTS1: DEF, Schleswig-Holstein) with respect to the H2
        # demand in full Germany model (NUTS0: DE). The task has been outsourced
        # to save processing cost
        H2_total_PES *= 0.01855683050330346
        CH4_total_PES *= 0.01855683050330346

    H2_total = industrial_gas_demand_H2["p_set"].apply(sum).astype(float).sum()

    industrial_gas_demand_CH4["p_set"] = industrial_gas_demand_H2[
        "p_set"
    ].apply(lambda x: [val / H2_total * CH4_total_PES for val in x])
    industrial_gas_demand_H2["p_set"] = industrial_gas_demand_H2[
        "p_set"
    ].apply(lambda x: [val / H2_total * H2_total_PES for val in x])

    # consistency check
    total_CH4_distributed = sum(
        [sum(x) for x in industrial_gas_demand_CH4["p_set"].to_list()]
    )
    total_H2_distributed = sum(
        [sum(x) for x in industrial_gas_demand_H2["p_set"].to_list()]
    )

    print(
        f"Total amount of industrial H2 demand distributed is "
        f"{total_H2_distributed} MWh. Total amount of industrial CH4 demand "
        f"distributed is {total_CH4_distributed} MWh."
    )
    msg = (
        f"Total amount of industrial H2 demand from P-E-S is equal to "
        f"{H2_total_PES}, which should be identical to the distributed amount "
        f"of {total_H2_distributed}, but it is not."
    )
    assert round(H2_total_PES) == round(total_H2_distributed), msg

    msg = (
        f"Total amount of industrial CH4 demand from P-E-S is equal to "
        f"{CH4_total_PES}, which should be identical to the distributed amount "
        f"of {total_CH4_distributed}, but it is not."
    )
    assert round(CH4_total_PES) == round(total_CH4_distributed), msg

    industrial_gas_demand = pd.concat(
        [
            industrial_gas_demand_CH4,
            industrial_gas_demand_H2,
        ]
    )
    industrial_gas_demand = (
        industrial_gas_demand.groupby(["bus", "carrier"])["p_set"]
        .apply(lambda x: [sum(y) for y in zip(*x)])
        .reset_index(drop=False)
    )

    industrial_gas_demand = insert_new_entries(industrial_gas_demand, scn_name)
    insert_industrial_gas_demand_time_series(industrial_gas_demand)


def insert_industrial_gas_demand_time_series(egon_etrago_load_gas):
    """
    Insert list of industrial gas demand time series (one per NUTS3)
    """
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas

    # Connect to local database
    engine = db.engine()

    # Adjust columns
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas_timeseries.drop(
        columns=["carrier", "bus", "sign"]
    )
    egon_etrago_load_gas_timeseries["temp_id"] = 1

    # Insert data to db
    egon_etrago_load_gas_timeseries.to_sql(
        "egon_etrago_load_timeseries",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )


def download_industrial_gas_demand():
    """Download the industrial gas demand data from opendata.ffe database."""
    correspondance_url = (
        "http://opendata.ffe.de:3000/region?id_region_type=eq.38"
    )

    # Read and save data
    result_corr = requests.get(correspondance_url)
    target_file = Path(".") / "datasets/gas_data/demand/region_corr.json"
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    pd.read_json(result_corr.content).to_json(target_file)

    carriers = {"H2": "2,162", "CH4": "2,11"}
    url = "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."

    for scn_name in ["eGon2035", "eGon100RE"]:
        year = str(
            get_sector_parameters("global", scn_name)["population_year"]
        )

        for carrier, internal_id in carriers.items():
            # Download the data
            datafilter = "&&internal_id=eq.{" + internal_id + "}"
            request = url + year + datafilter

            # Read and save data
            result = requests.get(request)
            target_file = (
                Path(".")
                / "datasets/gas_data/demand"
                / (carrier + "_" + scn_name + ".json")
            )
            pd.read_json(result.content).to_json(target_file)
