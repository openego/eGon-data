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
from egon.data.datasets.ch4_prod import assign_bus_id
from egon.data.datasets.etrago_helpers import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.datasets.etrago_setup import link_geom_from_buses
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
            version="0.0.1",
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand_egon2035),
        )


class IndustrialGasDemandeGon100RE(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemandeGon100RE",
            version="0.0.1",
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
    df_corr = df_corr[["id_region", "name_short"]].copy()
    df_corr = df_corr.set_index("id_region")

    target_file = (
        Path(".")
        / "datasets/gas_data/demand"
        / (carrier + "_" + scn_name + ".json")
    )
    industrial_loads_list = pd.read_json(target_file)
    industrial_loads_list = industrial_loads_list[
        ["id_region", "values"]
    ].copy()
    industrial_loads_list = industrial_loads_list.set_index("id_region")

    # Match the id_region to obtain the NUT3 region names
    industrial_loads_list = pd.concat(
        [industrial_loads_list, df_corr], axis=1, join="inner"
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


def read_industrial_CH4_demand(scn_name="eGon2035"):
    """Download the CH4 industrial demand in Germany from the FfE open data portal

    Parameters
    ----------
    df_corr : pandas.core.frame.DataFrame
        Correspondance for openffe region to NUTS
    scn_name : str
        Name of the scenario

    Returns
    -------
    CH4_industrial_demand :
        Dataframe containing the CH4 industrial demand in Germany

    """
    carrier = "CH4"
    industrial_loads_list = read_industrial_demand(scn_name, carrier)

    # Match to associated gas bus
    industrial_loads_list = assign_bus_id(
        industrial_loads_list, scn_name, carrier
    )

    # Add carrier
    industrial_loads_list["carrier"] = carrier

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"], errors="ignore"
    )

    return industrial_loads_list


def read_industrial_H2_demand(scn_name="eGon2035"):
    """Download the H2 industrial demand in Germany from the FfE open data portal

    Parameters
    ----------
    df_corr : pandas.core.frame.DataFrame
        Correspondance for openffe region to NUTS
    scn_name : str
        Name of the scenario

    Returns
    -------
    H2_industrial_demand :
        Dataframe containing the H2 industrial demand in Germany

    """
    industrial_loads_list = read_industrial_demand(scn_name, "H2")
    industrial_loads_list_copy = industrial_loads_list.copy()
    # Match to associated gas bus
    industrial_loads_list = assign_bus_id(
        industrial_loads_list, scn_name, "H2_grid"
    )
    industrial_loads_saltcavern = assign_bus_id(
        industrial_loads_list_copy, scn_name, "H2_saltcavern"
    )

    saltcavern_buses = (
        db.select_geodataframe(
            f"""
            SELECT * FROM grid.egon_etrago_bus WHERE carrier = 'H2_saltcavern'
            AND scn_name = '{scn_name}'
        """
        )
        .to_crs(epsg=3035)
        .set_index("bus_id")
    )

    nearest_saltcavern_buses = saltcavern_buses.loc[
        industrial_loads_saltcavern["bus_id"]
    ].geometry

    industrial_loads_saltcavern[
        "distance"
    ] = industrial_loads_saltcavern.to_crs(epsg=3035).distance(
        nearest_saltcavern_buses, align=False
    )

    new_connections = industrial_loads_saltcavern[
        industrial_loads_saltcavern["distance"] <= 10000
    ]
    num_new_connections = len(new_connections)

    if num_new_connections > 0:

        carrier = "H2_ind_load"
        target = {"schema": "grid", "table": "egon_etrago_bus"}
        bus_gdf = initialise_bus_insertion(carrier, target, scenario=scn_name)

        bus_gdf["geom"] = new_connections["geom"]

        bus_gdf = finalize_bus_insertion(
            bus_gdf, carrier, target, scenario=scn_name
        )

        # Delete existing buses
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_link
            WHERE scn_name = '{scn_name}'
            AND carrier = '{carrier}'
            """
        )

        # initalize dataframe for new buses
        grid_links = pd.DataFrame(
            columns=["scn_name", "link_id", "bus0", "bus1", "carrier"]
        )

        grid_links["bus0"] = industrial_loads_list.loc[bus_gdf.index]["bus_id"]
        grid_links["bus1"] = bus_gdf["bus_id"]
        grid_links["p_nom"] = 1e9
        grid_links["carrier"] = carrier
        grid_links["scn_name"] = scn_name

        cavern_links = grid_links.copy()

        cavern_links["bus0"] = new_connections["bus_id"]

        engine = db.engine()
        for table in [grid_links, cavern_links]:
            new_id = db.next_etrago_id("link")
            table["link_id"] = range(new_id, new_id + len(table))

            link_geom_from_buses(table, scn_name).to_postgis(
                "egon_etrago_link",
                engine,
                schema="grid",
                index=False,
                if_exists="append",
                dtype={"topo": Geometry()},
            )

        industrial_loads_list.loc[bus_gdf.index, "bus"] = bus_gdf["bus_id"]

    # Add carrier
    c = {"carrier": "H2"}
    industrial_loads_list = industrial_loads_list.assign(**c)

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"], errors="ignore"
    )

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
            read_industrial_CH4_demand(scn_name=scn_name),
            read_industrial_H2_demand(scn_name=scn_name),
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

    # concatenate loads
    industrial_gas_demand_CH4 = read_industrial_CH4_demand(scn_name=scn_name)
    industrial_gas_demand_H2 = read_industrial_H2_demand(scn_name=scn_name)

    # adjust H2 and CH4 total demands (values from PES)
    # CH4 demand = 0 in 100RE, therefore scale H2 ts

    # see https://github.com/openego/eGon-data/issues/626
    # On test mode data are stupidly incorrect, since PES data is for whole Germany
    CH4_total_PES = 105490000
    H2_total_PES = 42090000
    H2_total = industrial_gas_demand_H2["p_set"].apply(sum).astype(float).sum()
    industrial_gas_demand_CH4["p_set"] = industrial_gas_demand_H2[
        "p_set"
    ].apply(lambda x: [val / H2_total * CH4_total_PES for val in x])
    industrial_gas_demand_H2["p_set"] = industrial_gas_demand_H2[
        "p_set"
    ].apply(lambda x: [val / H2_total * H2_total_PES for val in x])

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
