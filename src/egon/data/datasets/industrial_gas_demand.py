# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas industrial demand
"""
from shapely import wkt
import numpy as np
import pandas as pd
import requests

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.gas_prod import assign_ch4_bus_id, assign_h2_bus_id
from egon.data.datasets.scenario_parameters import get_sector_parameters


class IndustrialGasDemand(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemand",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand),
        )


def download_CH4_industrial_demand(scn_name="eGon2035"):
    """Download the CH4 industrial demand in Germany from the FfE open data portal

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    CH4_industrial_demand :
        Dataframe containing the CH4 industrial demand in Germany

    """
    # Download the data and the id_region correspondance table
    correspondance_url = (
        "http://opendata.ffe.de:3000/region?id_region_type=eq.38"
    )
    url = "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."

    year = str(get_sector_parameters("global", scn_name)["population_year"])

    internal_id = "2,11"  # Natural_Gas
    datafilter = "&&internal_id=eq.{" + internal_id + "}"
    request = url + year + datafilter

    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)
    industrial_loads_list = industrial_loads_list[
        ["id_region", "values"]
    ].copy()
    industrial_loads_list = industrial_loads_list.set_index("id_region")

    result_corr = requests.get(correspondance_url)
    df_corr = pd.read_json(result_corr.content)
    df_corr = df_corr[["id_region", "name_short"]].copy()
    df_corr = df_corr.set_index("id_region")

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
    industrial_loads_list = industrial_loads_list.rename(
        columns={"point": "geom"}
    ).set_geometry("geom", crs=4326)

    # Match to associated gas bus
    industrial_loads_list = assign_ch4_bus_id(industrial_loads_list, scn_name)

    # Add carrier
    c = {"carrier": "CH4"}
    industrial_loads_list = industrial_loads_list.assign(**c)

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"]
    )

    return industrial_loads_list


def download_H2_industrial_demand(scn_name="eGon2035"):
    """Download the H2 industrial demand in Germany from the FfE open data portal

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    H2_industrial_demand :
        Dataframe containing the H2 industrial demand in Germany

    """
    # Download the data and the id_region correspondance table
    correspondance_url = (
        "http://opendata.ffe.de:3000/region?id_region_type=eq.38"
    )
    url = "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."

    year = str(get_sector_parameters("global", scn_name)["population_year"])

    internal_id = "2,162"  # Hydrogen
    datafilter = "&&internal_id=eq.{" + internal_id + "}"
    request = url + year + datafilter

    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)
    industrial_loads_list = industrial_loads_list[
        ["id_region", "values"]
    ].copy()
    industrial_loads_list = industrial_loads_list.set_index("id_region")

    result_corr = requests.get(correspondance_url)
    df_corr = pd.read_json(result_corr.content)
    df_corr = df_corr[["id_region", "name_short"]].copy()
    df_corr = df_corr.set_index("id_region")

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
    industrial_loads_list = industrial_loads_list.rename(
        columns={"point": "geom"}
    ).set_geometry("geom", crs=4326)

    # Match to associated gas bus
    industrial_loads_list = assign_h2_bus_id(industrial_loads_list, scn_name)

    # Add carrier
    c = {"carrier": "H2"}
    industrial_loads_list = industrial_loads_list.assign(**c)

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"]
    )

    return industrial_loads_list


def import_industrial_gas_demand(scn_name="eGon2035"):
    """Insert list of industrial gas demand (one per NUTS3) in database

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
        industrial_gas_demand : Dataframe
            Dataframe containing the industrial gas demand in Germany
        load_id : list
            IDs of the loads that should be aggregated
        load_id_grouped :list
            List of lists containing the load ID that belong together
    """
    # Connect to local database
    engine = db.engine()

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load WHERE "carrier" IN ('CH4', 'H2') AND
        scn_name = '{scn_name}' AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn_name}' AND country = 'DE'
        );
        """
    )

    # Select next id value
    new_id = db.next_etrago_id("load")

    industrial_gas_demand = pd.concat(
        [
            download_CH4_industrial_demand(scn_name=scn_name),
            download_H2_industrial_demand(scn_name=scn_name),
        ]
    )
    industrial_gas_demand["load_id"] = range(
        new_id, new_id + len(industrial_gas_demand)
    )

    # Add missing columns
    c = {"scn_name": scn_name, "sign": -1}
    industrial_gas_demand = industrial_gas_demand.assign(**c)

    industrial_gas_demand = industrial_gas_demand.reset_index(drop=True)

    # Remove useless columns
    egon_etrago_load_gas = industrial_gas_demand.drop(columns=["p_set"])

    # Aggregate load with same properties at the same bus
    print(egon_etrago_load_gas)
    gas_loads_duplicated_buses = (
        egon_etrago_load_gas[
            egon_etrago_load_gas.duplicated(
                subset=["bus", "carrier"], keep=False
            )
        ]["bus"]
        .drop_duplicates()
        .tolist()
    )
    load_id = egon_etrago_load_gas[
        egon_etrago_load_gas["bus"].isin(gas_loads_duplicated_buses)
    ]["load_id"].tolist()
    load_id_grouped = []
    for i in gas_loads_duplicated_buses:
        load_id_grouped.append(
            egon_etrago_load_gas[egon_etrago_load_gas["bus"] == i][
                "load_id"
            ].tolist()
        )
    print(load_id)
    print(load_id_grouped)

    strategies = {
        "scn_name": "first",
        "load_id": "min",
        "sign": "first",
        "bus": "first",
        "carrier": "first",
    }

    egon_etrago_load_gas = egon_etrago_load_gas.groupby(
        ["bus", "carrier"]
    ).agg(strategies)
    print(egon_etrago_load_gas)

    # Insert data to db
    egon_etrago_load_gas.to_sql(
        "egon_etrago_load",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )

    return industrial_gas_demand, load_id, load_id_grouped


def import_industrial_gas_demand_time_series(
    egon_etrago_load_gas, load_id, load_id_grouped, scn_name="eGon2035"
):
    """Insert list of industrial gas demand time series (one per NUTS3) in database

    Parameters
    ----------
    industrial_gas_demand : Dataframe
            Dataframe containing the industrial gas demand in Germany
        load_id : list
            IDs of the loads that should be aggregated
        load_id_grouped :list
            List of lists containing the load ID that belong together

    Returns
    -------
    None.
    """
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas

    # Connect to local database
    engine = db.engine()

    # Adjust columns
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas_timeseries.drop(
        columns=["carrier", "bus", "sign"]
    )
    egon_etrago_load_gas_timeseries["temp_id"] = 1

    # Aggregate load with same properties at the same bus
    df_aggregated = pd.DataFrame(
        [], columns=["p_set", "load_id", "scn_name", "temp_id"]
    )
    for i in range(len(load_id_grouped)):
        df_filtered = egon_etrago_load_gas_timeseries[
            egon_etrago_load_gas_timeseries["load_id"].isin(load_id_grouped[i])
        ]
        #    print(df_filtered)
        TS = []
        load_id = []
        for index, row in df_filtered.iterrows():
            TS.append(row["p_set"])
            load_id.append(row["load_id"])
        df_aggregated.loc[i] = [
            np.array(TS).sum(axis=0).tolist(),
            np.array(load_id).min(),
            scn_name,
            1,
        ]
    print(df_aggregated)
    # delete useless rows in egon_etrago_load_gas_timeseries
    print(egon_etrago_load_gas_timeseries)
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas_timeseries.drop(
        egon_etrago_load_gas_timeseries[
            egon_etrago_load_gas_timeseries["load_id"].isin(load_id)
        ].index
    )
    print(egon_etrago_load_gas_timeseries)
    # add df_aggregated
    egon_etrago_load_gas_timeseries = egon_etrago_load_gas_timeseries.append(
        df_aggregated, ignore_index=True
    )

    print(egon_etrago_load_gas_timeseries)
    # Insert data to db
    egon_etrago_load_gas_timeseries.to_sql(
        "egon_etrago_load_timeseries",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )


def insert_industrial_gas_demand():
    """Overall function for inserting the industrial gas demand

    Returns
    -------
    None.
    """

    (
        egon_etrago_load_gas,
        load_id,
        load_id_grouped,
    ) = import_industrial_gas_demand(scn_name="eGon2035")

    import_industrial_gas_demand_time_series(
        egon_etrago_load_gas, load_id, load_id_grouped, scn_name="eGon2035"
    )
