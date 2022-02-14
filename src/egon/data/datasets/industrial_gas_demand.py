# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing gas industrial demand
"""
from geoalchemy2.types import Geometry
from shapely import wkt
import geopandas as gpd
import numpy as np
import pandas as pd
import requests

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset, scenario_parameters
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.gas_prod import assign_bus_id
from egon.data.datasets.insert_etrago_buses import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters


class IndustrialGasDemand(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="IndustrialGasDemand",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand),
        )


def download_CH4_industrial_demand(df_corr, scn_name="eGon2035"):
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
    # # Download the data
    url = "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."

    year = str(get_sector_parameters("global", scn_name)["population_year"])

    internal_id = "2,11"  # Natural_Gas
    datafilter = "&&internal_id=eq.{" + internal_id + "}"
    request = url + year + datafilter

    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)

    # For debugging
    # industrial_loads_list.to_json("CH4_ind_demand_tmp.json")
    # industrial_loads_list = pd.read_json("CH4_ind_demand_tmp.json")

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
    industrial_loads_list = industrial_loads_list.rename(
        columns={"point": "geom"}
    ).set_geometry("geom", crs=4326)

    # Match to associated gas bus
    industrial_loads_list = assign_bus_id(
        industrial_loads_list, scn_name, "CH4"
    )

    # Add carrier
    c = {"carrier": "CH4"}
    industrial_loads_list = industrial_loads_list.assign(**c)

    # Remove useless columns
    industrial_loads_list = industrial_loads_list.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"]
    )

    return industrial_loads_list


def download_H2_industrial_demand(df_corr, scn_name="eGon2035"):
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
    # Download the data
    url = "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."

    year = str(get_sector_parameters("global", scn_name)["population_year"])

    internal_id = "2,162"  # Hydrogen
    datafilter = "&&internal_id=eq.{" + internal_id + "}"
    request = url + year + datafilter

    result = requests.get(request)
    industrial_loads_list = pd.read_json(result.content)
    # For debugging
    # industrial_loads_list.to_json("H2_ind_demand_tmp.json")
    # industrial_loads_list = pd.read_json("H2_ind_demand_tmp.json")
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
    industrial_loads_list = industrial_loads_list.rename(
        columns={"point": "geom"}
    ).set_geometry("geom", crs=4326)

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
        industrial_gas_demand : Dataframe containing the industrial gas demand
        in Germany
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

    correspondance_url = (
        "http://opendata.ffe.de:3000/region?id_region_type=eq.38"
    )

    result_corr = requests.get(correspondance_url)
    df_corr = pd.read_json(result_corr.content)
    # For debugging
    # df_corr.to_json("region_corr.json")
    # df_corr = pd.read_json("region_corr.json")
    df_corr = df_corr[["id_region", "name_short"]].copy()
    df_corr = df_corr.set_index("id_region")

    industrial_gas_demand = pd.concat(
        [
            download_CH4_industrial_demand(df_corr, scn_name=scn_name),
            download_H2_industrial_demand(df_corr, scn_name=scn_name),
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

    # Insert data to db
    egon_etrago_load_gas.to_sql(
        "egon_etrago_load",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )

    return industrial_gas_demand


def import_industrial_gas_demand_time_series(egon_etrago_load_gas):
    """Insert list of industrial gas demand time series (one per NUTS3) in database

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

    egon_etrago_load_gas = import_industrial_gas_demand(scn_name="eGon2035")

    import_industrial_gas_demand_time_series(egon_etrago_load_gas)
