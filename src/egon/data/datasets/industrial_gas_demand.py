# -*- coding: utf-8 -*-
"""
The central module containing code dealing with gas industrial demand

In this module, the functions to import the industrial hydrogen and
methane demands from the opendata.ffe database and to insert them into
the database after modification are to be found.

"""

from pathlib import Path
import logging
import os
import shutil

from geoalchemy2.types import Geometry
from shapely import wkt
import numpy as np
import pandas as pd
import requests

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets.etrago_helpers import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.pypsaeur import prepared_network, read_network
from egon.data.datasets.scenario_parameters import (
    align_weekdays,
    get_sector_parameters,
)

logger = logging.getLogger(__name__)
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets


class IndustrialGasDemand(Dataset):
    """
    Download the industrial gas demands from the opendata.ffe database

    Data is downloaded to the folder ./datasets/gas_data/demand using
    the function :py:func:`download_industrial_gas_demand` and no dataset is resulting.

    *Dependencies*
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`

    """

    #:
    name: str = "IndustrialGasDemand"
    version: str = "0.0.9.dev"

    sources = DatasetSources(
        tables={
            "boundaries_vg250_krs": "boundaries.vg250_krs",
            "egon_etrago_bus": "grid.egon_etrago_bus",
        },
        files={
            "region_mapping_json": "./datasets/gas_data/demand/region_corr.json",
            "industrial_demand_folder": "./datasets/gas_data/demand",
            "industrial_gas_bundle_src": "./data_bundle_egon_data/industrial_gas_demand",
        },
    )

    targets = DatasetTargets(
        tables={
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download_industrial_gas_demand),
        )


class IndustrialGasDemandScenarios(Dataset):
    """Insert the hourly resolved industrial gas demands into the database

    Insert the industrial methane and hydrogen demands and their
    associated time series for the scenarios by executing the
    function :py:func:`insert_industrial_gas_demand`.

    *Dependencies*
      * :py:class:`GasAreas <egon.data.datasets.gas_areas.GasAreas>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`IndustrialGasDemand <IndustrialGasDemand>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_load_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended

    """

    #:
    name: str = "IndustrialGasDemandScenarios"
    #:
    version: str = "0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_industrial_gas_demand),
        )


def read_industrial_demand(scn_name, carrier):
    """Read the industrial gas demand data in Germany

    This function reads the methane or hydrogen industrial demand time
    series previously downloaded in :py:func:`download_industrial_gas_demand`.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier : str
        Name of the gas carrier

    Returns
    -------
    df : pandas.DataFrame
        Dataframe containing the industrial gas demand time series

    """
    target_file = Path(
        IndustrialGasDemand.sources.files["region_mapping_json"]
    )
    df_corr = pd.read_json(target_file)
    df_corr = df_corr.loc[:, ["id_region", "name_short"]]
    df_corr.set_index("id_region", inplace=True)

    target_file = (
        Path(IndustrialGasDemand.sources.files["industrial_demand_folder"])
        / f"{carrier}_{scn_name}.json"
    )
    industrial_loads = pd.read_json(target_file)

    # The FfE profiles are created for their own weather year (2012),
    # align them to the weekdays of the scenario's weather year
    (source_year,) = industrial_loads["year_weather"].unique()
    weather_year = get_sector_parameters("global", scn_name)["weather_year"]
    industrial_loads["values"] = industrial_loads["values"].apply(
        lambda values: align_weekdays(
            values, source_year=int(source_year), target_year=weather_year
        ).tolist()
    )

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
    sql_vg250 = f"""SELECT nuts as nuts3, geometry as geom
                FROM {IndustrialGasDemand.sources.tables['boundaries_vg250_krs']}
                WHERE gf = 4;"""
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


def read_and_process_demand(
    scn_name, carrier, grid_carrier
):
    """Assign the industrial gas demand in Germany to buses

    This function prepares and returns the industrial gas demand time
    series for CH4 or H2 and for a specific scenario by executing the
    following steps:

      * Read the industrial demand time series in Germany with the
        function :py:func:`read_industrial_demand`
      * Attribute the bus_id to which each load and it associated time
        series is associated by calling the function :py:func:`assign_gas_bus_id <egon.data.db.assign_gas_bus_id>`
        from :py:mod:`egon.data.db <egon.data.db>`
      * Adjust the columns: add "carrier" and remove useless ones

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
    industrial_demand : pandas.DataFrame
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
    Delete CH4 and H2 loads and load time series for the specified scenario

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """
    targets = IndustrialGasDemand.targets
    sources = IndustrialGasDemand.sources
    # Clean tables
    db.execute_sql(f"""
        DELETE FROM {targets.tables['etrago_load_timeseries']}
        WHERE "load_id" IN (
            SELECT load_id FROM {targets.tables['etrago_load']}
            WHERE "carrier" IN ('CH4_for_industry', 'H2_for_industry') AND
            scn_name = '{scn_name}' AND bus not IN (
                SELECT bus_id FROM {sources.tables['egon_etrago_bus']}
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            )
        );
        """)

    db.execute_sql(f"""
        DELETE FROM {targets.tables['etrago_load']}
        WHERE "load_id" IN (
            SELECT load_id FROM {targets.tables['etrago_load']}
            WHERE "carrier" IN ('CH4_for_industry', 'H2_for_industry') AND
            scn_name = '{scn_name}' AND bus not IN (
                SELECT bus_id FROM {sources.tables['egon_etrago_bus']}
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            )
        );
        """)


def insert_new_entries(industrial_gas_demand, scn_name):
    """
    Insert industrial gas loads into the database

    This function prepares and imports the industrial gas loads by
    executing the following steps:

      * Attribution of an id to each load in the list received as parameter
      * Deletion of the column containing the time series (they will be
        inserted in another table (grid.egon_etrago_load_timeseries) in
        the :py:func:`insert_industrial_gas_demand_time_series`)
      * Insertion of the loads into the database
      * Return of the dataframe still containing the time series columns

    Parameters
    ----------
    industrial_gas_demand : pandas.DataFrame
        Load data to insert (containing the time series)
    scn_name : str
        Name of the scenario.

    Returns
    -------
    industrial_gas_demand : pandas.DataFrame
        Dataframe containing the loads that have been inserted in
        the database with their time series

    """
    targets = IndustrialGasDemand.targets
    industrial_gas_demand["load_id"] = db.next_etrago_id(
        "load", len(industrial_gas_demand)
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
        targets.get_table_name("etrago_load"),
        engine,
        schema=targets.get_table_schema("etrago_load"),
        index=False,
        if_exists="append",
    )

    return industrial_gas_demand


def insert_industrial_gas_demand():
    """Insert industrial gas demands into the database

    Insert the industrial CH4 and H2 demands and their associated time
    series into the database for the scenarios. The data
    previously downloaded in :py:func:`download_industrial_gas_demand`
    is adjusted by executing the following steps:

      * Clean the database with the function :py:func:`delete_old_entries`
      * Read and prepare the CH4 and the H2 industrial demands and their
        associated time series in Germany with the function :py:func:`read_and_process_demand`
      * Aggregate the demands with the same properties at the same gas bus
      * Insert the loads into the database by executing :py:func:`insert_new_entries`
      * Insert the time series associated to the loads into the database
        by executing :py:func:`insert_industrial_gas_demand_time_series`

    Returns
    -------
    None

    """
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if scn_name in ["eGon2035", "reGon2037"]:

            delete_old_entries(scn_name)

            industrial_gas_demand = pd.concat(
                [
                    read_and_process_demand(
                        scn_name=scn_name,
                        carrier="CH4_for_industry",
                        grid_carrier="CH4",
                    ),
                    read_and_process_demand(
                        scn_name=scn_name,
                        carrier="H2_for_industry",
                        grid_carrier="H2",
                    ),
                ]
            )

            industrial_gas_demand = (
                industrial_gas_demand.groupby(["bus", "carrier"])["p_set"]
                .apply(lambda x: [sum(y) for y in zip(*x)])
                .reset_index(drop=False)
            )

            industrial_gas_demand = insert_new_entries(
                industrial_gas_demand, scn_name
            )
            insert_industrial_gas_demand_time_series(industrial_gas_demand)
        # TO DO: treatment for reGon2045
        else:
            print(f"""{scn_name} is not part of the scenario list. This task is not
                executed""")


def insert_industrial_gas_demand_time_series(egon_etrago_load_gas):
    """
    Insert list of industrial gas demand time series (one per NUTS3 region)

    These loads are hourly and on NUTS3 level resolved.

    Parameters
    ----------
    industrial_gas_demand : pandas.DataFrame
        Dataframe containing the loads that have been inserted into
        the database and whose time series will be inserted into the
        database.

    Returns
    -------
    None

    """
    targets = IndustrialGasDemand.targets
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
        targets.get_table_name("etrago_load_timeseries"),
        engine,
        schema=targets.get_table_schema("etrago_load_timeseries"),
        index=False,
        if_exists="append",
    )


def download_industrial_gas_demand():
    """Download the industrial gas demand data from opendata.ffe database

    The industrial demands for hydrogen and methane are downloaded in
    the folder ./datasets/gas_data/demand
    These loads are hourly and NUTS3-level resolved. For more
    information on these data, refer to the `Extremos project documentation <https://opendata.ffe.de/project/extremos/>`_.
    
    Returns
    -------
    None

    """
    try:
        # TO DO: check source validity for new scenarios
        correspondance_url = (
            "http://opendata.ffe.de:3000/region?id_region_type=eq.38"
        )

        # Read and save data
        result_corr = requests.get(correspondance_url)
        target_file = Path(
            IndustrialGasDemand.sources.files["region_mapping_json"]
        )
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        pd.read_json(result_corr.content).to_json(target_file)

        carriers = {"H2_for_industry": "2,162", "CH4_for_industry": "2,11"}
        url = (
            "http://opendata.ffe.de:3000/opendata?id_opendata=eq.66&&year=eq."
        )

        for scn_name in config.settings()["egon-data"]["--scenarios"]:
            if scn_name in ["eGon2035", "reGon2037"]:

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
                        Path(
                            IndustrialGasDemand.sources.files[
                                "industrial_demand_folder"
                            ]
                        )
                        / f"{carrier}_{scn_name}.json"
                    )
                    pd.read_json(result.content).to_json(target_file)
    except:
        logger.warning("""
        Due to temporal problems in the FFE platform, data for scenarios
         are imported lately from csv files. Data for
        other scenarios is unfortunately unavailable.
            """)
        shutil.copytree(
            IndustrialGasDemand.sources.files["industrial_gas_bundle_src"],
            IndustrialGasDemand.sources.files["industrial_demand_folder"],
            dirs_exist_ok=True,
        )
        # Temporal patch: reuse 2035 data for the reGon scenarios until the source is decided.
        folder = Path(
            IndustrialGasDemand.sources.files["industrial_demand_folder"]
        )
        for scn_name in config.settings()["egon-data"]["--scenarios"]:
            for carrier in ["H2_for_industry", "CH4_for_industry"]:
                src = folder / f"{carrier}_eGon2035.json"
                dst = folder / f"{carrier}_{scn_name}.json"
                if src.is_file() and not dst.is_file():
                    shutil.copy(src, dst)
