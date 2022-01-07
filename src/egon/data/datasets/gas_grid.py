# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing data from SciGRID_gas IGGIELGN data
"""
from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile
import ast
import json
import os

from geoalchemy2.types import Geometry
from shapely import geometry
import geopandas
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters


class GasNodesandPipes(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasNodesandPipes",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_gas_data),
        )


def download_SciGRID_gas_data():
    """
    Download SciGRID_gas IGGIELGN data from Zenodo

    """
    path = Path(".") / "datasets" / "gas_data"
    os.makedirs(path, exist_ok=True)

    basename = "IGGIELGN"
    zip_file = Path(".") / "datasets" / "gas_data" / "IGGIELGN.zip"
    zenodo_zip_file_url = (
        "https://zenodo.org/record/4767098/files/" + basename + ".zip"
    )
    if not os.path.isfile(zip_file):
        urlretrieve(zenodo_zip_file_url, zip_file)

    components = [
        "Nodes",
        "PipeSegments",
        "Productions",
        "Storages",
    ]  #'Compressors'
    files = []
    for i in components:
        files.append("data/" + basename + "_" + i + ".csv")

    with ZipFile(zip_file, "r") as zipObj:
        listOfFileNames = zipObj.namelist()
        for fileName in listOfFileNames:
            if fileName in files:
                zipObj.extract(fileName, path)


def define_gas_nodes_list():
    """Define list of gas nodes from SciGRID_gas IGGIELGN data

    Returns
    -------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)

    """
    # Select next id value
    new_id = db.next_etrago_id("bus")

    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Nodes.csv"
    )

    gas_nodes_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["lat", "long", "id", "country_code", "param"],
    )

    # Ajouter tri pour ne conserver que les pays ayant des pipelines en commun.

    gas_nodes_list = gas_nodes_list.rename(columns={"lat": "y", "long": "x"})

    # Remove buses disconnected of the rest of the grid, until the SciGRID_gas data has been corrected.
    gas_nodes_list = gas_nodes_list[
        ~gas_nodes_list["id"].str.match("SEQ_11790_p")
    ]
    gas_nodes_list = gas_nodes_list[
        ~gas_nodes_list["id"].str.match("Stor_EU_107")
    ]

    gas_nodes_list["bus_id"] = range(new_id, new_id + len(gas_nodes_list))
    gas_nodes_list = gas_nodes_list.set_index("id")

    return gas_nodes_list


def ch4_nodes_number_G(gas_nodes_list):
    """Insert list of CH4 nodes from SciGRID_gas IGGIELGN data
        Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    Returns
    -------
        N_ch4_nodes_G : int
            Number of CH4 buses in Germany (independantly from the mode used)
    """

    ch4_nodes_list = gas_nodes_list[
        gas_nodes_list["country_code"].str.match("DE")
    ]
    N_ch4_nodes_G = len(ch4_nodes_list)

    return N_ch4_nodes_G


def insert_CH4_nodes_list(gas_nodes_list):
    """Insert list of CH4 nodes from SciGRID_gas IGGIELGN data
        Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    Returns
    -------
    None
    """
    # Connect to local database
    engine = db.engine()

    gas_nodes_list = gas_nodes_list[
        gas_nodes_list["country_code"].str.match("DE")
    ]  # A remplacer evtmt par un test sur le NUTS0 ?

    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in gas_nodes_list.iterrows():
        param = ast.literal_eval(row["param"])
        NUTS1.append(param["nuts_id_1"])
    gas_nodes_list = gas_nodes_list.assign(NUTS1=NUTS1)

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

        gas_nodes_list = gas_nodes_list[
            gas_nodes_list["NUTS1"].isin([map_states[boundary], np.nan])
        ]

        # A completer avec nodes related to pipelines which have an end in the selected area et evt deplacer ds define_gas_nodes_list

    # Add missing columns
    c = {"scn_name": "eGon2035", "carrier": "CH4"}
    gas_nodes_list = gas_nodes_list.assign(**c)

    gas_nodes_list = geopandas.GeoDataFrame(
        gas_nodes_list,
        geometry=geopandas.points_from_xy(
            gas_nodes_list["x"], gas_nodes_list["y"]
        ),
    )
    gas_nodes_list = gas_nodes_list.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    gas_nodes_list = gas_nodes_list.reset_index(drop=True)
    gas_nodes_list = gas_nodes_list.drop(
        columns=["NUTS1", "param", "country_code"]
    )

    # Insert data to db
    db.execute_sql(
        f"""
    DELETE FROM grid.egon_etrago_bus WHERE "carrier" = 'CH4' AND
    scn_name = '{c['scn_name']}' AND country = 'DE';
    """
    )

    # Insert CH4 data to db
    print(gas_nodes_list)
    gas_nodes_list.to_postgis(
        "egon_etrago_bus",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"geom": Geometry()},
    )


def insert_gas_buses_abroad(scn_name="eGon2035"):
    """Insert central gas buses in foreign countries to db, same buses than the foreign AC buses
    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    gdf_abroad_buses : dataframe
        Dataframe containing the gas in the neighbouring countries and one in the center of Germany in test mode
    """
    main_gas_carrier = get_sector_parameters("gas", scenario=scn_name)[
        "main_gas_carrier"
    ]

    # Connect to local database
    engine = db.engine()
    db.execute_sql(
        f"""
    DELETE FROM grid.egon_etrago_bus WHERE "carrier" = '{main_gas_carrier}' AND
    scn_name = '{scn_name}' AND country != 'DE';
    """
    )

    # Select the foreign buses
    sql_abroad_buses = f"""SELECT bus_id, scn_name, x, y, carrier, country
                            FROM grid.egon_etrago_bus
                            WHERE country != 'DE'
                            AND carrier = 'AC'
                            AND scn_name = '{scn_name}';"""

    gdf_abroad_buses = db.select_dataframe(sql_abroad_buses)
    gdf_abroad_buses = gdf_abroad_buses.drop_duplicates(subset=["country"])

    # Select next id value
    new_id = db.next_etrago_id("bus")

    gdf_abroad_buses["carrier"] = main_gas_carrier
    gdf_abroad_buses["bus_id"] = range(new_id, new_id + len(gdf_abroad_buses))

    # Add bus in Finland
    gdf_abroad_buses = gdf_abroad_buses.append(
        {
            "scn_name": scn_name,
            "bus_id": (new_id + len(gdf_abroad_buses) + 1),
            "x": 28.30912,
            "y": 60.44315,
            "country": "FI",
            "carrier": main_gas_carrier,
        },
        ignore_index=True,
    )

    # if in test mode, add bus in center of Germany
    boundary = settings()["egon-data"]["--dataset-boundary"]

    if boundary != "Everything":
        gdf_abroad_buses = gdf_abroad_buses.append(
            {
                "scn_name": scn_name,
                "bus_id": (new_id + len(gdf_abroad_buses) + 1),
                "x": 10.4234469,
                "y": 51.0834196,
                "country": "DE",
                "carrier": main_gas_carrier,
            },
            ignore_index=True,
        )

    gdf_abroad_buses = geopandas.GeoDataFrame(
        gdf_abroad_buses,
        geometry=geopandas.points_from_xy(
            gdf_abroad_buses["x"], gdf_abroad_buses["y"]
        ),
    )
    gdf_abroad_buses = gdf_abroad_buses.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    # Insert to db
    print(gdf_abroad_buses)
    gdf_abroad_buses.to_postgis(
        "egon_etrago_bus",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"geom": Geometry()},
    )
    return gdf_abroad_buses


def insert_gas_pipeline_list(
    gas_nodes_list, abroad_gas_nodes_list, scn_name="eGon2035"
):
    """Insert list of gas pipelines from SciGRID_gas IGGIELGN data
    Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    scn_name : str
        Name of the scenario

    Returns
    -------
    None.
    """
    abroad_gas_nodes_list = abroad_gas_nodes_list.set_index("country")

    main_gas_carrier = get_sector_parameters("gas", scenario=scn_name)[
        "main_gas_carrier"
    ]

    engine = db.engine()

    # Select next id value
    new_id = db.next_etrago_id("link")

    classifiaction_file = (
        Path(".")
        / "data_bundle_egon_data"
        / "pipeline_classification_gas"
        / "pipeline_classification.csv"
    )

    classification = pd.read_csv(
        classifiaction_file,
        delimiter=",",
        usecols=["classification", "max_transport_capacity_Gwh/d"],
    )

    target_file = (
        Path(".")
        / "datasets"
        / "gas_data"
        / "data"
        / "IGGIELGN_PipeSegments.csv"
    )

    gas_pipelines_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["id", "node_id", "lat", "long", "country_code", "param"],
    )

    # Select the links having at least one bus in Germany
    gas_pipelines_list = gas_pipelines_list[
        gas_pipelines_list["country_code"].str.contains("DE")
    ]

    # Remove links disconnected of the rest of the grid, until the SciGRID_gas data has been corrected.
    # TODO: automatic test for disconnected links
    # TODO: remove link test if length = 0
    gas_pipelines_list = gas_pipelines_list[
        ~gas_pipelines_list["id"].str.match("EntsoG_Map__ST_195")
    ]
    gas_pipelines_list = gas_pipelines_list[
        ~gas_pipelines_list["id"].str.match("EntsoG_Map__ST_5")
    ]

    gas_pipelines_list["link_id"] = range(
        new_id, new_id + len(gas_pipelines_list)
    )
    gas_pipelines_list["link_id"] = gas_pipelines_list["link_id"].astype(int)

    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in gas_pipelines_list.iterrows():
        param = ast.literal_eval(row["param"])
        NUTS1.append(param["nuts_id_1"])
    gas_pipelines_list["NUTS1"] = NUTS1

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
        "Everything": "Nan",
    }
    gas_pipelines_list["NUTS1_0"] = [x[0] for x in gas_pipelines_list["NUTS1"]]
    gas_pipelines_list["NUTS1_1"] = [x[1] for x in gas_pipelines_list["NUTS1"]]

    boundary = settings()["egon-data"]["--dataset-boundary"]

    if boundary != "Everything":

        gas_pipelines_list = gas_pipelines_list[
            gas_pipelines_list["NUTS1_0"].str.contains(map_states[boundary])
            | gas_pipelines_list["NUTS1_1"].str.contains(map_states[boundary])
        ]

    # Add missing columns
    gas_pipelines_list["scn_name"] = scn_name
    gas_pipelines_list["carrier"] = main_gas_carrier
    gas_pipelines_list["p_nom_extandable"] = False

    diameter = []
    geom = []
    topo = []

    for index, row in gas_pipelines_list.iterrows():

        param = ast.literal_eval(row["param"])
        diameter.append(param["diameter_mm"])

        long_e = json.loads(row["long"])
        lat_e = json.loads(row["lat"])
        crd_e = list(zip(long_e, lat_e))
        topo.append(geometry.LineString(crd_e))

        long_path = param["path_long"]
        lat_path = param["path_lat"]
        crd = list(zip(long_path, lat_path))
        crd.insert(0, crd_e[0])
        crd.append(crd_e[1])
        lines = []
        for i in range(len(crd) - 1):
            lines.append(geometry.LineString([crd[i], crd[i + 1]]))
        geom.append(geometry.MultiLineString(lines))

    gas_pipelines_list["diameter"] = diameter
    gas_pipelines_list["geom"] = geom
    gas_pipelines_list["topo"] = topo
    gas_pipelines_list = gas_pipelines_list.set_geometry("geom", crs=4326)

    country_0 = []
    country_1 = []
    for index, row in gas_pipelines_list.iterrows():
        c = ast.literal_eval(row["country_code"])
        country_0.append(c[0])
        country_1.append(c[1])
    gas_pipelines_list["country_0"] = country_0
    gas_pipelines_list["country_1"] = country_1

    # Correct non valid neighbouring country nodes
    gas_pipelines_list.loc[
        gas_pipelines_list["country_0"] == "XX", "country_0"
    ] = "NO"

    # Adjust columns
    bus0 = []
    bus1 = []
    geom_adjusted = []
    topo_adjusted = []
    length_adjusted = []
    pipe_class = []

    for index, row in gas_pipelines_list.iterrows():
        buses = row["node_id"].strip("][").split(", ")

        if (
            (boundary != "Everything")
            & (row["NUTS1_0"] != map_states[boundary])
            & (row["country_0"] == "DE")
        ):
            bus0.append(abroad_gas_nodes_list.loc["DE", "bus_id"])
            bus1.append(gas_nodes_list.loc[buses[1][1:-1], "bus_id"])
            long_e = [
                abroad_gas_nodes_list.loc["DE", "x"],
                json.loads(row["long"])[1],
            ]
            lat_e = [
                abroad_gas_nodes_list.loc["DE", "y"],
                json.loads(row["lat"])[1],
            ]
            geom_pipe = geometry.MultiLineString(
                [geometry.LineString(list(zip(long_e, lat_e)))]
            )
            topo_adjusted.append(geometry.LineString(list(zip(long_e, lat_e))))

        elif row["country_0"] != "DE":
            country = str(row["country_0"])
            bus0.append(abroad_gas_nodes_list.loc[country, "bus_id"])
            bus1.append(gas_nodes_list.loc[buses[1][1:-1], "bus_id"])
            long_e = [
                abroad_gas_nodes_list.loc[country, "x"],
                json.loads(row["long"])[1],
            ]
            lat_e = [
                abroad_gas_nodes_list.loc[country, "y"],
                json.loads(row["lat"])[1],
            ]
            geom_pipe = geometry.MultiLineString(
                [geometry.LineString(list(zip(long_e, lat_e)))]
            )
            topo_adjusted.append(geometry.LineString(list(zip(long_e, lat_e))))

        elif (
            (boundary != "Everything")
            & (row["NUTS1_1"] != map_states[boundary])
            & (row["country_1"] == "DE")
        ):
            bus0.append(gas_nodes_list.loc[buses[0][1:-1], "bus_id"])
            bus1.append(abroad_gas_nodes_list.loc["DE", "bus_id"])
            long_e = [
                json.loads(row["long"])[0],
                abroad_gas_nodes_list.loc["DE", "x"],
            ]
            lat_e = [
                json.loads(row["lat"])[0],
                abroad_gas_nodes_list.loc["DE", "y"],
            ]
            geom_pipe = geometry.MultiLineString(
                [geometry.LineString(list(zip(long_e, lat_e)))]
            )
            topo_adjusted.append(geometry.LineString(list(zip(long_e, lat_e))))

        elif row["country_1"] != "DE":
            country = str(row["country_1"])
            bus0.append(gas_nodes_list.loc[buses[0][1:-1], "bus_id"])
            bus1.append(abroad_gas_nodes_list.loc[country, "bus_id"])
            long_e = [
                json.loads(row["long"])[0],
                abroad_gas_nodes_list.loc[country, "x"],
            ]
            lat_e = [
                json.loads(row["lat"])[0],
                abroad_gas_nodes_list.loc[country, "y"],
            ]
            geom_pipe = geometry.MultiLineString(
                [geometry.LineString(list(zip(long_e, lat_e)))]
            )
            topo_adjusted.append(geometry.LineString(list(zip(long_e, lat_e))))

        else:
            bus0.append(gas_nodes_list.loc[buses[0][1:-1], "bus_id"])
            bus1.append(gas_nodes_list.loc[buses[1][1:-1], "bus_id"])
            geom_pipe = row["geom"]
            topo_adjusted.append(row["topo"])

        geom_adjusted.append(geom_pipe)
        length_adjusted.append(geom_pipe.length)

        if row["diameter"] >= 1000:
            pipe_class = "A"
        elif 700 <= row["diameter"] <= 1000:
            pipe_class = "B"
        elif 500 <= row["diameter"] <= 700:
            pipe_class = "C"
        elif 350 <= row["diameter"] <= 500:
            pipe_class = "D"
        elif 200 <= row["diameter"] <= 350:
            pipe_class = "E"
        elif 100 <= row["diameter"] <= 200:
            pipe_class = "F"
        elif row["diameter"] <= 100:
            pipe_class = "G"

    gas_pipelines_list["bus0"] = bus0
    gas_pipelines_list["bus1"] = bus1
    gas_pipelines_list["geom"] = geom_adjusted
    gas_pipelines_list["topo"] = topo_adjusted
    gas_pipelines_list["length"] = length_adjusted
    gas_pipelines_list["pipe_class"] = pipe_class

    gas_pipelines_list = gas_pipelines_list.merge(
        classification,
        how="left",
        left_on="pipe_class",
        right_on="classification",
    )
    gas_pipelines_list["p_nom"] = gas_pipelines_list[
        "max_transport_capacity_Gwh/d"
    ] * (1000 / 24)

    # Remove useless columns
    gas_pipelines_list = gas_pipelines_list.drop(
        columns=[
            "id",
            "node_id",
            "param",
            "NUTS1",
            "NUTS1_0",
            "NUTS1_1",
            "country_code",
            "country_0",
            "country_1",
            "diameter",
            "pipe_class",
            "classification",
            "max_transport_capacity_Gwh/d",
            "lat",
            "long",
        ]
    )

    # Insert data to db
    db.execute_sql(
        f"""DELETE FROM grid.egon_etrago_link WHERE "carrier" = '{main_gas_carrier}' AND
           scn_name = '{scn_name}';
        """
    )

    print(gas_pipelines_list)
    gas_pipelines_list.to_postgis(
        "egon_etrago_gas_link",
        engine,
        schema="grid",
        index=False,
        if_exists="replace",
        dtype={"geom": Geometry(), "topo": Geometry()},
    )

    db.execute_sql(
        """
    select UpdateGeometrySRID('grid', 'egon_etrago_gas_link', 'topo', 4326) ;

    INSERT INTO grid.egon_etrago_link (scn_name,
                                              link_id, carrier,
                                              bus0, bus1,
                                              p_nom, length,
                                              geom, topo)
    SELECT scn_name,
                link_id, carrier,
                bus0, bus1,
                p_nom, length,
                geom, topo

    FROM grid.egon_etrago_gas_link;

    DROP TABLE grid.egon_etrago_gas_link;
        """
    )


def insert_gas_data():
    """Overall function for importing gas data from SciGRID_gas
    Returns
    -------
    None.
    """
    download_SciGRID_gas_data()

    gas_nodes_list = define_gas_nodes_list()

    insert_CH4_nodes_list(gas_nodes_list)
    abroad_gas_nodes_list = insert_gas_buses_abroad()

    insert_gas_pipeline_list(gas_nodes_list, abroad_gas_nodes_list)
