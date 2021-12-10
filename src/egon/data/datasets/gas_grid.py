# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing data from SciGRID_gas IGGIELGN data
"""
import ast
import json
import os
from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile

import numpy as np

import geopandas
import pandas as pd
from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from geoalchemy2.types import Geometry
from shapely import geometry

class GasNodesandPipes(Dataset):
     def __init__(self, dependencies):
         super().__init__(
             name="GasNodesandPipes",
             version="0.0.0",
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
    ]  # A remplacer evtmt par un test sur le NUTS0 ?
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


def insert_gas_pipeline_list(gas_nodes_list):
    """Insert list of gas pipelines from SciGRID_gas IGGIELGN data
    Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    Returns
    -------
    None.
    """
    # Connect to local database
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
    ]  # A remplacer evtmt par un test sur le NUTS0 ?

    # Remove links disconnected of the rest of the grid, until the SciGRID_gas data has been corrected.
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
        gas_pipelines_list["NUTS1"] = [
            x[0] for x in gas_pipelines_list["NUTS1"]
        ]
        gas_pipelines_list = gas_pipelines_list[
            gas_pipelines_list["NUTS1"].str.contains(map_states[boundary])
        ]

        # A completer avec nodes related to pipelines which have an end in the selected area

    # Add missing columns
    scn_name = "eGon2035"
    gas_pipelines_list["scn_name"] = scn_name
    gas_pipelines_list["carrier"] = "CH4"

    diameter = []
    length = []
    geom = []
    topo = []

    for index, row in gas_pipelines_list.iterrows():

        param = ast.literal_eval(row["param"])
        diameter.append(param["diameter_mm"])
        length.append(param["length_km"])

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

    print(topo)
    gas_pipelines_list["diameter"] = diameter
    gas_pipelines_list["length"] = length
    gas_pipelines_list["geom"] = geom
    gas_pipelines_list["topo"] = topo
    gas_pipelines_list = gas_pipelines_list.set_geometry("geom", crs=4326)

    # Adjust columns
    bus0 = []
    bus1 = []
    pipe_class = []

    for index, row in gas_pipelines_list.iterrows():

        buses = row["node_id"].strip("][").split(", ")
        bus0.append(gas_nodes_list.loc[buses[0][1:-1], "bus_id"])
        bus1.append(gas_nodes_list.loc[buses[1][1:-1], "bus_id"])

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

    gas_pipelines_list = gas_pipelines_list.drop(
        columns=[
            "id",
            "node_id",
            "param",
            "NUTS1",
            "country_code",
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
        f"""DELETE FROM grid.egon_etrago_link WHERE "carrier" = 'CH4' AND
           scn_name = '{scn_name}' AND country = 'DE';
        """
    )

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

    insert_gas_pipeline_list(gas_nodes_list)
