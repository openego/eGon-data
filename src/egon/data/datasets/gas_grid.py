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

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.electrical_neighbours import central_buses_egon100
from egon.data.datasets.etrago_helpers import copy_and_modify_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


class GasNodesandPipes(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasNodesandPipes",
            version="0.0.9",
            dependencies=dependencies,
            tasks=(insert_gas_data, insert_gas_data_eGon100RE),
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
        "LNGs",
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

    # Correct non valid neighbouring country nodes
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "INET_N_1182", "country_code"
    ] = "AT"
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "SEQ_10608_p", "country_code"
    ] = "NL"
    gas_nodes_list.loc[
        gas_nodes_list["id"] == "N_88_NS_LMGN", "country_code"
    ] = "XX"

    gas_nodes_list = gas_nodes_list.rename(columns={"lat": "y", "long": "x"})

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

    Insert detailled description

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
    ]  # To eventually replace with a test if the nodes are in the german boundaries.

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
    """Insert central CH4 buses in foreign countries for eGon2035

    Detailled description to be completed:
    Insert central gas buses in foreign countries to db, same buses
    than the foreign AC buses

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    gdf_abroad_buses : dataframe
        Dataframe containing the CH4 buses in the neighbouring countries
        and one in the center of Germany in test mode
    """
    # Select sources and targets from dataset configuration
    sources = config.datasets()["electrical_neighbours"]["sources"]

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
    gdf_abroad_buses = central_buses_egon100(sources)
    gdf_abroad_buses = gdf_abroad_buses.drop_duplicates(subset=["country"])

    # Select next id value
    new_id = db.next_etrago_id("bus")

    gdf_abroad_buses = gdf_abroad_buses.drop(
        columns=[
            "v_nom",
            "v_mag_pu_set",
            "v_mag_pu_min",
            "v_mag_pu_max",
            "geom",
        ]
    )
    gdf_abroad_buses["scn_name"] = "eGon2035"
    gdf_abroad_buses["carrier"] = main_gas_carrier
    gdf_abroad_buses["bus_id"] = range(new_id, new_id + len(gdf_abroad_buses))

    # Add central bus in Russia
    gdf_abroad_buses = gdf_abroad_buses.append(
        {
            "scn_name": scn_name,
            "bus_id": (new_id + len(gdf_abroad_buses) + 1),
            "x": 41,
            "y": 55,
            "country": "RU",
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

    Insert detailled description

    Parameters
    ----------
    gas_nodes_list : dataframe
        description missing
    abroad_gas_nodes_list: dataframe
        description missing
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

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
    # Remove links disconnected of the rest of the grid
    # Remove manually for disconnected link EntsoG_Map__ST_195 and EntsoG_Map__ST_108
    gas_pipelines_list = gas_pipelines_list[
        gas_pipelines_list["node_id"] != "['SEQ_11790_p', 'Stor_EU_107']"
    ]
    gas_pipelines_list = gas_pipelines_list[
        ~gas_pipelines_list["id"].str.match("EntsoG_Map__ST_108")
    ]

    # Manually add pipeline to artificially connect isolated pipeline
    gas_pipelines_list.at["new_pipe", "param"] = gas_pipelines_list[
        gas_pipelines_list["id"] == "NO_PS_8_Seg_0_Seg_23"
    ]["param"].values[0]
    gas_pipelines_list.at[
        "new_pipe", "node_id"
    ] = "['SEQ_12442_p', 'LKD_N_200']"
    gas_pipelines_list.at["new_pipe", "lat"] = "[53.358536, 53.412719]"
    gas_pipelines_list.at["new_pipe", "long"] = "[7.041677, 7.093251]"
    gas_pipelines_list.at["new_pipe", "country_code"] = "['DE', 'DE']"

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
    gas_pipelines_list["p_nom_extendable"] = False
    gas_pipelines_list["p_min_pu"] = -1.0

    diameter = []
    geom = []
    topo = []
    length_km = []

    for index, row in gas_pipelines_list.iterrows():

        param = ast.literal_eval(row["param"])
        diameter.append(param["diameter_mm"])
        length_km.append(param["length_km"])

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
    gas_pipelines_list["length_km"] = length_km
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
    gas_pipelines_list.loc[
        gas_pipelines_list["country_1"] == "FI", "country_1"
    ] = "RU"
    gas_pipelines_list.loc[
        gas_pipelines_list["id"] == "ST_2612_Seg_0_Seg_0", "country_0"
    ] = "AT"  # bus "INET_N_1182" DE -> AT
    gas_pipelines_list.loc[
        gas_pipelines_list["id"] == "INET_PL_385_EE_3_Seg_0_Seg_1", "country_1"
    ] = "AT"  # "INET_N_1182" DE -> AT
    gas_pipelines_list.loc[
        gas_pipelines_list["id"] == "LKD_PS_0_Seg_0_Seg_3", "country_0"
    ] = "NL"  # bus "SEQ_10608_p" DE -> NL

    # Remove uncorrect pipelines
    gas_pipelines_list = gas_pipelines_list[
        (gas_pipelines_list["id"] != "PLNG_2637_Seg_0_Seg_0_Seg_0")
        & (gas_pipelines_list["id"] != "NSG_6650_Seg_2_Seg_0")
        & (gas_pipelines_list["id"] != "NSG_6734_Seg_2_Seg_0")
    ]

    # Remove link test if length = 0
    gas_pipelines_list = gas_pipelines_list[
        gas_pipelines_list["length_km"] != 0
    ]

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

    # Remove pipes having the same node for start and end
    gas_pipelines_list = gas_pipelines_list[
        gas_pipelines_list["bus0"] != gas_pipelines_list["bus1"]
    ]

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
            "length_km",
        ]
    )

    # Clean db
    db.execute_sql(
        f"""DELETE FROM grid.egon_etrago_link
        WHERE "carrier" = '{main_gas_carrier}'
        AND scn_name = '{scn_name}';
        """
    )

    print(gas_pipelines_list)
    # Insert data to db
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
                                              bus0, bus1, p_min_pu,
                                              p_nom, p_nom_extendable, length,
                                              geom, topo)
    SELECT scn_name,
                link_id, carrier,
                bus0, bus1, p_min_pu,
                p_nom, p_nom_extendable, length,
                geom, topo

    FROM grid.egon_etrago_gas_link;

    DROP TABLE grid.egon_etrago_gas_link;
        """
    )


def remove_isolated_gas_buses():
    """Delete gas buses which are not connected to the gas grid.
    Returns
    -------
    None.
    """
    targets = config.datasets()["gas_grid"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['buses']['schema']}.{targets['buses']['table']}
        WHERE "carrier" = 'CH4'
        AND scn_name = 'eGon2035'
        AND country = 'DE'
        AND "bus_id" NOT IN
            (SELECT bus0 FROM {targets['links']['schema']}.{targets['links']['table']}
            WHERE scn_name = 'eGon2035'
            AND carrier = 'CH4')
        AND "bus_id" NOT IN
            (SELECT bus1 FROM {targets['links']['schema']}.{targets['links']['table']}
            WHERE scn_name = 'eGon2035'
            AND carrier = 'CH4');
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
    remove_isolated_gas_buses()


def insert_gas_data_eGon100RE():
    """Overall function for importing gas data from SciGRID_gas
    Returns
    -------
    None.
    """
    # copy buses
    copy_and_modify_buses("eGon2035", "eGon100RE", {"carrier": ["CH4"]})

    # get CH4 pipelines and modify their nominal capacity with the
    # retrofitting factor
    gdf = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE carrier = 'CH4' AND scn_name = 'eGon2035' AND
        bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon2035' AND country = 'DE'
        ) AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon2035' AND country = 'DE'
        );
        """,
        epsg=4326,
        geom_col="topo",
    )

    # Update scenario specific information
    scn_name = "eGon100RE"
    gdf["scn_name"] = scn_name
    scn_params = get_sector_parameters("gas", scn_name)

    for param in ["capital_cost", "marginal_cost", "efficiency"]:
        try:
            gdf.loc[:, param] = scn_params[param]["CH4"]
        except KeyError:
            pass

    # remaining CH4 share is 1 - retroffited pipeline share
    gdf["p_nom"] *= (
        1 - scn_params["retrofitted_CH4pipeline-to-H2pipeline_share"]
    )

    # delete old entries
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_link
        WHERE carrier = 'CH4' AND scn_name = '{scn_name}' AND
        bus0 NOT IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn_name}' AND country != 'DE'
        ) AND bus1 NOT IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn_name}' AND country != 'DE'
        );
        """
    )

    gdf.to_postgis(
        "egon_etrago_link",
        schema="grid",
        if_exists="append",
        con=db.engine(),
        index=False,
        dtype={"geom": Geometry(), "topo": Geometry()},
    )
