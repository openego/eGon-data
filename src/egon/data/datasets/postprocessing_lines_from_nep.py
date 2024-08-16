#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 10:17:31 2023

@author: clara
"""
from geoalchemy2.types import Geometry
from shapely.geometry import LineString
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import numpy as np

from egon.data.datasets.electrical_neighbours import choose_transformer
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data import db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class EgonPfHvExtensionBus(Base):
    __tablename__ = "egon_etrago_extension_bus"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    v_nom = Column(Float(53), server_default="1.")
    type = Column(Text)
    carrier = Column(Text)
    v_mag_pu_set = Column(Float(53))
    v_mag_pu_min = Column(Float(53), server_default="0.")
    v_mag_pu_max = Column(Float(53), server_default="inf")
    x = Column(Float(53), server_default="0.")
    y = Column(Float(53), server_default="0.")
    geom = Column(Geometry("POINT", 4326), index=True)
    country = Column(Text, server_default=text("'DE'::text"))


class EgonPfHvExtensionLine(Base):
    __tablename__ = "egon_etrago_extension_line"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    line_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    x = Column(Numeric, server_default="0.")
    r = Column(Numeric, server_default="0.")
    g = Column(Numeric, server_default="0.")
    b = Column(Numeric, server_default="0.")
    s_nom = Column(Numeric, server_default="0.")
    s_nom_extendable = Column(Boolean, server_default="False")
    s_nom_min = Column(Float(53), server_default="0.")
    s_nom_max = Column(Float(53), server_default="inf")
    s_max_pu = Column(Float(53), server_default="1.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    capital_cost = Column(Float(53), server_default="0.")
    length = Column(Float(53), server_default="0.")
    cables = Column(Integer)
    terrain_factor = Column(Float(53), server_default="1.")
    num_parallel = Column(Float(53), server_default="1.")
    v_ang_min = Column(Float(53), server_default="-inf")
    v_ang_max = Column(Float(53), server_default="inf")
    v_nom = Column(Float(53))
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


class EgonPfHvExtensionLink(Base):
    __tablename__ = "egon_etrago_extension_link"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    link_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    efficiency = Column(Float(53), server_default="1.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    p_nom = Column(Numeric, server_default="0.")
    p_nom_extendable = Column(Boolean, server_default="False")
    p_nom_min = Column(Float(53), server_default="0.")
    p_nom_max = Column(Float(53), server_default="inf")
    p_min_pu = Column(Float(53), server_default="0.")
    p_max_pu = Column(Float(53), server_default="1.")
    p_set = Column(Float(53))
    capital_cost = Column(Float(53), server_default="0.")
    marginal_cost = Column(Float(53), server_default="0.")
    length = Column(Float(53), server_default="0.")
    terrain_factor = Column(Float(53), server_default="1.")
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


class EgonPfHvExtensionTransformer(Base):
    __tablename__ = "egon_etrago_extension_transformer"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    trafo_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    model = Column((Text), server_default="t")
    x = Column((Numeric), server_default="0.")
    r = Column((Numeric), server_default="0.")
    g = Column((Numeric), server_default="0.")
    b = Column((Numeric), server_default="0.")
    s_nom = Column(Float(53), server_default="0.")
    s_nom_extendable = Column((Boolean), server_default="False")
    s_nom_min = Column(Float(53), server_default="0.")
    s_nom_max = Column(Float(53), server_default="inf")
    s_max_pu = Column(Float(53), server_default="1.")
    tap_ratio = Column(Float(53), server_default="1.")
    tap_side = Column((BigInteger), server_default="0")
    tap_position = Column((BigInteger), server_default="0")
    phase_shift = Column(Float(53), server_default="0.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    v_ang_min = Column(Float(53), server_default="-inf")
    v_ang_max = Column(Float(53), server_default="inf")
    capital_cost = Column(Float(53), server_default="0.")
    num_parallel = Column(Float(53), server_default="1.")
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


def create_tables():
    EgonPfHvExtensionBus.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvExtensionLine.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvExtensionLink.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvExtensionTransformer.__table__.drop(bind=engine, checkfirst=True)

    EgonPfHvExtensionBus.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvExtensionLine.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvExtensionLink.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvExtensionTransformer.__table__.create(bind=engine, checkfirst=True)


HOST = "xxx"
PORT = "xxx"
USER = "xxx"
PASSWORD = "xxx"
DATABASE = "xxx"

# Create connection with pgAdmin4 - Offline
engine = create_engine(
    f"postgresql+psycopg2://{USER}:"
    f"{PASSWORD}@{HOST}:"
    f"{PORT}/{DATABASE}",
    echo=False,
)


def read_table_from_nep(path):
    """Reads csv file including lines from NEP as a pandas.DataFrame

    Parameters
    ----------
    path : str
        Path to csv_file

    Returns
    -------
    lines_df : pandas.DataFrame
        Lines from NEP

    """

    # Read the Destination file from CSV
    lines_df = pd.read_csv(path, encoding="utf8")
    lines_df.loc[lines_df.Endpunkt == "Punkt Adlkofen", "Endpunkt"] = (
        lines_df.loc[lines_df.Startpunkt == "Punkt Adlkofen", "Endpunkt"].iloc[
            0
        ]
    )
    lines_df.drop(
        lines_df[lines_df.Startpunkt == "Punkt Adlkofen"].index,
        inplace=True,
        axis="index",
    )

    return lines_df


def unify_name(name):
    """Removes prefixes from names of substations to simplify mapping

    Commom prefixes where identified (manually) and are removed from the names.

    Parameters
    ----------
    name : str or pd.Series of strings
        Name(s) of substation

    Returns
    -------
    name : str or pd.Series of strings
        Updated name(s) of substations

    """

    for prefix in [
        "220 kV-Umspannwerk ",
        "Umspannwerk ",
        "Hauptumspannwerk ",
        "Lastverteilung/Schaltanlage ",
        "Station ",
        "Umspannanlage ",
        "Umspannstation ",
        "Schaltanlage ",
        "UW ",
        "UA ",
        "Kovertertstation ",
    ]:
        if type(name) == str:
            name = name.replace(prefix, "")
        else:
            name = name.str.replace(prefix, "")

    # Replace all different seperators with white spaces
    for seperator in ["/", "-"]:
        if type(name) == str:
            name = name.replace(seperator, " ")
        else:
            name = name.str.replace(seperator, " ")

    if type(name) == str:
        name = name.replace("ß", "ss")
    else:
        name = name.str.replace("ß", "ss")

    return name


def find_bus(name, missing_substations_csv):
    """Identifies bus_id from the eTraGo model for a given substation name.



    Parameters
    ----------
    name : TYPE
        DESCRIPTION.
    missing_substations_csv : TYPE
        DESCRIPTION.

    Returns
    -------
    bus : TYPE
        DESCRIPTION.

    """

    substation_ehv = db.select_geodataframe(
        """
        SELECT bus_id, lon, lat, point, polygon, voltage as v_nom, power_type,
               substation, osm_id, osm_www, frequency, subst_name
               
        FROM grid.egon_ehv_substation
        WHERE subst_name != 'NA'
        
        """,
        geom_col="point",
        epsg=4326,
    )

    # Remove prefix from names
    substation_ehv.subst_name = unify_name(substation_ehv.subst_name)

    substation_ehv.drop_duplicates("subst_name", keep="first", inplace=True)

    substation_hv = db.select_geodataframe(
        """
        SELECT bus_id, lon, lat, point, polygon, voltage as v_nom, power_type,
               substation, osm_id, osm_www, frequency, subst_name
               
        FROM grid.egon_hvmv_substation
        WHERE subst_name NOT IN (
            'NA', 'Umspannwerk', 'Umspannstation')
        """,
        epsg=4326,
        geom_col="point",
    )
    substation_hv.subst_name = unify_name(substation_hv.subst_name)

    substation_hv.drop_duplicates("subst_name", keep="first", inplace=True)

    additional_substations = pd.read_csv(missing_substations_csv)

    additional_substations = gpd.GeoDataFrame(
        additional_substations,
        geometry=gpd.points_from_xy(
            additional_substations.x, additional_substations.y
        ),
        crs="EPSG:4326",
    )

    additional_substations.to_crs(3035, inplace=True)

    additional_substations.geometry = additional_substations.geometry.buffer(
        10
    )  # 0.000001)

    additional_substations.to_crs(4326, inplace=True)

    other_buses = db.select_geodataframe(
        """
        SELECT DISTINCT ON (geom)
               bus_id, geom AS point, v_nom
                FROM grid.egon_etrago_bus
                WHERE scn_name = 'eGon2035'
                AND carrier = 'AC'
        ORDER  BY geom, v_nom;
        """,
        geom_col="point",
        epsg=4326,
    )

    additional_substations = other_buses.sjoin(
        additional_substations.to_crs(4326)
    )

    # 1st step: substation is in manually created file for substations
    # that were not part of the egon-data substations
    bus = additional_substations[additional_substations["subst_name"] == name]

    # Unify the name of the substation (e.g. removing "Umspannwerk" prefix)
    name = unify_name(name)

    # 2nd step: Look for substation name in eHV substations from egon-data
    if bus.empty:
        bus = substation_ehv[substation_ehv["subst_name"] == name]

    # 3rd step: Look for substation name in HV substations from egon-data
    if bus.empty:
        bus = substation_hv[substation_hv["subst_name"] == name]

    # 4th step: Look for a eHV substations from egon-data that contains the
    # name of the substation
    if bus.empty:
        bus = substation_ehv[substation_ehv["subst_name"].str.contains(name)]

    # 5th step: Look for a HV substations from egon-data that contains the
    # name of the substation
    if bus.empty:
        bus = substation_hv[substation_hv["subst_name"].str.contains(name)]

    # If no substation was found, a warning is printed. When this happens,
    # manual additions to the csv_file with additional substations are
    # required.
    if bus.empty:
        print(f"No matching bus found for {name}.")
    elif len(bus) > 1:
        print(f"More than one matching bus found for {name}:  {bus}")
    return bus


def extract_lines(lines_df, missing_substations_csv):
    """Match new lines from NEP to grid model

    Parameters
    ----------
    lines_df : pandas.DataFrame
        List of new lines from NEP including names of start and end substation
    missing_substations_csv : str
        name of file containing mission substations that were manually added

    Returns
    -------
    lines_df : pandas.DataFrame
        List of new lines from NEP including buses from the grid model

    """

    unique_line_id = db.next_etrago_id("line")

    for index, row in lines_df.iterrows():
        # Add Unique line id
        unique_line_id += 1
        lines_df.at[index, "line_id"] = unique_line_id

        # Match Similarity of Source & Destination files for Start point
        matching_rows_start = find_bus(
            row["Startpunkt"], missing_substations_csv
        )

        lines_df.at[index, "bus0"] = matching_rows_start.iloc[0]["bus_id"]
        lines_df.at[index, "v_nom0"] = matching_rows_start.iloc[0]["v_nom"]
        lines_df.at[index, "subst_name0"] = matching_rows_start.iloc[0][
            "subst_name"
        ]

        # Find coordinate for start point
        point_0 = matching_rows_start.iloc[0]["point"]
        formatted_point_0 = f"{point_0.x} {point_0.y}"
        lines_df.at[index, "Coordinate0"] = formatted_point_0

        # Match Similarity of Source & Destination files for Start point
        matching_rows_end = find_bus(row["Endpunkt"], missing_substations_csv)
        lines_df.at[index, "bus1"] = matching_rows_end.iloc[0]["bus_id"]
        lines_df.at[index, "v_nom1"] = matching_rows_end.iloc[0]["v_nom"]
        lines_df.at[index, "subst_name1"] = matching_rows_end.iloc[0][
            "subst_name"
        ]

        # Find coordinate for start point
        point_1 = matching_rows_end.iloc[0]["point"]
        formatted_point_1 = f"{point_1.x} {point_1.y}"
        lines_df.at[index, "Coordinate1"] = formatted_point_1

        # Calculate lenght of Transmission Line
        coordinate_0_str = str(lines_df.at[index, "Coordinate0"])
        lon0, lat0 = map(float, coordinate_0_str.split(" "))
        coordinate_1_str = str(lines_df.at[index, "Coordinate1"])
        lon1, lat1 = map(float, coordinate_1_str.split(" "))
        distance = geodesic((lat0, lon0), (lat1, lon1)).kilometers
        lines_df.at[index, "length"] = round(distance * 1.14890133371257, 1)
        lines_df.at[index, "length1"] = (
            f"HV {round(distance*1.14890133371257,1)}"
        )

    return lines_df


def ac_parameters(df):
    """Sets parameters for AC lines using default settings from egon-data

    Parameters
    ----------
    df : pandas.DataFrame
        List of new lines from NEP without electrical parameters

    Returns
    -------
    ac_lines : pandas.DataFrame
        List of new lines from NEP with electrical parameters

    """
    # Add electrical parameters
    electrical_parameters = get_sector_parameters("electricity", "eGon2035")[
        "electrical_parameters"
    ]
    capital_cost = get_sector_parameters("electricity", "eGon2035")[
        "capital_cost"
    ]

    ac_lines = df[df.carrier == "AC"].copy()
    ac_lines.loc[:, "Spannung"].fillna(380.0, inplace=True)
    ac_lines.loc[:, "num_parallel"].fillna(2.0, inplace=True)
    ac_lines.loc[:, "s_nom_min"] = 0
    ac_lines.loc[:, "s_nom_extendable"] = True

    # Overhead lines
    ac_lines.loc[ac_lines["cable/line"] == "line", "s_nom"] = (
        ac_lines.loc[ac_lines["cable/line"] == "line", "num_parallel"]
        * electrical_parameters["ac_line_380kV"]["s_nom"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "line", "s_nom_max"] = (
        ac_lines.loc[ac_lines["cable/line"] == "line", "num_parallel"]
        * electrical_parameters["ac_line_380kV"]["s_nom"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "line", "x"] = (
        electrical_parameters["ac_line_380kV"]["L"]
        * 2
        * 50
        * np.pi
        * 1e-3
        * ac_lines.loc[ac_lines["cable/line"] == "line", "length"]
        / ac_lines.loc[ac_lines["cable/line"] == "line", "num_parallel"]
    )
    ac_lines.loc[ac_lines["cable/line"] == "line", "r"] = (
        electrical_parameters["ac_line_380kV"]["R"]
        * ac_lines.loc[ac_lines["cable/line"] == "line", "length"]
        / ac_lines.loc[ac_lines["cable/line"] == "line", "num_parallel"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "line", "capital_cost"] = (
        ac_lines.loc[ac_lines["cable/line"] == "line", "length"].mul(
            capital_cost["ac_ehv_overhead_line"]
        )
    )

    # Underground cables
    ac_lines.loc[ac_lines["cable/line"] == "line", "s_nom"] = (
        ac_lines.loc[ac_lines["cable/line"] == "cable", "num_parallel"]
        * electrical_parameters["ac_cable_380kV"]["s_nom"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "cable", "s_nom_max"] = (
        ac_lines.loc[ac_lines["cable/line"] == "cable", "num_parallel"]
        * electrical_parameters["ac_cable_380kV"]["s_nom"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "cable", "x"] = (
        electrical_parameters["ac_cable_380kV"]["L"]
        * 2
        * 50
        * np.pi
        * 1e-3
        * ac_lines.loc[ac_lines["cable/line"] == "cable", "length"]
        / ac_lines.loc[ac_lines["cable/line"] == "cable", "num_parallel"]
    )
    ac_lines.loc[ac_lines["cable/line"] == "cable", "r"] = (
        electrical_parameters["ac_cable_380kV"]["R"]
        * ac_lines.loc[ac_lines["cable/line"] == "cable", "length"]
        / ac_lines.loc[ac_lines["cable/line"] == "cable", "num_parallel"]
    )

    ac_lines.loc[ac_lines["cable/line"] == "cable", "capital_cost"] = (
        ac_lines.loc[ac_lines["cable/line"] == "cable", "length"].mul(
            capital_cost["ac_ehv_cable"]
        )
    )

    df.loc[df.carrier == "AC", "s_nom"] = ac_lines.s_nom.values
    return ac_lines


def dc_parameters(df):
    """Sets parameters for DC lines using default settings from egon-data

    Parameters
    ----------
    df : pandas.DataFrame
        List of new lines from NEP without electrical parameters

    Returns
    -------
    dc_lines : pandas.DataFrame
        List of new lines from NEP with electrical parameters

    """
    # Add electrical parameters
    dc_lines = df[df.carrier == "DC"].copy()
    dc_lines.loc[:, "Spannung"].fillna(380.0, inplace=True)
    dc_lines["p_nom"] = dc_lines.loc[:, "s_nom"]
    dc_lines["p_nom_min"] = 0
    dc_lines["p_nom_max"] = dc_lines.loc[:, "s_nom"]
    dc_lines["p_nom_extendable"] = True
    dc_lines["efficiency"] = 1
    dc_lines["p_min_pu"] = -1

    max_link_id = db.next_etrago_id("link")

    dc_lines.line_id = dc_lines.index + max_link_id
    capital_cost = get_sector_parameters("electricity", "eGon2035")[
        "capital_cost"
    ]
    dc_lines["capital_cost"] = (
        dc_lines.loc[:, "length"].mul(capital_cost["dc_cable"])
        + capital_cost["dc_inverter"]
    )
    return dc_lines


def add_geometry(df):
    """Sets geometry for new lines from NEP based on bus geometry

    Parameters
    ----------
    df : pandas.DataFrame
        List of new lines from NEP without geometry

    Returns
    -------
    pandas.DataFrame
        List of new lines from NEP with geometry

    """
    # Add geometry
    df.loc[:, "geom_0"] = gpd.points_from_xy(
        df.Coordinate0.str.split(" ").str[0].astype(float),
        df.Coordinate0.str.split(" ").str[1].astype(float),
    )
    df.loc[:, "geom_1"] = gpd.points_from_xy(
        df.Coordinate1.str.split(" ").str[0].astype(float),
        df.Coordinate1.str.split(" ").str[1].astype(float),
    )

    geometry = gpd.GeoDataFrame(
        df[["Startpunkt", "Endpunkt"]],
        geometry=df.apply(
            lambda x: LineString([x["geom_0"], x["geom_1"]]), axis=1
        ),
        crs=4326,
    )

    return gpd.GeoDataFrame(
        df,
        geometry=geometry.geometry,
    ).rename_geometry("topo")


def add_transformers(df):
    """Creates trafos to connect new lines if needed

    Parameters
    ----------
    df : pandas.DataFrame
        List of new lines from NEP

    Returns
    -------
    trafos : pandas.DataFrame
        List of transformers needed to connect the new lines
    new_buses : pandas.DataFrame
        List of buses needed to connect the new lines

    """

    # Add trafos if needed
    trafo_for_buses = pd.concat(
        [
            df.loc[~df.v_nom0.astype(str).str.contains("38"), :].bus0,
            df.loc[~df.v_nom1.astype(str).str.contains("38"), :].bus1,
        ]
    ).unique()

    max_bus_id = db.next_etrago_id("bus")

    max_trafo_id = db.next_etrago_id("transformer")

    existing_buses_geometry = db.select_dataframe(
        """
        SELECT bus_id, x, y
        FROM grid.egon_etrago_bus
        WHERE scn_name = 'eGon2035'
        AND carrier = 'AC'
        """,
    ).set_index("bus_id")

    new_buses = pd.DataFrame(columns=["bus_id", "x", "y", "old_bus"])

    new_buses["old_bus"] = trafo_for_buses.astype(int)

    new_buses["x"] = existing_buses_geometry.loc[
        new_buses["old_bus"], "x"
    ].values
    new_buses["y"] = existing_buses_geometry.loc[
        new_buses["old_bus"], "y"
    ].values
    new_buses["v_nom"] = 380.0
    new_buses["carrier"] = "AC"

    new_buses["bus_id"] = new_buses.index + max_bus_id

    trafos = pd.DataFrame(columns=["bus0", "bus1", "s_nom", "x"])
    trafos["bus0"] = new_buses.loc[:, "old_bus"]
    trafos["bus1"] = new_buses.loc[:, "bus_id"]

    # Set capacity for transformer based on attach line capacities
    trafos.s_nom.fillna(0, inplace=True)
    trafos.loc[trafos["bus0"].isin(df.bus0), "s_nom"] += (
        df.set_index("bus0")
        .loc[trafos["bus0"][trafos["bus0"].isin(df.bus0)], "s_nom"]
        .groupby("bus0")
        .sum()
        .values
    )

    trafos.loc[trafos["bus0"].isin(df.bus1), "s_nom"] += (
        df.set_index("bus1")
        .loc[trafos["bus0"][trafos["bus0"].isin(df.bus1)], "s_nom"]
        .groupby("bus1")
        .max()
        .values
    )

    for i in trafos.index:
        trafos.loc[i, "s_nom"] = choose_transformer(trafos.loc[i, "s_nom"])[0]
        trafos.loc[i, "x"] = choose_transformer(trafos.loc[i, "s_nom"])[1]

    trafos["trafo_id"] = trafos.index + max_trafo_id

    return trafos, new_buses


def reset_buses_based_on_trafos(df, new_buses):

    df.loc[df.bus0.isin(new_buses.old_bus), "bus0"] = (
        new_buses.set_index("old_bus")
        .loc[df.loc[df.bus0.isin(new_buses.old_bus), "bus0"], "bus_id"]
        .values
    )
    df.loc[df.bus1.isin(new_buses.old_bus), "bus1"] = (
        new_buses.set_index("old_bus")
        .loc[df.loc[df.bus1.isin(new_buses.old_bus), "bus1"], "bus_id"]
        .values
    )


def decomissioining_create_file():
    """Used to create a shapefile including decomissioned lines.

    Uses hard-coded line_ids and should be therefore only used with the
    matching data model (last run from Hetzner server)
    The shapefile can be used with any data model.

    Returns
    -------
    None.

    """

    decomissioning = pd.read_csv("decomissioning.csv")

    decomissioning = decomissioning[decomissioning.Szenario_C_2035]

    liste = []

    for i in decomissioning.index:
        if (
            not decomissioning.to_decomission[i]
            != decomissioning.to_decomission[i]
        ):
            liste.extend(decomissioning.to_decomission[i].split(", "))

    existing_lines = db.select_geodataframe(
        """
        SELECT * 
        FROM grid.egon_etrago_line
        WHERE scn_name = 'eGon2035'
        """,
        geom_col="geom",
    )

    to_decomission = existing_lines[
        existing_lines.line_id.astype(str).isin(liste)
    ]
    to_decomission["scn_name"] = "decomissioining_nep2021_c2035"

    decomissioning_conf = pd.read_csv("decomissioning.csv")
    decomissioning_conf = decomissioning_conf[decomissioning_conf.confirmed]
    liste = []
    for i in decomissioning_conf.index:
        if (
            not decomissioning_conf.to_decomission[i]
            != decomissioning_conf.to_decomission[i]
        ):
            liste.extend(decomissioning_conf.to_decomission[i].split(", "))

    to_decomission_conf = existing_lines[
        existing_lines.line_id.astype(str).isin(liste)
    ]
    to_decomission_conf["scn_name"] = "decomissioining_nep2021_confirmed"
    pd.concat(
        [
            to_decomission[["geom", "v_nom", "scn_name"]],
            to_decomission_conf[["geom", "v_nom", "scn_name"]],
        ],
        ignore_index=True,
    ).to_file("lines_to_decomission.shp")


def decomissioning(scenario, path_to_file_decomissioning_lines):
    """Adds lines that are decomissioned based on shapefile including geoms

    Parameters
    ----------
    scenario : str
        Name of scenario.
    path_to_file_decomissioning_lines : str
        Path to shapefile including decomissioning lines.

    Returns
    -------
    None.

    """

    to_decom = gpd.read_file(path_to_file_decomissioning_lines)

    to_decom = to_decom.loc[
        to_decom.scn_name == f"decomissioining_{scenario}", :
    ]

    existing_lines = db.select_geodataframe(
        """
        SELECT * 
        FROM grid.egon_etrago_line
        WHERE scn_name = 'eGon2035'
        """,
        geom_col="geom",
    )

    existing_lines[existing_lines.geom.isin(to_decom.geometry)]

    join = gpd.sjoin(to_decom, existing_lines, predicate="covers")

    join = join[join.v_nom_left == join.v_nom_right]

    to_database = existing_lines[existing_lines.line_id.isin(join.line_id)]

    to_database["scn_name"] = f"decomissioining_{scenario}"

    to_database.to_crs(4326).to_postgis(
        EgonPfHvExtensionLine.__table__.name,
        schema="grid",
        if_exists="append",
        con=engine,
        index=False,
    )


def run():
    """Central function to create scenario variations for lines from NEP

    Returns
    -------
    None.

    """

    path_to_file_new_lines = "lines_from_nep.csv"
    path_to_file_decomissioning_lines = "lines_to_decomission.shp"
    path_to_file_missing_substations = "missing_substations.csv"

    create_tables()

    scenarios = ["nep2021_c2035", "nep2021_confirmed"]

    lines_df = read_table_from_nep(path_to_file_new_lines)
    for scenario in scenarios:

        df = extract_lines(
            lines_df, missing_substations_csv=path_to_file_missing_substations
        )

        if scenario == "nep_2021_c2035":
            df = df.loc[df.Szenario_C_2035, :]

        elif scenario == "nep2021_confirmed":
            df = df.loc[df.confirmed, :]

        gdf = add_geometry(df)

        ac_lines = ac_parameters(gdf)

        dc_lines = dc_parameters(gdf)
        df.loc[df.carrier == "AC", "s_nom"] = ac_lines.s_nom.values

        df.v_nom.fillna(380.0, inplace=True)

        trafos, new_buses = add_transformers(df)

        reset_buses_based_on_trafos(ac_lines, new_buses)
        reset_buses_based_on_trafos(dc_lines, new_buses)

        ac_lines["scn_name"] = scenario
        dc_lines["scn_name"] = scenario
        trafos["scn_name"] = scenario
        new_buses["scn_name"] = scenario

        ac_lines.line_id = ac_lines.line_id.astype(int)
        ac_lines.bus0 = ac_lines.bus0.astype(int)
        ac_lines.bus1 = ac_lines.bus1.astype(int)

        dc_lines["link_id"] = dc_lines.line_id.astype(int)
        dc_lines.bus0 = dc_lines.bus0.astype(int)
        dc_lines.bus1 = dc_lines.bus1.astype(int)

        trafos.trafo_id = trafos.trafo_id.astype(int)
        trafos.bus0 = trafos.bus0.astype(int)
        trafos.bus1 = trafos.bus1.astype(int)

        new_buses.bus_id = new_buses.bus_id.astype(int)
        buses_df = gpd.GeoDataFrame(
            new_buses.drop("old_bus", axis="columns"),
            geometry=gpd.points_from_xy(new_buses.x, new_buses.y),
            crs=4326,
        ).rename_geometry("geom")

        buses_df.to_postgis(
            EgonPfHvExtensionBus.__table__.name,
            schema="grid",
            if_exists="append",
            con=engine,
            index=False,
        )

        ac_lines.loc[
            :,
            [
                "scn_name",
                "line_id",
                "carrier",
                "s_nom",
                "lifetime",
                "length",  # "cables",
                "num_parallel",
                "v_nom",
                "bus0",
                "bus1",
                "x",
                "r",
                "s_nom_min",
                "s_nom_max",
                "s_nom_extendable",
                "topo",
                "capital_cost",
            ],
        ].to_postgis(
            EgonPfHvExtensionLine.__table__.name,
            schema="grid",
            if_exists="append",
            con=engine,
            index=False,
        )

        dc_lines.loc[
            :,
            [
                "scn_name",
                "link_id",
                "carrier",
                "p_nom",
                "lifetime",
                "length",  # "cables",
                "bus0",
                "bus1",
                "efficiency",
                "p_min_pu",
                "p_nom_min",
                "p_nom_max",
                "p_nom_extendable",
                "topo",
                "capital_cost",
            ],
        ].to_postgis(
            EgonPfHvExtensionLink.__table__.name,
            schema="grid",
            if_exists="append",
            con=engine,
            index=False,
        )

        trafos.loc[
            :, ["scn_name", "trafo_id", "s_nom", "bus0", "bus1", "x"]
        ].to_sql(
            EgonPfHvExtensionTransformer.__table__.name,
            schema="grid",
            if_exists="append",
            con=engine,
            index=False,
        )
        decomissioning(scenario, path_to_file_decomissioning_lines)

    print("Operation successful")
