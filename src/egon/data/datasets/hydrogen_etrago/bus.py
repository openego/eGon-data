"""The central module containing all code dealing with heat sector in etrago
"""
import datetime
import json

from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets.etrago_helpers import (
    copy_and_modify_buses,
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.metadata import context, contributors, meta_metadata, sources


def insert_hydrogen_buses(scenario="eGon2035"):
    """Insert hydrogen buses to etrago table

    Hydrogen buses are divided into cavern and methane grid attached buses

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    """
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    target = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_buses"]
    # initalize dataframe for hydrogen buses
    carrier = "H2_saltcavern"
    hydrogen_buses = initialise_bus_insertion(
        carrier, target, scenario=scenario
    )
    insert_H2_buses_from_saltcavern(
        hydrogen_buses, carrier, sources, target, scenario
    )

    carrier = "H2_grid"
    hydrogen_buses = initialise_bus_insertion(
        carrier, target, scenario=scenario
    )
    insert_H2_buses_from_CH4_grid(hydrogen_buses, carrier, target, scenario)


Base = declarative_base()


class EgonMapACH2(Base):
    source_list = [
        sources()["openstreetmap"],
        sources()["SciGRID_gas"],
        sources()["bgr_inspeeds_data_bundle"],
    ]
    meta_ac_h2 = {
        "name": "grid.egon_etrago_ac_h2",
        "title": "Mapping table of AC-H2 buses",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Table mapping AC and H2 buses in Germany",
        "language": ["en-EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": source_list,
        "licenses": [data["licenses"] for data in source_list],
        "contributors": contributors(["fw"]),
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "grid.egon_etrago_ac_h2",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "scn_name",
                            "description": "Name of the scenario",
                            "type": "str",
                            "unit": None,
                        },
                        {
                            "name": "bus_H2",
                            "description": "H2 bus_id",
                            "type": "integer",
                            "unit": None,
                        },
                        {
                            "name": "bus_AC",
                            "description": "AC bus_id",
                            "type": "integer",
                            "unit": None,
                        },
                    ],
                    "primaryKey": ["scn_name", "bus_H2"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }
    # Create json dump
    meta_json_ac_h2 = (
        "'" + json.dumps(meta_ac_h2, indent=4, ensure_ascii=False) + "'"
    )

    __tablename__ = "egon_etrago_ac_h2"
    __table_args__ = {
        "schema": "grid",
        "comment": meta_json_ac_h2,
    }

    scn_name = Column(Text, primary_key=True, nullable=False)
    bus_H2 = Column(BigInteger, primary_key=True, nullable=False)
    bus_AC = Column(BigInteger, primary_key=False, nullable=False)


def create_AC_H2_table():
    engine = db.engine()
    EgonMapACH2.__table__.drop(bind=engine, checkfirst=True)
    EgonMapACH2.__table__.create(bind=engine, checkfirst=True)


def insert_H2_buses_from_saltcavern(gdf, carrier, sources, target, scn_name):
    """Insert the H2 buses based saltcavern locations to db.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    carrier : str
        Name of the carrier.
    sources : dict
        Sources schema and table information.
    target : dict
        Target schema and table information.
    scn_name : str
        Name of the scenario.
    """
    # electrical buses related to saltcavern storage
    el_buses = db.select_dataframe(
        f"""
        SELECT bus_id
        FROM  {sources['saltcavern_data']['schema']}.
        {sources['saltcavern_data']['table']}"""
    )["bus_id"]

    # locations of electrical buses (filtering not necessarily required)
    locations = db.select_geodataframe(
        f"""
        SELECT bus_id, geom
        FROM  {sources['buses']['schema']}.
        {sources['buses']['table']} WHERE scn_name = '{scn_name}'
        AND country = 'DE'""",
        index_col="bus_id",
    ).to_crs(epsg=4326)

    # filter by related electrical buses and drop duplicates
    locations = locations.loc[el_buses]
    locations = locations[~locations.index.duplicated(keep="first")]

    # AC bus ids and respective hydrogen bus ids are written to db for
    # later use (hydrogen storage mapping)
    AC_bus_ids = locations.index.copy()

    # create H2 bus data
    hydrogen_bus_ids = finalize_bus_insertion(
        locations, carrier, target, scenario=scn_name
    )

    gdf_H2_cavern = hydrogen_bus_ids[["bus_id"]].rename(
        columns={"bus_id": "bus_H2"}
    )
    gdf_H2_cavern["bus_AC"] = AC_bus_ids
    gdf_H2_cavern["scn_name"] = hydrogen_bus_ids["scn_name"]

    create_AC_H2_table()

    # Insert data to db
    gdf_H2_cavern.to_sql(
        "egon_etrago_ac_h2",
        db.engine(),
        schema="grid",
        index=False,
        if_exists="append",
    )


class EgonMapH2CH4(Base):
    source_list = [
        sources()["openstreetmap"],
        sources()["SciGRID_gas"],
        sources()["bgr_inspeeds_data_bundle"],
    ]
    meta_H2_CH4 = {
        "name": "grid.egon_etrago_ch4_h2",
        "title": "Mapping table of CH4-H2 buses",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Table mapping CH4 and H2 buses in Germany",
        "language": ["en-EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": source_list,
        "licenses": [data["licenses"] for data in source_list],
        "contributors": contributors(["fw"]),
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "grid.egon_etrago_ch4_h2",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "scn_name",
                            "description": "Name of the scenario",
                            "type": "str",
                            "unit": None,
                        },
                        {
                            "name": "bus_H2",
                            "description": "H2 bus_id",
                            "type": "integer",
                            "unit": None,
                        },
                        {
                            "name": "bus_CH4",
                            "description": "CH4 bus_id",
                            "type": "integer",
                            "unit": None,
                        },
                    ],
                    "primaryKey": ["scn_name", "bus_H2"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    # Create json dump
    meta_json_H2_CH4 = (
        "'" + json.dumps(meta_H2_CH4, indent=4, ensure_ascii=False) + "'"
    )

    __tablename__ = "egon_etrago_ch4_h2"
    __table_args__ = {
        "schema": "grid",
        "comment": meta_json_H2_CH4,
    }

    scn_name = Column(Text, primary_key=True, nullable=False)
    bus_H2 = Column(BigInteger, primary_key=True, nullable=False)
    bus_CH4 = Column(BigInteger, primary_key=False, nullable=False)


def create_H2_CH4_table():
    engine = db.engine()
    EgonMapH2CH4.__table__.drop(bind=engine, checkfirst=True)
    EgonMapH2CH4.__table__.create(bind=engine, checkfirst=True)


def insert_H2_buses_from_CH4_grid(gdf, carrier, target, scn_name):
    """Insert the H2 buses based on CH4 grid to db.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    carrier : str
        Name of the carrier.
    target : dict
        Target schema and table information.
    scn_name : str
        Name of the scenario.

    """
    # Connect to local database
    engine = db.engine()

    # Select the CH4 buses
    sql_CH4 = f"""SELECT bus_id, scn_name, geom
                 FROM grid.egon_etrago_bus
                 WHERE carrier = 'CH4' AND scn_name = '{scn_name}'
                 AND country = 'DE';"""

    gdf_H2 = db.select_geodataframe(sql_CH4, epsg=4326)
    # CH4 bus ids and respective hydrogen bus ids are written to db for
    # later use (CH4 grid to H2 links)
    CH4_bus_ids = gdf_H2[["bus_id", "scn_name"]].copy()

    H2_bus_ids = finalize_bus_insertion(
        gdf_H2, carrier, target, scenario=scn_name
    )

    gdf_H2_CH4 = H2_bus_ids[["bus_id"]].rename(columns={"bus_id": "bus_H2"})
    gdf_H2_CH4["bus_CH4"] = CH4_bus_ids["bus_id"]
    gdf_H2_CH4["scn_name"] = CH4_bus_ids["scn_name"]

    create_H2_CH4_table()

    # Insert data to db
    gdf_H2_CH4.to_sql(
        "egon_etrago_ch4_h2",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )


def insert_hydrogen_buses_eGon100RE():
    """Copy H2 buses from the eGon2035 to the eGon100RE scenario."""
    copy_and_modify_buses(
        "eGon2035",
        "eGon100RE",
        {"carrier": ["H2_grid", "H2_saltcavern"]},
    )
