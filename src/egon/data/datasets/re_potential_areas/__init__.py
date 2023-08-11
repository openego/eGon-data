"""The central module containing all code dealing with importing data on
potential areas for wind onshore and ground-mounted PV.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset, Tasks
import egon.data.config

Base = declarative_base()


class EgonRePotentialAreaPvAgriculture(Base):
    __tablename__ = "egon_re_potential_area_pv_agriculture"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry("MULTIPOLYGON", 3035))


class EgonRePotentialAreaPvRoadRailway(Base):
    __tablename__ = "egon_re_potential_area_pv_road_railway"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry("MULTIPOLYGON", 3035))


class EgonRePotentialAreaWind(Base):
    __tablename__ = "egon_re_potential_area_wind"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry("MULTIPOLYGON", 3035))


def create_tables():
    """Create tables for RE potential areas"""

    data_config = egon.data.config.datasets()
    schema = data_config["re_potential_areas"]["target"].get(
        "schema", "supply"
    )

    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    engine = db.engine()

    # drop tables
    EgonRePotentialAreaPvAgriculture.__table__.drop(engine, checkfirst=True)
    EgonRePotentialAreaPvRoadRailway.__table__.drop(engine, checkfirst=True)
    EgonRePotentialAreaWind.__table__.drop(engine, checkfirst=True)

    # create tables
    EgonRePotentialAreaPvAgriculture.__table__.create(
        bind=engine, checkfirst=True
    )
    EgonRePotentialAreaPvRoadRailway.__table__.create(
        bind=engine, checkfirst=True
    )
    EgonRePotentialAreaWind.__table__.create(bind=engine, checkfirst=True)


def insert_data():
    """Insert data into DB"""

    data_bundle_dir = Path(
        ".",
        "data_bundle_egon_data",
        "re_potential_areas",
    )

    dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    if dataset == "Everything":
        map_section = "path_table_map"
    elif dataset == "Schleswig-Holstein":
        map_section = "path_table_map_testmode"
    else:
        raise ValueError(f"'{dataset}' is not a valid dataset boundary.")

    data_config = egon.data.config.datasets()
    pa_config = data_config["re_potential_areas"]

    file_table_map = {
        data_bundle_dir / Path(file).name: table
        for file, table in pa_config["target"][map_section].items()
    }

    engine = db.engine()

    for file, table in file_table_map.items():
        data = gpd.read_file(file).to_crs("EPSG:3035")
        data.rename(columns={"geometry": "geom"}, inplace=True)
        data.set_geometry("geom", inplace=True)

        schema = pa_config["target"].get("schema", "supply")

        # create database table from geopandas dataframe
        data[["id", "geom"]].to_postgis(
            table,
            engine,
            schema=schema,
            index=False,
            if_exists="append",
            dtype={"geom": Geometry()},
        )


@dataclass
class re_potential_area_setup(Dataset):
    #:
    name: str = "RePotentialAreas"
    #:
    version: str = "0.0.1"
    #:
    tasks: Tasks = (create_tables, insert_data)
