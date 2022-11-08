"""The central module containing all code to create tables for osm landuse
extraction.
"""

import os

from airflow.operators.postgres_operator import PostgresOperator
from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base
import importlib_resources as resources

from egon.data import db
from egon.data.datasets import Dataset
import egon.data.config

# will be later imported from another file ###
Base = declarative_base()


class OsmPolygonUrban(Base):
    __tablename__ = "osm_landuse"
    __table_args__ = {"schema": "openstreetmap"}
    id = Column(Integer, primary_key=True)
    osm_id = Column(Integer)
    name = Column(String)
    sector = Column(Integer)
    sector_name = Column(String(20))
    area_ha = Column(Float)
    tags = Column(HSTORE)
    vg250 = Column(String(10))
    geom = Column(Geometry("MultiPolygon", 3035))


class OsmLanduse(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OsmLanduse",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                create_landuse_table,
                PostgresOperator(
                    task_id="osm_landuse_extraction",
                    sql=resources.read_text(
                        __name__, "osm_landuse_extraction.sql"
                    ),
                    postgres_conn_id="egon_data",
                    autocommit=True,
                ),
            ),
        )


class LoadArea(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="LoadArea",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                osm_landuse_melt,
                census_cells_melt,
                osm_landuse_census_cells_melt,
                loadareas_create,
                loadareas_add_demand,
                drop_temp_tables,
            ),
        )


def create_landuse_table():
    """Create tables for landuse data
    Returns
    -------
    None.
    """
    cfg = egon.data.config.datasets()["landuse"]["target"]

    # Create schema if not exists
    db.execute_sql(f"""CREATE SCHEMA IF NOT EXISTS {cfg['schema']};""")

    # Drop tables
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {cfg['schema']}.{cfg['table']} CASCADE;"""
    )

    engine = db.engine()
    OsmPolygonUrban.__table__.create(bind=engine, checkfirst=True)


def execute_sql_script(script):
    """Execute SQL script

    Parameters
    ----------
    script : str
        Filename of script
    """
    db.execute_sql_script(os.path.join(os.path.dirname(__file__), script))


def osm_landuse_melt():
    """Melt all OSM landuse areas by: buffer, union, unbuffer"""
    print("Melting OSM landuse areas from openstreetmap.osm_landuse...")
    execute_sql_script("osm_landuse_melt.sql")


def census_cells_melt():
    """Melt all census cells: buffer, union, unbuffer"""
    print(
        "Melting census cells from "
        "society.destatis_zensus_population_per_ha_inside_germany..."
    )
    execute_sql_script("census_cells_melt.sql")


def osm_landuse_census_cells_melt():
    """Melt OSM landuse areas and census cells"""
    print(
        "Melting OSM landuse areas from openstreetmap.osm_landuse_melted and "
        "census cells from "
        "society.egon_destatis_zensus_cells_melted_cluster..."
    )
    execute_sql_script("osm_landuse_census_cells_melt.sql")


def loadareas_create():
    """Create load areas from merged OSM landuse and census cells:

    * Cut Loadarea with MV Griddistrict
    * Identify and exclude Loadarea smaller than 100m².
    * Generate Centre of Loadareas with Centroid and PointOnSurface.
    * Calculate population from Census 2011.
    * Cut all 4 OSM sectors with MV Griddistricts.
    * Calculate statistics like NUTS and AGS code.
    * Check for Loadareas without AGS code.
    """
    print("Create initial load areas and add some sector stats...")
    execute_sql_script("loadareas_create.sql")


def loadareas_add_demand():
    """Adds consumption and peak load per sector to load areas"""
    print("Add consumption and peak loads to load areas...")
    execute_sql_script("loadareas_add_demand.sql")


def drop_temp_tables():
    print("Dropping temp tables, views and sequences...")
    execute_sql_script("drop_temp_tables.sql")
