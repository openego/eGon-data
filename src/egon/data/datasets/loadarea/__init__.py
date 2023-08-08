"""
OSM landuse extraction and load areas creation.

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
    """
    Class definition of table openstreetmap.osm_landuse.
    """
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
    """
    OSM landuse extraction.

    * Landuse data is extracted from OpenStreetMap: residential, retail,
      industrial, Agricultural
    * Data is cut with German borders (VG 250), data outside is dropped
    * Invalid geometries are fixed
    * Results are stored in table `openstreetmap.osm_landuse`

    Note: industrial demand contains:
      * voltage levels 4-7
      * only demand from ind. sites+osm located in LA!

    """

    #:
    name: str = "OsmLanduse"
    #:
    version: str = "0.0.0"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
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
    """
    Creates load area data based on OSM and census data.

    Create and update the `demand.egon_loadarea` table with new data, based on OSM and
    census data. Among other things, area updates are carried out, smaller load areas
    are removed, center calculations are performed, and census data are added.
    Statistics for various OSM sectors are also calculated and inserted.

    *Dependencies*
      * :py:class:`OsmLanduse <egon.data.datasets.loadarea.OsmLanduse>`
      * :py:class:`ZensusVg250 <egon.data.datasets.zensus_vg250.ZensusVg250>`
      * :py:class:`HouseholdElectricityDemand <egon.data.datasets.electricity_demand.HouseholdElectricityDemand>`
      * :py:func:`get_building_peak_loads <egon.data.datasets.electricity_demand_timeseries.hh_buildings.get_building_peak_loads>`
      * :py:class:`CtsDemandBuildings <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsDemandBuildings>`
      * :py:class:`IndustrialDemandCurves <egon.data.datasets.industry.IndustrialDemandCurves>`

    *Resulting tables*
      * :class:`demand.egon_loadarea` is created and filled (no associated Python class)

    Note: industrial demand contains:
      * voltage levels 4-7
      * only demand from ind. sites+osm located in LA!

    """

    #:
    name: str = "LoadArea"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                osm_landuse_melt,
                census_cells_melt,
                osm_landuse_census_cells_melt,
                loadareas_create,
                {
                    loadareas_add_demand_hh,
                    loadareas_add_demand_cts,
                    loadareas_add_demand_ind,
                },
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


def loadareas_add_demand_hh():
    """Adds consumption and peak load to load areas for households"""
    print("Add consumption and peak loads to load areas for households...")
    execute_sql_script("loadareas_add_demand_hh.sql")


def loadareas_add_demand_cts():
    """Adds consumption and peak load to load areas for CTS"""
    print("Add consumption and peak loads to load areas for CTS...")
    execute_sql_script("loadareas_add_demand_cts.sql")


def loadareas_add_demand_ind():
    """Adds consumption and peak load to load areas for industry"""
    print("Add consumption and peak loads to load areas for industry...")
    execute_sql_script("loadareas_add_demand_ind.sql")


def drop_temp_tables():
    print("Dropping temp tables, views and sequences...")
    execute_sql_script("drop_temp_tables.sql")
