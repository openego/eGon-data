"""The central module containing all code to create tables for osm landuse
extraction.
"""

from egon.data import db
import egon.data.config

from sqlalchemy import Column, Float, Integer, String
from geoalchemy2.types import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import HSTORE

# will be later imported from another file ###
Base = declarative_base()


class OsmPolygonUrban(Base):
    __tablename__ = "osm_polygon_urban"
    __table_args__ = {"schema": "openstreetmap"}
    gid = Column(Integer, primary_key=True)
    osm_id = Column(Integer)
    name = Column(String)
    sector = Column(Integer)
    sector_name = Column(String(20))
    area_ha = Column(Float)
    tags = Column(HSTORE)
    vg250 = Column(String(10))
    geom = Column(Geometry('MultiPolygon', 3035))


def create_landuse_table():
    """Create tables for landuse data
    Returns
    -------
    None.
    """
    cfg = egon.data.config.datasets()['landuse']['target']

    # Drop tables
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {cfg['schema']}.{cfg['table']} CASCADE;"""
    )

    engine = db.engine()
    OsmPolygonUrban.__table__.create(bind=engine, checkfirst=True)

