"""The central module containing code to create substation tables

"""

import egon.data.config
from egon.data import db
from sqlalchemy import Column, Float, Integer, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry

Base = declarative_base()


class EgonEhvSubstation(Base):
    __tablename__ = 'egon_ehv_substation'
    __table_args__ = {'schema': 'grid'}
    subst_id = Column(Integer, Sequence('ehv_id_seq'), primary_key=True)
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry('POINT', 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)


class EgonHvmvSubstation(Base):
    __tablename__ = 'egon_hvmv_substation'
    __table_args__ = {'schema': 'grid'}
    subst_id = Column(Integer, Sequence('hvmv_id_seq'), primary_key=True)
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry('POINT', 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)

def create_tables():
    """Create tables for substation data
    Returns
    -------
    None.
    """
    cfg_ehv = egon.data.config.datasets()['ehv_substation']
    cfg_hvmv = egon.data.config.datasets()['hvmv_substation']
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg_ehv['processed']['schema']};")

    db.execute_sql(f"""DROP TABLE IF EXISTS {cfg_ehv['processed']['schema']}.
                   {cfg_ehv['processed']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {cfg_hvmv['processed']['schema']}.
                   {cfg_hvmv['processed']['table']} CASCADE;""")

    engine = db.engine()
    EgonEhvSubstation.__table__.create(bind=engine, checkfirst=True)
    EgonHvmvSubstation.__table__.create(bind=engine, checkfirst=True)
