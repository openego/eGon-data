"""The central module containing all code dealing with power plant data.
"""
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, Sequence
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class EgonPowerPlants(Base):
    __tablename__ = 'egon_power_plants'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, Sequence('pp_seq'), primary_key=True)
    source = Column(String)     # source of data (MaStr/NEP/...)
    source_id = Column(String)  # id used in original source
    carrier = Column(String)
    chp = Column(String)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    subst_id = Column(Integer)
    voltage_level = Column(Integer)
    w_id = Column(Integer)      # id of corresponding weather grid cell
    scenario = Column(String)
    geom = Column(Geometry('POINT', 4326))
    geom_source = Column(String)    # source of geom, eg. MaStR/NEP...

def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonPowerPlants.__table__.create(bind=engine, checkfirst=True)