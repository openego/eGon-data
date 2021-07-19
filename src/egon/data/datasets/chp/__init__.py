
"""
The central module containing all code dealing with chp.
"""

from egon.data import db, config
from egon.data.datasets.chp.match_nep import insert_large_chp
from egon.data.datasets.chp.small_chp import existing_chp_smaller_10mw
from sqlalchemy import Column, String, Float, Integer, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
Base = declarative_base()

class EgonChp(Base):
    __tablename__ = "egon_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("chp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    use_case = Column(String)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    electrical_bus_id = Column(Integer)
    heat_bus_id = Column(Integer)
    gas_bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))

def create_tables():
    """Create tables for chp data
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonChp.__table__.drop(bind=engine, checkfirst=True)
    EgonChp.__table__.create(bind=engine, checkfirst=True)


def insert_chp_egon2035():
    """ Insert large CHP plants for eGon2035 considering NEP and MaStR

    Returns
    -------
    None.

    """

    create_tables()

    target = config.datasets()["chp_location"]["targets"]["power_plants"]

    # Insert large CHPs based on NEP's list of conventional power plants
    MaStR_konv = insert_large_chp(target)

    # Insert smaller CHPs (< 10MW) based on existing locations from MaStR
    additional_capacitiy = existing_chp_smaller_10mw(MaStR_konv)


