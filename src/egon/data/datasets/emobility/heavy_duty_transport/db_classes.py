"""
DB tables / SQLAlchemy ORM classes for heavy duty transport
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config

Base = declarative_base()
DATASET_CFG = config.datasets()["mobility_hgv"]


class EgonHeavyDutyTransportVoronoi(Base):

    __tablename__ = "egon_heavy_duty_transport_voronoi"
    __table_args__ = {"schema": "demand"}

    nuts3_id = Column(String, primary_key=True, index=True)
    nuts3_name = Column(String)
    geometry = Column(Geometry(srid=DATASET_CFG["tables"]["srid"]))
    area = Column(Float)
    truck_traffic = Column(Float)
    normalized_truck_traffic = Column(Float)
    hydrogen_consumption = Column(Float)
