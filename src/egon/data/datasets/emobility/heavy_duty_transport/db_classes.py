"""
DB tables / SQLAlchemy ORM classes for heavy duty transport.
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, ForeignKey, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config
from egon.data.datasets.scenario_parameters import EgonScenario

Base = declarative_base()
DATASET_CFG = config.datasets()["mobility_hgv"]


class EgonHeavyDutyTransportVoronoi(Base):
    """
    Class definition of table demand.egon_heavy_duty_transport_voronoi.
    """
    __tablename__ = "egon_heavy_duty_transport_voronoi"
    __table_args__ = {"schema": "demand"}

    nuts3 = Column(String, primary_key=True)
    geometry = Column(Geometry(srid=DATASET_CFG["tables"]["srid"]))
    area = Column(Float)
    truck_traffic = Column(Float)
    normalized_truck_traffic = Column(Float)
    hydrogen_consumption = Column(Float)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
