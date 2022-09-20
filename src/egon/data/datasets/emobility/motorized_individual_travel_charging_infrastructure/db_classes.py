"""
DB tables / SQLAlchemy ORM classes for charging infrastructure
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config

Base = declarative_base()
DATASET_CFG = config.datasets()["charging_infrastructure"]


class EgonHeavyDutyTransportVoronoi(Base):

    __tablename__ = DATASET_CFG["targets"]["charging_infrastructure"]["table"]
    __table_args__ = {
        "schema": DATASET_CFG["targets"]["charging_infrastructure"]["schema"]
    }

    mv_grid_id = Column(Integer, primary_key=True)
    weight = Column(Float)
    geometry = Column(
        Geometry(
            srid=DATASET_CFG["original_data"]["sources"]["tracbev"]["srid"]
        )
    )
