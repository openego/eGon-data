"""
DB tables / SQLAlchemy ORM classes for charging infrastructure
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config

Base = declarative_base()
DATASET_CFG = config.datasets()["charging_infrastructure"]


class EgonEmobChargingInfrastructure(Base):

    __tablename__ = DATASET_CFG["targets"]["charging_infrastructure"]["table"]
    __table_args__ = {
        "schema": DATASET_CFG["targets"]["charging_infrastructure"]["schema"]
    }

    cp_id = Column(Integer, primary_key=True)
    mv_grid_id = Column(Integer)
    use_case = Column(String)
    weight = Column(Float)
    geometry = Column(
        Geometry(
            srid=DATASET_CFG["original_data"]["sources"]["tracbev"]["srid"]
        )
    )
