"""
DB tables / SQLAlchemy ORM classes for charging infrastructure
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class EgonEmobChargingInfrastructure(Base):
    """
    Class definition of table grid.egon_emob_charging_infrastructure.
    """

    __tablename__ = "egon_emob_charging_infrastructure"
    __table_args__ = {"schema": "grid"}

    cp_id = Column(Integer, primary_key=True)
    mv_grid_id = Column(Integer)
    use_case = Column(String)
    weight = Column(Float)

    # SRID 3035 from YML)
    geometry = Column(Geometry(srid=3035))
