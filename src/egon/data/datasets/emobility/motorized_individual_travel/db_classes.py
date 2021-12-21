from sqlalchemy import Column, Integer, String, SmallInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from egon.data.datasets.scenario_parameters import (
    EgonScenario
)


class EgonEvsPerRegistrationDistrict(Base):
    __tablename__ = "egon_evs_per_registration_district"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    ags_reg_district = Column(Integer)
    reg_district = Column(String)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)


class EgonEvsPerMunicipality(Base):
    __tablename__ = "egon_evs_per_municipality"
    __table_args__ = {"schema": "demand"}

    ags = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvsPerMvGridDistrict(Base):
    __tablename__ = "egon_evs_per_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    bus_id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)
