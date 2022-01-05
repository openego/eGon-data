from sqlalchemy import Column, Float, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.scenario_parameters import EgonScenario

Base = declarative_base()


class EgonEvPool(Base):
    """EV pool for motorized individual travel

    Each row is one event of a specific electric vehicle (EV) which is
    uniquely defined by `rs7_id`, `ev_id` and `event_id`.

    Columns
    -------
    id:
        Unique id of EV event
    rs7_id:
        id of RegioStar7 region
    ev_id:
        id of EV
    event_id:
        id of EV event, unique in a specific EV dataset
    location:
        Location of EV event, one of
            * 0_work
            * 1_business
            * 2_school
            * 3_shopping
            * 4_private/ridesharing
            * 5_leisure
            * 6_home
            * 7_charging_hub
            * driving
    charging_capacity_nominal:
        Nominal charging capacity in kW
    charging_capacity_grid:
        Charging capacity at grid side in kW,
        includes efficiency of charging infrastructure
    charging_capacity_battery:
        Charging capacity at battery side in kW,
        includes efficiency of car charger
    soc_start:
        State of charge at start of event
    soc_start:
        State of charge at end of event
    charging_demand:
        Energy demand during parking/charging event in kWh.
        0 if no charging takes place.
    park_start:
        Start timestep of parking event (15min interval, e.g. 4 = 1h)
    park_end:
        End timestep of parking event (15min interval)
    drive_start:
        Start timestep of driving event (15min interval)
    drive_end:
        End timestep of driving event (15min interval)
    consumption:
        Energy demand during driving event in kWh
    """

    __tablename__ = "egon_ev_pool"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, index=True, unique=True)
    rs7_id = Column(SmallInteger, primary_key=True)
    ev_id = Column(String(20), primary_key=True)
    event_id = Column(Integer, primary_key=True)
    location = Column(String(21))
    charging_capacity_nominal = Column(Float)
    charging_capacity_grid = Column(Float)
    charging_capacity_battery = Column(Float)
    soc_start = Column(Float)
    soc_end = Column(Float)
    charging_demand = Column(Float)
    park_start = Column(Integer)
    park_end = Column(Integer)
    drive_start = Column(Integer)
    drive_end = Column(Integer)
    consumption = Column(Float)


class EgonEvCountRegistrationDistrict(Base):
    """Electric vehicle counts per registration district"""

    __tablename__ = "egon_ev_count_registration_district"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    ags_reg_district = Column(Integer, primary_key=True)
    reg_district = Column(String)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)


class EgonEvCountMunicipality(Base):
    """Electric vehicle counts per municipality"""

    __tablename__ = "egon_ev_count_municipality"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    ags = Column(Integer, primary_key=True)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvCountMvGridDistrict(Base):
    """Electric vehicle counts per MV grid district"""

    __tablename__ = "egon_ev_count_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvMvGridDistrict(Base):
    """List of electric vehicles per MV grid district"""

    __tablename__ = "egon_ev_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    egon_ev_trip_pool_id = Column(
        Integer, ForeignKey(EgonEvPool.id), primary_key=True
    )
