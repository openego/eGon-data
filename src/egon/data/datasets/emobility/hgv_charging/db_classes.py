"""
SQLAlchemy ORM classes for HGV charging tables (Tables 1–4).
Table 5 (egon_hgv_charging_flex) is created during eDisGo integration.
"""

from geoalchemy2 import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets.scenario_parameters import EgonScenario

Base = declarative_base()


class EgonHgvChargingSite(Base):
    """demand.egon_hgv_charging_site — one row per physical site per scenario."""

    __tablename__ = "egon_hgv_charging_site"
    __table_args__ = {"schema": "demand"}

    site_id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    location_type = Column(Text)
    category_name = Column(Text)
    geom = Column(Geometry("POINT", srid=3035))
    area_m2 = Column(Float)
    N2 = Column(Boolean)
    N3 = Column(Boolean)
    N3S = Column(Boolean)
    el_demand_N2_mwh = Column(Float)
    el_demand_N3_mwh = Column(Float)
    el_demand_N3S_mwh = Column(Float)
    el_demand_day_mwh = Column(Float)
    el_demand_night_mwh = Column(Float)
    # Per-vehicle-class day/night split (highway sites only -- depot sites
    # have no day/night split at all, N2/N3/N3S columns above already cover
    # them). Needed to check a highway site's night-only energy (the only
    # share with real charging events -- daytime/MCS has none) against
    # events/flex-model energy without falling back to an approximation
    # that applies the combined day/night ratio uniformly across classes.
    el_demand_day_N2_mwh = Column(Float)
    el_demand_day_N3_mwh = Column(Float)
    el_demand_day_N3S_mwh = Column(Float)
    el_demand_night_N2_mwh = Column(Float)
    el_demand_night_N3_mwh = Column(Float)
    el_demand_night_N3S_mwh = Column(Float)
    p_set_aggregated_mw = Column(Float)
    mv_grid_id = Column(Integer)
    bus_id = Column(BigInteger)
    voltage_level = Column(SmallInteger)
    carrier = Column(Text)


class EgonHgvChargingPoint(Base):
    """demand.egon_hgv_charging_point — one row per (site × vehicle_class × time_of_day) per scenario.

    cp_id maps 1:1 to one eDisGo charging_park_id and one edisgo_id in the MV topology.
    num_cp is the number of physical chargers in the group (metadata).
    """

    __tablename__ = "egon_hgv_charging_point"
    __table_args__ = {"schema": "demand"}

    cp_id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    site_id = Column(Integer)
    vehicle_class = Column(Text)
    num_cp = Column(Integer)
    p_set_mw = Column(Float)
    sector = Column(Text)
    time_of_day = Column(Text)


class EgonHgvChargingEvent(Base):
    """demand.egon_hgv_charging_event — individual parking/charging/vacant
    events, one row per physical slot per event (see docs/adr/0002).

    Column names mirror MIV egon_ev_trip. HGV-specific extras: cp_id, site_id,
    vehicle_class, bat_cap, slot_id. Columns computed internally by
    generate_load_time_series (charge_end, last_timestep, flex_*) are NOT
    stored here.
    """

    __tablename__ = "egon_hgv_charging_event"
    __table_args__ = {"schema": "demand"}

    event_id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    # HGV-specific
    cp_id = Column(Integer)
    site_id = Column(Integer)
    vehicle_class = Column(Text)
    bat_cap = Column(Float)
    # Physical slot within cp_id this event is assigned to, from greedy slot
    # assignment (energy_distribution_depots.py) — see docs/adr/0002. Needed
    # to chain a slot's own events (and vacant events between them) into a
    # continuous per-slot trajectory, and to group _calc_e_initial correctly
    # when a cp_id has multiple slots.
    slot_id = Column(Integer)
    # MIV-compatible columns
    location = Column(Text)
    use_case = Column(Text)
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


class EgonHgvProfile(Base):
    """demand.egon_hgv_profile — normalized 8760-h demand shapes per sector.

    All profiles sum to 1 independently (depot, highway day, highway night).
    Multiply profile[t] * annual_consumption_mwh to get MW at each 1-h timestep
    (since profile sums to 1 and each step = 1 h, the product is MWh/h = MW).
    For highway CPs, annual_consumption_mwh is el_demand_day_mwh or el_demand_night_mwh
    from egon_hgv_charging_site.
    """

    __tablename__ = "egon_hgv_profile"
    __table_args__ = {"schema": "demand"}

    sector = Column(Text, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    profile = Column(ARRAY(Float))
