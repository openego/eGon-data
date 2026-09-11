"""
DB tables / SQLAlchemy ORM classes for public bus charging (vehicle class M3).
"""

from geoalchemy2 import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
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


class EgonEvBusChargingDepot(Base):
    """
    Class definition of table demand.egon_ev_bus_charging_depot.

    One row per bus depot per scenario. This is the per-location table read
    by eDisGo; the aggregated per-bus loads written to
    ``grid.egon_etrago_load`` are derived from it (see ``docs/adr/0002`` in
    the FC_M3 working directory).

    **Columns**

    depot_id:
        Depot identifier. An OSM id for real depots, ``standin_<cluster>``
        for synthetic ones. Kept as the natural key so it joins directly to
        the input time-series columns and stays stable across scenarios and
        input-data regenerations.
    scenario:
        Scenario name. A depot exists in a scenario only if that scenario's
        input data contains a series for it.
    depot_name:
        Descriptive name from the input data.
    depot_type:
        ``real`` (an actual surveyed site, coordinates trustworthy) or
        ``standin`` (synthetic, placed at a route cluster's busiest stop --
        demand is real, siting is inferred).
    fleet_size:
        Number of buses assigned to the depot.
    geom:
        Depot location.
    bus_id:
        eTraGo bus the depot's load is attached to. From the containing MV
        grid district (voltage level 3-7) or EHV substation Voronoi cell
        (voltage level 1-2).
    mv_grid_id:
        MV grid district the depot falls in; equals ``bus_id`` for voltage
        levels 3-7 and is NULL for depots assigned via the EHV Voronoi.
    voltage_level:
        eGon-data grid level 1-7, derived from this scenario's own peak load.
        A property of the (depot, scenario) pair, not of the depot -- the
        same depot may sit at different levels in different scenarios and
        connection upgrades are not modelled (``docs/adr/0001``).
    peak_load_mw:
        Maximum of ``p_set``, in MW.
    annual_demand_mwh:
        Sum of ``p_set``, in MWh (1 h timesteps).
    p_set:
        Hourly charging power over the year, 8760 values in MW.
    """

    __tablename__ = "egon_ev_bus_charging_depot"
    __table_args__ = {"schema": "demand"}

    depot_id = Column(String, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    depot_name = Column(Text)
    depot_type = Column(Text)
    fleet_size = Column(Float)
    geom = Column(Geometry("POINT", srid=3035))
    bus_id = Column(BigInteger, index=True)
    mv_grid_id = Column(Integer)
    voltage_level = Column(SmallInteger)
    peak_load_mw = Column(Float)
    annual_demand_mwh = Column(Float)
    p_set = Column(ARRAY(Float))
