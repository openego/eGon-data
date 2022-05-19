"""
Distribute PV rooftop capacities to buildings
"""

#import geopandas as gpd
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import REAL, Column, Integer, String, Table, func, inspect, ForeignKey
#from geoalchemy2 import Geometry
#from shapely.geometry import Point

from egon.data import config, db
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    OsmBuildingsSynthetic
)
from egon.data.datasets.scenario_parameters import EgonScenario

engine = db.engine()
Base = declarative_base()


class EgonPowerPlantPvRoofBuildingMapping(Base):
    __tablename__ = "egon_power_plants_pv_roof_building_mapping"
    __table_args__ = {"schema": "supply"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    osm_buildings_id = Column(Integer, primary_key=True)
    pv_roof_unit_id = Column(Integer, primary_key=True) # will later point to new power plant table


def geocode(pv_units):
    """Geocode locations"""
    # ...
    return pv_units


def load_mastr_data():
    """Read PV rooftop data from MaStR CSV

    Note: the source will be replaced as soon as the MaStR data is available
    in DB.
    """
    cfg = config.datasets()["power_plants"]
    pv_units = pd.read_csv(cfg["sources"]["mastr_pv"])
    pv_units = geocode(pv_units)

    # Cleaning, ...

    return pv_units


def load_building_data():
    """Read buildings from DB

    Tables:
    * `openstreetmap.osm_buildings_filtered` (from OSM)
    * `openstreetmap.osm_buildings_synthetic` (synthetic, creaed by us)

    Use column `id` for both as it is unique hen you concat both datasets.
    """

    # OSM data (infer the table as there is no SQLA model)
    buildings_osm = Table(
        "osm_buildings_filtered",
        Base.metadata,
        schema="openstreetmap"
    )
    inspect(engine).reflecttable(buildings_osm, None)

    # Synthetic buildings
    with db.session_scope() as session:
        cells_query = (
            session.query(
                OsmBuildingsSynthetic. ...
            ).order_by(OsmBuildingsSynthetic.id)
        )
    buildings_synthetic = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="id"
    )

    # CONCAT

    return []


def allocate_to_buildings(pv_units, buildings):
    """Do the allocation"""
    return []


def create_mapping_table(alloc_data):
    """Create mapping table pv_unit <-> building"""
    EgonPowerPlantPvRoofBuildingMapping.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonPowerPlantPvRoofBuildingMapping.__table__.create(
        bind=engine, checkfirst=True
    )

    alloc_data.to_sql( # or .to_postgis()
        name=EgonPowerPlantPvRoofBuildingMapping.__table__.name,
        schema=EgonPowerPlantPvRoofBuildingMapping.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
        #dtype={}
    )


def pv_rooftop_to_buildings():
    """Main script"""

    pv_units = load_mastr_data()
    buildings = load_building_data()
    alloc_data = allocate_to_buildings(pv_units, buildings)
    create_mapping_table(alloc_data)
    #

