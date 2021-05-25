"""The central module containing all code dealing with heat supply data

"""

from egon.data import db

from egon.data.processing.heat_supply.district_heating import (
    cascade_heat_supply)
from egon.data.processing.heat_supply.power_to_heat import (
    insert_central_power_to_heat)
from egon.data.processing.district_heating_areas import DistrictHeatingAreas
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry
import geopandas as gpd
### will be later imported from another file ###
Base = declarative_base()

# TODO: set district_heating_id as ForeignKey
class EgonDistrictHeatingSupply(Base):
    __tablename__ = 'egon_district_heating'
    __table_args__ = {'schema': 'supply'}
    index = Column(Integer, primary_key=True)
    district_heating_id = Column(Integer)
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry('POINT', 3035))
    scenario = Column(String(50))


def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonDistrictHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonDistrictHeatingSupply.__table__.create(bind=engine, checkfirst=True)


def insert_district_heating_supply():
    """ Insert supply for district heating areas

    Returns
    -------
    None.

    """

    supply_2035 = cascade_heat_supply('eGon2035', plotting=False)

    supply_2035['scenario'] = 'eGon2035'

    supply_2035.to_postgis(
        'egon_district_heating', schema='supply',
        con=db.engine(), if_exists='append')

def insert_buses(version='0.0.0', scenario='eGon2035'):

    carrier = 'central_heat'

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_pf_hv_bus
        WHERE scn_name = '{scenario}'
        AND carrier = '{carrier}'
        AND version = '{version}'
        """)

    areas = db.select_geodataframe(
        f"""
        SELECT area_id, geom_polygon as geom
        FROM demand.district_heating_areas
        WHERE scenario = '{scenario}'
        """,
        index_col='area_id'
        )

    max_bus_id_used = db.select_dataframe(
        """
        SELECT MAX(bus_id) FROM grid.egon_pf_hv_bus
        """)['max'][0]

    dh_buses = gpd.GeoDataFrame(columns = [
        'version', 'scn_name', 'bus_id', 'carrier',
        'x', 'y', 'geom']).set_geometry('geom').set_crs(epsg=4326)

    dh_buses.geom = areas.centroid.to_crs(epsg=4326)
    dh_buses.version = '0.0.0'
    dh_buses.scn_name = scenario
    dh_buses.carrier = 'central_heat'
    dh_buses.x = dh_buses.geom.x
    dh_buses.y = dh_buses.geom.y
    dh_buses.bus_id = range(max_bus_id_used+1,
                            max_bus_id_used+1+len(dh_buses))

    dh_buses.to_postgis('egon_pf_hv_bus',
                        schema='grid',
                        if_exists='append',
                        con=db.engine())

def insert_heat_etrago(version='0.0.0'):

    insert_buses('central_heat', version=version, scenario='eGon2035')
    insert_buses('rural_heat', version=version, scenario='eGon2035')

    insert_central_power_to_heat(version, scenario='eGon2035')