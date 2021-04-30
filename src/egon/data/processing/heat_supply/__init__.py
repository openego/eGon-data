"""The central module containing all code dealing with heat supply data

"""

from egon.data import db

from egon.data.processing.heat_supply.district_heating import (
    cascade_heat_supply)
from egon.data.processing.district_heating_areas import DistrictHeatingAreas
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry

### will be later imported from another file ###
Base = declarative_base()

# TODO: set district_heating_id as ForeignKey
class EgonDistrictHeatingSupply(Base):
    __tablename__ = 'egon_district_heating'
    __table_args__ = {'schema': 'supply'}
    index = Column(Integer, primary_key=True)
    district_heating_id = Column(Integer)
    carrier = Column(String(25))
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


