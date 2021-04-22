"""
Central module containing all code dealing with importing era5 weather data.
"""

import atlite
import os
from pathlib import Path

import geopandas as gpd
import egon.data.config
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
# will be later imported from another file ###
Base = declarative_base()

class EgonEra5Cells(Base):
    __tablename__ = 'egon_era5_weather_cells'
    __table_args__ = {'schema': 'supply'}
    w_id = Column(Integer, primary_key=True)
    geom = Column(Geometry('POLYGON', 4326))
    geom_point = Column(Geometry('POINT', 4326))


class EgonRenewableFeedIn(Base):
    __tablename__ = 'egon_renewable_feed_in'
    __table_args__ = {'schema': 'supply'}
    w_id = Column(Integer, primary_key=True)
    weather_year = Column(Integer, primary_key=True)
    carrier = Column(String, primary_key=True)
    feedin = Column(ARRAY(Float()))

def create_tables():

    db.execute_sql(
        "CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonEra5Cells.__table__.drop(bind=engine, checkfirst=True)
    EgonEra5Cells.__table__.create(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.drop(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.create(bind=engine, checkfirst=True)

def import_cutout():
    """ Import weather data from cutout

    Returns
    -------
    cutout : atlite.cutout.Cutout
        Weather data stored in cutout

    """

    directory = Path(".") / (
        egon.data.config.datasets()
        ['era5_weather_data']['targets']['weather_data']['path'])

    # TODO: Replace with scenario table query
    weather_year = 2011

    cutout = atlite.Cutout(
            f"europe-{str(weather_year)}-era5",
            cutout_dir = directory.absolute(),
            module="era5",
            xs= slice(7.5, 12),
            ys=slice(55., 53.),
            years= slice(weather_year, weather_year)
            )

    return cutout

def download_era5():
    """ Download weather data from era5

    Returns
    -------
    None.

    """

    directory = Path(".") / (
        egon.data.config.datasets()
        ['era5_weather_data']['targets']['weather_data']['path'])

    if not os.path.exists(directory):

        os.mkdir(directory)

        cutout = import_cutout()

        cutout.prepare()


def insert_weather_cells():
    """ Insert weather cells from era5 into database table

    Returns
    -------
    None.

    """

    cfg = egon.data.config.datasets()['era5_weather_data']

    cutout = import_cutout()

    df = gpd.GeoDataFrame(
        {'geom': cutout.grid_cells()}, geometry='geom', crs=4326)

    df.to_postgis(cfg['targets']['weather_cells']['table'],
                  schema=cfg['targets']['weather_cells']['schema'],
                  con=db.engine(), if_exists='append')

    db.execute_sql(
        f"""UPDATE {cfg['targets']['weather_cells']['schema']}.
        {cfg['targets']['weather_cells']['table']}
        SET geom_point=ST_Centroid(geom);"""
        )