"""The central module containing all code dealing with downloading and
importing data from ERA5

"""
import os
import geopandas as gpd
import egon.data.config
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from feedinlib import era5
import xarray as xr
from shapely.geometry import Point
# will be later imported from another file ###
Base = declarative_base()

class EgonEra5Cells(Base):
    __tablename__ = 'egon_era5_weather_cells'
    __table_args__ = {'schema': 'supply'}
    w_id = Column(Integer, primary_key=True)
    geom = Column(Geometry('POLYGON', 4326))
    geom_point = Column(Geometry('POINT', 4326))


class EgonReFeedIn(Base):
    __tablename__ = 'egon_re_feed_in'
    __table_args__ = {'schema': 'supply'}
    w_id = Column(Integer, primary_key=True)
    weather_year = Column(Integer, primary_key=True)
    carrier = Column(String, primary_key=True)
    feedin = Column(ARRAY(Float()))

def create_tables():

    db.execute_sql(
        "CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonEra5Cells.__table__.create(bind=engine, checkfirst=True)
    EgonReFeedIn.__table__.create(bind=engine, checkfirst=True)


def download_weather_data():
    """ Download weather data for Germany from era5"""
    cfg = egon.data.config.datasets()['era5']
    geom_de = gpd.read_postgis(
        "SELECT geometry as geom FROM boundaries.vg250_sta_bbox",
        db.engine()).to_crs(4623).geom
    latitude = [geom_de.bounds.miny[0], geom_de.bounds.maxy[0]]
    longitude = [geom_de.bounds.minx[0], geom_de.bounds.maxx[0]]
    start_date =  f"{cfg['source']['year']}-01-01'"
    end_date = f"{cfg['source']['year']}-12-31'"
    # set variable set to download
    target_file = os.path.join(
        os.path.dirname(__file__),
        f"{cfg['source']['year']}_{cfg['target']['file']}")
    # get feedinlin data
    if not os.path.isfile(target_file):
        era5.get_era5_data_from_datespan_and_position(
            variable='feedinlib',
            start_date=start_date, end_date=end_date,
            latitude=latitude, longitude=longitude,
            target_file=target_file)


def get_weather_cells():
    """ Insert weather cell geometry in table"""
    cfg = egon.data.config.datasets()['era5']
    target_file = os.path.join(
        os.path.dirname(__file__),
        f"{cfg['source']['year']}_{cfg['target']['file']}")
    ds = xr.open_dataset(target_file)
    points = []
    for x in ds.longitude:
        for y in ds.latitude:
            points.append(Point(x, y))
    df = gpd.GeoDataFrame({'geometry': points})

    df['geom'] = df.buffer(0.125).envelope

    df = df.drop('geometry', axis=1).set_geometry('geom').set_crs(4326)

    db.execute_sql(
        f"DELETE FROM {cfg['target']['schema']}.{cfg['target']['table_cells']}"
    )

    df.to_postgis(cfg['target']['table_cells'], schema=cfg['target']['schema'],
                  con=db.engine(), if_exists='append')

    db.execute_sql(
        f"""UPDATE {cfg['target']['schema']}.{cfg['target']['table_cells']}
        SET geom_point=ST_Centroid(geom);"""
        )
