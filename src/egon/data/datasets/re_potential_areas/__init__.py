"""The central module containing all code dealing with importing data on
potential areas for wind onshore and ground-mounted PV.
"""

import os
from urllib.request import urlretrieve
import geopandas as gpd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from geoalchemy2 import Geometry

import egon.data.config
from egon.data import db

from functools import partial
from egon.data.datasets import Dataset

Base = declarative_base()


class EgonRePotentialAreaPvAgriculture(Base):
    __tablename__ = 'egon_re_potential_area_pv_agriculture'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry('MULTIPOLYGON', 3035))


class EgonRePotentialAreaPvRoadRailway(Base):
    __tablename__ = 'egon_re_potential_area_pv_road_railway'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry('MULTIPOLYGON', 3035))


class EgonRePotentialAreaWind(Base):
    __tablename__ = 'egon_re_potential_area_wind'
    __table_args__ = {'schema': 'supply'}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry('MULTIPOLYGON', 3035))


def download_datasets():
    """Download geopackages from Zenodo."""

    data_config = egon.data.config.datasets()
    pa_config = data_config['re_potential_areas']

    def ve(s):
        raise (ValueError(s))

    dataset = egon.data.config.settings()['egon-data']['--dataset-boundary']
    url_section = (
        'url'
        if dataset == 'Everything'
        else 'url_testmode'
        if dataset == 'Schleswig-Holstein'
        else ve(f"'{dataset}' is not a valid dataset boundary.")
    )

    url_target_file_map = zip(
        pa_config['original_data']['source'][url_section],
        [os.path.join(os.path.dirname(__file__), file)
         for file in pa_config['original_data']['target'][
             'path_table_map'].keys()]
    )

    for url, file in url_target_file_map:
        if not os.path.isfile(file):
            urlretrieve(url, file)


def create_tables():
    """Create tables for RE potential areas"""

    data_config = egon.data.config.datasets()

    schema = data_config['re_potential_areas']['original_data'][
        'target'].get('schema', 'supply')

    db.execute_sql(f'CREATE SCHEMA IF NOT EXISTS {schema};')
    engine = db.engine()

    # drop tables
    EgonRePotentialAreaPvAgriculture.__table__.drop(engine, checkfirst=True)
    EgonRePotentialAreaPvRoadRailway.__table__.drop(engine, checkfirst=True)
    EgonRePotentialAreaWind.__table__.drop(engine, checkfirst=True)

    # create tables
    EgonRePotentialAreaPvAgriculture.__table__.create(bind=engine,
                                                      checkfirst=True)
    EgonRePotentialAreaPvRoadRailway.__table__.create(bind=engine,
                                                      checkfirst=True)
    EgonRePotentialAreaWind.__table__.create(bind=engine,
                                             checkfirst=True)


def insert_data():
    """Insert data into DB"""

    data_config = egon.data.config.datasets()
    pa_config = data_config['re_potential_areas']

    file_table_map = {
        os.path.join(os.path.dirname(__file__), file): table
        for file, table in pa_config['original_data']['target'][
            'path_table_map'].items()
    }

    engine_local_db = db.engine()

    for file, table in file_table_map.items():
        data = gpd.read_file(file).to_crs("EPSG:3035")
        data.rename(columns={'geometry': 'geom'}, inplace=True)
        data.set_geometry('geom', inplace=True)

        schema = pa_config['original_data']['target'].get('schema', 'supply')

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema=schema,
            index=False,
            if_exists="append",
            dtype={"geom": Geometry()},
        )


# create re_potential_areas dataset partial object
re_potential_area_setup = partial(
    Dataset,
    name="RePotentialAreas",
    version="0.0.0",
    dependencies=[],
    tasks=(download_datasets, create_tables, insert_data),
)
