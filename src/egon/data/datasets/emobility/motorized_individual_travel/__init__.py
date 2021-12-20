"""
Motorized Individual Travel (MIT)
"""

from egon.data.datasets import Dataset
from egon.data import db
from egon.data.datasets.emobility.motorized_individual_travel.ev_allocation import (
    allocate_evs
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    COLUMNS_KBA,
    DOWNLOAD_DIRECTORY
)
import egon.data.config
from egon.data.datasets.scenario_parameters import (
    EgonScenario
)

import os
from urllib.request import urlretrieve
import pandas as pd
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, SmallInteger, ForeignKey
Base = declarative_base()


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


def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonEvsPerRegistrationDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvsPerRegistrationDistrict.__table__.create(bind=engine, checkfirst=True)
    EgonEvsPerMunicipality.__table__.drop(bind=engine, checkfirst=True)
    EgonEvsPerMunicipality.__table__.create(bind=engine, checkfirst=True)
    EgonEvsPerMvGridDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvsPerMvGridDistrict.__table__.create(bind=engine, checkfirst=True)


def download_and_preprocess():
    """Downloads and preprocesses data from KBA and BMVI

    Returns
    -------
    pandas.DataFrame
        Vehicle registration data for registration district
    pandas.DataFrame
        RegioStaR7 data
    """

    mit_sources = egon.data.config.datasets()[
        "emobility_mit"]["original_data"]["sources"]

    # Create the folder, if it does not exists
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.mkdir(DOWNLOAD_DIRECTORY)

    ################################
    # Download and import KBA data #
    ################################
    url = mit_sources["KBA"]["url"]
    file = DOWNLOAD_DIRECTORY / mit_sources["KBA"]["file"]
    if not os.path.isfile(file):
        urlretrieve(url, file)

    kba_data = pd.read_excel(
        file,
        sheet_name=mit_sources["KBA"]["sheet"],
        usecols=mit_sources["KBA"]["columns"],
        skiprows=mit_sources["KBA"]["skiprows"],
    )
    kba_data.columns = COLUMNS_KBA
    kba_data.replace(
        ' ',
        np.nan,
        inplace = True,
    )
    kba_data = kba_data.dropna()
    kba_data[["ags_reg_district",
              "reg_district"]] = kba_data.reg_district.str.split(
        " ",
        1,
        expand=True,
    )
    kba_data.ags_reg_district = kba_data.ags_reg_district.astype('int')

    kba_data.to_csv(DOWNLOAD_DIRECTORY / mit_sources["KBA"]["file_processed"],
                    index=None)

    #######################################
    # Download and import RegioStaR7 data #
    #######################################

    url = mit_sources["RS7"]["url"]
    file = DOWNLOAD_DIRECTORY / mit_sources["RS7"]["file"]
    if not os.path.isfile(file):
        urlretrieve(url, file)

    rs7_data = pd.read_excel(
        file,
        sheet_name=mit_sources["RS7"]["sheet"]
    )

    rs7_data['ags_district'] = rs7_data.gem_20.multiply(
        1/1000).apply(np.floor).astype('int')
    rs7_data = rs7_data.rename(
        columns={
            'gem_20': 'ags',
            'RegioStaR7': 'RS7_id'
        }
    )
    rs7_data.RS7_id = rs7_data.RS7_id.astype('int')

    rs7_data.to_csv(DOWNLOAD_DIRECTORY /
                    mit_sources["RS7"]["file_processed"],
                    index=None)


class MotorizedIndividualTravel(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="MotorizedIndividualTravel",
            version="0.0.0.dev",
            dependencies=dependencies,
            tasks=(
                create_tables,
                download_and_preprocess,
                allocate_evs
            ),
        )
