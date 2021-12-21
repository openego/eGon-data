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
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (
    EgonEvsPerRegistrationDistrict,
    EgonEvsPerMunicipality,
    EgonEvsPerMvGridDistrict
)
import egon.data.config

import os
from urllib.request import urlretrieve
import pandas as pd
import numpy as np


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
