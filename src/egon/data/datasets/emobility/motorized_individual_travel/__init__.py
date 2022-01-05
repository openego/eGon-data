"""
Motorized Individual Travel (MIT)
"""

from pathlib import Path
from urllib.request import urlretrieve
import os

import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (
    EgonEvCountMunicipality,
    EgonEvCountMvGridDistrict,
    EgonEvCountRegistrationDistrict,
    EgonEvPool,
    EgonEvMvGridDistrict
)
from egon.data.datasets.emobility.motorized_individual_travel.ev_allocation import (
    allocate_evs_numbers,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    COLUMNS_KBA,
    DOWNLOAD_DIRECTORY,
    TESTMODE_OFF,
    TRIP_COLUMN_MAPPING,
)
import egon.data.config


def create_tables():
    """Create tables for electric vehicles

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonEvCountRegistrationDistrict.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonEvCountRegistrationDistrict.__table__.create(
        bind=engine, checkfirst=True
    )
    EgonEvCountMunicipality.__table__.drop(bind=engine, checkfirst=True)
    EgonEvCountMunicipality.__table__.create(bind=engine, checkfirst=True)
    EgonEvCountMvGridDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvCountMvGridDistrict.__table__.create(bind=engine, checkfirst=True)
    EgonEvPool.__table__.drop(bind=engine, checkfirst=True)
    EgonEvPool.__table__.create(bind=engine, checkfirst=True)
    EgonEvMvGridDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvMvGridDistrict.__table__.create(bind=engine, checkfirst=True)

def download_and_preprocess():
    """Downloads and preprocesses data from KBA and BMVI

    Returns
    -------
    pandas.DataFrame
        Vehicle registration data for registration district
    pandas.DataFrame
        RegioStaR7 data
    """

    mit_sources = egon.data.config.datasets()["emobility_mit"][
        "original_data"
    ]["sources"]

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
        " ",
        np.nan,
        inplace=True,
    )
    kba_data = kba_data.dropna()
    kba_data[
        ["ags_reg_district", "reg_district"]
    ] = kba_data.reg_district.str.split(
        " ",
        1,
        expand=True,
    )
    kba_data.ags_reg_district = kba_data.ags_reg_district.astype("int")

    kba_data.to_csv(
        DOWNLOAD_DIRECTORY / mit_sources["KBA"]["file_processed"], index=None
    )

    #######################################
    # Download and import RegioStaR7 data #
    #######################################

    url = mit_sources["RS7"]["url"]
    file = DOWNLOAD_DIRECTORY / mit_sources["RS7"]["file"]
    if not os.path.isfile(file):
        urlretrieve(url, file)

    rs7_data = pd.read_excel(file, sheet_name=mit_sources["RS7"]["sheet"])

    rs7_data["ags_district"] = (
        rs7_data.gem_20.multiply(1 / 1000).apply(np.floor).astype("int")
    )
    rs7_data = rs7_data.rename(
        columns={"gem_20": "ags", "RegioStaR7": "rs7_id"}
    )
    rs7_data.rs7_id = rs7_data.rs7_id.astype("int")

    rs7_data.to_csv(
        DOWNLOAD_DIRECTORY / mit_sources["RS7"]["file_processed"], index=None
    )


def write_trips_to_db():
    """Write EV trips generated by simBEV from data bundle to database table"""

    def import_csv(f):
        df = pd.read_csv(f, usecols=TRIP_COLUMN_MAPPING.keys())
        df["rs7_id"] = int(f.parent.name)
        df["ev_id"] = "_".join(f.name.split("_")[0:3])
        return df

    trip_dir_root = Path(
        ".", "data_bundle_egon_data", "emobility", "mit_trip_data"
    )

    if TESTMODE_OFF:
        trip_files = trip_dir_root.glob("*/*.csv")
    else:
        # Load only 100 EVs per region if in test mode
        trip_files = [
            list(rdir.glob("*.csv"))[:100]
            for rdir in [_ for _ in trip_dir_root.iterdir() if _.is_dir()]
        ]
        # Flatten
        trip_files = [i for sub in trip_files for i in sub]

    # Read, concat and reorder cols
    trip_data = pd.concat(map(import_csv, trip_files))
    trip_data.rename(columns=TRIP_COLUMN_MAPPING, inplace=True)
    trip_data = trip_data.reset_index().rename(columns={"index": "event_id"})
    cols = ["rs7_id", "ev_id", "event_id"] + list(TRIP_COLUMN_MAPPING.values())
    trip_data.index.name = "id"
    trip_data = trip_data[cols]

    trip_data.to_sql(
        name=EgonEvPool.__table__.name,
        schema=EgonEvPool.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=True,
    )


class MotorizedIndividualTravel(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="MotorizedIndividualTravel",
            version="0.0.0.dev",
            dependencies=dependencies,
            tasks=(
                create_tables,
                {(download_and_preprocess, allocate_evs_numbers),
                 write_trips_to_db},
            ),
        )
