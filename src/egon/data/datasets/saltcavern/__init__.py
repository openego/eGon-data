"""The central module containing all code dealing with bgr data.

This module either directly contains the code dealing with importing bgr
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from pathlib import Path
from urllib.request import urlretrieve
import codecs
import datetime
import json
import os
import time

from geoalchemy2 import Geometry
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.metadata import (
    context,
    licenses_datenlizenz_deutschland,
    meta_metadata,
)
import egon.data.config


def to_postgres():
    """Write BGR saline structures to database."""

    # Get information from data configuraiton file
    data_config = egon.data.config.datasets()
    bgr_processed = data_config["bgr"]["processed"]

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {bgr_processed['schema']};")

    shp_file_path = (
        Path(".")
        / "data_bundle_egon_data"
        / "hydrogen_storage_potential_saltstructures"
        / "saltstructures_updated.shp"
    )

    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in bgr_processed["file_table_map"].items():
        # Open files and read .shp (within .zip) with geopandas
        data = gpd.read_file(shp_file_path)

        # Set index column and format column headings
        data.index.set_names("salstructure_id", inplace=True)
        data.columns = [x.lower() for x in data.columns]
        # data.potential = 1e9  # to fill with respective data at later time

        # Drop table before inserting data
        db.execute_sql(
            f"DROP TABLE IF EXISTS "
            f"{bgr_processed['schema']}.{table} CASCADE;"
        )

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema=bgr_processed["schema"],
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        # add primary key
        db.execute_sql(
            f"ALTER TABLE {bgr_processed['schema']}.{table} "
            f"ADD PRIMARY KEY (salstructure_id);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON "
            f"{bgr_processed['schema']}.{table} USING gist (geometry);"
        )


class SaltcavernData(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SaltcavernData",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(to_postgres,),
        )
