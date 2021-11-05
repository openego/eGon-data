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


def calculate_and_insert_storage_potential():
    """Calculate site specific storage potential based on InSpEE-DS report."""

    # select onshore vg250 data
    sources = egon.data.config.datasets()["bgr"]["sources"]
    vg250_data = db.select_geodataframe(
        f"""SELECT * FROM
                {sources['vg250_federal_states']['schema']}.
                {sources['vg250_federal_states']['table']}
            WHERE gf = '4'""",
        index_col="id",
        geom_col="geometry",
    )

    # hydrogen storage potential data from InSpEE-DS report
    hydrogen_storage_potential = pd.DataFrame(
        columns=["federal_state", "INSPEEDS", "INSPEE"]
    )

    hydrogen_storage_potential.loc[0] = ["Brandenburg", 353e6, 159e6]
    hydrogen_storage_potential.loc[1] = ["Niedersachsen", 253e6, 702e6]
    hydrogen_storage_potential.loc[2] = ["Schleswig-Holstein", 0, 413e6]
    hydrogen_storage_potential.loc[3] = ["Mecklenburg-Vorpommern", 25e6, 193e6]
    hydrogen_storage_potential.loc[4] = ["Nordrhein-Westfalen", 168e6, 0]
    hydrogen_storage_potential.loc[5] = ["Sachsen-Anhalt", 318e6, 1614e6]
    hydrogen_storage_potential.loc[6] = ["Thüringen", 595e6, 1614e6]

    hydrogen_storage_potential["total"] = (
        hydrogen_storage_potential["INSPEEDS"]
        + hydrogen_storage_potential["INSPEE"]
    )

    # get saltcavern shapes
    saltcavern_data = db.select_geodataframe(
        f"""SELECT * FROM
                {sources['saltcaverns']['schema']}.
                {sources['saltcaverns']['table']}
            """,
        index_col="salstructure_id",
        geom_col="geometry",
    )

    saltcavern_potential_data = pd.DataFrame()

    for row in hydrogen_storage_potential.index:
        federal_state = hydrogen_storage_potential.loc[row, "federal_state"]
        potential = hydrogen_storage_potential.loc[row, "total"]
        federal_state_data = vg250_data[vg250_data["gen"] == federal_state]

        # skip if federal state not available (e.g. local testing)
        if federal_state_data.size > 0:
            saltcaverns_in_fed_state = saltcavern_data.overlay(
                federal_state_data, how="intersection"
            )

            # area calculation in equal surface epsg
            saltcaverns_in_fed_state["area"] = saltcaverns_in_fed_state.to_crs(
                epsg=6689
            ).area

            # map potential via fraction of total area
            saltcaverns_in_fed_state["potential"] = (
                saltcaverns_in_fed_state["area"]
                / saltcaverns_in_fed_state["area"].sum()
                * potential
            )

            saltcavern_potential_data = saltcavern_potential_data.append(
                saltcaverns_in_fed_state[["potential", "geometry"]],
                ignore_index=True,
            )

        else:
            continue

    # write result to database
    # Set index column and format column headings
    saltcavern_potential_data.index.set_names("id", inplace=True)
    saltcavern_potential_data.columns = [
        x.lower() for x in saltcavern_potential_data.columns
    ]

    data_config = egon.data.config.datasets()
    table = data_config["bgr"]["targets"]["map"]["table"]
    schema = data_config["bgr"]["targets"]["map"]["schema"]
    engine_local_db = db.engine()

    # Drop table before inserting data
    db.execute_sql(f"DROP TABLE IF EXISTS " f"{schema}.{table} CASCADE;")

    # create database table from geopandas dataframe
    gdf = gpd.GeoDataFrame(
        saltcavern_potential_data[["potential"]],
        geometry=saltcavern_potential_data.centroid,
    ).set_crs(epsg=3035)

    gdf.to_postgis(
        table,
        engine_local_db,
        schema=schema,
        index=True,
        if_exists="replace",
        dtype={"geometry": Geometry()},
    )

    # add primary key
    db.execute_sql(f"ALTER TABLE {schema}.{table} " f"ADD PRIMARY KEY (id);")

    # Add index on geometry column
    db.execute_sql(
        f"CREATE INDEX {table}_geometry_idx ON "
        f"{schema}.{table} USING gist (geometry);"
    )


class SaltcavernData(Dataset):

    filename = egon.data.config.datasets()["bgr"]["original_data"]["source"][
        "url"
    ]

    def __init__(self, dependencies):
        super().__init__(
            name="SaltcavernData",
            version=self.filename + "0.0.0",
            dependencies=dependencies,
            tasks=(
                download_files,
                to_postgres,
                calculate_and_insert_storage_potential,
            ),
        )
