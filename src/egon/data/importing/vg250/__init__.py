"""The central module containing all code dealing with importing VG250 data.

This module either directly contains the code dealing with importing VG250
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from urllib.request import urlretrieve
from geoalchemy2 import Geometry
import geopandas as gpd
import os

from egon.data import db
import egon.data.config


def download_vg250_files():
    """Download VG250 (Verwaltungsgebiete) shape files."""
    data_config = egon.data.config.datasets()
    vg250_config = data_config["vg250"]["original_data"]

    target_file = os.path.join(
        os.path.dirname(__file__), vg250_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(vg250_config["source"]["url"], target_file)


def to_postgres():

    # Get information from data configuraiton file
    data_config = egon.data.config.datasets()
    vg250_orig = data_config["vg250"]["original_data"]
    vg250_processed = data_config["vg250"]["processed"]

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {vg250_processed['schema']};")

    zip_file = os.path.join(
        os.path.dirname(__file__),
        vg250_orig["target"]["path"]
    )
    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in vg250_processed["file_table_map"].items():
        # Open files and read .shp with geopandas
        data = gpd.read_file(f"zip://{zip_file}!vg250_01-01.gk3.shape.ebenen/vg250_ebenen_0101/{filename}")

        # Define 'geom' as geometry column and convert to hex
        data['geom'] = data['geometry'].apply(
            lambda geom: geom.wkb_hex)
        data.drop("geometry", axis=1, inplace=True)
        data.index.set_names("gid", inplace=True)
        data.columns = [x.lower() for x in data.columns]

        # create database table from geopandas dataframe
        data.to_sql(table,
                    engine_local_db,
                    vg250_processed["schema"],
                    index=True,
                    if_exists="replace",
                    method="multi",
                    # dtype={'geom': Geometry(geometry_type="MultiPolygon", srid=31467)}
                    dtype={'geom': Geometry()}
                    )
        db.execute_sql(f"ALTER TABLE {vg250_processed['schema']}.{table} ADD PRIMARY KEY (gid);")

