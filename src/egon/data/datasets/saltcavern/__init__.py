"""
The central module containing all code dealing with BGR data.

This module directly contains the code dealing with importing data of
Bundesanstalt für Geowissenschaften und Rohstoffe (BGR) from the
databundle into the database.

"""

from pathlib import Path

from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
import egon.data.config


def to_postgres():
    """
    Write BGR saline structures to database.

    This function inserts data into the database and has no return.

    """

    # Get information from data configuraiton file
    #data_config = egon.data.config.datasets()
    #bgr_processed = data_config["bgr"]["processed"]
    schema = SaltcavernData.targets.tables["saltcaverns"]["schema"]
    table = SaltcavernData.targets.tables["saltcaverns"]["table"]
    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, path in SaltcavernData.sources.files.items():
        # Open files and read .shp (within .zip) with geopandas
        shp_file_path = Path(path)
        data = gpd.read_file(shp_file_path).to_crs(epsg=4326)
        data = (
            data[
                (data["Bewertung"] == "Eignung InSpEE-DS")
                | (data["Bewertung"] == "geeignet")
            ]
            .drop(columns=["Bewertung", "Typ", "Salzstrukt"])
            .rename(
                columns={
                    "Shape_Area": "shape_star",
                    "Shape_Leng": "shape_stle",
                }
            )
        )

        # Set index column and format column headings
        data.index.set_names("saltstructure_id", inplace=True)
        data.columns = [x.lower() for x in data.columns]

        # Drop table before inserting data
        db.execute_sql(
            f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema= schema,
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        # add primary key
        db.execute_sql(
            f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY (saltstructure_id);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON {schema}.{table} USING gist (geometry);"
        )


class SaltcavernData(Dataset):
    """
    Insert the BGR saline structures into the database.

    Insert the BGR saline structures into the database by executing the
    function :py:func:`to_postgres`.

    *Dependencies*
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`

    *Resulting tables*
      * boundaries.inspee_saltstructures is created

    """

    #:
    name: str = "SaltcavernData"
    #:
    version: str = "0.0.1"
    
    
    sources = DatasetSources(
        files={
            "inspee_saltstructures": "data_bundle_egon_data/hydrogen_storage_potential_saltstructures/Potenzialbewertung_InSpEE_InSpEE_DS.shp"
        }
    )

    targets = DatasetTargets(
        tables={
            "saltcaverns": {
                "schema": "boundaries",
                "table": "inspee_saltstructures",
            },
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(to_postgres,),
        )
