"""The central module containing all code dealing with VG250 data.

This module either directly contains the code dealing with importing VG250
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from pathlib import Path
from urllib.request import urlretrieve
import os

from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.validation import TableValidation, resolve_boundary_dependence


def download_files():
    """
    Download VG250 (Verwaltungsgebiete) shape files.

    Data is downloaded from source specified in *datasets.yml* in section
    *vg250/original_data/source/url* and saved to file specified in
    *vg250/original_data/target/file*.

    """

    download_directory = Path(".") / "vg250"
    # Create the folder, if it does not exist already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = (
        download_directory / Path(Vg250.sources.files["vg250_zip"]).name
    )

    if not os.path.isfile(target_file):
        urlretrieve(Vg250.sources.urls["vg250_zip"], target_file)


def to_postgres():
    """
    Writes original VG250 data to database.

    Creates schema boundaries if it does not yet exist.
    Newly creates all tables specified as keys in *datasets.yml* in section
    *vg250/processed/file_table_map*.

    """

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS boundaries;")

    zip_file = Path(Vg250.sources.files["vg250_zip"])
    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in Vg250.file_table_map.items():
        # Open files and read .shp (within .zip) with geopandas
        data = gpd.read_file(
            f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
            f"vg250_ebenen_0101/{filename}"
        )

        boundary = settings()["egon-data"]["--dataset-boundary"]
        if boundary != "Everything":
            # read-in borders of federal state Schleswig-Holstein
            data_sta = gpd.read_file(
                f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
                f"vg250_ebenen_0101/VG250_LAN.shp"
            ).query(f"GEN == '{boundary}'")
            data_sta.BEZ = "Bundesrepublik"
            data_sta.NUTS = "DE"
            # import borders of Schleswig-Holstein as borders of state
            if table == "vg250_sta":
                data = data_sta
            # choose only areas in Schleswig-Holstein
            else:
                data = data[
                    data.within(data_sta.dissolve(by="GEN").geometry.values[0])
                ]

        # Set index column and format column headings
        data.index.set_names("id", inplace=True)
        data.columns = [x.lower() for x in data.columns]

        # Drop table before inserting data
        db.execute_sql(
            f"DROP TABLE IF EXISTS {Vg250.targets.tables[table]} CASCADE;"
        )

        # create database table from geopandas dataframe
        data.to_postgis(
            Vg250.targets.get_table_name(table),
            engine_local_db,
            schema=Vg250.targets.get_table_schema(table),
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        db.execute_sql(
            f"ALTER TABLE {Vg250.targets.tables[table]} "
            f"ADD PRIMARY KEY (id);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON "
            f"{Vg250.targets.tables[table]} USING gist (geometry);"
        )


def nuts_mview():
    """
    Creates MView boundaries.vg250_lan_nuts_id.

    """
    db.execute_sql_script(
        os.path.join(os.path.dirname(__file__), "vg250_lan_nuts_id_mview.sql")
    )


def cleaning_and_preperation():
    """
    Creates tables and MViews with cleaned and corrected geometry data.

    The following table is created:
      * boundaries.vg250_gem_clean where municipalities (Gemeinden) that are fragmented
        are cleaned from ringholes

    The following MViews are created:
      * boundaries.vg250_gem_hole
      * boundaries.vg250_gem_valid
      * boundaries.vg250_krs_area
      * boundaries.vg250_lan_union
      * boundaries.vg250_sta_bbox
      * boundaries.vg250_sta_invalid_geometry
      * boundaries.vg250_sta_tiny_buffer
      * boundaries.vg250_sta_union

    """

    db.execute_sql_script(
        os.path.join(os.path.dirname(__file__), "cleaning_and_preparation.sql")
    )


class Vg250(Dataset):

    sources = DatasetSources(
        urls={
            "vg250_zip": "https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/2020/vg250_01-01.geo84.shape.ebenen.zip"
        },
        files={
            # The downloaded file is a source for the 'to_postgres' step
            "vg250_zip": "vg250/vg250_01-01.geo84.shape.ebenen.zip"
        },
    )
    targets = DatasetTargets(
        files={
            # The downloaded file is a target of the 'download' step
            "vg250_zip": "vg250/vg250_01-01.geo84.shape.ebenen.zip"
        },
        tables={
            "vg250_sta": "boundaries.vg250_sta",
            "vg250_lan": "boundaries.vg250_lan",
            "vg250_rbz": "boundaries.vg250_rbz",
            "vg250_krs": "boundaries.vg250_krs",
            "vg250_vwg": "boundaries.vg250_vwg",
            "vg250_gem": "boundaries.vg250_gem",
        },
    )

    file_table_map = {
        "VG250_STA.shp": "vg250_sta",
        "VG250_LAN.shp": "vg250_lan",
        "VG250_RBZ.shp": "vg250_rbz",
        "VG250_KRS.shp": "vg250_krs",
        "VG250_VWG.shp": "vg250_vwg",
        "VG250_GEM.shp": "vg250_gem",
    }

    """
    Obtains and processes VG250 data and writes it to database.

    Original data is downloaded using :py:func:`download_files` function and written
    to database using :py:func:`to_postgres` function.

    *Dependencies*
      No dependencies

    *Resulting tables*
      * :py:func:`boundaries.vg250_gem <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_krs <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_lan <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_rbz <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_sta <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_vwg <to_postgres>` is created and filled
      * :py:func:`boundaries.vg250_lan_nuts_id <nuts_mview>` is created and filled
      * :py:func:`boundaries.vg250_gem_hole <cleaning_and_preperation>` is created and
        filled
      * :py:func:`boundaries.vg250_gem_valid <cleaning_and_preperation>` is created and
        filled
      * :py:func:`boundaries.vg250_krs_area <cleaning_and_preperation>` is created and
        filled
      * :py:func:`boundaries.vg250_lan_union <cleaning_and_preperation>` is created and
        filled
      * :py:func:`boundaries.vg250_sta_bbox <cleaning_and_preperation>` is created and
        filled
      * :py:func:`boundaries.vg250_sta_invalid_geometry <cleaning_and_preperation>` is
        created and filled
      * :py:func:`boundaries.vg250_sta_tiny_buffer <cleaning_and_preperation>` is
        created and filled
      * :py:func:`boundaries.vg250_sta_union <cleaning_and_preperation>` is
        created and filled

    """
    filename = sources.urls["vg250_zip"]

    #:
    name: str = "VG250"
    version: str = f"{filename}-0.0.10"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                download_files,
                to_postgres,
                nuts_mview,
                cleaning_and_preperation,
            ),
            validation={
                "data_quality": [
                    # boundaries.vg250_sta
                    TableValidation(
                        table_name="boundaries.vg250_sta",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 5,
                                "Everything": 11,
                            }
                        ),
                        geometry_columns=["geometry"],
                        data_type_columns={
                            "ade": "integer",
                            "ags": "text",
                            "ags_0": "text",
                            "ars": "text",
                            "ars_0": "text",
                            "bem": "text",
                            "bez": "text",
                            "bsg": "integer",
                            "debkg_id": "text",
                            "fk_s3": "text",
                            "gen": "text",
                            "geometry": "geometry",
                            "gf": "integer",
                            "ibz": "integer",
                            "id": "bigint",
                            "nbd": "text",
                            "nuts": "text",
                            "rs": "text",
                            "rs_0": "text",
                            "sdv_ars": "text",
                            "sdv_rs": "text",
                            "sn_g": "text",
                            "sn_k": "text",
                            "sn_l": "text",
                            "sn_r": "text",
                            "sn_v1": "text",
                            "sn_v2": "text",
                            "wsk": "timestamp without time zone",
                        },
                        not_null_columns=[
                            "ade",
                            "ags",
                            "ags_0",
                            "ars",
                            "ars_0",
                            "bem",
                            "bez",
                            "bsg",
                            "debkg_id",
                            "fk_s3",
                            "gen",
                            "gf",
                            "ibz",
                            "id",
                            "nbd",
                            "nuts",
                            "rs",
                            "rs_0",
                            "sdv_ars",
                            "sdv_rs",
                            "sn_g",
                            "sn_k",
                            "sn_l",
                            "sn_r",
                            "sn_v1",
                            "sn_v2",
                            "wsk",
                        ],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                    # boundaries.vg250_lan
                    TableValidation(
                        table_name="boundaries.vg250_lan",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 5,
                                "Everything": 35,
                            }
                        ),
                        geometry_columns=["geometry"],
                        data_type_columns={
                            "ade": "integer",
                            "ags": "text",
                            "ags_0": "text",
                            "ars": "text",
                            "ars_0": "text",
                            "bem": "text",
                            "bez": "text",
                            "bsg": "integer",
                            "debkg_id": "text",
                            "fk_s3": "text",
                            "gen": "text",
                            "geometry": "geometry",
                            "gf": "integer",
                            "ibz": "integer",
                            "id": "bigint",
                            "nbd": "text",
                            "nuts": "text",
                            "rs": "text",
                            "rs_0": "text",
                            "sdv_ars": "text",
                            "sdv_rs": "text",
                            "sn_g": "text",
                            "sn_k": "text",
                            "sn_l": "text",
                            "sn_r": "text",
                            "sn_v1": "text",
                            "sn_v2": "text",
                            "wsk": "timestamp without time zone",
                        },
                        not_null_columns=[
                            "ade",
                            "ags",
                            "ags_0",
                            "ars",
                            "ars_0",
                            "bem",
                            "bez",
                            "bsg",
                            "debkg_id",
                            "fk_s3",
                            "gen",
                            "gf",
                            "ibz",
                            "id",
                            "nbd",
                            "nuts",
                            "rs",
                            "rs_0",
                            "sdv_ars",
                            "sdv_rs",
                            "sn_g",
                            "sn_k",
                            "sn_l",
                            "sn_r",
                            "sn_v1",
                            "sn_v2",
                            "wsk",
                        ],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                    # boundaries.vg250_krs
                    TableValidation(
                        table_name="boundaries.vg250_krs",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 27,
                                "Everything": 431,
                            }
                        ),
                        geometry_columns=["geometry"],
                        data_type_columns={
                            "ade": "integer",
                            "ags": "text",
                            "ags_0": "text",
                            "ars": "text",
                            "ars_0": "text",
                            "bem": "text",
                            "bez": "text",
                            "bsg": "integer",
                            "debkg_id": "text",
                            "fk_s3": "text",
                            "gen": "text",
                            "geometry": "geometry",
                            "gf": "integer",
                            "ibz": "integer",
                            "id": "bigint",
                            "nbd": "text",
                            "nuts": "text",
                            "rs": "text",
                            "rs_0": "text",
                            "sdv_ars": "text",
                            "sdv_rs": "text",
                            "sn_g": "text",
                            "sn_k": "text",
                            "sn_l": "text",
                            "sn_r": "text",
                            "sn_v1": "text",
                            "sn_v2": "text",
                            "wsk": "timestamp without time zone",
                        },
                        not_null_columns=[
                            "ade",
                            "ags",
                            "ags_0",
                            "ars",
                            "ars_0",
                            "bem",
                            "bez",
                            "bsg",
                            "debkg_id",
                            "fk_s3",
                            "gen",
                            "gf",
                            "ibz",
                            "id",
                            "nbd",
                            "nuts",
                            "rs",
                            "rs_0",
                            "sdv_ars",
                            "sdv_rs",
                            "sn_g",
                            "sn_k",
                            "sn_l",
                            "sn_r",
                            "sn_v1",
                            "sn_v2",
                            "wsk",
                        ],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                    # boundaries.vg250_vwg
                    TableValidation(
                        table_name="boundaries.vg250_vwg",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 205,
                                "Everything": 4688,
                            }
                        ),
                        geometry_columns=["geometry"],
                        data_type_columns={
                            "ade": "integer",
                            "ags": "text",
                            "ags_0": "text",
                            "ars": "text",
                            "ars_0": "text",
                            "bem": "text",
                            "bez": "text",
                            "bsg": "integer",
                            "debkg_id": "text",
                            "fk_s3": "text",
                            "gen": "text",
                            "geometry": "geometry",
                            "gf": "integer",
                            "ibz": "integer",
                            "id": "bigint",
                            "nbd": "text",
                            "nuts": "text",
                            "rs": "text",
                            "rs_0": "text",
                            "sdv_ars": "text",
                            "sdv_rs": "text",
                            "sn_g": "text",
                            "sn_k": "text",
                            "sn_l": "text",
                            "sn_r": "text",
                            "sn_v1": "text",
                            "sn_v2": "text",
                            "wsk": "timestamp without time zone",
                        },
                        not_null_columns=[
                            "ade",
                            "ags",
                            "ags_0",
                            "ars",
                            "ars_0",
                            "bem",
                            "bez",
                            "bsg",
                            "debkg_id",
                            "fk_s3",
                            "gen",
                            "gf",
                            "ibz",
                            "id",
                            "nbd",
                            "nuts",
                            "rs",
                            "rs_0",
                            "sdv_ars",
                            "sdv_rs",
                            "sn_g",
                            "sn_k",
                            "sn_l",
                            "sn_r",
                            "sn_v1",
                            "sn_v2",
                            "wsk",
                        ],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                    # boundaries.vg250_gem
                    TableValidation(
                        table_name="boundaries.vg250_gem",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1171,
                                "Everything": 11135,
                            }
                        ),
                        geometry_columns=["geometry"],
                        data_type_columns={
                            "ade": "integer",
                            "ags": "text",
                            "ags_0": "text",
                            "ars": "text",
                            "ars_0": "text",
                            "bem": "text",
                            "bez": "text",
                            "bsg": "integer",
                            "debkg_id": "text",
                            "fk_s3": "text",
                            "gen": "text",
                            "geometry": "geometry",
                            "gf": "integer",
                            "ibz": "integer",
                            "id": "bigint",
                            "nbd": "text",
                            "nuts": "text",
                            "rs": "text",
                            "rs_0": "text",
                            "sdv_ars": "text",
                            "sdv_rs": "text",
                            "sn_g": "text",
                            "sn_k": "text",
                            "sn_l": "text",
                            "sn_r": "text",
                            "sn_v1": "text",
                            "sn_v2": "text",
                            "wsk": "timestamp without time zone",
                        },
                        not_null_columns=[
                            "ade",
                            "ags",
                            "ags_0",
                            "ars",
                            "ars_0",
                            "bem",
                            "bez",
                            "bsg",
                            "debkg_id",
                            "fk_s3",
                            "gen",
                            "gf",
                            "ibz",
                            "id",
                            "nbd",
                            "nuts",
                            "rs",
                            "rs_0",
                            "sdv_ars",
                            "sdv_rs",
                            "sn_g",
                            "sn_k",
                            "sn_l",
                            "sn_r",
                            "sn_v1",
                            "sn_v2",
                            "wsk",
                        ],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                ]
            },
            proceed_on_validation_failure=True,
        )
