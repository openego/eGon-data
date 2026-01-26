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
import codecs
import datetime
import json
import os
import time

from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.metadata import (
    context,
    licenses_datenlizenz_deutschland,
    meta_metadata,
)
import egon.data.config
from egon.data.validation import TableValidation, resolve_boundary_dependence
from egon_validation import (
    RowCountValidation,
    DataTypeValidation,
    NotNullAndNotNaNValidation,
    WholeTableNotNullAndNotNaNValidation,
    ValueSetValidation,
    SRIDUniqueNonZero
)


def download_files():
    """
    Download VG250 (Verwaltungsgebiete) shape files.

    Data is downloaded from source specified in *datasets.yml* in section
    *vg250/original_data/source/url* and saved to file specified in
    *vg250/original_data/target/file*.

    """
    data_config = egon.data.config.datasets()
    vg250_config = data_config["vg250"]["original_data"]

    download_directory = Path(".") / "vg250"
    # Create the folder, if it does not exist already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = download_directory / vg250_config["target"]["file"]

    if not os.path.isfile(target_file):
        urlretrieve(vg250_config["source"]["url"], target_file)


def to_postgres():
    """
    Writes original VG250 data to database.

    Creates schema boundaries if it does not yet exist.
    Newly creates all tables specified as keys in *datasets.yml* in section
    *vg250/processed/file_table_map*.

    """

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    vg250_orig = data_config["vg250"]["original_data"]
    vg250_processed = data_config["vg250"]["processed"]

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {vg250_processed['schema']};")

    zip_file = Path(".") / "vg250" / vg250_orig["target"]["file"]
    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in vg250_processed["file_table_map"].items():
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
            f"DROP TABLE IF EXISTS "
            f"{vg250_processed['schema']}.{table} CASCADE;"
        )

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema=vg250_processed["schema"],
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        db.execute_sql(
            f"ALTER TABLE {vg250_processed['schema']}.{table} "
            f"ADD PRIMARY KEY (id);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON "
            f"{vg250_processed['schema']}.{table} USING gist (geometry);"
        )


def add_metadata():
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    vg250_config = egon.data.config.datasets()["vg250"]

    title_and_description = {
        "vg250_sta": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Staat (STA)",
            "description": "Staatsgrenzen der Bundesrepublik Deutschland",
        },
        "vg250_lan": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Länder (LAN)",
            "description": "Landesgrenzen der Bundesländer in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_rbz": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Regierungsbezirke "
            "(RBZ)",
            "description": "Grenzen der Regierungsbezirke in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_krs": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Kreise (KRS)",
            "description": "Grenzen der Landkreise in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_vwg": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - "
            "Verwaltungsgemeinschaften (VWG)",
            "description": "Grenzen der Verwaltungsgemeinschaften in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_gem": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Gemeinden (GEM)",
            "description": "Grenzen der Gemeinden in der "
            "Bundesrepublik Deutschland",
        },
    }

    licenses = [
        licenses_datenlizenz_deutschland(
            attribution="© Bundesamt für Kartographie und Geodäsie "
            "2020 (Daten verändert)"
        )
    ]

    vg250_source = {
        "title": "Verwaltungsgebiete 1:250 000 (Ebenen)",
        "description": "Der Datenbestand umfasst sämtliche Verwaltungseinheiten der "
        "hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden "
        "mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der "
        "Verwaltungseinheit sowie die spezifische Bezeichnung der "
        "Verwaltungsebene des jeweiligen Landes.",
        "path": vg250_config["original_data"]["source"]["url"],
        "licenses": licenses,
    }

    for table in vg250_config["processed"]["file_table_map"].values():
        schema_table = ".".join([vg250_config["processed"]["schema"], table])
        meta = {
            "name": schema_table,
            "title": title_and_description[table]["title"],
            "id": "WILL_BE_SET_AT_PUBLICATION",
            "description": title_and_description[table]["title"],
            "language": ["de-DE"],
            "publicationDate": datetime.date.today().isoformat(),
            "context": context(),
            "spatial": {
                "location": None,
                "extent": "Germany",
                "resolution": "1:250000",
            },
            "temporal": {
                "referenceDate": "2020-01-01",
                "timeseries": {
                    "start": None,
                    "end": None,
                    "resolution": None,
                    "alignment": None,
                    "aggregationType": None,
                },
            },
            "sources": [vg250_source],
            "licenses": licenses,
            "contributors": [
                {
                    "title": "Guido Pleßmann",
                    "email": "http://github.com/gplssm",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": None,
                    "comment": "Imported data",
                },
                {
                    "title": "Jonathan Amme",
                    "email": "http://github.com/nesnoj",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": None,
                    "comment": "Metadata extended",
                },
            ],
            "resources": [
                {
                    "profile": "tabular-data-resource",
                    "name": schema_table,
                    "path": None,
                    "format": "PostgreSQL",
                    "encoding": "UTF-8",
                    "schema": {
                        "fields": vg250_metadata_resources_fields(),
                        "primaryKey": ["id"],
                        "foreignKeys": [],
                    },
                    "dialect": {"delimiter": None, "decimalSeparator": "."},
                }
            ],
            "metaMetadata": meta_metadata(),
        }

        meta_json = "'" + json.dumps(meta) + "'"

        db.submit_comment(
            meta_json, vg250_config["processed"]["schema"], table
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


def vg250_metadata_resources_fields():
    """
    Returns metadata string for VG250 tables.

    """

    return [
        {
            "description": "Index",
            "name": "id",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Administrative level",
            "name": "ade",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Geofactor",
            "name": "gf",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Particular areas",
            "name": "bsg",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Territorial code",
            "name": "ars",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Official Municipality Key",
            "name": "ags",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Seat of the administration (territorial code)",
            "name": "sdv_ars",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Geographical name",
            "name": "gen",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Designation of the administrative unit",
            "name": "bez",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Identifier",
            "name": "ibz",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Note",
            "name": "bem",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Name generation",
            "name": "nbd",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Land (state)",
            "name": "sn_l",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Administrative district",
            "name": "sn_r",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "District",
            "name": "sn_k",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Administrative association – front part",
            "name": "sn_v1",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Administrative association – rear part",
            "name": "sn_v2",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Municipality",
            "name": "sn_g",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Function of the 3rd key digit",
            "name": "fk_s3",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "European statistics key",
            "name": "nuts",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Filled territorial code",
            "name": "ars_0",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Filled Official Municipality Key",
            "name": "ags_0",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Effectiveness",
            "name": "wsk",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "DLM identifier",
            "name": "debkg_id",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Territorial code (deprecated column)",
            "name": "rs",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Seat of the administration (territorial code, deprecated column)",
            "name": "sdv_rs",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Filled territorial code (deprecated column)",
            "name": "rs_0",
            "type": "string",
            "unit": "none",
        },
        {
            "description": "Geometry of areas as WKB",
            "name": "geometry",
            "type": "Geometry(Polygon, srid=4326)",
            "unit": "none",
        },
    ]


class Vg250(Dataset):
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

    filename = egon.data.config.datasets()["vg250"]["original_data"]["source"][
        "url"
    ]

    #:
    name: str = "VG250"
    #:
    version: str = filename + "-0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                download_files,
                to_postgres,
                nuts_mview,
                add_metadata,
                cleaning_and_preperation,
            ),
            validation={
                "data_quality": [
                    TableValidation(
                        table_name="boundaries.vg250_krs",
                        row_count=resolve_boundary_dependence({"Schleswig-Holstein": 27, "Everything": 537}),
                        geometry_columns=["geometry"],
                        data_type_columns={"Schleswig-Holstein":{"id":"bigint","ade":"integer", "gf":"integer", "bsg":"integer","ars":"text",
                                      "ags":"text", "sdv_ars":"text", "gen":"text", "bez":"text","ibz":"integer",
                                      "bem":"text", "nbd":"text", "sn_l":"text", "sn_r":"text", "sn_k":"text",
                                      "sn_v1":"text", "sn_v2":"text", "sn_g":"text", "fk_s3":"text", "nuts":"text",
                                      "ars_0":"text", "ags_0":"text", "wsk":"timestamp without time zone", "debkg_id":"text", "rs":"text",
                                      "sdv_rs":"text", "rs_0":"text", "geometry":"geometry"},
                                      "Everything":{"id":"bigint","ade":"bigint", "gf":"bigint", "bsg":"bigint","ars":"text",
                                      "ags":"text", "sdv_ars":"text", "gen":"text", "bez":"text","ibz":"bigint",
                                      "bem":"text", "nbd":"text", "sn_l":"text", "sn_r":"text", "sn_k":"text",
                                      "sn_v1":"text", "sn_v2":"text", "sn_g":"text", "fk_s3":"text", "nuts":"text",
                                      "ars_0":"text", "ags_0":"text", "wsk":"text", "debkg_id":"text", "rs":"text",
                                      "sdv_rs":"text", "rs_0":"text", "geometry":"geometry"}
                                      },
                        not_null_columns=["gf", "bsg"],
                        value_set_columns={"nbd": ["ja", "nein"]},
                    ),
                    RowCountValidation(
                        table="boundaries.vg250_krs",
                        rule_id="ROW_COUNT.vg250_krs",
                        expected_count=resolve_boundary_dependence({"Schleswig-Holstein":27, "Everything":431})
                    ),
                    DataTypeValidation(
                        table="boundaries.vg250_krs",
                        rule_id="DATA_TYPES.vg250_krs",
                        column_types={
                            "Schleswig-Holstein": {
                                "id": "bigint",
                                "ade": "integer",
                                "gf": "integer",
                                "bsg": "integer",
                                "ars": "text",
                                "ags": "text",
                                "sdv_ars": "text",
                                "gen": "text",
                                "bez": "text",
                                "ibz": "integer",
                                "bem": "text",
                                "nbd": "text",
                                "sn_l": "text",
                                "sn_r": "text",
                                "sn_k": "text",
                                "sn_v1": "text",
                                "sn_v2": "text",
                                "sn_g": "text",
                                "fk_s3": "text",
                                "nuts": "text",
                                "ars_0": "text",
                                "ags_0": "text",
                                "wsk": "timestamp without time zone",
                                "debkg_id": "text",
                                "rs": "text",
                                "sdv_rs": "text",
                                "rs_0": "text",
                                "geometry": "geometry"
                            },
                            "Everything": {
                                "id": "bigint",
                                "ade": "bigint",
                                "gf": "bigint",
                                "bsg": "bigint",
                                "ars": "text",
                                "ags": "text",
                                "sdv_ars": "text",
                                "gen": "text",
                                "bez": "text",
                                "ibz": "bigint",
                                "bem": "text",
                                "nbd": "text",
                                "sn_l": "text",
                                "sn_r": "text",
                                "sn_k": "text",
                                "sn_v1": "text",
                                "sn_v2": "text",
                                "sn_g": "text",
                                "fk_s3": "text",
                                "nuts": "text",
                                "ars_0": "text",
                                "ags_0": "text",
                                "wsk": "text",
                                "debkg_id": "text",
                                "rs": "text",
                                "sdv_rs": "text",
                                "rs_0": "text",
                                "geometry": "geometry"
                            }
                        }
                    ),
                    NotNullAndNotNaNValidation(
                        table="boundaries.vg250_krs",
                        rule_id="NOT_NAN.vg250_krs",
                        columns=["gf", "bsg"]
                    ),
                    WholeTableNotNullAndNotNaNValidation(
                        table="boundaries.vg250_krs",
                        rule_id="TABLE_NOT_NAN.vg250_krs"
                    ),
                    SRIDUniqueNonZero(
                        table="boundaries.vg250_krs",
                        rule_id="SRIDUniqueNonZero.vg250_krs.geometry",
                        column="geometry"
                    ),
                    ValueSetValidation(
                        table="boundaries.vg250_krs",
                        rule_id="VALUE_SET_NBD.vg250_krs",
                        column="nbd",
                        expected_values=["ja", "nein"]
                    ),
                    RowCountValidation(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="ROW_COUNT.destatis_zensus_population_per_ha_inside_germany",
                        expected_count={
                            "Schleswig-Holstein": 143521,
                            "Everything": 3177723
                        }
                    ),
                    DataTypeValidation(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="DATA_TYPES.destatis_zensus_population_per_ha_inside_germany",
                        column_types={
                            "id": "integer",
                            "grid_id": "character varying (254)",
                            "population": "smallint",
                            "geom_point": "geometry",
                            "geom": "geometry"
                        }
                    ),
                    NotNullAndNotNaNValidation(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="NOT_NAN.destatis_zensus_population_per_ha_inside_germany",
                        columns=[
                            "id",
                            "grid_id",
                            "population",
                            "geom_point",
                            "geom"
                        ]
                    ),
                    WholeTableNotNullAndNotNaNValidation(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="TABLE_NOT_NAN.destatis_zensus_population_per_ha_inside_germany"
                    ),
                    SRIDUniqueNonZero(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="SRIDUniqueNonZero.destatis_zensus_population_per_ha_inside_germany.geom_point",
                        column="geom_point"
                    ),
                    SRIDUniqueNonZero(
                        table="society.destatis_zensus_population_per_ha_inside_germany",
                        rule_id="SRIDUniqueNonZero.destatis_zensus_population_per_ha_inside_germany.geom",
                        column="geom"
                    ),
                ]
            },
            on_validation_failure="continue"
        )
