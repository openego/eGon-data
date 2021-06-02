"""The central module containing all code dealing with importing OSM data.

This module either directly contains the code dealing with importing OSM
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from urllib.request import urlretrieve
import json
import os
import time

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
import egon.data.config
import egon.data.subprocess as subprocess


def download():
    """Download OpenStreetMap `.pbf` file."""
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        source_url = osm_config["source"]["url"]
        target_path = osm_config["target"]["path"]
    else:
        source_url = osm_config["source"]["url_testmode"]
        target_path = osm_config["target"]["path_testmode"]

    target_file = os.path.join(os.path.dirname(__file__), target_path)

    if not os.path.isfile(target_file):
        urlretrieve(source_url, target_file)


def to_postgres(num_processes=1, cache_size=4096):
    """Import OSM data from a Geofabrik `.pbf` file into a PostgreSQL database.

    Parameters
    ----------
    num_processes : int, optional
        Number of parallel processes used for processing during data import
    cache_size: int, optional
        Memory used during data import

    """
    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    # Get dataset config
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        target_path = osm_config["target"]["path"]
    else:
        target_path = osm_config["target"]["path_testmode"]

    input_file = os.path.join(os.path.dirname(__file__), target_path)

    # Prepare osm2pgsql command
    cmd = [
        "osm2pgsql",
        "--create",
        "--slim",
        "--hstore-all",
        f"--number-processes {num_processes}",
        f"--cache {cache_size}",
        f"-H {docker_db_config['HOST']} -P {docker_db_config['PORT']} "
        f"-d {docker_db_config['POSTGRES_DB']} "
        f"-U {docker_db_config['POSTGRES_USER']}",
        f"-p {osm_config['target']['table_prefix']}",
        f"-S {osm_config['source']['stylefile']}",
        f"{input_file}",
    ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(
        " ".join(cmd),
        shell=True,
        env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
        cwd=os.path.dirname(__file__),
    )


def add_metadata():
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    osm_config = egon.data.config.datasets()["openstreetmap"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        osm_url = osm_config["original_data"]["source"]["url"]
        target_path = osm_config["original_data"]["target"]["path"]
    else:
        osm_url = osm_config["original_data"]["source"]["url_testmode"]
        target_path = osm_config["original_data"]["target"]["path_testmode"]
    spatial_and_date = os.path.basename(target_path).split("-")
    spatial_extend = spatial_and_date[0]
    osm_data_date = (
        "20"
        + spatial_and_date[1][0:2]
        + "-"
        + spatial_and_date[1][2:4]
        + "-"
        + spatial_and_date[1][4:6]
    )

    # Insert metadata for each table
    licenses = [
        {
            "name": "Open Data Commons Open Database License 1.0",
            "title": "",
            "path": "https://opendatacommons.org/licenses/odbl/1.0/",
            "instruction": (
                "You are free: To Share, To Create, To Adapt;"
                " As long as you: Attribute, Share-Alike, Keep open!"
            ),
            "attribution": "© Reiner Lemoine Institut",
        }
    ]
    for table in osm_config["processed"]["tables"]:
        table_suffix = table.split("_")[1]
        meta = {
            "title": f"OpenStreetMap (OSM) - Germany - {table_suffix}",
            "description": (
                "OpenStreetMap is a free, editable map of the"
                " whole world that is being built by volunteers"
                " largely from scratch and released with"
                " an open-content license."
            ),
            "language": ["EN", "DE"],
            "spatial": {
                "location": "",
                "extent": f"{spatial_extend}",
                "resolution": "",
            },
            "temporal": {
                "referenceDate": f"{osm_data_date}",
                "timeseries": {
                    "start": "",
                    "end": "",
                    "resolution": "",
                    "alignment": "",
                    "aggregationType": "",
                },
            },
            "sources": [
                {
                    "title": (
                        "Geofabrik - Download - OpenStreetMap Data Extracts"
                    ),
                    "description": (
                        'Data dump taken on "referenceDate",'
                        f" i.e. {osm_data_date}."
                        " A subset of this is selected using osm2pgsql"
                        ' using the style file "oedb.style".'
                    ),
                    "path": f"{osm_url}",
                    "licenses": licenses,
                }
            ],
            "licenses": licenses,
            "contributors": [
                {
                    "title": "Guido Pleßmann",
                    "email": "http://github.com/gplssm",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": "",
                    "comment": "Imported data",
                }
            ],
            "metaMetadata": {
                "metadataVersion": "OEP-1.4.0",
                "metadataLicense": {
                    "name": "CC0-1.0",
                    "title": "Creative Commons Zero v1.0 Universal",
                    "path": (
                        "https://creativecommons.org/publicdomain/zero/1.0/"
                    ),
                },
            },
        }

        meta_json = "'" + json.dumps(meta) + "'"

        db.submit_comment(meta_json, "openstreetmap", table)


def modify_tables():
    """Adjust primary keys, indices and schema of OSM tables.

    * The Column "gid" is added and used as the new primary key.
    * Indices (GIST, GIN) are reset
    * The tables are moved to the schema configured as the "output_schema".
    """
    # Get dataset config
    data_config = egon.data.config.datasets()["openstreetmap"]

    # Replace indices and primary keys
    for table in [
        f"{data_config['original_data']['target']['table_prefix']}_" + suffix
        for suffix in ["line", "point", "polygon", "roads"]
    ]:

        # Drop indices
        sql_statements = [f"DROP INDEX IF EXISTS {table}_index;"]

        # Drop primary keys
        sql_statements.append(f"DROP INDEX IF EXISTS {table}_pkey;")

        # Add primary key on newly created column "gid"
        sql_statements.append(f"ALTER TABLE public.{table} ADD gid SERIAL;")
        sql_statements.append(
            f"ALTER TABLE public.{table} ADD PRIMARY KEY (gid);"
        )
        sql_statements.append(
            f"ALTER TABLE public.{table} RENAME COLUMN way TO geom;"
        )

        # Add indices (GIST and GIN)
        sql_statements.append(
            f"CREATE INDEX {table}_geom_idx ON public.{table} "
            f"USING gist (geom);"
        )
        sql_statements.append(
            f"CREATE INDEX {table}_tags_idx ON public.{table} "
            f"USING GIN (tags);"
        )

        # Execute collected SQL statements
        for statement in sql_statements:
            db.execute_sql(statement)

    # Move table to schema "openstreetmap"
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {data_config['processed']['schema']};"
    )

    for out_table in data_config["processed"]["tables"]:
        db.execute_sql(
            f"DROP TABLE IF EXISTS "
            f"{data_config['processed']['schema']}.{out_table};"
        )

        sql_statement = (
            f"ALTER TABLE public.{out_table} "
            f"SET SCHEMA {data_config['processed']['schema']};"
        )

        db.execute_sql(sql_statement)


class OpenStreetMap(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OpenStreetMap",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(download, to_postgres, modify_tables, add_metadata),
        )
