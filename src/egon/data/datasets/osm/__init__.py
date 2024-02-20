"""The central module containing all code dealing with importing OSM data.

This module either directly contains the code dealing with importing OSM
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from pathlib import Path
from urllib.request import urlretrieve
import datetime
import json
import os
import re
import shutil
import time

import importlib_resources as resources

from egon.data import db, logger
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.metadata import (
    context,
    generate_resource_fields_from_db_table,
    license_odbl,
    meta_metadata,
)
import egon.data.config
import egon.data.subprocess as subprocess


def download():
    """Download OpenStreetMap `.pbf` file."""
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    download_directory = Path(".") / "openstreetmap"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        source_url = osm_config["source"]["url"]
        target_filename = osm_config["target"]["file"]
    else:
        source_url = osm_config["source"]["url_testmode"]
        target_filename = osm_config["target"]["file_testmode"]

    target_file = download_directory / target_filename

    if not os.path.isfile(target_file):
        urlretrieve(source_url, target_file)


def to_postgres(cache_size=4096):
    """Import OSM data from a Geofabrik `.pbf` file into a PostgreSQL database.

    Parameters
    ----------
    cache_size: int, optional
        Memory used during data import

    """
    # Read maximum number of threads per task from egon-data.configuration.yaml
    num_processes = settings()["egon-data"]["--processes-per-task"]

    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    # Get dataset config
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        input_filename = osm_config["target"]["file"]
        logger.info("Using Everything DE dataset.")
    else:
        input_filename = osm_config["target"]["file_testmode"]
        logger.info("Using testmode SH dataset.")

    input_file = Path(".") / "openstreetmap" / input_filename
    style_file = (
        Path(".") / "openstreetmap" / osm_config["source"]["stylefile"]
    )
    with resources.path(
        "egon.data.datasets.osm", osm_config["source"]["stylefile"]
    ) as p:
        shutil.copy(p, style_file)

    # Prepare osm2pgsql command
    cmd = [
        "osm2pgsql",
        "--create",
        "--slim",
        "--hstore-all",
        "--number-processes",
        f"{num_processes}",
        "--cache",
        f"{cache_size}",
        "-H",
        f"{docker_db_config['HOST']}",
        "-P",
        f"{docker_db_config['PORT']}",
        "-d",
        f"{docker_db_config['POSTGRES_DB']}",
        "-U",
        f"{docker_db_config['POSTGRES_USER']}",
        "-p",
        f"{osm_config['target']['table_prefix']}",
        "-S",
        f"{style_file.absolute()}",
        f"{input_file.absolute()}",
    ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(
        cmd,
        env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
        cwd=Path(__file__).parent,
    )


def add_metadata():
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    osm_config = egon.data.config.datasets()["openstreetmap"]

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        osm_url = osm_config["original_data"]["source"]["url"]
        input_filename = osm_config["original_data"]["target"]["file"]
    else:
        osm_url = osm_config["original_data"]["source"]["url_testmode"]
        input_filename = osm_config["original_data"]["target"]["file_testmode"]

    # Extract spatial extend and date
    (spatial_extend, osm_data_date) = re.compile(
        "^([\\w-]*).*-(\\d+)$"
    ).findall(Path(input_filename).name.split(".")[0])[0]
    osm_data_date = datetime.datetime.strptime(
        osm_data_date, "%y%m%d"
    ).strftime("%y-%m-%d")

    # Insert metadata for each table
    licenses = [license_odbl(attribution="© OpenStreetMap contributors")]

    for table in osm_config["processed"]["tables"]:
        schema_table = ".".join([osm_config["processed"]["schema"], table])
        table_suffix = table.split("_")[1]
        meta = {
            "name": schema_table,
            "title": f"OpenStreetMap (OSM) - Germany - {table_suffix}",
            "id": "WILL_BE_SET_AT_PUBLICATION",
            "description": (
                "OpenStreetMap is a free, editable map of the"
                " whole world that is being built by volunteers"
                " largely from scratch and released with"
                " an open-content license.\n\n"
                "The OpenStreetMap data here is the result of an PostgreSQL "
                "database import using osm2pgsql with a custom style file."
            ),
            "language": ["en-EN", "de-DE"],
            "publicationDate": datetime.date.today().isoformat(),
            "context": context(),
            "spatial": {
                "location": None,
                "extent": f"{spatial_extend}",
                "resolution": None,
            },
            "temporal": {
                "referenceDate": f"{osm_data_date}",
                "timeseries": {
                    "start": None,
                    "end": None,
                    "resolution": None,
                    "alignment": None,
                    "aggregationType": None,
                },
            },
            "sources": [
                {
                    "title": "OpenStreetMap Data Extracts (Geofabrik)",
                    "description": (
                        "Full data extract of OpenStreetMap data for defined "
                        "spatial extent at ''referenceDate''"
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
                        "fields": generate_resource_fields_from_db_table(
                            osm_config["processed"]["schema"], table
                        ),
                        "primaryKey": ["id"],
                        "foreignKeys": [],
                    },
                    "dialect": {"delimiter": None, "decimalSeparator": "."},
                }
            ],
            "metaMetadata": meta_metadata(),
        }

        meta_json = "'" + json.dumps(meta) + "'"

        db.submit_comment(meta_json, "openstreetmap", table)


def modify_tables():
    """Adjust primary keys, indices and schema of OSM tables.

    * The Column "id" is added and used as the new primary key.
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

        # Add primary key on newly created column "id"
        sql_statements.append(f"ALTER TABLE public.{table} ADD id SERIAL;")
        sql_statements.append(
            f"ALTER TABLE public.{table} ADD PRIMARY KEY (id);"
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
            version="0.0.5",
            dependencies=dependencies,
            tasks=(download, to_postgres, modify_tables, add_metadata),
        )
