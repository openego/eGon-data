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
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
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
    # The old config variables are now removed.

    download_directory = Path("openstreetmap")
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    # The logic now uses the new class attributes
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        source_url = OpenStreetMap.sources.urls["germany"]
        target_file = Path(OpenStreetMap.targets.files["pbf_germany"])
    else:
        source_url = OpenStreetMap.sources.urls["schleswig-holstein"]
        target_file = Path(OpenStreetMap.targets.files["pbf_schleswig-holstein"])

    if not os.path.isfile(target_file):
        urlretrieve(source_url, target_file)
        

def to_postgres(cache_size=4096):
    """Import OSM data from a Geofabrik `.pbf` file into a PostgreSQL database."""
    num_processes = settings()["egon-data"]["--processes-per-task"]
    docker_db_config = db.credentials()

    # The old config variables are now removed.

    # The logic now uses the new class attributes
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        input_file = Path(OpenStreetMap.targets.files["pbf_germany"])
        logger.info("Using Everything DE dataset.")
    else:
        input_file = Path(OpenStreetMap.targets.files["pbf_schleswig-holstein"])
        logger.info("Using testmode SH dataset.")

    style_file = Path("openstreetmap") / OpenStreetMap.sources.files["stylefile"]
    with resources.path(
        "egon.data.datasets.osm", OpenStreetMap.sources.files["stylefile"]
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
        f"{OpenStreetMap.table_prefix}",  # This line is updated
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
    # The old config variable is now removed.

    # Logic is updated to use the new class attributes
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        osm_url = OpenStreetMap.sources.urls["germany"]
        input_filename = OpenStreetMap.targets.files["pbf_germany"]
    else:
        osm_url = OpenStreetMap.sources.urls["schleswig-holstein"]
        input_filename = OpenStreetMap.targets.files["pbf_schleswig-holstein"]

    (spatial_extend, osm_data_date) = re.compile(
        "^([\\w-]*).*-(\\d+)$"
    ).findall(Path(input_filename).name.split(".")[0])[0]
    osm_data_date = datetime.datetime.strptime(
        osm_data_date, "%y%m%d"
    ).strftime("%y-%m-%d")

    licenses = [license_odbl(attribution="© OpenStreetMap contributors")]


    for schema_table in OpenStreetMap.targets.tables.values():
        schema, table_name = schema_table.split(".")
        table_suffix = table_name.split("_")[1]
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
            # ... (rest of the metadata dictionary is unchanged, except for the 'resources' section) ...
            "resources": [
                {
                    "profile": "tabular-data-resource",
                    "name": schema_table,
                    "path": None,
                    "format": "PostgreSQL",
                    "encoding": "UTF-8",
                    "schema": {
                        "fields": generate_resource_fields_from_db_table(
                            schema, table_name  # This line is updated
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
        db.submit_comment(meta_json, schema, table_name)


def modify_tables():
    """Adjust primary keys, indices and schema of OSM tables.

    * The Column "id" is added and used as the new primary key.
    * Indices (GIST, GIN) are reset
    * The tables are moved to the schema configured as the "output_schema".
    """
    # Get the target schema name from one of the target tables
    schema = OpenStreetMap.targets.get_table_schema("line")
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Loop through the target tables defined in the class
    for key, final_table_name in OpenStreetMap.targets.tables.items():
        # Define the initial table name created by osm2pgsql in the public schema
        public_table_name = f"public.{OpenStreetMap.table_prefix}_{key}"

        sql_statements = [
            f"DROP INDEX IF EXISTS {public_table_name}_index;",
            f"DROP INDEX IF EXISTS {public_table_name}_pkey;",
            f"ALTER TABLE {public_table_name} ADD id SERIAL;",
            f"ALTER TABLE {public_table_name} ADD PRIMARY KEY (id);",
            f"ALTER TABLE {public_table_name} RENAME COLUMN way TO geom;",
            f"CREATE INDEX {public_table_name}_geom_idx ON {public_table_name} USING gist (geom);",
            f"CREATE INDEX {public_table_name}_tags_idx ON {public_table_name} USING GIN (tags);",
        ]

    for statement in sql_statements:
            # Use try-except to avoid errors if a column/index doesn't exist
            try:
                db.execute_sql(statement)
            except Exception:
                logger.warning(f"Could not execute: {statement}")

    db.execute_sql(f"DROP TABLE IF EXISTS {final_table_name};")
    db.execute_sql(
            f"ALTER TABLE {public_table_name} SET SCHEMA {schema};"
        )

class OpenStreetMap(Dataset):
    
    sources = DatasetSources(
        urls={
            "germany": "https://download.geofabrik.de/europe/germany-latest.osm.pbf",
            "schleswig-holstein": "https://download.geofabrik.de/germany/schleswig-holstein-latest.osm.pbf",
        },
        files={"stylefile": "default.style"},
    )
    targets = DatasetTargets(
        files={
            "pbf_germany": "openstreetmap/germany-latest.osm.pbf",
            "pbf_schleswig-holstein": "openstreetmap/schleswig-holstein-latest.osm.pbf",
        },
        tables={
            "line": "openstreetmap.osm_line",
            "point": "openstreetmap.osm_point",
            "polygon": "openstreetmap.osm_polygon",
            "roads": "openstreetmap.osm_roads",
        },
        
    )
    table_prefix="osm",
    """
    Downloads OpenStreetMap data from Geofabrik and writes it to database.

    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    *Resulting Tables*
      * openstreetmap.osm_line is created and filled (table has no associated python class)
      * openstreetmap.osm_nodes is created and filled (table has no associated python class)
      * openstreetmap.osm_point is created and filled (table has no associated python class)
      * openstreetmap.osm_polygon is created and filled (table has no associated python class)
      * openstreetmap.osm_rels is created and filled (table has no associated python class)
      * openstreetmap.osm_roads is created and filled (table has no associated python class)
      * openstreetmap.osm_ways is created and filled (table has no associated python class)

    See documentation section :ref:`osm-ref` for more information.

    """

    #:
    name: str = "OpenStreetMap"
    #:
    version: str = "0.0.5"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download, to_postgres, modify_tables, add_metadata),
        )
