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
    
    download_directory = Path(".") / "openstreetmap"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        source_url = OpenStreetMap.sources.urls["germany"]
        target_filename = Path(OpenStreetMap.targets.files["germany"])
    else:
        source_url = OpenStreetMap.sources.urls["schleswig-holstein"]
        target_filename = Path(OpenStreetMap.targets.files["schleswig-holstein"])
        
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

    # Drop old target tables (the list is in OpenStreetMap.targets.tables)
    for table in OpenStreetMap.targets.tables:
        db.execute_sql(f"DROP TABLE IF EXISTS {OpenStreetMap.schema}.{table} CASCADE;")
        
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        input_filename = Path(OpenStreetMap.targets.files["germany"])
        logger.info("Using Everything DE dataset.")
    else:
        input_filename = Path(OpenStreetMap.targets.files["schleswig-holstein"])
        logger.info("Using testmode SH dataset.")

    input_file = Path(".") / "openstreetmap" / input_filename
    style_file = (
    Path(".") / "openstreetmap" / OpenStreetMap.sources.files["stylefile"]
    )
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
    

    
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        osm_url = OpenStreetMap.sources.urls["germany"]
        input_filename = OpenStreetMap.targets.files["germany"]
    else:
        osm_url = OpenStreetMap.sources.urls["schleswig-holstein"]
        input_filename = OpenStreetMap.targets.files["schleswig-holstein"]

    (spatial_extend, osm_data_date) = re.compile(
        "^([\\w-]*).*-(\\d+)$"
    ).findall(Path(input_filename).name.split(".")[0])[0]
    osm_data_date = datetime.datetime.strptime(
        osm_data_date, "%y%m%d"
    ).strftime("%y-%m-%d")

    licenses = [license_odbl(attribution="© OpenStreetMap contributors")]


    for table in OpenStreetMap.targets.tables:
        schema_table = ".".join([OpenStreetMap.schema, table])
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
                            OpenStreetMap.schema, table
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
        db.submit_comment(meta_json, OpenStreetMap.schema, table)


def modify_tables():
    """Adjust primary keys, indices and schema of OSM tables.

    * The Column "id" is added and used as the new primary key.
    * Indices (GIST, GIN) are reset
    * The tables are moved to the schema configured as the "output_schema".
    """

    # Replace indices and primary keys
    for table in [
        f"{OpenStreetMap.table_prefix}_" + suffix
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

    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {OpenStreetMap.schema};"
    )

    for out_table in OpenStreetMap.targets.tables:
        db.execute_sql(
            f"DROP TABLE IF EXISTS "
            f"{OpenStreetMap.schema}.{out_table};"
        )

        sql_statement = (
            f"ALTER TABLE public.{out_table} "
           f"SET SCHEMA {OpenStreetMap.schema};"
        )

        db.execute_sql(sql_statement)
        
class OpenStreetMap(Dataset):
    
    #:
    name: str = "OpenStreetMap"
    #:
    version: str = "0.0.7"
    
    table_prefix: str = "osm"
    schema: str = "openstreetmap"
    
    sources = DatasetSources(
        files={"stylefile": "oedb.style"},
        urls={
            "germany": "https://download.geofabrik.de/europe/germany-250101.osm.pbf",
            "schleswig-holstein": "https://download.geofabrik.de/europe/germany/schleswig-holstein-250101.osm.pbf",
        },
    )
    targets = DatasetTargets(
        files={
            "germany": "germany-250101.osm.pbf",
            "schleswig-holstein": "schleswig-holstein-250101.osm.pbf",
        },
        tables=[
            "osm_line",
            "osm_nodes",
            "osm_point",
            "osm_polygon",
            "osm_rels",
            "osm_roads",
            "osm_ways",
        ],
    )
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
    

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download, to_postgres, modify_tables, add_metadata),
        )
