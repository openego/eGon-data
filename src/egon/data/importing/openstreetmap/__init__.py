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
import datetime

from egon.data import db
from egon.data.config import settings
import egon.data.config
from egon.data.metadata import license_odbl, context
import egon.data.subprocess as subprocess


def download_pbf_file():
    """Download OpenStreetMap `.pbf` file.

    """
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    if settings()['egon-data']['--dataset-boundary'] == 'Everything':
        source_url =osm_config["source"]["url"]
        target_path = osm_config["target"]["path"]
    else:
        source_url = osm_config["source"]["url_testmode"]
        target_path = osm_config["target"]["path_testmode"]

    target_file = os.path.join(
        os.path.dirname(__file__), target_path
    )

    if not os.path.isfile(target_file):
        urlretrieve(source_url, target_file)


def to_postgres(num_processes=4, cache_size=4096):
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

    if settings()['egon-data']['--dataset-boundary'] == 'Everything':
        target_path = osm_config["target"]["path"]
    else:
        target_path = osm_config["target"]["path_testmode"]

    input_file = os.path.join(
        os.path.dirname(__file__), target_path
    )

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
    """Writes metadata JSON string into table comment.

    """
    # Prepare variables
    osm_config = egon.data.config.datasets()["openstreetmap"]

    if settings()['egon-data']['--dataset-boundary'] == 'Everything':
        osm_url = osm_config["original_data"]["source"]["url"]
        target_path = osm_config["original_data"]["target"]["path"]
    else:
        osm_url = osm_config["original_data"]["source"]["url_testmode"]
        target_path = osm_config["original_data"]["target"]["path_testmode"]
    spatial_and_date = os.path.basename(
        target_path
    ).split("-")
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
    licenses = [license_odbl()]

    for table in osm_config["processed"]["tables"]:
        table_suffix = table.split("_")[1]
        meta = {
            "name": ".".join([osm_config["processed"]["tables"], table]),
            "title": f"OpenStreetMap (OSM) - Germany - {table_suffix}",
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
                    "title": (
                        "OpenStreetMap Data Extracts (Geofabrik)"
                    ),
                    "description": (
                        "Full data extract of OpenStreetMap data for defined "
                        "spatial extent at 'referenceDate'"
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
                    "date": "",
                    "object": "",
                    "comment": "",
                }
            ],
            "metaMetadata": {
                "metadataVersion": "OEP-1.4.1",
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
