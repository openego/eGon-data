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
import egon.data.config
import egon.data.subprocess as subprocess


def download_pbf_file():
    """Download OpenStreetMap `.pbf` file."""
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]

    target_file = os.path.join(
        os.path.dirname(__file__), osm_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(osm_config["source"]["url"], target_file)


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
    input_file = os.path.join(
        os.path.dirname(__file__), osm_config["target"]["path"]
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
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    osm_config = egon.data.config.datasets()["openstreetmap"]
    spatial_and_date = os.path.basename(
        osm_config["original_data"]["target"]["path"]
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
    osm_url = osm_config["original_data"]["source"]["url"]

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
