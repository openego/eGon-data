"""The central module containing all code dealing with importing Zensus data.
"""

from pathlib import Path
from urllib.request import urlretrieve
import csv
import json
import os
import zipfile

from shapely.geometry import Point, shape
from shapely.prepared import prep
import pandas as pd

from egon.data import db, subprocess
from egon.data.config import settings
from egon.data.datasets import Dataset
import egon.data.config


class ZensusPopulation(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ZensusPopulation",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                download_zensus_pop,
                create_zensus_pop_table,
                population_to_postgres,
            ),
        )


class ZensusMiscellaneous(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ZensusMiscellaneous",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                download_zensus_misc,
                create_zensus_misc_tables,
                zensus_misc_to_postgres,
            ),
        )


def download_zensus_pop():
    """Download Zensus csv file on population per hectar grid cell."""
    data_config = egon.data.config.datasets()
    zensus_population_config = data_config["zensus_population"][
        "original_data"
    ]
    download_directory = Path(".") / "zensus_population"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = (
        download_directory / zensus_population_config["target"]["file"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(zensus_population_config["source"]["url"], target_file)


def download_zensus_misc():
    """Download Zensus csv files on data per hectar grid cell."""

    def download_and_check(target_file_misc, max_iteration=5):
        """Download file if doesnt exist and check afterwards. If badzip
        remove file and re-download. Repeat until file is fine or reached
        maximum iterations."""
        bad_file = True
        count = 0
        while bad_file:

            if not os.path.isfile(target_file_misc):
                urlretrieve(url, target_file_misc)

            # check zipfile
            try:
                _ = zipfile.ZipFile(target_file_misc)
                print(f"Zip file {target_file_misc} is good.")
                bad_file = False
            except zipfile.BadZipFile:
                os.remove(target_file_misc)
                count += 1
                if count > max_iteration:
                    raise zipfile.BadZipFile(
                        f"{target_file_misc} is" f" not a zip file"
                    )
                pass

    # Get data config
    data_config = egon.data.config.datasets()
    download_directory = Path(".") / "zensus_population"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)
    # Download remaining zensus data set on households, buildings, apartments

    zensus_config = data_config["zensus_misc"]["original_data"]
    zensus_misc_processed = data_config["zensus_misc"]["processed"]
    zensus_url = zensus_config["source"]["url"]
    zensus_files = zensus_misc_processed["file_table_map"].keys()
    url_path_map = list(zip(zensus_url, zensus_files))

    for url, path in url_path_map:
        target_file_misc = download_directory / path

        download_and_check(target_file_misc, max_iteration=5)


def create_zensus_pop_table():
    """Create tables for zensus data in postgres database"""

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_processed = data_config["zensus_population"]["processed"]

    # Create target schema
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {zensus_population_processed['schema']};"
    )

    # Create table for population data
    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    db.execute_sql(f"DROP TABLE IF EXISTS {population_table} CASCADE;")

    db.execute_sql(
        f"CREATE TABLE {population_table}"
        f""" (id        SERIAL NOT NULL,
              grid_id    character varying(254) NOT NULL,
              x_mp       int,
              y_mp       int,
              population smallint,
              geom_point geometry(Point,3035),
              geom geometry (Polygon, 3035),
              CONSTRAINT {zensus_population_processed['table']}_pkey
              PRIMARY KEY (id)
        );
        """
    )


def create_zensus_misc_tables():
    """Create tables for zensus data in postgres database"""

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_misc_processed = data_config["zensus_misc"]["processed"]

    # Create target schema
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {zensus_misc_processed['schema']};"
    )

    # Create tables for household, apartment and building
    for table in zensus_misc_processed["file_table_map"].values():
        misc_table = f"{zensus_misc_processed['schema']}.{table}"

        db.execute_sql(f"DROP TABLE IF EXISTS {misc_table} CASCADE;")
        db.execute_sql(
            f"CREATE TABLE {misc_table}"
            f""" (id                 SERIAL,
                  grid_id            VARCHAR(50),
                  grid_id_new        VARCHAR (50),
                  attribute          VARCHAR(50),
                  characteristics_code smallint,
                  characteristics_text text,
                  quantity           smallint,
                  quantity_q         smallint,
                  zensus_population_id int,
                  CONSTRAINT {table}_pkey PRIMARY KEY (id)
            );
            """
        )


def target(source, dataset):
    """Generate the target path corresponding to a source path.

    Parameters
    ----------
    dataset: str
        Toggles between production (`dataset='Everything'`) and test mode e.g.
        (`dataset='Schleswig-Holstein'`).
        In production mode, data covering entire Germany
        is used. In the test mode a subset of this data is used for testing the
        workflow.
    Returns
    -------
    Path
        Path to target csv-file

    """
    return Path(
        os.path.join(Path("."), "zensus_population", source.stem)
        + "."
        + dataset
        + source.suffix
    )


def select_geom():
    """Select the union of the geometries of Schleswig-Holstein from the
    database, convert their projection to the one used in the CSV file,
    output the result to stdout as a GeoJSON string and read it into a
    prepared shape for filtering.

    """
    docker_db_config = db.credentials()

    geojson = subprocess.run(
        ["ogr2ogr"]
        + ["-s_srs", "epsg:4326"]
        + ["-t_srs", "epsg:3035"]
        + ["-f", "GeoJSON"]
        + ["/vsistdout/"]
        + [
            f"PG:host={docker_db_config['HOST']}"
            f" user='{docker_db_config['POSTGRES_USER']}'"
            f" password='{docker_db_config['POSTGRES_PASSWORD']}'"
            f" port={docker_db_config['PORT']}"
            f" dbname='{docker_db_config['POSTGRES_DB']}'"
        ]
        + ["-sql", "SELECT ST_Union(geometry) FROM boundaries.vg250_lan"],
        text=True,
    )
    features = json.loads(geojson.stdout)["features"]
    assert (
        len(features) == 1
    ), f"Found {len(features)} geometry features, expected exactly one."

    return prep(shape(features[0]["geometry"]))


def filter_zensus_population(filename, dataset):
    """This block filters lines in the source CSV file and copies
    the appropriate ones to the destination based on geometry.


    Parameters
    ----------
    filename : str
        Path to input csv-file
    dataset: str, optional
        Toggles between production (`dataset='Everything'`) and test mode e.g.
        (`dataset='Schleswig-Holstein'`).
        In production mode, data covering entire Germany
        is used. In the test mode a subset of this data is used for testing the
        workflow.
    Returns
    -------
    str
        Path to output csv-file

    """

    csv_file = Path(filename).resolve(strict=True)

    schleswig_holstein = select_geom()

    if not os.path.isfile(target(csv_file, dataset)):

        with open(csv_file, mode="r", newline="") as input_lines:
            rows = csv.DictReader(input_lines, delimiter=";")
            gitter_ids = set()
            with open(
                target(csv_file, dataset), mode="w", newline=""
            ) as destination:
                output = csv.DictWriter(
                    destination, delimiter=";", fieldnames=rows.fieldnames
                )
                output.writeheader()
                output.writerows(
                    gitter_ids.add(row["Gitter_ID_100m"]) or row
                    for row in rows
                    if schleswig_holstein.intersects(
                        Point(float(row["x_mp_100m"]), float(row["y_mp_100m"]))
                    )
                )
    return target(csv_file, dataset)


def filter_zensus_misc(filename, dataset):
    """This block filters lines in the source CSV file and copies
    the appropriate ones to the destination based on grid_id values.


    Parameters
    ----------
    filename : str
        Path to input csv-file
    dataset: str, optional
        Toggles between production (`dataset='Everything'`) and test mode e.g.
        (`dataset='Schleswig-Holstein'`).
        In production mode, data covering entire Germany
        is used. In the test mode a subset of this data is used for testing the
        workflow.
    Returns
    -------
    str
        Path to output csv-file

    """
    csv_file = Path(filename).resolve(strict=True)

    gitter_ids = set(
        pd.read_sql(
            "SELECT grid_id from society.destatis_zensus_population_per_ha",
            con=db.engine(),
        ).grid_id.values
    )

    if not os.path.isfile(target(csv_file, dataset)):
        with open(
            csv_file, mode="r", newline="", encoding="iso-8859-1"
        ) as inputs:
            rows = csv.DictReader(inputs, delimiter=",")
            with open(
                target(csv_file, dataset),
                mode="w",
                newline="",
                encoding="iso-8859-1",
            ) as destination:
                output = csv.DictWriter(
                    destination, delimiter=",", fieldnames=rows.fieldnames
                )
                output.writeheader()
                output.writerows(
                    row for row in rows if row["Gitter_ID_100m"] in gitter_ids
                )
    return target(csv_file, dataset)


def population_to_postgres():
    """Import Zensus population data to postgres database"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_orig = data_config["zensus_population"]["original_data"]
    zensus_population_processed = data_config["zensus_population"]["processed"]
    input_file = (
        Path(".")
        / "zensus_population"
        / zensus_population_orig["target"]["file"]
    )
    dataset = settings()["egon-data"]["--dataset-boundary"]

    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    with zipfile.ZipFile(input_file) as zf:
        for filename in zf.namelist():

            zf.extract(filename)

            if dataset == "Everything":
                filename_insert = filename
            else:
                filename_insert = filter_zensus_population(filename, dataset)

            host = ["-h", f"{docker_db_config['HOST']}"]
            port = ["-p", f"{docker_db_config['PORT']}"]
            pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
            user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
            command = [
                "-c",
                rf"\copy {population_table} (grid_id, x_mp, y_mp, population)"
                rf" FROM '{filename_insert}' DELIMITER ';' CSV HEADER;",
            ]
            subprocess.run(
                ["psql"] + host + port + pgdb + user + command,
                env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
            )

        os.remove(filename)

    db.execute_sql(
        f"UPDATE {population_table} zs"
        " SET geom_point=ST_SetSRID(ST_MakePoint(zs.x_mp, zs.y_mp), 3035);"
    )

    db.execute_sql(
        f"UPDATE {population_table} zs"
        """ SET geom=ST_SetSRID(
                (ST_MakeEnvelope(zs.x_mp-50,zs.y_mp-50,zs.x_mp+50,zs.y_mp+50)),
                3035
            );
        """
    )

    db.execute_sql(
        f"CREATE INDEX {zensus_population_processed['table']}_geom_idx ON"
        f" {population_table} USING gist (geom);"
    )

    db.execute_sql(
        f"CREATE INDEX"
        f" {zensus_population_processed['table']}_geom_point_idx"
        f" ON  {population_table} USING gist (geom_point);"
    )


def zensus_misc_to_postgres():
    """Import data on buildings, households and apartments to postgres db"""

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_misc_processed = data_config["zensus_misc"]["processed"]
    zensus_population_processed = data_config["zensus_population"]["processed"]
    file_path = Path(".") / "zensus_population"
    dataset = settings()["egon-data"]["--dataset-boundary"]

    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    for input_file, table in zensus_misc_processed["file_table_map"].items():
        with zipfile.ZipFile(file_path / input_file) as zf:
            csvfiles = [n for n in zf.namelist() if n.lower()[-3:] == "csv"]
            for filename in csvfiles:
                zf.extract(filename)

                if dataset == "Everything":
                    filename_insert = filename
                else:
                    filename_insert = filter_zensus_misc(filename, dataset)

                host = ["-h", f"{docker_db_config['HOST']}"]
                port = ["-p", f"{docker_db_config['PORT']}"]
                pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
                user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
                command = [
                    "-c",
                    rf"\copy {zensus_population_processed['schema']}.{table}"
                    f"""(grid_id,
                        grid_id_new,
                        attribute,
                        characteristics_code,
                        characteristics_text,
                        quantity,
                        quantity_q)
                        FROM '{filename_insert}' DELIMITER ','
                        CSV HEADER
                        ENCODING 'iso-8859-1';""",
                ]
                subprocess.run(
                    ["psql"] + host + port + pgdb + user + command,
                    env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
                )

            os.remove(filename)

        db.execute_sql(
            f"""UPDATE {zensus_population_processed['schema']}.{table} as b
                    SET zensus_population_id = zs.id
                    FROM {population_table} zs
                    WHERE b.grid_id = zs.grid_id;"""
        )

        db.execute_sql(
            f"""ALTER TABLE {zensus_population_processed['schema']}.{table}
                    ADD CONSTRAINT {table}_fkey
                    FOREIGN KEY (zensus_population_id)
                    REFERENCES {population_table}(id);"""
        )

    # Create combined table
    create_combined_zensus_table()

    # Delete entries for unpopulated cells
    adjust_zensus_misc()


def create_combined_zensus_table():
    """Create combined table with buildings, apartments and population per cell

    Only apartment and building data with acceptable data quality
    (quantity_q<2) is used, all other data is dropped. For more details on data
    quality see Zensus docs:
    https://www.zensus2011.de/DE/Home/Aktuelles/DemografischeGrunddaten.html

    If there's no data on buildings or apartments for a certain cell, the value
    for building_count resp. apartment_count contains NULL.
    """
    sql_script = os.path.join(
        os.path.dirname(__file__), "create_combined_zensus_table.sql"
    )
    db.execute_sql_script(sql_script)


def adjust_zensus_misc():
    """Deletes zensus households, buildings and aparments in unpopulated cells

    Some unpopulated zensus cells are listed in the table of households,
    buildings and/or aparments. This can be caused by missing population
    information due to privacy or other special cases (e.g. holiday homes
    are listed as buildings but are not permanently populated.)
    In the follwong tasks of egon-data, only data of populated cells is used.

    Returns
    -------
    None.

    """
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_processed = data_config["zensus_population"]["processed"]
    zensus_misc_processed = data_config["zensus_misc"]["processed"]

    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    for input_file, table in zensus_misc_processed["file_table_map"].items():
        db.execute_sql(
            f"""
             DELETE FROM {zensus_population_processed['schema']}.{table} as b
             WHERE b.zensus_population_id IN (
                 SELECT id FROM {population_table}
                 WHERE population < 0);"""
        )


if __name__ == "__main__":
    download_zensus_misc()
