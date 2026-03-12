"""The central module containing all code dealing with importing Zensus data."""

from pathlib import Path
import csv
import json
import os
import zipfile

from shapely.geometry import Point, shape
from shapely.prepared import prep
import pandas as pd
import requests

from egon.data import db, subprocess
from egon.data.config import settings
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets


class ZensusPopulation(Dataset):
    sources = DatasetSources(
        files={
            "zensus_population": "data_bundle_egon_data/zensus_population/csv_Bevoelkerung_100m_Gitter.zip"
        },
        tables={
            "boundaries_vg250_lan": "boundaries.vg250_lan",
        },
    )
    targets = DatasetTargets(
        tables={
            "zensus_population": "society.destatis_zensus_population_per_ha"
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name="ZensusPopulation",
            version="0.0.4",
            dependencies=dependencies,
            tasks=(
                create_zensus_pop_table,
                population_to_postgres,
            ),
        )


class ZensusMiscellaneous(Dataset):
    sources = DatasetSources(
        urls={
            "zensus_households": (
                "https://www.zensus2011.de/SharedDocs/Downloads/DE/"
                "Pressemitteilung/DemografischeGrunddaten/"
                "csv_Haushalte_100m_Gitter.zip?__blob=publicationFile&v=2"
            ),
            "zensus_buildings": (
                "https://www.zensus2011.de/SharedDocs/Downloads/DE/"
                "Pressemitteilung/DemografischeGrunddaten/"
                "csv_Gebaeude_100m_Gitter.zip?__blob=publicationFile&v=2"
            ),
            "zensus_apartments": (
                "https://www.zensus2011.de/SharedDocs/Downloads/DE/"
                "Pressemitteilung/DemografischeGrunddaten/"
                "csv_Wohnungen_100m_Gitter.zip?__blob=publicationFile&v=5"
            ),
        }
    )
    targets = DatasetTargets(
        files={
            "zensus_households": "data_bundle_egon_data/zensus_population/csv_Haushalte_100m_Gitter.zip",
            "zensus_buildings": "data_bundle_egon_data/zensus_population/csv_Gebaeude_100m_Gitter.zip",
            "zensus_apartments": "data_bundle_egon_data/zensus_population/csv_Wohnungen_100m_Gitter.zip",
        },
        tables={
            "zensus_households": "society.egon_destatis_zensus_household_per_ha",
            "zensus_buildings": "society.egon_destatis_zensus_building_per_ha",
            "zensus_apartments": "society.egon_destatis_zensus_apartment_per_ha",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name="ZensusMiscellaneous",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(
                create_zensus_misc_tables,
                zensus_misc_to_postgres,
            ),
        )


def create_zensus_pop_table():
    """Create tables for zensus data in postgres database"""

    # Create table for population data
    population_table = ZensusPopulation.targets.tables["zensus_population"]

    db.execute_sql(f"""
        CREATE SCHEMA IF NOT EXISTS
        {ZensusPopulation.targets.get_table_schema("zensus_population")};
        """)

    db.execute_sql(f"DROP TABLE IF EXISTS {population_table} CASCADE;")

    db.execute_sql(
        f"CREATE TABLE {population_table}" f""" (id        SERIAL NOT NULL,
              grid_id    character varying(254) NOT NULL,
              x_mp       int,
              y_mp       int,
              population smallint,
              geom_point geometry(Point,3035),
              geom geometry (Polygon, 3035),
              CONSTRAINT {population_table.split('.')[1]}_pkey
              PRIMARY KEY (id)
    
        );
        """
    )


def create_zensus_misc_tables():
    """Create tables for zensus data in postgres database"""

    # Create tables for household, apartment and building
    for table in ZensusMiscellaneous.targets.tables:
        table_name = ZensusMiscellaneous.targets.tables[table]
        # Create target schema
        db.execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS {table_name.split('.')[0]};"
        )
        db.execute_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        db.execute_sql(
            f"CREATE TABLE {table_name}" f""" (id                 SERIAL,
                  grid_id            VARCHAR(50),
                  grid_id_new        VARCHAR (50),
                  attribute          VARCHAR(50),
                  characteristics_code smallint,
                  characteristics_text text,
                  quantity           smallint,
                  quantity_q         smallint,
                  zensus_population_id int,
                  CONSTRAINT {table_name.split('.')[1]}_pkey PRIMARY KEY (id)
            );
            """
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
        + [
            "-sql",
            f"SELECT ST_Union(geometry) FROM {ZensusPopulation.sources.tables['boundaries_vg250_lan']}",
        ],
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

    # compute the filtered file path inline
    filtered_target = (
        csv_file.parent / f"{csv_file.stem}.{dataset}{csv_file.suffix}"
    )
    filtered_target.parent.mkdir(parents=True, exist_ok=True)

    if not filtered_target.exists():
        with open(csv_file, mode="r", newline="") as input_lines:
            rows = csv.DictReader(input_lines, delimiter=";")
            gitter_ids = set()
            with open(filtered_target, mode="w", newline="") as destination:
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
    return filtered_target


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
            f"SELECT grid_id FROM {ZensusPopulation.targets.tables['zensus_population']}",
            con=db.engine(),
        ).grid_id.values
    )

    # inline target path (no helper)
    filtered_target = (
        csv_file.parent / f"{csv_file.stem}.{dataset}{csv_file.suffix}"
    )
    filtered_target.parent.mkdir(parents=True, exist_ok=True)

    if not filtered_target.exists():
        with open(
            csv_file, mode="r", newline="", encoding="iso-8859-1"
        ) as inputs:
            rows = csv.DictReader(inputs, delimiter=",")
            with open(
                filtered_target,
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
    return filtered_target


def population_to_postgres():
    """Import Zensus population data to postgres database"""
    input_file = Path(
        ZensusPopulation.sources.files["zensus_population"]
    ).resolve()
    dataset = settings()["egon-data"]["--dataset-boundary"]
    docker_db_config = db.credentials()
    population_table = ZensusPopulation.targets.tables["zensus_population"]

    with zipfile.ZipFile(input_file) as zf:
        for filename in zf.namelist():
            if not filename.lower().endswith(".csv"):
                continue
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
                rf"\copy {population_table} (grid_id, x_mp, y_mp, population) "
                rf"FROM '{filename_insert}' DELIMITER ';' CSV HEADER;",
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
    db.execute_sql(f"UPDATE {population_table} zs" """ SET geom=ST_SetSRID(
                (ST_MakeEnvelope(zs.x_mp-50,zs.y_mp-50,zs.x_mp+50,zs.y_mp+50)),
                3035
            );
        """)
    db.execute_sql(
        f"CREATE INDEX {population_table.split('.')[1]}_geom_idx ON"
        f" {population_table} USING gist (geom);"
    )
    db.execute_sql(
        f"CREATE INDEX"
        f" {population_table.split('.')[1]}_geom_point_idx"
        f" ON  {population_table} USING gist (geom_point);"
    )


def zensus_misc_to_postgres():
    """Import data on buildings, households and apartments to postgres db"""

    dataset = settings()["egon-data"]["--dataset-boundary"]
    docker_db_config = db.credentials()

    for key, file_path in ZensusMiscellaneous.targets.files.items():
        zip_path = Path(file_path).resolve()

        with zipfile.ZipFile(zip_path) as zf:
            csvfiles = [n for n in zf.namelist() if n.lower().endswith(".csv")]
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
                    rf"\copy {ZensusMiscellaneous.targets.tables[key]}"
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
            f"""UPDATE {ZensusMiscellaneous.targets.tables[key]} as b
                    SET zensus_population_id = zs.id
                    FROM {ZensusPopulation.targets.tables["zensus_population"]} zs
                    WHERE b.grid_id = zs.grid_id;"""
        )

        db.execute_sql(
            f"""ALTER TABLE {ZensusMiscellaneous.targets.tables[key]}
                    ADD CONSTRAINT
                    {ZensusMiscellaneous.targets.get_table_name(key)}_fkey
                    FOREIGN KEY (zensus_population_id)
                    REFERENCES {ZensusPopulation.targets.tables["zensus_population"]}(id);"""
        )

    # combined table
    create_combined_zensus_table()
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
    """Delete unpopulated cells in zensus-households, -buildings and -apartments

    Some unpopulated zensus cells are listed in:
    - egon_destatis_zensus_household_per_ha
    - egon_destatis_zensus_building_per_ha
    - egon_destatis_zensus_apartment_per_ha

    This can be caused by missing population
    information due to privacy or other special cases (e.g. holiday homes
    are listed as buildings but are not permanently populated.)
    In the following tasks of egon-data, only data of populated cells is used.

    Returns
    -------
    None.

    """

    for table in ZensusMiscellaneous.targets.tables:
        db.execute_sql(f"""
             DELETE FROM {ZensusMiscellaneous.targets.tables[table]} as b
             WHERE b.zensus_population_id IN (
                 SELECT id FROM {
                     ZensusPopulation.targets.tables["zensus_population"]}
                 WHERE population < 0);""")
