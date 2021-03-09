"""The central module containing all code dealing with importing Zensus data.
"""

from urllib.request import urlretrieve
import os
import zipfile

from importlib_resources import files

from egon.data import db, subprocess
import egon.data.config
import egon.data.importing.zensus as import_zensus


def download_zensus_pop():
    """Download Zensus csv file on population per hectar grid cell."""
    data_config = egon.data.config.datasets()
    zensus_population_config = data_config["zensus_population"][
        "original_data"
    ]

    target_file = os.path.join(
        files(import_zensus), zensus_population_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(zensus_population_config["source"]["url"], target_file)


def download_zensus_misc():
    """Download Zensus csv files on data per hectar grid cell."""

    # Get data config
    data_config = egon.data.config.datasets()

    # Download remaining zensus data set on households, buildings, apartments

    zensus_config = data_config["zensus_misc"]["original_data"]
    zensus_misc_processed = data_config["zensus_misc"]["processed"]
    zensus_url = zensus_config["source"]["url"]
    zensus_path = zensus_misc_processed["path_table_map"].keys()
    url_path_map = list(zip(zensus_url, zensus_path))

    for url, path in url_path_map:
        target_file_misc = os.path.join(files(import_zensus), path)

        if not os.path.isfile(target_file_misc):
            urlretrieve(url, target_file_misc)


def create_zensus_tables():
    """Create tables for zensus data in postgres database"""

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_processed = data_config["zensus_population"]["processed"]
    zensus_misc_processed = data_config["zensus_misc"]["processed"]

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
        f""" (gid        SERIAL NOT NULL,
              grid_id    character varying(254) NOT NULL,
              x_mp       int,
              y_mp       int,
              population smallint,
              geom_point geometry(Point,3035),
              geom geometry (Polygon, 3035),
              CONSTRAINT {zensus_population_processed['table']}_pkey
              PRIMARY KEY (gid)
        );
        """
    )

    # Create tables for household, apartment and building
    for table in zensus_misc_processed["path_table_map"].values():
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
                  gid_ha             int,
                  CONSTRAINT {table}_pkey PRIMARY KEY (id)
            );
            """
        )


def population_to_postgres():
    """Import Zensus population data to postgres database"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_orig = data_config["zensus_population"]["original_data"]
    zensus_population_processed = data_config["zensus_population"]["processed"]
    input_file = os.path.join(
        files(import_zensus), zensus_population_orig["target"]["path"]
    )

    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    with zipfile.ZipFile(input_file) as zf:
        for filename in zf.namelist():
            zf.extract(filename)
            host = ["-h", f"{docker_db_config['HOST']}"]
            port = ["-p", f"{docker_db_config['PORT']}"]
            pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
            user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
            command = [
                "-c",
                rf"\copy {population_table} (grid_id, x_mp, y_mp, population)"
                rf" FROM '{filename}' DELIMITER ';' CSV HEADER;",
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

    population_table = (
        f"{zensus_population_processed['schema']}"
        f".{zensus_population_processed['table']}"
    )

    # Read database configuration from docker-compose.yml
    docker_db_config = db.credentials()

    for input_file, table in zensus_misc_processed["path_table_map"].items():
        with zipfile.ZipFile(os.path.join(
                files(import_zensus), input_file)) as zf:
            csvfiles = [n for n in zf.namelist() if n.lower()[-3:] == "csv"]
            for filename in csvfiles:
                zf.extract(filename)
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
                        FROM '{filename}' DELIMITER ','
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
                    SET gid_ha = zs.gid
                    FROM {population_table} zs
                    WHERE b.grid_id = zs.grid_id;"""
        )

        db.execute_sql(
            f"""ALTER TABLE {zensus_population_processed['schema']}.{table}
                    ADD CONSTRAINT {table}_fkey
                    FOREIGN KEY (gid_ha)
                    REFERENCES {population_table}(gid);"""
        )
