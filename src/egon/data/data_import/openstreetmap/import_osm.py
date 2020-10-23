import subprocess
import os
from urllib.request import urlretrieve
import yaml
import egon.data
from egon.data import utils


def download_osm_file():
    data_config = utils.data_set_configuration()
    osm_config = data_config["openstreetmap"]["original_data"]["osm"]

    if not os.path.isfile(osm_config["file"]):
        urlretrieve(osm_config["url"] + osm_config["file"], osm_config["file"])


def osm2postgres(osm_file=None, num_processes=4, cache_size=4096):

    # Read database configuration from docker-compose.yml
    docker_db_config = utils.egon_data_db_credentials()

    # Get data set config
    data_config = utils.data_set_configuration()
    osm_config = data_config["openstreetmap"]["original_data"]["osm"]

    # Prepare osm2pgsql command
    cmd = ["osm2pgsql",
           "--create",
           "--slim",
           "--hstore-all",
           f"--number-processes {num_processes}",
           f"--cache {cache_size}",
           f"-H {docker_db_config['HOST']} -P {docker_db_config['PORT']} "
           f"-d {docker_db_config['POSTGRES_DB']} -U {docker_db_config['POSTGRES_USER']}",
           f"-p {osm_config['table_prefix']}",
           f"-S {osm_config['stylefile']}",
           f"{osm_config['file']}"
           ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(" ".join(cmd),
                   shell=True,
                   env={"PGPASSWORD": docker_db_config['POSTGRES_PASSWORD']},
                   cwd=os.path.dirname(__file__))


def indices_and_constraints():

    # Replace indices and primary keys
    for table in ["osm_" + suffix for suffix in ["line", "point", "polygon", "roads"]]:

        # Drop indices
        sql_statements = [f"DROP INDEX {table}_index;"]

        # Drop primary keys
        sql_statements.append(f"DROP INDEX {table}_pkey;")

        # Add primary key on newly created column "gid"
        sql_statements.append(f"ALTER TABLE public.{table} ADD gid SERIAL;")
        sql_statements.append(f"ALTER TABLE public.{table} ADD PRIMARY KEY (gid);")
        sql_statements.append(f"ALTER TABLE public.{table} RENAME COLUMN way TO geom;")

        # Add indices (GIST and GIN)
        sql_statements.append(f"CREATE INDEX {table}_geom_idx ON public.{table} USING gist (geom);")
        sql_statements.append(f"CREATE INDEX {table}_tags_idx ON public.{table} USING GIN (tags);")

        # Execute collected SQL statements
        for statement in sql_statements:
            utils.execute_sql(statement)

    # Get data set config
    data_config = utils.data_set_configuration()["openstreetmap"]["original_data"]["osm"]

    # Move table to schema "openstreetmap"
    utils.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {data_config['output_schema']};")

    for out_table in data_config['output_tables']:
        sql_statement = f"ALTER TABLE public.{out_table} SET SCHEMA {data_config['output_schema']};"

        utils.execute_sql(sql_statement)


# TODO: call osm download from airflow
# TODO: call osm import (.py) from airflow
# TODO: Insert table names in SQL script read from data_sets.yml
# TODO: Write Metadata JSON in table comment
# TODO: add SQL import script and call it with airflow
# TODO: rename "data_import" to "import"
# TODO: write docstrings for added functions
# TODO: add module docstring
# TODO: report in CHANGELOG
# TODO: maybe downgrade Postgres version to the same as in data_processing


if __name__ == "__main__":

    # from egon.data.airflow.tasks import initdb
    # initdb()

    download_osm_file()
    osm2postgres()
    indices_and_constraints()
