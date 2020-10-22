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
    package_path = egon.data.__path__[0]
    docker_compose_file = os.path.join(package_path, "airflow", "docker-compose.yml")
    docker_compose = yaml.load(open(docker_compose_file), Loader=yaml.SafeLoader)
    docker_db_config = docker_compose['services']['egon-data-local-database']["environment"]
    docker_db_config_additional = docker_compose['services']['egon-data-local-database']["ports"][0].split(":")
    docker_db_config["HOST"] = docker_db_config_additional[0]
    docker_db_config["PORT"] = docker_db_config_additional[1]

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
           f"-H {docker_db_config['HOST']} -P {docker_db_config['PORT']} -d {docker_db_config['POSTGRES_DB']} -U {docker_db_config['POSTGRES_USER']}",
           f"-p {osm_config['table_prefix']}",
           f"-S {osm_config['stylefile']}",
           f"{osm_config['file']}"
           ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(" ".join(cmd),
                   shell=True,
                   env={"PGPASSWORD": docker_db_config['POSTGRES_PASSWORD']},
                   cwd=os.path.dirname(__file__))


# TODO: Insert table names in SQL script read from data_sets.yml
# TODO: call osm download from airflow
# TODO: call osm import (.py) from airflow
# TODO: add SQL import script and call it with airflow
# TODO: write docstrings for added functions
# TODO: add module docstring
# TODO: maybe downgrade Postgres version to the same as in data_processing


if __name__ == "__main__":

    # from egon.data.airflow.tasks import initdb
    # initdb()

    download_osm_file()
    osm2postgres()
