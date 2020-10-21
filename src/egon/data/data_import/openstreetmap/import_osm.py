import subprocess
import os
from urllib.request import urlretrieve
import yaml
import egon.data


STYLEFILE = "oedb.style"
PBFFILEURL = "https://download.geofabrik.de/europe/germany/bremen-200101.osm.pbf"
PBFFILE = "bremen-200101.osm.pbf"
TABLE_PREFIX = "osm"


def download_osm_file(url=PBFFILEURL, file=PBFFILE):
    # file = os.path.join(DOWNLOADDIR, filename)

    if not os.path.isfile(file):
        urlretrieve(url, file)


def osm2postgres(num_processes=4, cache_size=4096):

    # Read database configuration from docker-compose.yml
    package_path = egon.data.__path__[0]
    docker_compose_file = os.path.join(package_path, "airflow", "docker-compose.yml")
    docker_compose = yaml.load(open(docker_compose_file), Loader=yaml.SafeLoader)
    docker_db_config = docker_compose['services']['egon-data-local-database']["environment"]
    docker_db_config_additional = docker_compose['services']['egon-data-local-database']["ports"][0].split(":")
    docker_db_config["HOST"] = docker_db_config_additional[0]
    docker_db_config["PORT"] = docker_db_config_additional[1]

    # Prepare osm2pgsql command
    cmd = ["osm2pgsql",
           "--create",
           "--slim",
           "--hstore-all",
           f"--number-processes {num_processes}",
           f"--cache {cache_size}",
           f"-H {docker_db_config['HOST']} -P {docker_db_config['PORT']} -d {docker_db_config['POSTGRES_DB']} -U {docker_db_config['POSTGRES_USER']}",
           f"-p {TABLE_PREFIX}",
           f"-S {STYLEFILE}",
           f"{PBFFILE}"
           ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(" ".join(cmd),
                   shell=True,
                   env={"PGPASSWORD": docker_db_config['POSTGRES_PASSWORD']},
                   cwd=os.path.dirname(__file__))

# TODO: Specify OSM related params (version, etc.) in yaml file
# TODO: call osm download from airflow
# TODO: call osm import (.py) from airflow
# TODO: add SQL import script and call it with airflow
# TODO: maybe downgrade Postgres version to the same as in data_processing


if __name__ == "__main__":

    # from egon.data.airflow.tasks import initdb
    # initdb()

    download_osm_file()
    osm2postgres()
