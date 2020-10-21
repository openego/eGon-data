import subprocess
import os
from urllib.request import urlretrieve


NUM_PROCESSES = 4
CACHE_SIZE = 4096
HOST = "localhost"
POSTGRES_DB = "egon-data"
POSTGRES_USER = "egon"
POSTGRES_PASSWORD = "data"
PORT = "54321"
STYLEFILE = "oedb.style"
PBFFILEURL = "https://download.geofabrik.de/europe/germany/bremen-200101.osm.pbf"
PBFFILE = "bremen-200101.osm.pbf"
TABLE_PREFIX = "osm"


def download_osm_file(url=PBFFILEURL, file=PBFFILE):
    # file = os.path.join(DOWNLOADDIR, filename)

    if not os.path.isfile(file):
        urlretrieve(url, file)


def osm2postgres():

    cmd = ["osm2pgsql",
           "--create",
           "--slim",
           "--hstore-all",
           f"--number-processes {NUM_PROCESSES}",
           f"--cache {CACHE_SIZE}",
           f"-H {HOST} -P {PORT} -d {POSTGRES_DB} -U {POSTGRES_USER}",
           f"-p {TABLE_PREFIX}",
           f"-S {STYLEFILE}",
           f"{PBFFILE}"
           ]

    subprocess.run(" ".join(cmd),
                   shell=True,
                   env={"PGPASSWORD": POSTGRES_PASSWORD},
                   cwd=os.path.dirname(__file__))

# TODO: read database config params from docker-compose.yml
# TODO: maybe downgrade Postgres version to the same as in data_processing
# TODO: Decide which of the parameters specified at top of file shall be passed as arguments when function is called from airflow


if __name__ == "__main__":

    # from egon.data.airflow.tasks import initdb
    # initdb()

    download_osm_file()
    osm2postgres()
