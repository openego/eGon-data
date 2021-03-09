import os.path
import socket

from importlib_resources import files

from egon.data.db import credentials
import egon.data.subprocess as subprocess

def initdb():
    """ Initialize the local database used for data processing. """
    db = credentials()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        code = s.connect_ex((db["HOST"], int(db["PORT"])))
    if code != 0:
        subprocess.run(
            ["docker-compose", "up", "-d", "--build"],
            cwd=files('egon.data.airflow'),
        )
