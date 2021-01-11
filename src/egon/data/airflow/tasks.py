import os.path
import socket
import subprocess

from egon.data.db import credentials


def initdb():
    """ Initialize the local database used for data processing. """
    db = credentials()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    code = s.connect_ex((db["HOST"], int(db["PORT"])))
    s.close()
    if code != 0:
        subprocess.run(
            ["docker-compose", "up", "-d", "--build"],
            cwd=os.path.dirname(__file__),
        )
