import os.path
import subprocess


def initdb():
    """ Initialize the local database used for data processing. """
    subprocess.run(
        ["docker-compose", "up", "-d", "--build"],
        cwd=os.path.dirname(__file__),
    )
