import os.path
import subprocess

import egon.data


def initdb():
    """ Initialize the local database used for data processing. """
    subprocess.run(
        ["docker-compose", "up", "-d", "--build"],
        cwd=os.path.dirname(__file__),
    )
    subprocess.run(["alembic", "upgrade", "head"], cwd=egon.data.__path__[0])
