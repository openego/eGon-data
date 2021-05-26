import functools

from egon.data import db
from egon.data.datasets import Dataset


def setup():
    """ Initialize the local database used for data processing. """
    engine = db.engine()
    with engine.connect().execution_options(autocommit=True) as connection:
        for extension in ["hstore", "postgis", "postgis_raster"]:
            connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")


Setup = functools.partial(
    Dataset,
    name="Database Setup",
    version="0.0.0",
    dependencies=[],
    tasks=setup,
)
