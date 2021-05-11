from airflow.operators.python_operator import PythonOperator

from egon.data import db
from egon.data.datasets import Dataset


def initdb():
    """ Initialize the local database used for data processing. """
    engine = db.engine()
    with engine.connect().execution_options(autocommit=True) as connection:
        for extension in ["hstore", "postgis", "postgis_raster"]:
            connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")


def dataset(): return Dataset(
    name="database-structure",
    version="0.0.0",
    dependencies=[],
    tasks=PythonOperator(task_id="initdb", python_callable=initdb),
)
