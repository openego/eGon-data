"""The central module to create low flex scenarios

"""
from airflow.operators.postgres_operator import PostgresOperator
from sqlalchemy.ext.declarative import declarative_base
from importlib_resources import files

from egon.data.datasets import Dataset


Base = declarative_base()


class LowFlexScenario(Dataset):
    def __init__(self, dependencies, name = "low_flex_scenario", version = "0.0.1"):
        if tasks is None
            tasks = (
                {
                    PostgresOperator(
                        task_id="low_flex_eGon2035",
                        sql=files(__name__)
                        .joinpath("low_flex_eGon2035.sql")
                        .read_text(encoding="utf-8"),
                        postgres_conn_id="egon_data",
                        autocommit=True,
                    ),
                },
            )
        super().__init__(
            name=name,
            version=version,
            dependencies=dependencies,
            tasks=tasks,
        )
