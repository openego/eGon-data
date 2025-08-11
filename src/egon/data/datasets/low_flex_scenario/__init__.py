"""The central module to create low flex scenarios

"""
from airflow.models import Connection
from airflow.operators.postgres_operator import PostgresOperator
from airflow.settings import Session
from importlib_resources import files
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets import Dataset

Base = declarative_base()


def ensure_postgres_connection():
    session = Session()

    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == "egon_data")
        .one_or_none()
    )

    conn.conn_type = "postgres"

    session.commit()
    session.close()


class LowFlexScenario(Dataset):
    def __init__(self, dependencies):
        ensure_postgres_connection()
        super().__init__(
            name="low_flex_scenario",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
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
            ),
        )
