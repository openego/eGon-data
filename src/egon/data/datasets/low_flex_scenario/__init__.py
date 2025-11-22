"""The central module to create low flex scenarios

"""

from airflow.operators.postgres_operator import PostgresOperator
from importlib_resources import files
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets import Dataset, DatasetSources, DatasetTargets

Base = declarative_base()


class LowFlexScenario(Dataset):
    
    sources = DatasetSources(
        files={
            "low_flex_sql": "low_flex_eGon2035.sql"
        }
    )

    targets = DatasetTargets()
    
    
    def __init__(self, dependencies):
        super().__init__(
            name="low_flex_scenario",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(
                {
                    PostgresOperator(
                        task_id="low_flex_eGon2035",
                        sql=files(__name__)
                        .joinpath(LowFlexScenario.sources.files["low_flex_sql"])
                        .read_text(encoding="utf-8"),
                        postgres_conn_id="egon_data",
                        autocommit=True,
                    ),
                },
            ),
        )   