"""The central module to create low flex scenarios"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from egon_validation import ArrayCardinalityValidation
from importlib_resources import files
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets import Dataset, DatasetSources, DatasetTargets

Base = declarative_base()


class LowFlexScenario(Dataset):

    sources = DatasetSources(files={"low_flex_sql": "low_flex_eGon2035.sql"})

    targets = DatasetTargets()

    def __init__(self, dependencies):
        super().__init__(
            name="low_flex_scenario",
            version="0.0.4",
            dependencies=dependencies,
            tasks=(
                {
                    SQLExecuteQueryOperator(
                        task_id="low_flex_eGon2035",
                        sql=files(__name__)
                        .joinpath("low_flex_eGon2035.sql")
                        .read_text(encoding="utf-8"),
                        conn_id="egon_data",
                        autocommit=True,
                    ),
                },
            ),
            validation={
                "data-quality": [
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_bus_timeseries",
                        rule_id="ARRAY.egon_etrago_bus_timeseries",
                        array_column="v_mag_pu_set",
                        expected_length=8760,
                    ),
                ]
            },
            proceed_on_validation_failure=True,
        )
