"""The central module to create low flex scenarios

"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from importlib_resources import files
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets import Dataset

from egon_validation import ArrayCardinalityValidation

Base = declarative_base()


class LowFlexScenario(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="low_flex_scenario",
            version="0.0.2",
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
                "data-quality":[
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_bus_timeseries",
                        rule_id="ARRAY.egon_etrago_bus_timeseries",
                        array_column="v_mag_pu_set",
                        expected_length=8760,
                    ),
                ]
            },
            on_validaiton_failure="continue"
        )
