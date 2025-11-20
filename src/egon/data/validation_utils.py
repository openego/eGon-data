"""Airflow integration for egon-validation."""

from typing import Dict, List
from airflow.operators.python import PythonOperator
from egon_validation import run_validations, RunContext
from egon_validation.rules.base import Rule
from egon_validation.config import get_env, build_db_url
from egon_validation import db
import logging

logger = logging.getLogger(__name__)


def create_validation_tasks(
    validation_dict: Dict[str, List[Rule]],
    dataset_name: str,
    on_failure: str = "continue"
) -> List[PythonOperator]:
    """Convert validation dict to Airflow tasks.

    Args:
        validation_dict: {"task_name": [Rule1(), Rule2()]}
        dataset_name: Name of dataset
        on_failure: "continue" or "fail"

    Returns:
        List of PythonOperator tasks
    """
    if not validation_dict:
        return []

    tasks = []

    for task_name, rules in validation_dict.items():
        def make_callable(rules, task_name):
            def run_validation(**context):
                from datetime import datetime

                execution_date = context.get("execution_date", datetime.now())
                run_id = f"airflow-{dataset_name}-{task_name}-{execution_date.strftime('%Y%m%dT%H%M%S')}"

                logger.info(f"Validation: {dataset_name}.{task_name}")

                db_url = get_env("EGON_DB_URL") or build_db_url()
                engine = db.make_engine(db_url)

                try:
                    ctx = RunContext(run_id=run_id, source="airflow")
                    results = run_validations(engine, ctx, rules, task_name)

                    total = len(results)
                    failed = sum(1 for r in results if not r.success)

                    logger.info(f"Complete: {total - failed}/{total} passed")

                    if failed > 0 and on_failure == "fail":
                        raise Exception(f"{failed}/{total} validations failed")

                    return {"total": total, "passed": total - failed, "failed": failed}
                finally:
                    engine.dispose()

            return run_validation

        func = make_callable(rules, task_name)
        func.__name__ = f"validate_{task_name}"

        operator = PythonOperator(
            task_id=f"{dataset_name}.validate.{task_name}",
            python_callable=func,
            provide_context=True,
        )

        tasks.append(operator)

    return tasks
