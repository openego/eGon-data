"""Airflow integration for egon-validation."""

from typing import Dict, List
from airflow.operators.python import PythonOperator
from egon_validation import run_validations, RunContext
from egon_validation.rules.base import Rule
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
                import os
                import time
                from datetime import datetime
                from egon.data import db as egon_db

                # Use same run_id as validation report for consistency
                # This allows the validation report to collect results from all validation tasks
                run_id = (
                    os.environ.get('AIRFLOW_CTX_DAG_RUN_ID') or
                    context.get('run_id') or
                    (context.get('ti') and hasattr(context['ti'], 'dag_run') and context['ti'].dag_run.run_id) or
                    (context.get('dag_run') and context['dag_run'].run_id) or
                    f"airflow-{dataset_name}-{task_name}-{int(time.time())}"
                )

                # Use absolute path to ensure consistent location regardless of working directory
                # Priority: EGON_VALIDATION_DIR env var > current working directory
                out_dir = os.path.join(
                    os.environ.get('EGON_VALIDATION_DIR', os.getcwd()),
                    "validation_runs"
                )

                # Include execution timestamp in task name so retries write to separate directories
                # The validation report will filter to keep only the most recent execution per task
                execution_date = context.get('execution_date') or datetime.now()
                timestamp = execution_date.strftime('%Y%m%dT%H%M%S')
                full_task_name = f"{dataset_name}.{task_name}.{timestamp}"

                logger.info(f"Validation: {full_task_name} (run_id: {run_id})")

                # Use existing engine from egon.data.db
                engine = egon_db.engine()

                # Set task and dataset on all rules (required by Rule base class)
                for rule in rules:
                    if not hasattr(rule, 'task') or rule.task is None:
                        rule.task = task_name
                    if not hasattr(rule, 'dataset') or rule.dataset is None:
                        rule.dataset = dataset_name

                ctx = RunContext(run_id=run_id, source="airflow", out_dir=out_dir)
                results = run_validations(engine, ctx, rules, full_task_name)

                total = len(results)
                failed = sum(1 for r in results if not r.success)

                logger.info(f"Complete: {total - failed}/{total} passed")

                if failed > 0 and on_failure == "fail":
                    raise Exception(f"{failed}/{total} validations failed")

                return {"total": total, "passed": total - failed, "failed": failed}

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
