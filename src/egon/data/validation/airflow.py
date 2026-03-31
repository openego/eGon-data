"""Airflow integration for validation tasks."""

from __future__ import annotations

import logging
from functools import partial
import re
from typing import Any, Dict, List, Sequence

from airflow.operators.python import PythonOperator
from egon_validation import RunContext, run_validations

from .specs import ValidationSpec, prepare_rules

logger = logging.getLogger(__name__)


def run_validation_task(
    *,
    specs: Sequence[ValidationSpec],
    task_name: str,
    dataset_name: str,
    proceed_on_validation_failure: bool,
    **context: Any,
) -> Dict[str, int]:
    """
    This is the function Airflow actually calls.

    It's top-level (not nested), so:
      - easier to test
      - easier stack traces
      - fewer closure surprises
    """
    import os
    import time
    from datetime import datetime
    from egon.data import db as egon_db
    from egon.data.config import settings

    # Consistent run_id across tasks so reports can correlate results
    run_id = (
        os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        or context.get("run_id")
        or (
            context.get("ti")
            and hasattr(context["ti"], "dag_run")
            and context["ti"].dag_run.run_id
        )
        or (context.get("dag_run") and context["dag_run"].run_id)
        or f"airflow-{dataset_name}-{task_name}-{int(time.time())}"
    )

    out_dir = os.path.join(
        os.environ.get("EGON_VALIDATION_DIR", os.getcwd()),
        "validation_runs",
    )

    execution_date = context.get("execution_date") or datetime.now()
    timestamp = execution_date.strftime("%Y%m%dT%H%M%S")
    full_task_name = f"{dataset_name}.{task_name}.{timestamp}"

    logger.info("Validation: %s (run_id: %s)", full_task_name, run_id)

    engine = egon_db.engine()

    config = settings()["egon-data"]
    boundary = config["--dataset-boundary"]
    logger.info("Resolving validation parameters for boundary='%s'", boundary)

    rules = prepare_rules(
        specs=specs,
        boundary=boundary,
        dataset_name=dataset_name,
        task_name=task_name,
    )

    ctx = RunContext(run_id=run_id, source="airflow", out_dir=out_dir)
    results = run_validations(engine, ctx, rules, full_task_name)

    total = len(results)
    failed = sum(1 for r in results if not r.success)

    # Log individual rule results
    for r in results:
        status = "PASSED" if r.success else "FAILED"
        rule_id = getattr(r, "rule_id", "unknown")
        message = getattr(r, "message", "")
        if r.success:
            logger.info("Rule %s: %s", rule_id, status)
        else:
            logger.warning("Rule %s: %s - %s", rule_id, status, message)

    logger.info("Complete: %s/%s passed", total - failed, total)

    if failed > 0 and not proceed_on_validation_failure:
        raise Exception(f"{failed}/{total} validations failed")

    return {"total": total, "passed": total - failed, "failed": failed}


def create_validation_tasks(
    validation_dict: Dict[str, Sequence[ValidationSpec]],
    dataset_name: str,
    proceed_on_validation_failure: bool = False,
) -> List[PythonOperator]:
    """
    Creates one PythonOperator per task_name in validation_dict.

      - values can still be List[Rule]
      - values can be List[TableValidation]

    Mixed lists also work.
    """
    if not validation_dict:
        return []

    tasks: List[PythonOperator] = []

    safe_dataset = sanitize_airflow_key(dataset_name)

    for task_name, specs in validation_dict.items():
        callable_for_airflow = partial(
            run_validation_task,
            specs=specs,
            task_name=task_name,
            dataset_name=dataset_name,
            proceed_on_validation_failure=proceed_on_validation_failure,
        )

        tasks.append(
            PythonOperator(
                task_id=f"{safe_dataset}.validate.{task_name}",
                python_callable=callable_for_airflow,
                provide_context=True,
            )
        )

    return tasks


def sanitize_airflow_key(value: str) -> str:
    """
    Airflow task_id/key must match: [A-Za-z0-9_.-]+
    Replace everything else with underscores.
    """
    # 1) strip outer whitespace
    v = value.strip()

    # 2) replace any run of invalid characters (including spaces) with "_"
    v = re.sub(r"[^A-Za-z0-9_.-]+", "_", v)

    # 3) collapse multiple underscores
    v = re.sub(r"_+", "_", v)

    # 4) avoid leading/trailing separators that can look ugly / confusing
    v = v.strip("._-")

    # 5) don't return empty
    return v or "unnamed"
