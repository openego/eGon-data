"""Airflow integration for egon-validation."""

from typing import Any, Dict, List
from airflow.operators.python import PythonOperator
from egon_validation import run_validations, RunContext
from egon_validation.rules.base import Rule
import logging

logger = logging.getLogger(__name__)


def _resolve_context_value(value: Any, boundary: str, scenarios: List[str]) -> Any:
    """Resolve a value that may be context-dependent (boundary/scenario).

    Args:
        value: The value to resolve. Can be:
            - A dict with boundary keys: {"Schleswig-Holstein": 27, "Everything": 537}
            - A dict with scenario keys: {"eGon2035": 100, "eGon100RE": 200}
            - Any other value (returned as-is)
        boundary: Current dataset boundary setting
        scenarios: List of active scenarios

    Returns:
        Resolved value based on current context

    Examples:
        >>> _resolve_context_value({"Schleswig-Holstein": 27, "Everything": 537},
        ...                        "Schleswig-Holstein", ["eGon2035"])
        27

        >>> _resolve_context_value({"eGon2035": 100, "eGon100RE": 200},
        ...                        "Everything", ["eGon2035"])
        100

        >>> _resolve_context_value(42, "Everything", ["eGon2035"])
        42
    """
    # If not a dict, return as-is
    if not isinstance(value, dict):
        return value

    # Try to resolve by boundary
    if boundary in value:
        logger.debug(f"Resolved boundary-dependent value: {boundary} -> {value[boundary]}")
        return value[boundary]

    # Try to resolve by scenario
    for scenario in scenarios:
        if scenario in value:
            logger.debug(f"Resolved scenario-dependent value: {scenario} -> {value[scenario]}")
            return value[scenario]

    # If dict doesn't match boundary/scenario pattern, return as-is
    # This handles cases like column_types dicts which are not context-dependent
    return value


def _resolve_rule_params(rule: Rule, boundary: str, scenarios: List[str]) -> None:
    """Recursively resolve context-dependent parameters in a rule.

    Modifies rule.params in-place, resolving any dict values that match
    boundary or scenario patterns.

    Args:
        rule: The validation rule to process
        boundary: Current dataset boundary setting
        scenarios: List of active scenarios
    """
    if not hasattr(rule, 'params') or not isinstance(rule.params, dict):
        return

    # Recursively resolve all parameter values
    for param_name, param_value in rule.params.items():
        resolved_value = _resolve_context_value(param_value, boundary, scenarios)

        # If the value was resolved (changed), update it
        if resolved_value is not param_value:
            logger.info(
                f"Rule {rule.rule_id}: Resolved {param_name} for "
                f"boundary='{boundary}', scenarios={scenarios}"
            )
            rule.params[param_name] = resolved_value

def create_validation_tasks(
    validation_dict: Dict[str, List[Rule]],
    dataset_name: str,
    on_failure: str = "continue"
) -> List[PythonOperator]:
    """Convert validation dict to Airflow tasks.

    Automatically resolves context-dependent parameters in validation rules.
    Parameters can be specified as dicts with boundary or scenario keys:

    - Boundary-dependent: {"Schleswig-Holstein": 27, "Everything": 537}
    - Scenario-dependent: {"eGon2035": 100, "eGon100RE": 200}

    The appropriate value is selected based on the current configuration.

    Args:
        validation_dict: {"task_name": [Rule1(), Rule2()]}
        dataset_name: Name of dataset
        on_failure: "continue" or "fail"

    Returns:
        List of PythonOperator tasks

    Example:
        >>> validation_dict = {
        ...     "data_quality": [
        ...         RowCountValidation(
        ...             table="boundaries.vg250_krs",
        ...             rule_id="TEST_ROW_COUNT",
        ...             expected_count={"Schleswig-Holstein": 27, "Everything": 537}
        ...         )
        ...     ]
        ... }
        >>> tasks = create_validation_tasks(validation_dict, "VG250")
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
                from egon.data.config import settings

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

                # Get current configuration context
                config = settings()["egon-data"]
                boundary = config["--dataset-boundary"]
                scenarios = config.get("--scenarios", [])

                logger.info(f"Resolving validation parameters for boundary='{boundary}', scenarios={scenarios}")

                # Set task and dataset on all rules (required by Rule base class)
                # Also resolve context-dependent parameters
                for rule in rules:
                    if not hasattr(rule, 'task') or rule.task is None:
                        rule.task = task_name
                    if not hasattr(rule, 'dataset') or rule.dataset is None:
                        rule.dataset = dataset_name

                    # Automatically resolve boundary/scenario-dependent parameters
                    _resolve_rule_params(rule, boundary, scenarios)

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
