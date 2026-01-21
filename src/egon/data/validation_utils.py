"""Airflow integration for egon-validation.

This module supports two configuration styles:

1) Backwards compatible "rule-first":
   validation_dict = {"task": [Rule(...), Rule(...)]}

2) New "table-first":
   validation_dict = {"task": [TableValidation(...), TableValidation(...)]}

Both styles can be mixed in the same list.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from airflow.operators.python import PythonOperator
from egon_validation import run_validations, RunContext
from egon_validation.rules.base import Rule
import logging

from egon_validation import (  # noqa: F401
    DataTypeValidation,
    NotNullAndNotNaNValidation,
    RowCountValidation,
    SRIDUniqueNonZero,
    ValueSetValidation,
    WholeTableNotNullAndNotNaNValidation,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TableValidation:
    """
    Table-first validation specification.

    Properties you asked for:
      - table_name
      - row_count
      - geometry_columns
      - data_type_columns
      - not_null_columns
      - value_set_columns

    Behavior:
      - Generates rule_ids exactly like your manual convention:
          ROW_COUNT.<table_suffix>
          DATA_TYPES.<table_suffix>
          NOT_NAN.<table_suffix>
          TABLE_NOT_NAN.<table_suffix>        <-- always added automatically
          SRIDUniqueNonZero.<table_suffix>.<geom_col>
          VALUE_SET_<COL>.<table_suffix>
      - Boundary-dependent dict values are preserved and resolved later in _resolve_rule_params().
    """

    table_name: str

    row_count: Optional[Any] = None
    geometry_columns: Optional[Sequence[str]] = None
    data_type_columns: Optional[Mapping[str, Any]] = None
    not_null_columns: Optional[Sequence[str]] = None
    value_set_columns: Optional[Mapping[str, Any]] = None

    def to_rules(self) -> List[Rule]:
        rules: List[Rule] = []
        table_suffix = self.table_name.split(".")[-1]

        # 1) Row count
        if self.row_count is not None:
            rules.append(
                RowCountValidation(
                    table=self.table_name,
                    rule_id=f"ROW_COUNT.{table_suffix}",
                    expected_count=self.row_count,
                )
            )

        # 2) Data types
        if self.data_type_columns is not None:
            rules.append(
                DataTypeValidation(
                    table=self.table_name,
                    rule_id=f"DATA_TYPES.{table_suffix}",
                    column_types=dict(self.data_type_columns),
                )
            )

        # 3) Column-level not-null / not-NaN
        if self.not_null_columns:
            rules.append(
                NotNullAndNotNaNValidation(
                    table=self.table_name,
                    rule_id=f"NOT_NAN.{table_suffix}",
                    columns=list(self.not_null_columns),
                )
            )

        # 4) Geometry checks (one rule per geometry column)
        if self.geometry_columns:
            for geom_col in self.geometry_columns:
                rules.append(
                    SRIDUniqueNonZero(
                        table=self.table_name,
                        rule_id=f"SRIDUniqueNonZero.{table_suffix}.{geom_col}",
                        column=geom_col,
                    )
                )

        # 5) Value sets (one rule per column)
        if self.value_set_columns:
            for col_name, expected_values in self.value_set_columns.items():
                rules.append(
                    ValueSetValidation(
                        table=self.table_name,
                        rule_id=f"VALUE_SET_{str(col_name).upper()}.{table_suffix}",
                        column=str(col_name),
                        expected_values=expected_values,
                    )
                )

        # 6) Whole-table not-null / not-NaN (automatic, as requested)
        rules.append(
            WholeTableNotNullAndNotNaNValidation(
                table=self.table_name,
                rule_id=f"TABLE_NOT_NAN.{table_suffix}",
            )
        )

        return rules


ValidationSpec = Union[Rule, TableValidation]


def _expand_specs(specs: Sequence[ValidationSpec]) -> List[Rule]:
    """Turn a mixed list of Rule/TableValidation into a flat list of Rule."""
    expanded: List[Rule] = []
    for spec in specs:
        if isinstance(spec, TableValidation):
            expanded.extend(spec.to_rules())
        else:
            expanded.append(spec)
    return expanded


def _resolve_context_value(value: Any, boundary: str) -> Any:
    """Resolve a value that may be boundary-dependent.

    Args:
        value: The value to resolve. Can be:
            - A dict with boundary keys: {"Schleswig-Holstein": 27, "Everything": 537}
            - Any other value (returned as-is)
        boundary: Current dataset boundary setting

    Returns:
        Resolved value based on current boundary

    Examples:
        >>> _resolve_context_value({"Schleswig-Holstein": 27, "Everything": 537},
        ...                        "Schleswig-Holstein")
        27

        >>> _resolve_context_value(42, "Everything")
        42
    """
    # If not a dict, return as-is
    if not isinstance(value, dict):
        return value

    # Try to resolve by boundary
    if boundary in value:
        logger.debug(f"Resolved boundary-dependent value: {boundary} -> {value[boundary]}")
        return value[boundary]

    # If dict doesn't match boundary pattern, return as-is
    # This handles cases like column_types dicts which are not context-dependent
    return value


def _resolve_rule_params(rule: Rule, boundary: str) -> None:
    """Resolve boundary-dependent parameters in a rule.

    Modifies rule.params in-place, resolving any dict values that match
    boundary patterns.

    Args:
        rule: The validation rule to process
        boundary: Current dataset boundary setting
    """
    if not hasattr(rule, 'params') or not isinstance(rule.params, dict):
        return

    # Resolve all parameter values
    for param_name, param_value in rule.params.items():
        resolved_value = _resolve_context_value(param_value, boundary)

        # If the value was resolved (changed), update it
        if resolved_value is not param_value:
            logger.info(
                f"Rule {rule.rule_id}: Resolved {param_name} for "
                f"boundary='{boundary}'"
            )
            rule.params[param_name] = resolved_value

def create_validation_tasks(
    validation_dict: Dict[str, Sequence[ValidationSpec]],
    dataset_name: str,
    on_failure: str = "continue"
) -> List[PythonOperator]:
    """Convert validation dict to Airflow tasks.

    Values can be List[Rule], values can be List[TableValidation] or mixed.
    """
    if not validation_dict:
        return []

    tasks: List[PythonOperator] = []

    for task_name, specs in validation_dict.items():

        def make_callable(specs: Sequence[ValidationSpec], task_name: str):
            def run_validation(**context):
                import os
                import time
                from datetime import datetime
                from egon.data import db as egon_db
                from egon.data.config import settings

                # Run id selection (unchanged logic)
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

                logger.info(f"Resolving validation parameters for boundary='{boundary}'")

                rules: List[Rule] = copy.deepcopy(_expand_specs(specs))

                # Set task and dataset on all rules (required by Rule base class)
                # Also resolve boundary-dependent parameters
                for rule in rules:
                    if not hasattr(rule, 'task') or rule.task is None:
                        rule.task = task_name
                    if not hasattr(rule, 'dataset') or rule.dataset is None:
                        rule.dataset = dataset_name

                    # Automatically resolve boundary-dependent parameters
                    _resolve_rule_params(rule, boundary)

                ctx = RunContext(run_id=run_id, source="airflow", out_dir=out_dir)
                results = run_validations(engine, ctx, rules, full_task_name)

                total = len(results)
                failed = sum(1 for r in results if not r.success)

                logger.info(f"Complete: {total - failed}/{total} passed")

                if failed > 0 and on_failure == "fail":
                    raise Exception(f"{failed}/{total} validations failed")

                return {"total": total, "passed": total - failed, "failed": failed}

            return run_validation

        func = make_callable(specs, task_name)
        func.__name__ = f"validate_{task_name}"

        operator = PythonOperator(
            task_id=f"{dataset_name}.validate.{task_name}",
            python_callable=func,
            provide_context=True,
        )

        tasks.append(operator)

    return tasks
