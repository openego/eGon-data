"""
Validation framework for egon-data.

Supports two configuration styles (can be mixed):

1) "rule-first":
   validation_dict = {"task_name": [Rule(...), Rule(...)]}

2) "table-first":
   validation_dict = {"task_name": [TableValidation(...), ...]}
"""

from .resolver import (
    BoundaryDependent,
    resolve_boundary_dependence,
    resolve_value,
)
from .specs import (
    TableValidation,
    ValidationSpec,
    clone_rule,
    expand_specs,
    prepare_rules,
    resolve_rule_params,
)
from .airflow import (
    create_validation_tasks,
    run_validation_task,
)

__all__ = [
    # resolver
    "BoundaryDependent",
    "resolve_boundary_dependence",
    "resolve_value",
    # specs
    "TableValidation",
    "ValidationSpec",
    "clone_rule",
    "expand_specs",
    "prepare_rules",
    "resolve_rule_params",
    # airflow
    "create_validation_tasks",
    "run_validation_task",
]
