"""Validation specifications and expansion logic."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence, Union
import copy
import logging

from egon_validation import (
    DataTypeValidation,
    NotNullAndNotNaNValidation,
    RowCountValidation,
    SRIDUniqueNonZero,
    ValueSetValidation,
    WholeTableNotNullAndNotNaNValidation,
)
from egon_validation.rules.base import Rule

from .resolver import BoundaryDependent, resolve_value

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TableValidation:
    """
    A compact, table-first spec that expands into Rule objects at runtime.

    Properties:
      - table_name
      - row_count
      - geometry_columns
      - data_type_columns
      - not_null_columns
      - value_set_columns

    Behavior:
      - Adds WholeTableNotNullAndNotNaNValidation automatically.
      - Generates rule_id strings matching your prior manual convention.
    """

    table_name: str
    row_count: Optional[Any] = None
    geometry_columns: Optional[Sequence[str]] = None
    data_type_columns: Optional[
        Union[Mapping[str, Any], BoundaryDependent]
    ] = None
    not_null_columns: Optional[Sequence[str]] = None
    value_set_columns: Optional[
        Union[Mapping[str, Any], BoundaryDependent]
    ] = None

    def to_rules(self, boundary: str) -> List[Rule]:
        rules: List[Rule] = []
        table_suffix = self.table_name.split(".")[-1]

        # Resolve all boundary-dependent fields up-front
        row_count = (
            resolve_value(self.row_count, boundary)
            if self.row_count is not None
            else None
        )
        data_type_columns = (
            resolve_value(self.data_type_columns, boundary)
            if self.data_type_columns is not None
            else None
        )
        value_set_columns = (
            resolve_value(self.value_set_columns, boundary)
            if self.value_set_columns is not None
            else None
        )

        if row_count is not None:
            rules.append(
                RowCountValidation(
                    table=self.table_name,
                    rule_id=f"ROW_COUNT.{table_suffix}",
                    expected_count=row_count,
                )
            )

        if data_type_columns is not None:
            rules.append(
                DataTypeValidation(
                    table=self.table_name,
                    rule_id=f"DATA_TYPES.{table_suffix}",
                    column_types=dict(data_type_columns),
                )
            )

        if self.not_null_columns:
            rules.append(
                NotNullAndNotNaNValidation(
                    table=self.table_name,
                    rule_id=f"NOT_NAN.{table_suffix}",
                    columns=list(self.not_null_columns),
                )
            )

        if self.geometry_columns:
            for geom_col in self.geometry_columns:
                rules.append(
                    SRIDUniqueNonZero(
                        table=self.table_name,
                        rule_id=f"SRIDUniqueNonZero.{table_suffix}.{geom_col}",
                        geom=geom_col,
                    )
                )

        if value_set_columns:
            for col_name, expected_values in value_set_columns.items():
                rules.append(
                    ValueSetValidation(
                        table=self.table_name,
                        rule_id=f"VALUE_SET_{str(col_name).upper()}"
                        f".{table_suffix}",
                        column=str(col_name),
                        expected_values=expected_values,
                    )
                )

        # Always add the whole-table rule automatically
        rules.append(
            WholeTableNotNullAndNotNaNValidation(
                table=self.table_name,
                rule_id=f"TABLE_NOT_NAN.{table_suffix}",
            )
        )

        return rules


ValidationSpec = Union[Rule, TableValidation]


def clone_rule(rule: Rule) -> Rule:
    """
    Creates a per-run copy of a rule to avoid mutating DAG-parse-time objects.

    We avoid deepcopy as first choice (can break on complex objects).
    Strategy:
      1) Shallow copy the object
      2) Deep copy ONLY rule.params (the part we mutate)
      3) Fallback to deepcopy(rule) if shallow copy fails
    """
    try:
        # shallow copy: new object, same inner references
        cloned = copy.copy(rule)
    except Exception:
        # Last resort: full deepcopy
        return copy.deepcopy(rule)

    # Make params safe to mutate
    params = getattr(cloned, "params", None)
    if hasattr(cloned, "params") and isinstance(params, dict):
        cloned.params = copy.deepcopy(cloned.params)

    return cloned


def expand_specs(specs: Sequence[ValidationSpec]) -> List[Rule]:
    """
    Turn a mixed list of Rule/TableValidation into a plain list of Rules.

    TableValidation produces fresh rule instances.
    Rule instances are cloned to avoid cross-run mutation.
    """
    rules: List[Rule] = []

    for spec in specs:
        if isinstance(spec, TableValidation):
            rules.extend(spec.to_rules())
        else:
            rules.append(clone_rule(spec))

    return rules


def resolve_rule_params(rule: Rule, boundary: str) -> None:
    """
    Mutates rule.params on THIS rule instance only.
    We ensure these rule instances are runtime clones/fresh instances.
    """
    params = getattr(rule, "params", None)
    if not isinstance(params, dict):
        return

    for name, val in list(params.items()):
        resolved = resolve_value(val, boundary)
        if resolved is not val:
            rule_id = getattr(rule, "rule_id", "<no-id>")
            logger.info(
                "Rule %s: Resolved %s for boundary='%s'",
                rule_id,
                name,
                boundary,
            )
            params[name] = resolved


def prepare_rules(
    specs: Sequence[ValidationSpec],
    boundary: str,
    dataset_name: str,
    task_name: str,
) -> List[Rule]:
    """
    Build rules for this run:
      - expand specs
      - inject dataset/task if missing
      - resolve boundary-dependent params
    """
    rules: List[Rule] = []

    for spec in specs:
        if isinstance(spec, TableValidation):
            rules.extend(spec.to_rules(boundary))
        else:
            rules.append(clone_rule(spec))

    for rule in rules:
        if getattr(rule, "task", None) is None:
            rule.task = task_name
        if getattr(rule, "dataset", None) is None:
            rule.dataset = dataset_name

        resolve_rule_params(rule, boundary)

    return rules
