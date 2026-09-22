**********
Validation
**********

The eGon-data pipeline integrates with the egon-validation framework to ensure data quality
and consistency. Validations can be added to any Dataset and run automatically as part of
the pipeline.

Overview
========

Validation in eGon-data supports two main approaches:

1. **TableValidation** - a declarative spec for common checks (row counts, data
   types, NULL/NaN, value sets, geometries) that expands into several rules
2. **Rule instances** - ``Rule`` subclasses, either the generic ones from
   ``egon_validation`` or a custom class for complex business logic

Both produce structured results with pass/fail status, observed vs expected
values and detailed messages.

Adding Validation to a Dataset
==============================

Validations are declared in the ``validation`` parameter of a Dataset. The
keys of the dict become validation task names, the values are lists mixing
``TableValidation`` specs and rule instances freely:

.. code-block:: python

    from egon.data.datasets import Dataset
    from egon.data.validation import TableValidation
    from egon.data.validation.rules.custom.sanity import CH4StoresCapacity

    class MyDataset(Dataset):
        def __init__(self, dependencies):
            super().__init__(
                name="MyDataset",
                version="1.0.0",
                dependencies=dependencies,
                tasks=(task1, task2),
                validation={
                    "data_quality": [
                        TableValidation(
                            table_name="schema.my_table",
                            row_count=1,
                            not_null_columns=["id", "value"],
                        ),
                        CH4StoresCapacity(
                            table="grid.egon_etrago_store",
                            rule_id="SANITY_MY_CHECK",
                            scenario="eGon2035",
                        ),
                    ],
                },
                proceed_on_validation_failure=True,
            )

Rule instances go straight into the list; there is no wrapper class. Each key
produces one Airflow task named
``{dataset}.validate.{key}``, which runs after the dataset's own tasks.

.. note::

   Task names are used as directory names for the results, and rule ids as
   directory names below them, so a ``rule_id`` must be unique within its
   task. When several rules of the same class check different columns, suffix
   the id with the column -- as ``TableValidation`` itself does for
   geometries (``SRIDUniqueNonZero.<table>.<column>``).


TableValidation
===============

``TableValidation`` provides declarative validation for common data quality checks without
writing custom code.

Available Checks
----------------

Each field that is set adds one rule; ``rule_id``\ s are generated, so they
must not be passed. Every ``TableValidation`` additionally always emits a
``WholeTableNotNullAndNotNaNValidation``.

.. list-table::
   :header-rows: 1
   :widths: 22 26 52

   * - Parameter
     - Type
     - Adds
   * - ``table_name``
     - str
     - Target table as ``"schema.table"`` (required)
   * - ``row_count``
     - int, or ``BoundaryDependent``
     - ``RowCountValidation`` -- an **exact** expected count,
       ``ROW_COUNT.<table>``
   * - ``data_type_columns``
     - Dict[str, str]
     - ``DataTypeValidation`` with the expected PostgreSQL type per column,
       e.g. ``{"bus_id": "bigint", "v_nom": "double precision"}``;
       ``DATA_TYPES.<table>``
   * - ``not_null_columns``
     - Sequence[str]
     - ``NotNullAndNotNaNValidation`` for the listed columns,
       ``NOT_NAN.<table>``
   * - ``geometry_columns``
     - Sequence[str]
     - one ``SRIDUniqueNonZero`` per column,
       ``SRIDUniqueNonZero.<table>.<column>``
   * - ``value_set_columns``
     - Dict[str, list]
     - one ``ValueSetValidation`` per column, asserting the column contains
       only the listed values; ``VALUE_SET_<COLUMN>.<table>``
   * - *(always)*
     - --
     - ``WholeTableNotNullAndNotNaNValidation``, ``TABLE_NOT_NAN.<table>``

There is no ``unique`` or ``foreign_keys`` support. ``row_count`` is a single
expected value, not a ``{"min": ...}`` mapping.

Example
-------

.. code-block:: python

    from egon.data.validation import (
        TableValidation,
        resolve_boundary_dependence,
    )

    TableValidation(
        table_name="grid.egon_etrago_bus",
        row_count=resolve_boundary_dependence(
            {"Schleswig-Holstein": 6178, "Everything": 85710}
        ),
        geometry_columns=["geom"],
        data_type_columns={
            "bus_id": "bigint",
            "v_nom": "double precision",
        },
        not_null_columns=["bus_id", "scn_name", "carrier"],
        value_set_columns={"country": ["DE", "AT", "CH"]},
    )

This expands into six rules -- ``ROW_COUNT``, ``DATA_TYPES``, ``NOT_NAN``,
``SRIDUniqueNonZero.egon_etrago_bus.geom``,
``VALUE_SET_COUNTRY.egon_etrago_bus`` and the automatic
``TABLE_NOT_NAN.egon_etrago_bus``.

Boundary-dependent values
-------------------------

``row_count``, ``data_type_columns`` and ``value_set_columns`` may be wrapped
in :func:`resolve_boundary_dependence`, which defers the choice to task
runtime, when ``--dataset-boundary`` is known:

.. code-block:: python

    row_count=resolve_boundary_dependence(
        {"Schleswig-Holstein": 27, "Everything": 431}
    )

Both keys are required -- an unlisted boundary raises ``KeyError`` rather than
falling back.


Writing Custom Validation Rules
===============================

Custom rules inherit from base classes in ``egon_validation.rules.base``:

- ``DataFrameRule`` - For validations that need to process query results as a DataFrame
- ``SqlRule`` - For validations that process a single row result
- ``Rule`` - Base class for fully custom validation logic

DataFrameRule Example
---------------------

.. code-block:: python

    from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

    class MyValidationRule(DataFrameRule):
        """Validate something important."""

        def __init__(
            self,
            table: str,
            rule_id: str,
            scenario: str = "eGon2035",
            rtol: float = 0.10,
            **kwargs,
        ):
            super().__init__(
                rule_id=rule_id,
                table=table,
                scenario=scenario,
                rtol=rtol,
                **kwargs,
            )
            self.kind = "sanity"
            self.scenario = scenario

        def get_query(self, ctx):
            """Return SQL query with parameter placeholders."""
            return """
            SELECT COUNT(*) as count, SUM(value) as total
            FROM grid.egon_etrago_bus
            WHERE scn_name = :scenario
            AND carrier = :carrier
            """

        def get_params(self, ctx):
            """Return parameters for the query."""
            return {
                "scenario": self.scenario,
                "carrier": "CH4",
            }

        def evaluate_df(self, df, ctx):
            """Evaluate the query results."""
            observed = float(df["total"].values[0])
            expected = 1000.0
            rtol = self.params.get("rtol", 0.10)

            deviation = abs(observed - expected) / expected
            success = deviation <= rtol

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=success,
                observed=observed,
                expected=expected,
                message=f"Deviation: {deviation*100:.2f}%",
                severity=Severity.INFO if success else Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


Best Practices
==============

1. **Use descriptive rule_ids** - Follow pattern ``SANITY_{CATEGORY}_{CHECK_NAME}``

2. **Set appropriate tolerances** - Document why you chose specific ``rtol`` values

3. **Provide clear messages** - Include context in success/failure messages

4. **Return observed/expected values** - Helps with debugging failures

5. **Override** ``kind = "sanity"`` - Ensures rules are categorized correctly

6. **Use parameterized queries** - Prevent SQL injection by using ``:param`` placeholders:

   .. code-block:: python

       def get_query(self, ctx):
           return """
           SELECT COUNT(*) as count
           FROM grid.egon_etrago_bus
           WHERE scn_name = :scenario
           AND carrier = :carrier
           """

       def get_params(self, ctx):
           return {"scenario": self.scenario, "carrier": self.carrier}

   Note: Table/schema names from config cannot be parameterized (SQL identifiers),
   but all values should use parameters.


Running Validations
===================

Validations run automatically when a dataset's tasks complete. Results are stored in:

.. code-block:: text

    validation_runs/{run_id}/tasks/{dataset}.validate.{category}/{rule_id}/results.jsonl


Validation Reports
==================

After pipeline execution, validation results are aggregated into an HTML report:

.. code-block:: text

    validation_runs/{run_id}/final/report.html

The report includes:

- Summary of all validation results (pass/fail counts)
- Detailed results per rule with observed vs expected values
- Filtering by severity, category, and status


Running after a failed pipeline
-------------------------------

``FinalValidations`` and ``ValidationReport`` declare
``trigger_rule = "all_done"``, so they still run when an upstream data task
failed. The report is built from whatever results are on disk; if
there are none, this is logged as a warning rather than failing the task.

Both datasets also carry a ``.dev`` suffix in their version
(``0.0.1.dev``). :meth:`Dataset.check_version` normally skips a dataset that
has already run for the configured ``--scenarios``, and a version ending in
``.dev`` is exempt from that check. This is needed because a validation task
succeeds even when every one of its rules failed (see `How rule errors are
handled`_): a broken run would otherwise register both datasets as executed,
and the re-run after the fix would skip them, leaving an empty report.
With ``.dev`` the rules are re-evaluated and the report rebuilt on every run.

.. note::

   The *per-dataset* validations are not exempt. They belong to the datasets
   that produce the data, and those are skipped on a re-run once they have
   completed for the configured scenarios -- their validation tasks are
   skipped with them. Since results are written per DAG run
   (``validation_runs/{run_id}/``), a report from a re-run contains the full
   cross-cutting results from ``FinalValidations`` but only the per-dataset
   results of those datasets that actually re-executed.

   For a complete report, re-run the pipeline against a clean
   ``metadata.datasets``.


How rule errors are handled
---------------------------

A rule that raises does not fail its Airflow task. Errors are caught and
converted into failed results at two levels:

- ``DataFrameRule.evaluate`` wraps query and evaluation in ``try/except`` and
  returns an error result with ``severity=ERROR``. All custom sanity rules
  are ``DataFrameRule``\ s.
- The runner catches ``SQLAlchemyError`` (including the
  ``relation ... does not exist`` raised when an upstream task never created
  the table) and returns a failed result with ``severity=WARNING``, and any
  other exception with ``severity=ERROR``.

A table that exists but is empty, or a query whose filters match nothing,
likewise yields a failed result (``EMPTY TABLE`` / ``NO DATA FOUND``) rather
than an error.

Every failure is logged per rule, summarised as ``Complete: n/m passed`` and
written to the report. With ``proceed_on_validation_failure=True`` the task
then still reports success, so a run whose validations are entirely red looks
green in Airflow -- read the report, not the task state.

The task itself only fails on problems outside the rules: an unreachable
database, a boundary missing from a
:func:`resolve_boundary_dependence` mapping, or the output directory not
being writable.
