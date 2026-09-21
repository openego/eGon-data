**********
Validation
**********

The eGon-data pipeline integrates with the egon-validation framework to ensure data quality
and consistency. Validations can be added to any Dataset and run automatically as part of
the pipeline.

Overview
========

Validation in eGon-data supports two main approaches:

1. **TableValidation** - Declarative validation specs for common checks (null values, data types, row counts, etc.)
2. **RuleValidation** - Custom validation rules for complex business logic

Both approaches produce structured results with pass/fail status, observed vs expected values,
and detailed messages.

Adding Validation to a Dataset
==============================

Validations are declared in the ``validation`` parameter of a Dataset:

.. code-block:: python

    from egon.data.datasets import Dataset
    from egon.data.validation.specs import TableValidation, RuleValidation
    from egon.data.validation.rules.custom.sanity import MyCustomRule

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
                            table="schema.my_table",
                            not_null=["id", "value"],
                            row_count={"min": 1},
                        ),
                        RuleValidation(
                            rule=MyCustomRule(
                                table="schema.my_table",
                                rule_id="SANITY_MY_CHECK",
                                scenario="eGon2035",
                            ),
                        ),
                    ],
                },
            )


TableValidation
===============

``TableValidation`` provides declarative validation for common data quality checks without
writing custom code.

Available Checks
----------------

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Parameter
     - Type
     - Description
   * - ``table``
     - str
     - Target table in format "schema.table" (required)
   * - ``not_null``
     - List[str]
     - Columns that must not contain NULL values
   * - ``data_types``
     - Dict[str, str]
     - Expected data types for columns (e.g., ``{"id": "integer", "name": "text"}``)
   * - ``row_count``
     - Dict
     - Row count constraints: ``{"min": N}``, ``{"max": N}``, or ``{"exact": N}``
   * - ``unique``
     - List[str]
     - Columns or column combinations that must be unique
   * - ``foreign_keys``
     - List[Dict]
     - Foreign key references to validate

Example
-------

.. code-block:: python

    TableValidation(
        table="grid.egon_etrago_bus",
        not_null=["bus_id", "scn_name", "carrier"],
        data_types={
            "bus_id": "text",
            "v_nom": "numeric",
        },
        row_count={"min": 100},
        unique=["bus_id", "scn_name"],
    )


RuleValidation
==============

``RuleValidation`` wraps custom validation rules that implement complex business logic.

.. code-block:: python

    from egon.data.validation.specs import RuleValidation
    from egon.data.validation.rules.custom.sanity import CH4StoresCapacity

    RuleValidation(
        rule=CH4StoresCapacity(
            table="grid.egon_etrago_store",
            rule_id="SANITY_CH4_STORES_CAPACITY",
            scenario="eGon2035",
            rtol=0.02,
        ),
    )


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
