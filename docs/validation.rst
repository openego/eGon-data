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