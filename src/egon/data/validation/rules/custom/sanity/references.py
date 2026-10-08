"""
Sanity check validation rules for references between scenario tables.

egon-validation's ``ReferentialIntegrityValidation`` only compares a single
column. Many eGon tables, however, hold one row per scenario for the same
id, e.g. buses in ``grid.egon_etrago_bus``, so a reference is only valid if
it points to a row of the same scenario.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class ScenarioReferentialIntegrity(DataFrameRule):
    """
    Validate that a column references an existing row of the same scenario.

    NULL values in the referencing column are ignored, so the rule can be
    used for columns that are only filled for some rows. The referenced rows
    can be restricted further with ``ref_filter``, e.g. to buses of one
    carrier.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        fk_column: str,
        ref_table: str,
        ref_column: str,
        scenario_column: str = "scenario",
        ref_scenario_column: str = "scenario",
        ref_filter: str = "TRUE",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Table containing the reference.
        rule_id : str
            Unique identifier for this validation rule.
        fk_column : str
            Referencing column in ``table``.
        ref_table : str
            Referenced table.
        ref_column : str
            Referenced column in ``ref_table``.
        scenario_column : str
            Scenario column in ``table`` (default: "scenario").
        ref_scenario_column : str
            Scenario column in ``ref_table`` (default: "scenario").
        ref_filter : str
            Additional SQL condition on ``ref_table``, e.g. "carrier = 'AC'"
            (default: "TRUE").
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            fk_column=fk_column,
            ref_table=ref_table,
            ref_column=ref_column,
            scenario_column=scenario_column,
            ref_scenario_column=ref_scenario_column,
            ref_filter=ref_filter,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        """Per scenario: number of references and of orphaned references.

        Table and column names as well as the filter are configured SQL
        identifiers and conditions, so they are written into the query.
        """
        p = self.params
        return f"""
        SELECT
            child.{p["scenario_column"]} AS scenario,
            count(*) AS n_references,
            count(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {p["ref_table"]} AS parent
                    WHERE parent.{p["ref_column"]} = child.{p["fk_column"]}
                    AND parent.{p["ref_scenario_column"]}
                        = child.{p["scenario_column"]}
                    AND ({p["ref_filter"]})
                )
            ) AS n_orphaned
        FROM {self.table} AS child
        WHERE child.{p["fk_column"]} IS NOT NULL
        GROUP BY child.{p["scenario_column"]}
        ORDER BY child.{p["scenario_column"]}
        """

    def evaluate_df(self, df, ctx):
        """Fail if any reference has no matching row of its scenario."""
        p = self.params
        n_orphaned = int(df["n_orphaned"].sum())
        success = n_orphaned == 0

        details = "; ".join(
            f"{row.scenario}: {row.n_orphaned} of {row.n_references} orphaned"
            for row in df.itertuples()
        )
        target = (
            f"{p['ref_table']}.{p['ref_column']}"
            + (f" ({p['ref_filter']})" if p["ref_filter"] != "TRUE" else "")
        )
        message = (
            f"All references in {p['fk_column']} exist in {target} for "
            f"the same scenario. {details}"
            if success
            else f"{n_orphaned} references in {p['fk_column']} have no "
            f"match in {target} for the same scenario. {details}"
        )

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_orphaned,
            expected=0,
            message=message,
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )
