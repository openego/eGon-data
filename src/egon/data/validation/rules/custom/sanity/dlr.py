"""Sanity check validation rules for dynamic line rating."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class DLRValues(DataFrameRule):
    """Validate DLR array length, values and scenario coverage."""

    def __init__(self, table: str, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_params(self, ctx):
        return {"scenario": self.scenario}

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (
                WHERE s_max_pu IS NULL
                OR cardinality(s_max_pu) != 8760
            ) AS n_wrong_length,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM unnest(s_max_pu) v
                WHERE v IS NULL OR v = 'NaN'
            )) AS n_nan,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM unnest(s_max_pu) v
                WHERE v < 1.0 OR v > 1.5
            )) AS n_out_of_range
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]

        n_rows = int(row.n_rows)
        n_wrong_length = int(row.n_wrong_length)
        n_nan = int(row.n_nan)
        n_out_of_range = int(row.n_out_of_range)

        n_bad = n_wrong_length + n_nan + n_out_of_range
        success = n_rows > 0 and n_bad == 0

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': "
                f"{n_rows} DLR rows, "
                f"{n_wrong_length} arrays without 8760 values, "
                f"{n_nan} arrays with NULL/NaN values, "
                f"{n_out_of_range} arrays with values outside 1.0–1.5"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )