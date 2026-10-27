"""Sanity check validation rules for demand-side management."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class DSMValues(DataFrameRule):
    """Validate DSM time-series values for a specific scenario."""

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
                WHERE p_set IS NULL
                   OR p_max IS NULL
                   OR p_min IS NULL
                   OR e_max IS NULL
                   OR e_min IS NULL
            ) AS n_null_arrays,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_set, p_max, p_min, e_max, e_min)
                    AS v(ps, pmax, pmin, emax, emin)
                    WHERE ps IS NULL OR ps = 'NaN'
                       OR pmax IS NULL OR pmax = 'NaN'
                       OR pmin IS NULL OR pmin = 'NaN'
                       OR emax IS NULL OR emax = 'NaN'
                       OR emin IS NULL OR emin = 'NaN'
                )
            ) AS n_invalid_values,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_set) AS v
                    WHERE v < 0
                )
            ) AS n_negative_p_set,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_max) AS v
                    WHERE v < 0
                )
            ) AS n_negative_p_max,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_min) AS v
                    WHERE v > 0
                )
            ) AS n_positive_p_min,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(e_max) AS v
                    WHERE v < 0
                )
            ) AS n_negative_e_max,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(e_min) AS v
                    WHERE v > 0
                )
            ) AS n_positive_e_min

        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]

        n_rows = int(row.n_rows)
        n_null_arrays = int(row.n_null_arrays)
        n_invalid_values = int(row.n_invalid_values)
        n_negative_p_set = int(row.n_negative_p_set)
        n_negative_p_max = int(row.n_negative_p_max)
        n_positive_p_min = int(row.n_positive_p_min)
        n_negative_e_max = int(row.n_negative_e_max)
        n_positive_e_min = int(row.n_positive_e_min)

        n_bad = (
            n_null_arrays
            + n_invalid_values
            + n_negative_p_set
            + n_negative_p_max
            + n_positive_p_min
            + n_negative_e_max
            + n_positive_e_min
        )

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
                f"Scenario '{self.scenario}': {n_rows} DSM rows, "
                f"{n_null_arrays} rows with NULL arrays, "
                f"{n_invalid_values} rows with NULL/NaN values, "
                f"{n_negative_p_set} rows with negative p_set, "
                f"{n_negative_p_max} rows with negative p_max, "
                f"{n_positive_p_min} rows with positive p_min, "
                f"{n_negative_e_max} rows with negative e_max, "
                f"{n_positive_e_min} rows with positive e_min"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )# -*- coding: utf-8 -*-

