"""Residential electricity demand sanity check validation rules."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import numpy as np


class ResidentialElectricityAnnualSum(DataFrameRule):
    """Validate aggregated annual residential electricity demand.

    Matches DemandRegio at NUTS-3. Aggregates the annual demand of all
    census cells at NUTS3 to compare with initial scaling parameters
    from DemandRegio.

    Args:
        table: Primary table being validated
            (demand.egon_demandregio_zensus_electricity)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance for comparison (default: 0.005 = 0.5%)

    Example:
        >>> validation = {
        ...     "data_quality": [
        ...         ResidentialElectricityAnnualSum(
        ...             table="demand.egon_demandregio_zensus_electricity",
        ...             rule_id="SANITY_RESIDENTIAL_ELECTRICITY_ANNUAL_SUM",
        ...             rtol=0.005
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.005, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"  # Override inferred kind

    def get_query(self, ctx):
        return """
        SELECT dr.nuts3, dr.scenario, dr.demand_regio_sum, profiles.profile_sum
        FROM (
            SELECT scenario, SUM(demand) AS profile_sum, vg250_nuts3
            FROM demand.egon_demandregio_zensus_electricity AS egon,
             boundaries.egon_map_zensus_vg250 AS boundaries
            WHERE egon.zensus_population_id = boundaries.zensus_population_id
            AND sector = 'residential'
            GROUP BY vg250_nuts3, scenario
        ) AS profiles
        JOIN (
            SELECT nuts3, scenario, sum(demand) AS demand_regio_sum
            FROM demand.egon_demandregio_hh
            GROUP BY year, scenario, nuts3
        ) AS dr
        ON profiles.vg250_nuts3 = dr.nuts3 AND profiles.scenario = dr.scenario
        """

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.005)

        try:
            np.testing.assert_allclose(
                actual=df["profile_sum"],
                desired=df["demand_regio_sum"],
                rtol=rtol,
                verbose=False,
            )

            # Calculate actual max deviation for reporting
            max_diff = (
                (
                    (df["profile_sum"] - df["demand_regio_sum"])
                    / df["demand_regio_sum"]
                )
                .abs()
                .max()
            )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"Aggregated annual residential electricity demand "
                    f"matches with DemandRegio at NUTS-3 "
                    f"(max deviation: {max_diff:.4%}, tolerance: {rtol:.4%})"
                ),
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
        except AssertionError:
            max_diff = (
                (
                    (df["profile_sum"] - df["demand_regio_sum"])
                    / df["demand_regio_sum"]
                )
                .abs()
                .max()
            )
            violations = df[
                ~np.isclose(
                    df["profile_sum"], df["demand_regio_sum"], rtol=rtol
                )
            ]

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"Demand mismatch: max deviation {max_diff:.4%} exceeds "
                    f"tolerance {rtol:.4%}. {len(violations)} NUTS-3 regions "
                    f"have mismatches."
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class ResidentialElectricityHhRefinement(DataFrameRule):
    """Validate aggregated household types after refinement.

    Checks sum of aggregated household types after refinement method
    was applied and compares it to the original census values.

    Args:
        table: Primary table being validated
            (society.egon_destatis_zensus_household_per_ha_refined)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance for comparison (default: 1e-5 = 0.001%)

    Example:
        >>> validation = {
        ...     "data_quality": [
        ...         ResidentialElectricityHhRefinement(
        ...             table="society.egon_destatis_zensus_household_per_ha_refined",
        ...             rule_id="SANITY_RESIDENTIAL_HH_REFINEMENT",
        ...             rtol=1e-5
        ...         )
        ...     ]
        ... }
    """

    def __init__(self, table: str, rule_id: str, rtol: float = 1e-5, **kwargs):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return """
        SELECT refined.nuts3, refined.characteristics_code,
                refined.sum_refined::int, census.sum_census::int
        FROM(
            SELECT nuts3, characteristics_code, SUM(hh_10types) as sum_refined
            FROM society.egon_destatis_zensus_household_per_ha_refined
            GROUP BY nuts3, characteristics_code)
            AS refined
        JOIN(
            SELECT t.nuts3, t.characteristics_code, sum(orig) as sum_census
            FROM(
                SELECT nuts3, cell_id, characteristics_code,
                        sum(DISTINCT(hh_5types))as orig
                FROM society.egon_destatis_zensus_household_per_ha_refined
                GROUP BY cell_id, characteristics_code, nuts3) AS t
            GROUP BY t.nuts3, t.characteristics_code    ) AS census
        ON refined.nuts3 = census.nuts3
        AND refined.characteristics_code = census.characteristics_code
        """

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 1e-5)

        try:
            np.testing.assert_allclose(
                actual=df["sum_refined"],
                desired=df["sum_census"],
                rtol=rtol,
                verbose=False,
            )

            max_diff = (
                ((df["sum_refined"] - df["sum_census"]) / df["sum_census"])
                .abs()
                .max()
            )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"All aggregated household types match at NUTS-3 "
                    f"(max deviation: {max_diff:.6%}, tolerance: {rtol:.6%})"
                ),
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
        except AssertionError:
            max_diff = (
                ((df["sum_refined"] - df["sum_census"]) / df["sum_census"])
                .abs()
                .max()
            )
            violations = df[
                ~np.isclose(df["sum_refined"], df["sum_census"], rtol=rtol)
            ]

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"Household refinement mismatch: max deviation "
                    f"{max_diff:.6%} exceeds tolerance {rtol:.6%}. "
                    f"{len(violations)} NUTS-3/characteristic combinations "
                    f"have mismatches."
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
