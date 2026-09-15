"""CTS (Commercial, Trade, Services) demand sanity check validation rules."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import numpy as np


class CtsElectricityDemandShare(DataFrameRule):
    """Validate CTS electricity demand shares sum to 1 for each substation.

    Checks that the sum of aggregated CTS electricity demand share equals 1
    for every substation, as the substation profile is linearly disaggregated
    to all buildings.

    Args:
        table: Primary table being validated
            (demand.egon_cts_electricity_demand_building_share)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance for comparison (default: 0.005 = 0.5%)

    Example:
        >>> validation = {
        ...     "data_quality": [
        ...         CtsElectricityDemandShare(
        ...             table="demand.egon_cts_electricity_demand_"
        ...                   "building_share",
        ...             rule_id="SANITY_CTS_ELECTRICITY_DEMAND_SHARE",
        ...             rtol=0.005
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.005, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return """
        SELECT bus_id, scenario, SUM(profile_share) as total_share
        FROM demand.egon_cts_electricity_demand_building_share
        GROUP BY bus_id, scenario
        """

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.005)

        try:
            # Check that all shares sum to 1 (within tolerance)
            np.testing.assert_allclose(
                actual=df["total_share"],
                desired=1.0,
                rtol=rtol,
                verbose=False,
            )

            # Calculate actual max deviation for reporting
            max_diff = (df["total_share"] - 1.0).abs().max()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"CTS electricity demand shares sum to 1 for all "
                    f"{len(df)} bus/scenario combinations "
                    f"(max deviation: {max_diff:.6f}, tolerance: {rtol:.6f})"
                ),
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
        except AssertionError:
            max_diff = (df["total_share"] - 1.0).abs().max()
            violations = df[~np.isclose(df["total_share"], 1.0, rtol=rtol)]

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"CTS electricity demand share mismatch: max deviation "
                    f"{max_diff:.6f} exceeds tolerance {rtol:.6f}. "
                    f"{len(violations)} bus/scenario combinations have "
                    f"shares != 1."
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class CtsHeatDemandShare(DataFrameRule):
    """Validate CTS heat demand shares sum to 1 for each substation.

    Checks that the sum of aggregated CTS heat demand share equals 1
    for every substation, as the substation profile is linearly disaggregated
    to all buildings.

    Args:
        table: Primary table being validated
            (demand.egon_cts_heat_demand_building_share)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance for comparison (default: 0.005 = 0.5%)

    Example:
        >>> validation = {
        ...     "data_quality": [
        ...         CtsHeatDemandShare(
        ...             table="demand.egon_cts_heat_demand_building_share",
        ...             rule_id="SANITY_CTS_HEAT_DEMAND_SHARE",
        ...             rtol=0.005
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.005, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return """
        SELECT bus_id, scenario, SUM(profile_share) as total_share
        FROM demand.egon_cts_heat_demand_building_share
        GROUP BY bus_id, scenario
        """

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.005)

        try:
            # Check that all shares sum to 1 (within tolerance)
            np.testing.assert_allclose(
                actual=df["total_share"],
                desired=1.0,
                rtol=rtol,
                verbose=False,
            )

            # Calculate actual max deviation for reporting
            max_diff = (df["total_share"] - 1.0).abs().max()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"CTS heat demand shares sum to 1 for all "
                    f"{len(df)} bus/scenario combinations "
                    f"(max deviation: {max_diff:.6f}, tolerance: {rtol:.6f})"
                ),
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
        except AssertionError:
            max_diff = (df["total_share"] - 1.0).abs().max()
            violations = df[~np.isclose(df["total_share"], 1.0, rtol=rtol)]

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_diff),
                expected=rtol,
                message=(
                    f"CTS heat demand share mismatch: max deviation "
                    f"{max_diff:.6f} exceeds tolerance {rtol:.6f}. "
                    f"{len(violations)} bus/scenario combinations have "
                    f"shares != 1."
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
