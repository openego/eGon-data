"""Sanity check validation rules for PV rooftop plants on buildings."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

from .power_plants_capacity import PowerPlantsCapacityComparison


class PvRooftopCapacityComparison(PowerPlantsCapacityComparison):
    """Compare allocated PV rooftop capacity with the scenario input.

    Sums the capacity per scenario in
    supply.egon_power_plants_pv_roof_building and compares it with the
    carrier ``solar_rooftop`` of supply.egon_scenario_capacities. Scenario
    selection and evaluation are inherited from
    :class:`PowerPlantsCapacityComparison`.

    Args:
        table: Primary table being validated
            (supply.egon_power_plants_pv_roof_building)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance of the deviation (default: 0.03 = 3 %).
            The allocation overshoots the input by 0.8 - 1.9 % (observed).

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         PvRooftopCapacityComparison(
        ...             table="supply.egon_power_plants_pv_roof_building",
        ...             rule_id="SANITY_PV_ROOFTOP_CAPACITY",
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.03, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)

    def get_query(self, ctx):
        scenarios = self._scenarios()

        if not scenarios:
            return super().get_query(ctx)

        scenario_values = ", ".join(f"(:s{i})" for i in range(len(scenarios)))

        # Column names are those expected by the inherited evaluate_df
        return f"""
        SELECT g.scenario, 'solar_rooftop' AS carrier,
               COALESCE((
                   SELECT SUM(p.capacity) FROM {self.table} AS p
                   WHERE p.scenario = g.scenario
               ), 0) AS power_plants_mw,
               COALESCE((
                   SELECT SUM(c.capacity)
                   FROM supply.egon_scenario_capacities AS c
                   WHERE c.scenario_name = g.scenario
                   AND c.carrier = 'solar_rooftop'
               ), 0) AS input_mw
        FROM (VALUES {scenario_values}) AS g(scenario)
        ORDER BY g.scenario
        """


class PvRooftopDuplicateRows(DataFrameRule):
    """Validate that no PV rooftop plant is stored twice.

    A building may carry several plants, so only rows that are identical in
    scenario, building, ``gens_id`` and capacity count as duplicates. All
    scenarios in the table are checked, including status quo scenarios.

    Args:
        table: Primary table being validated
            (supply.egon_power_plants_pv_roof_building)
        rule_id: Unique identifier for this validation rule

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         PvRooftopDuplicateRows(
        ...             table="supply.egon_power_plants_pv_roof_building",
        ...             rule_id="SANITY_PV_ROOFTOP_DUPLICATES",
        ...         )
        ...     ]
        ... }
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        SELECT scenario, SUM(n) AS n_rows, SUM(n - 1) AS n_duplicates
        FROM (
            SELECT scenario, building_id, gens_id, capacity, COUNT(*) AS n
            FROM {self.table}
            GROUP BY scenario, building_id, gens_id, capacity
        ) AS grouped
        GROUP BY scenario
        ORDER BY scenario
        """

    def evaluate_df(self, df, ctx):
        duplicated = df[df["n_duplicates"] > 0]
        total = int(duplicated["n_duplicates"].sum())
        success = duplicated.empty

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=float(total),
            expected=0.0,
            message=(
                f"No duplicated rows in {len(df)} scenarios"
                if success
                else "Duplicated rows: "
                + "; ".join(
                    f"{row.scenario}: {int(row.n_duplicates)} of "
                    f"{int(row.n_rows)} rows"
                    for row in duplicated.itertuples()
                )
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )
