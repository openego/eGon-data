"""
Sanity check validation rules for combined heat and power (CHP) plants.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class ChpBiomassCapacityLimit(DataFrameRule):
    """
    Validate that the biomass CHP capacity does not exceed its target.

    For scenarios with a biomass target in ``supply.egon_scenario_capacities``,
    all MaStR biomass units are scaled to the target. Units with thermal
    output become CHP plants, all others are inserted as biomass power plants
    by the PowerPlants dataset. The electrical capacity of the biomass CHP
    therefore has to stay at or below the target of its scenario.

    Scenarios without a biomass target (e.g. status quo scenarios, which are
    taken from MaStR unscaled) are reported, not failed.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        capacities_table: str,
        rtol: float = 0.001,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            CHP table ("supply.egon_chp_plants").
        rule_id : str
            Unique identifier for this validation rule.
        capacities_table : str
            Table with the target capacities
            ("supply.egon_scenario_capacities").
        rtol : float
            Relative tolerance above the target (default: 0.001 = 0.1 %).
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            capacities_table=capacities_table,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        """Biomass CHP capacity and biomass target per scenario."""
        return f"""
        SELECT
            chp.scenario,
            chp.el_capacity,
            target.capacity AS target
        FROM (
            SELECT scenario, sum(el_capacity) AS el_capacity
            FROM {self.table}
            WHERE carrier = 'biomass'
            GROUP BY scenario
        ) AS chp
        LEFT JOIN (
            SELECT scenario_name, sum(capacity) AS capacity
            FROM {self.params["capacities_table"]}
            WHERE carrier = 'biomass'
            GROUP BY scenario_name
        ) AS target
        ON target.scenario_name = chp.scenario
        ORDER BY chp.scenario
        """

    def evaluate_df(self, df, ctx):
        """Fail if the biomass CHP capacity exceeds the target."""
        rtol = float(self.params.get("rtol", 0.001))

        checked = df[df["target"].notna()]
        unchecked = df[df["target"].isna()]
        exceeding = checked[
            checked["el_capacity"] > checked["target"] * (1 + rtol)
        ]
        success = exceeding.empty

        details = [
            f"{row.scenario}: {row.el_capacity:.1f} MW of "
            f"{row.target:.1f} MW ({row.el_capacity / row.target * 100:.1f} %)"
            if row.target
            else f"{row.scenario}: {row.el_capacity:.1f} MW, target 0 MW"
            for row in checked.itertuples()
        ]
        if not unchecked.empty:
            details.append(
                f"no biomass target for {', '.join(unchecked.scenario)}, "
                f"not checked"
            )
        details = "; ".join(details)

        message = (
            f"Biomass CHP capacity within target. {details}"
            if success
            else f"Biomass CHP capacity exceeds target in "
            f"{', '.join(exceeding.scenario)}. {details}"
        )

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=float(checked["el_capacity"].sum()),
            expected=float(checked["target"].sum()),
            message=message,
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )
