"""Sanity check validation rules for the power plant capacity distribution."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

from egon.data import config

#: Carriers whose allocated capacity is scaled to the input capacity in
#: supply.egon_scenario_capacities and can therefore be compared with it.
#: Biomass is split between supply.egon_power_plants (units without heat
#: output) and supply.egon_chp_plants (units with heat output), so both are
#: summed, see :data:`SUMMED_WITH_CHP`. Rooftop PV is stored in a separate
#: table, see :mod:`.pv_rooftop_buildings`.
#:
#: Gas is left out on purpose. It is not scaled to the input capacity: the
#: plants come unscaled from the NEP list of power plants, which does not
#: match the scenario table the input is taken from (reGon2037, observed:
#: list 51.9 GW vs. input 17.2 GW). 21.4 GW of planned CHP plants without
#: MaStR id are not inserted at all. Power plants (9.4 GW) plus CHP plants
#: (17.2 GW) exceed the input by 82 %.
COMPARED_CARRIERS = (
    "wind_onshore",
    "wind_offshore",
    "solar",
    "others",
    "reservoir",
    "run_of_river",
    "biomass",
)

#: Carriers whose allocated capacity also includes supply.egon_chp_plants
SUMMED_WITH_CHP = ("biomass",)


class PowerPlantsCapacityComparison(DataFrameRule):
    """Compare allocated power plant capacity with the scenario input.

    Sums the capacity per scenario and carrier in supply.egon_power_plants
    (plus supply.egon_chp_plants for the carriers in
    :data:`SUMMED_WITH_CHP`) and compares it with the input capacity in
    supply.egon_scenario_capacities. All scenarios from ``--scenarios``
    are checked except status quo scenarios, which have no input capacity.

    Per scenario and carrier the check fails if

    * capacity was allocated although the input is zero,
    * no capacity was allocated although the input is greater than zero or
    * the relative deviation exceeds ``rtol``.

    Args:
        table: Primary table being validated (supply.egon_power_plants)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance of the deviation (default: 0.02 = 2 %).
            Hydro carriers deviate by up to ~1.3 % (observed).

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         PowerPlantsCapacityComparison(
        ...             table="supply.egon_power_plants",
        ...             rule_id="SANITY_POWER_PLANTS_CAPACITY",
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.02, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    @staticmethod
    def _scenarios():
        return [
            scn
            for scn in config.settings()["egon-data"]["--scenarios"]
            if "status" not in scn
        ]

    def get_query(self, ctx):
        scenarios = self._scenarios()

        if not scenarios:
            # Placeholder row, so that the rule is not reported as
            # "no data found" when only status quo scenarios are active
            return """
            SELECT NULL::text AS scenario, NULL::text AS carrier,
                   0.0::float8 AS power_plants_mw, 0.0::float8 AS input_mw
            """

        scenario_values = ", ".join(f"(:s{i})" for i in range(len(scenarios)))
        carrier_values = ", ".join(f"('{c}')" for c in COMPARED_CARRIERS)
        chp_carriers = ", ".join(f"'{c}'" for c in SUMMED_WITH_CHP)

        return f"""
        WITH grid AS (
            SELECT s.scenario, c.carrier
            FROM (VALUES {scenario_values}) AS s(scenario)
            CROSS JOIN (VALUES {carrier_values}) AS c(carrier)
        )
        SELECT g.scenario, g.carrier,
               COALESCE((
                   SELECT SUM(p.el_capacity) FROM {self.table} AS p
                   WHERE p.scenario = g.scenario AND p.carrier = g.carrier
               ), 0) + COALESCE((
                   SELECT SUM(k.el_capacity)
                   FROM supply.egon_chp_plants AS k
                   WHERE k.scenario = g.scenario AND k.carrier = g.carrier
                   AND g.carrier IN ({chp_carriers})
               ), 0) AS power_plants_mw,
               COALESCE((
                   SELECT SUM(c.capacity)
                   FROM supply.egon_scenario_capacities AS c
                   WHERE c.scenario_name = g.scenario
                   AND c.carrier = g.carrier
               ), 0) AS input_mw
        FROM grid AS g
        ORDER BY g.scenario, g.carrier
        """

    def get_params(self, ctx):
        return {f"s{i}": scn for i, scn in enumerate(self._scenarios())}

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.02)

        if df["scenario"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=0.0,
                expected=0.0,
                message="No scenario with input capacity is active, "
                "nothing to compare",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        problems = []
        max_deviation = 0.0

        for row in df.itertuples():
            name = f"{row.scenario}/{row.carrier}"
            allocated = float(row.power_plants_mw)
            target = float(row.input_mw)

            if allocated == 0 and target == 0:
                continue

            if target == 0:
                problems.append(
                    f"{name}: {allocated:.1f} MW allocated without input"
                )
            elif allocated == 0:
                problems.append(
                    f"{name}: nothing allocated, input is {target:.1f} MW"
                )
            else:
                deviation = (allocated - target) / target
                max_deviation = max(max_deviation, abs(deviation))
                if abs(deviation) > rtol:
                    problems.append(
                        f"{name}: {allocated:.1f} MW vs. input "
                        f"{target:.1f} MW ({deviation:+.2%})"
                    )

        success = not problems

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=float(len(problems)),
            expected=0.0,
            message=(
                f"Allocated capacity matches the input for {len(df)} "
                f"scenario/carrier combinations (max. deviation "
                f"{max_deviation:.2%}, tolerance {rtol:.2%})"
                if success
                else "Capacity mismatch: " + "; ".join(problems)
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )
