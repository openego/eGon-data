"""Sanity check validation rules for storage units."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

from egon.data import config
from .power_plants_capacity import PowerPlantsCapacityComparison


class PumpedHydroCapacityComparison(PowerPlantsCapacityComparison):
    """Compare allocated pumped hydro capacity with the scenario input.

    Sums ``el_capacity`` per scenario for carrier ``pumped_hydro`` in
    supply.egon_storages and compares it with supply.egon_scenario_capacities.
    Scenario selection and evaluation are inherited from
    :class:`PowerPlantsCapacityComparison`.

    Unlike ``PowerPlantsCapacityComparison``, this does not compare
    ``BESS``: real BESS capacity is a deliberate lower bound, not scaled
    to the NEP target (see the decision recorded in
    :func:`egon.data.datasets.storages.allocate_battery_storage`,
    2026-08-31) -- eTraGo grows capacity beyond it through its own cost
    optimization instead. ``home_battery`` is compared separately in
    :class:`HomeBatteryCapacityComparison`, which -- unlike this class and
    :class:`PowerPlantsCapacityComparison` -- also checks status quo
    scenarios, since a ``home_battery`` target exists for those too.

    Args:
        table: Primary table being validated (supply.egon_storages)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance of the deviation (default: 0.02 = 2 %).
            Matched exactly (0 %) on both reference databases.

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         PumpedHydroCapacityComparison(
        ...             table="supply.egon_storages",
        ...             rule_id="SANITY_PUMPED_HYDRO_CAPACITY",
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.02, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)

    def get_query(self, ctx):
        scenarios = self._scenarios()

        if not scenarios:
            return super().get_query(ctx)

        scenario_values = ", ".join(f"(:s{i})" for i in range(len(scenarios)))

        return f"""
        SELECT g.scenario, 'pumped_hydro' AS carrier,
               COALESCE((
                   SELECT SUM(p.el_capacity) FROM {self.table} AS p
                   WHERE p.scenario = g.scenario
                   AND p.carrier = 'pumped_hydro'
               ), 0) AS power_plants_mw,
               COALESCE((
                   SELECT SUM(c.capacity)
                   FROM supply.egon_scenario_capacities AS c
                   WHERE c.scenario_name = g.scenario
                   AND c.carrier = 'pumped_hydro'
               ), 0) AS input_mw
        FROM (VALUES {scenario_values}) AS g(scenario)
        ORDER BY g.scenario
        """


class HomeBatteryCapacityComparison(PowerPlantsCapacityComparison):
    """Compare allocated home battery capacity with the scenario input.

    Sums ``el_capacity`` per scenario for carrier ``home_battery`` in
    supply.egon_storages (real MaStR units plus the modeled residual, see
    :func:`egon.data.datasets.storages.home_batteries_per_scenario`) and
    compares it with supply.egon_scenario_capacities.

    Unlike :class:`PowerPlantsCapacityComparison`, status quo scenarios
    are **included** here (``_scenarios()`` overridden to not filter them
    out): ``home_batteries_per_scenario()`` reads a ``home_battery``
    target for status quo scenarios too, unlike every carrier checked by
    the parent class, none of which has a status quo target at all. This
    is deliberate: it is meant to surface a known, unresolved mismatch --
    on the reference database, ``status2024`` allocated ~18.8 % more
    capacity than its target (target 9900 MW, allocated 11760 MW,
    2026-09-28), caused by two compounding issues in the allocation code
    (over-coverage at some buses is never credited against underserved
    buses nationally, and the modeled residual is silently dropped at
    buses where every PV-equipped building already has a real battery --
    see the egon-data electricity_supply scenario migration checklist,
    home battery allocation section, for the full analysis and a proposed
    fix). This rule intentionally stays red on that data until fixed.

    ``BESS`` is not compared here, see
    :class:`PumpedHydroCapacityComparison`.

    A reference database whose ScenarioCapacities run predates the
    ``rename_carrier`` fix for the home battery target (commit
    7b1817c65, 2026-08-27) still uses the carrier name ``battery``
    instead of ``home_battery`` and will show every scenario as
    "allocated without input" here -- a stale database, not a new
    modeling bug.

    Args:
        table: Primary table being validated (supply.egon_storages)
        rule_id: Unique identifier for this validation rule
        rtol: Relative tolerance of the deviation (default: 0.02 = 2 %).
            ``reGon2037`` matched within 0.06 % on the (non-stale)
            reference database; ``status2024`` is expected to stay red.

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         HomeBatteryCapacityComparison(
        ...             table="supply.egon_storages",
        ...             rule_id="SANITY_HOME_BATTERY_CAPACITY",
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, rtol: float = 0.02, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)

    @staticmethod
    def _scenarios():
        # Unlike PowerPlantsCapacityComparison, status quo scenarios are
        # not filtered out: home_batteries_per_scenario() reads a
        # home_battery target for them too.
        return config.settings()["egon-data"]["--scenarios"]

    def get_query(self, ctx):
        scenarios = self._scenarios()

        if not scenarios:
            return super().get_query(ctx)

        scenario_values = ", ".join(f"(:s{i})" for i in range(len(scenarios)))

        return f"""
        SELECT g.scenario, 'home_battery' AS carrier,
               COALESCE((
                   SELECT SUM(p.el_capacity) FROM {self.table} AS p
                   WHERE p.scenario = g.scenario
                   AND p.carrier = 'home_battery'
               ), 0) AS power_plants_mw,
               COALESCE((
                   SELECT SUM(c.capacity)
                   FROM supply.egon_scenario_capacities AS c
                   WHERE c.scenario_name = g.scenario
                   AND c.carrier = 'home_battery'
               ), 0) AS input_mw
        FROM (VALUES {scenario_values}) AS g(scenario)
        ORDER BY g.scenario
        """


class HomeBatteryAggregationComparison(DataFrameRule):
    """Validate home battery capacity aggregation from buildings to buses.

    supply.egon_home_batteries distributes home battery capacity (real
    MaStR units plus a modeled residual, see
    :func:`egon.data.datasets.storages.home_batteries.\
match_real_batteries_to_buildings` and
    :func:`~egon.data.datasets.storages.\
home_batteries_per_scenario`) to individual buildings.
    supply.egon_storages holds the same capacity aggregated per bus
    (carrier ``home_battery``). This rule checks that both aggregate to
    the same power (``p_nom``/``el_capacity``, in MW) per bus.

    A per-bus mismatch above ``atol`` fails; a bus present in only one of
    the two tables also fails.

    Args:
        table: Primary table being validated (supply.egon_home_batteries)
        rule_id: Unique identifier for this validation rule
        atol: Absolute tolerance in MW (default: 0.01). The exactly
            matching real (MaStR) share showed no floating-point noise
            above that on either reference database; deviations found
            there were 0.06 - 13.7 MW, in the modeled share.

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         HomeBatteryAggregationComparison(
        ...             table="supply.egon_home_batteries",
        ...             rule_id="SANITY_HOME_BATTERY_AGGREGATION",
        ...         )
        ...     ]
        ... }
    """

    def __init__(self, table: str, rule_id: str, atol: float = 0.01, **kwargs):
        super().__init__(rule_id=rule_id, table=table, atol=atol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        WITH storage AS (
            SELECT scenario, bus_id, SUM(el_capacity) AS storage_p_nom
            FROM supply.egon_storages
            WHERE carrier = 'home_battery'
            GROUP BY scenario, bus_id
        ),
        buildings AS (
            SELECT scenario, bus_id, SUM(p_nom) AS building_p_nom
            FROM {self.table}
            GROUP BY scenario, bus_id
        )
        SELECT COALESCE(s.scenario, b.scenario) AS scenario,
               COALESCE(s.bus_id, b.bus_id) AS bus_id,
               s.storage_p_nom, b.building_p_nom
        FROM storage AS s
        FULL OUTER JOIN buildings AS b
            ON s.scenario = b.scenario AND s.bus_id = b.bus_id
        """

    def evaluate_df(self, df, ctx):
        atol = self.params.get("atol", 0.01)
        problems = []

        missing_in_buildings = df[df["building_p_nom"].isna()]
        missing_in_storage = df[df["storage_p_nom"].isna()]

        for _, row in missing_in_buildings.iterrows():
            problems.append(
                f"{row.scenario}/bus {row.bus_id}: in egon_storages but "
                f"not in {self.table}"
            )
        for _, row in missing_in_storage.iterrows():
            problems.append(
                f"{row.scenario}/bus {row.bus_id}: in {self.table} but "
                f"not in egon_storages"
            )

        both = df.dropna(subset=["storage_p_nom", "building_p_nom"])
        diff = (both["storage_p_nom"] - both["building_p_nom"]).abs()
        mismatched = both[diff > atol]
        max_diff = float(diff.max()) if not diff.empty else 0.0

        for _, row in mismatched.iterrows():
            problems.append(
                f"{row.scenario}/bus {row.bus_id}: egon_storages "
                f"{row.storage_p_nom:.3f} MW vs. {self.table} "
                f"{row.building_p_nom:.3f} MW"
            )

        success = not problems

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=max_diff,
            expected=0.0,
            message=(
                f"Aggregated home battery capacity matches for {len(df)} "
                f"scenario/bus combinations (max. diff {max_diff:.3f} MW, "
                f"tolerance {atol} MW)"
                if success
                else f"Home battery aggregation mismatch ({len(problems)} "
                "scenario/bus combinations): " + "; ".join(problems[:10])
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class HomeBatteryDuplicateRows(DataFrameRule):
    """Validate that no home battery allocation row is stored twice.

    A building may carry several batteries, so only rows that are
    identical in scenario, building, power, capacity and source count as
    duplicates. All scenarios in the table are checked.

    Args:
        table: Primary table being validated (supply.egon_home_batteries)
        rule_id: Unique identifier for this validation rule

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         HomeBatteryDuplicateRows(
        ...             table="supply.egon_home_batteries",
        ...             rule_id="SANITY_HOME_BATTERY_DUPLICATES",
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
            SELECT scenario, building_id, bus_id, p_nom, capacity,
                   sources, COUNT(*) AS n
            FROM {self.table}
            GROUP BY scenario, building_id, bus_id, p_nom, capacity, sources
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
