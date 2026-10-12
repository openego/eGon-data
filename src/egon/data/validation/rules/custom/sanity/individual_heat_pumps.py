"""
Sanity check validation rules for the disaggregation of individual heat pumps.

Validates what :class:`~egon.data.datasets.heat_supply.individual_heating.
HeatPumpsStatusQuo` and :class:`~egon.data.datasets.heat_supply.
individual_heating.HeatPumpsCascade` write:

* ``demand.egon_building_heat_peak_loads`` -- peak heat load per building,
* ``demand.egon_hp_capacity_buildings`` -- heat pump capacity per building,
* ``demand.egon_etrago_timeseries_individual_heating`` -- aggregated heat
  demand time series per MV grid of buildings with heat pumps
  (``heat_pump``) and with gas boilers (``CH4``).

Most rules check one scenario. A scenario that is not part of
``--scenarios`` is skipped (success, severity INFO); a configured scenario
without rows fails. Reference values -- capacity targets, district heating
areas, the floor chain, the minimum heat pump size and the sizing rule --
are read from the database, the configuration and the dataset code when the
rule runs, so they follow the inputs of the run.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import pandas as pd

import egon.data.config

#: Largest plausible peak heat load of a single building in W. The largest
#: buildings measured on a full run reach about 12.5 MW (status2024); values
#: above this bound point to a unit error.
MAX_PEAK_LOAD_W = 20e6

#: Relative tolerance for comparisons that only differ by float rounding
#: between the Python write path and the SQL checks.
RTOL_ROUNDING = 1e-6

PEAK_LOAD_TABLE = "demand.egon_building_heat_peak_loads"
HP_CAPACITY_TABLE = "demand.egon_hp_capacity_buildings"
TIMESERIES_TABLE = "demand.egon_etrago_timeseries_individual_heating"
DH_MAP_TABLE = "demand.egon_map_zensus_district_heating_areas"

#: The building -> zensus cell lookups the dataset itself uses to build the
#: candidate buildings for individual heating: residential buildings via
#: their heat profiles, CTS buildings via the MV grid building mapping.
BUILDING_CELLS_SQL = """
    SELECT building_id, zensus_population_id
    FROM demand.egon_heat_timeseries_selected_profiles
    UNION
    SELECT building_id, zensus_population_id
    FROM boundaries.egon_map_zensus_mvgd_buildings
    WHERE sector = 'cts'
"""


def configured_scenarios():
    """Scenarios of the current run (``--scenarios``)."""
    return list(egon.data.config.settings()["egon-data"]["--scenarios"])


def _hp_sizing_factor():
    """Heat pump capacity in MW required per MW of peak heat load."""
    from egon.data.datasets.heat_supply.individual_heating import (
        determine_minimum_hp_capacity_per_building,
    )

    return float(determine_minimum_hp_capacity_per_building(1.0))


def _examples(values, n=5):
    """Up to ``n`` values as a comma-separated string."""
    values = [str(v) for v in list(values or [])[:n]]
    return ", ".join(values) if values else "-"


class _HeatPumpRule(DataFrameRule):
    """
    Common base for the heat pump rules.

    Subclasses return exactly one row per query (or evaluate the whole
    result in pandas), because the framework reports an empty result as a
    failure.
    """

    def __init__(self, table: str, rule_id: str, scenario=None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Table the rule is reported for.
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str or None
            Scenario to check, e.g. "reGon2037". ``None`` for rules that
            are not bound to a single scenario.
        """
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def _check_table_empty(self, engine, ctx):
        # The framework counts the whole table before every rule, which
        # costs a full scan of 50+ million rows for the peak load table.
        # Empty scenarios are detected per scenario by the rules instead.
        return None

    def evaluate(self, engine, ctx):
        if (
            self.scenario is not None
            and self.scenario not in configured_scenarios()
        ):
            return self._skip(
                f"Scenario '{self.scenario}' is not part of --scenarios; "
                f"check skipped"
            )
        return super().evaluate(engine, ctx)

    def get_params(self, ctx):
        return {"scenario": self.scenario}

    def _result(
        self, success, message, observed=None, expected=None, severity=None
    ):
        """A result with the common fields filled in."""
        if severity is None:
            severity = Severity.INFO if success else Severity.ERROR
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=observed,
            expected=expected,
            message=message,
            severity=severity,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

    def _skip(self, message):
        """A non-failure result for cases that cannot be checked."""
        return self._result(success=True, message=message)

    def _no_rows(self, table=None):
        """Failure for a configured scenario without rows."""
        return self._result(
            success=False,
            observed=0,
            message=(
                f"Scenario '{self.scenario}' is configured, but "
                f"{table or self.table} has no rows for it"
            ),
        )


class _FloorChainRule(_HeatPumpRule):
    """
    Base for rules comparing a scenario with its floor chain predecessor.

    The predecessor is resolved when the rule runs, like the dataset does
    it: the nearest earlier link of the heat pump floor chain that is part
    of ``--scenarios``. A scenario without such a predecessor was
    distributed unfloored, so there is nothing to compare.
    """

    def evaluate(self, engine, ctx):
        from egon.data.datasets.heat_supply.individual_heating import (
            get_hp_floor_predecessors,
        )

        scenarios = configured_scenarios()
        predecessors = [
            p
            for p in get_hp_floor_predecessors(self.scenario)
            if p in scenarios
        ]
        if self.scenario in scenarios and not predecessors:
            return self._skip(
                f"Scenario '{self.scenario}' has no floor chain predecessor "
                f"in --scenarios; check skipped"
            )
        self.predecessor = predecessors[0] if predecessors else None
        return super().evaluate(engine, ctx)

    def get_params(self, ctx):
        from egon.data.datasets.scenario_parameters import (
            demand_source_scenario,
        )

        return {
            "scenario": self.scenario,
            "predecessor": self.predecessor,
            "dh_scenario": demand_source_scenario(self.scenario),
        }


class HeatPumpScenarioCoverage(_HeatPumpRule):
    """
    Check that the heat pump tables hold every configured chain scenario.

    For each table, every scenario of the floor chain that is part of
    ``--scenarios`` must have rows. Rows of scenarios that are not
    configured (e.g. left over from an earlier run) are reported, but are
    not an error.
    """

    def __init__(self, table: str, rule_id: str, tables, **kwargs):
        """
        Parameters
        ----------
        table : str
            Table the rule is reported for.
        rule_id : str
            Unique identifier for this validation rule.
        tables : Sequence[str]
            Tables to check, each with a ``scenario`` column.
        """
        super().__init__(
            table=table, rule_id=rule_id, tables=list(tables), **kwargs
        )

    def get_params(self, ctx):
        return None

    def get_query(self, ctx):
        return " UNION ALL ".join(f"""
            SELECT '{t}' AS source, array_agg(DISTINCT scenario) AS scenarios
            FROM {t}
            """ for t in self.params["tables"])

    def evaluate_df(self, df, ctx):
        from egon.data.datasets.heat_supply.individual_heating import (
            get_hp_floor_chain,
        )

        configured = configured_scenarios()
        expected = [s for s in get_hp_floor_chain() if s in configured]

        missing, extra = [], set()
        for row in df.itertuples():
            present = set(row.scenarios or [])
            missing += [
                f"{row.source}: {s}" for s in expected if s not in present
            ]
            extra |= present - set(configured)

        note = (
            f" Rows of scenarios that are not configured: "
            f"{', '.join(sorted(extra))}."
            if extra
            else ""
        )
        if missing:
            return self._result(
                success=False,
                observed=len(missing),
                expected=0,
                message=(
                    f"Configured scenarios without rows: "
                    f"{'; '.join(missing)}.{note}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"All tables hold the configured chain scenarios "
                f"{', '.join(expected) or '(none)'}.{note}"
            ),
        )


class HeatPumpMinimumRows(_HeatPumpRule):
    """
    Check that a scenario has at least a minimum number of rows.

    Catches scenarios that were written only in part, e.g. because one of
    the parallel MV grid bulks failed. The expected minimum is measured per
    dataset boundary and passed via
    :func:`~egon.data.validation.resolve_boundary_dependence`.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, min_rows, **kwargs
    ):
        """
        Parameters
        ----------
        min_rows : int or BoundaryDependent
            Minimum number of rows of the scenario.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            min_rows=min_rows,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT count(*) AS n_rows
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_rows = int(df["n_rows"].values[0])
        min_rows = int(self.params["min_rows"])
        if n_rows == 0:
            return self._no_rows()
        success = n_rows >= min_rows
        return self._result(
            success=success,
            observed=n_rows,
            expected=min_rows,
            message=(
                f"Scenario '{self.scenario}': {n_rows:,} rows "
                f"({'at least' if success else 'fewer than'} the minimum "
                f"of {min_rows:,})"
            ),
        )


class HeatPeakLoadRange(_HeatPumpRule):
    """
    Check the range of the building peak heat loads of a scenario.

    Negative peak loads and peak loads above ``max_peak_load_w`` fail.
    Peak loads of exactly 0 W are reported, but are not an error: such
    buildings are excluded from the heat pump candidates.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        max_peak_load_w: float = MAX_PEAK_LOAD_W,
        **kwargs,
    ):
        """
        Parameters
        ----------
        max_peak_load_w : float
            Largest plausible peak load of a building in W.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            max_peak_load_w=max_peak_load_w,
            **kwargs,
        )

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "max_w": float(self.params["max_peak_load_w"]),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE peak_load_in_w < 0) AS n_negative,
            count(*) FILTER (WHERE peak_load_in_w > :max_w) AS n_too_high,
            count(*) FILTER (WHERE peak_load_in_w = 0) AS n_zero,
            max(peak_load_in_w) AS max_w
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        n_bad = int(row.n_negative) + int(row.n_too_high)
        max_w = float(self.params["max_peak_load_w"])
        message = (
            f"Scenario '{self.scenario}': {int(row.n_negative)} negative "
            f"peak loads, {int(row.n_too_high)} above {max_w / 1e6:.0f} MW "
            f"(max {float(row.max_w) / 1e6:.2f} MW), {int(row.n_zero)} of "
            f"{int(row.n_rows):,} buildings with a peak load of 0 W"
        )
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class HeatPumpCapacityRange(_HeatPumpRule):
    """
    Check the range of the heat pump capacities of a scenario.

    Every heat pump must be at least ``MIN_HP_CAPACITY`` of the dataset.
    The upper bound is derived from the largest plausible peak load via the
    sizing rule, so a larger heat pump can only stem from an implausible
    peak load or a floor error.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        max_peak_load_w: float = MAX_PEAK_LOAD_W,
        **kwargs,
    ):
        """
        Parameters
        ----------
        max_peak_load_w : float
            Largest plausible peak load of a building in W, from which the
            upper bound of the heat pump capacity is derived.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            max_peak_load_w=max_peak_load_w,
            **kwargs,
        )

    def _bounds(self):
        from egon.data.datasets.heat_supply.individual_heating import (
            MIN_HP_CAPACITY,
        )

        max_mw = (
            float(self.params["max_peak_load_w"]) / 1e6 * _hp_sizing_factor()
        )
        return float(MIN_HP_CAPACITY), max_mw

    def get_params(self, ctx):
        min_mw, max_mw = self._bounds()
        return {
            "scenario": self.scenario,
            "min_mw": min_mw * (1 - RTOL_ROUNDING),
            "max_mw": max_mw,
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE hp_capacity < :min_mw) AS n_too_small,
            count(*) FILTER (WHERE hp_capacity > :max_mw) AS n_too_large,
            min(hp_capacity) AS min_mw,
            max(hp_capacity) AS max_mw
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        min_mw, max_mw = self._bounds()
        n_bad = int(row.n_too_small) + int(row.n_too_large)
        message = (
            f"Scenario '{self.scenario}': {int(row.n_too_small)} heat pumps "
            f"below {min_mw * 1e3:.0f} kW and {int(row.n_too_large)} above "
            f"{max_mw:.1f} MW (observed {float(row.min_mw) * 1e3:.1f} kW to "
            f"{float(row.max_mw):.2f} MW, {int(row.n_rows):,} heat pumps)"
        )
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class HeatPumpPeakLoadExists(_HeatPumpRule):
    """
    Check that every building with a heat pump has a peak load.

    The heat pump is sized from the building's peak load of the same
    scenario, so a missing peak load row means the capacity has no basis.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        peak_load_table: str = PEAK_LOAD_TABLE,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            peak_load_table=peak_load_table,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE p.building_id IS NULL) AS n_missing,
            (array_agg(h.building_id ORDER BY h.building_id)
                FILTER (WHERE p.building_id IS NULL))[1:5] AS examples
        FROM {self.table} h
        LEFT JOIN {self.params["peak_load_table"]} p
            ON p.building_id = h.building_id
            AND p.scenario = h.scenario
        WHERE h.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        n_missing = int(row.n_missing)
        message = (
            f"Scenario '{self.scenario}': {n_missing} of {int(row.n_rows):,} "
            f"heat pump buildings have no peak load"
            + (f" (e.g. {_examples(row.examples)})" if n_missing else "")
        )
        return self._result(
            success=n_missing == 0,
            observed=n_missing,
            expected=0,
            message=message,
        )


class HeatPumpSizing(_HeatPumpRule):
    """
    Check that every heat pump covers its building's peak load.

    The capacity must be at least the minimum capacity the dataset's sizing
    rule (:func:`~egon.data.datasets.heat_supply.individual_heating.
    determine_minimum_hp_capacity_per_building`) requires for the
    building's peak load of the same scenario. This also holds for heat
    pumps inherited along the floor chain, which are raised where the
    building's demand has grown.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        peak_load_table: str = PEAK_LOAD_TABLE,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            peak_load_table=peak_load_table,
            **kwargs,
        )

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "factor": _hp_sizing_factor(),
            "rtol": RTOL_ROUNDING,
        }

    def get_query(self, ctx):
        return f"""
        WITH s AS (
            SELECT
                h.building_id,
                h.hp_capacity,
                p.peak_load_in_w / 1e6 * :factor AS min_capacity
            FROM {self.table} h
            JOIN {self.params["peak_load_table"]} p
                ON p.building_id = h.building_id
                AND p.scenario = h.scenario
            WHERE h.scenario = :scenario
        )
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (
                WHERE hp_capacity < min_capacity * (1 - :rtol)
            ) AS n_undersized,
            min(hp_capacity / nullif(min_capacity, 0)) AS min_ratio,
            (array_agg(building_id ORDER BY building_id) FILTER (
                WHERE hp_capacity < min_capacity * (1 - :rtol)
            ))[1:5] AS examples
        FROM s
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        n_bad = int(row.n_undersized)
        min_ratio = (
            f"{float(row.min_ratio):.4f}" if pd.notna(row.min_ratio) else "-"
        )
        message = (
            f"Scenario '{self.scenario}': {n_bad} of {int(row.n_rows):,} "
            f"heat pumps are smaller than the sizing rule requires "
            f"(min capacity / required = {min_ratio})"
            + (f", e.g. buildings {_examples(row.examples)}" if n_bad else "")
        )
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class HeatPumpCapacityTarget(_HeatPumpRule):
    """
    Compare the distributed heat pump capacity with the scenario target.

    The target is taken from ``supply.egon_scenario_capacities`` on the
    level the dataset distributes it on (see
    :func:`~egon.data.datasets.heat_supply.individual_heating.
    cascade_per_technology`):

    * status quo scenarios: one national target (carrier
      ``rural_heat_pump``),
    * other scenarios: one target per federal state (carrier
      ``residential_rural_heat_pump``), compared with the capacity of the
      buildings in the MV grids of that state.

    Deviations within ``rtol`` are expected: capacity that does not fit
    into any building is not assigned, and the floor chain can raise a
    grid above its target.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        rtol: float = 1e-3,
        **kwargs,
    ):
        """
        Parameters
        ----------
        rtol : float
            Relative tolerance per target (default: 1e-3 = 0.1 %).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )

    def _national(self):
        return "status" in self.scenario

    def get_query(self, ctx):
        if self._national():
            return f"""
            SELECT
                'Germany' AS region,
                (SELECT sum(capacity)
                 FROM supply.egon_scenario_capacities
                 WHERE scenario_name = :scenario
                 AND carrier = 'rural_heat_pump') AS target,
                (SELECT sum(hp_capacity)
                 FROM {self.table}
                 WHERE scenario = :scenario) AS observed
            """
        return f"""
        WITH target AS (
            SELECT DISTINCT ON (b.gen) b.gen AS region, a.capacity AS target
            FROM supply.egon_scenario_capacities a
            JOIN boundaries.vg250_lan b ON a.nuts = b.nuts
            WHERE a.scenario_name = :scenario
            AND a.carrier = 'residential_rural_heat_pump'
        ),
        building_grid AS (
            SELECT DISTINCT building_id, bus_id
            FROM boundaries.egon_map_zensus_mvgd_buildings
        ),
        observed AS (
            SELECT m.vg250_lan AS region, sum(h.hp_capacity) AS observed
            FROM {self.table} h
            JOIN building_grid g ON g.building_id = h.building_id
            JOIN boundaries.egon_map_mvgriddistrict_vg250 m
                ON m.bus_id = g.bus_id
            WHERE h.scenario = :scenario
            GROUP BY 1
        )
        SELECT region, t.target, o.observed
        FROM target t
        FULL JOIN observed o USING (region)
        -- vg250_lan also lists the Lake Constance parts of two states,
        -- which have no MV grids and are ignored by the dataset as well
        WHERE region IN (
            SELECT DISTINCT vg250_lan
            FROM boundaries.egon_map_mvgriddistrict_vg250
        )
        """

    def evaluate_df(self, df, ctx):
        rtol = float(self.params["rtol"])
        df = df.fillna(0.0)
        if df["observed"].sum() == 0:
            return self._no_rows()

        deviations = []
        for row in df.itertuples():
            target, observed = float(row.target), float(row.observed)
            if abs(observed - target) > rtol * max(abs(target), 1e-9):
                deviation = (
                    f"{(observed - target) / target * 100:+.3f} %"
                    if target
                    else "no target"
                )
                deviations.append(
                    f"{row.region}: {observed:.1f} MW vs {target:.1f} MW "
                    f"({deviation})"
                )

        level = "national" if self._national() else "federal state"
        total_target = float(df["target"].sum())
        total_observed = float(df["observed"].sum())
        summary = (
            f"Scenario '{self.scenario}' ({level} targets): "
            f"{total_observed:.1f} MW distributed vs {total_target:.1f} MW "
            f"target in {len(df)} region(s)"
        )
        if deviations:
            return self._result(
                success=False,
                observed=len(deviations),
                expected=0,
                message=(
                    f"{summary}; deviations above {rtol:.1%}: "
                    f"{'; '.join(deviations)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=f"{summary}, all within {rtol:.1%}",
        )


class HeatPumpFloorContainment(_FloorChainRule):
    """
    Check that no heat pump is lost along the floor chain without reason.

    Every building with a heat pump in the predecessor must keep it in the
    scenario, unless it is no longer a candidate for individual heating:

    * ``moved_to_dh`` -- all zensus cells of the building lie in a district
      heating area of the scenario,
    * ``no_peak_load`` -- the building has no peak load in the scenario,
      i.e. no heat demand.

    Both are reported, but are not an error. A building that is still a
    candidate but lost its heat pump (``unexplained``) is a floor
    violation. A building without any zensus cell (``no_zensus_cell``)
    cannot be classified; as the dataset finds every candidate through its
    cells, this means the rule no longer matches the dataset. Both fail,
    as does a classification that does not add up.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        peak_load_table: str = PEAK_LOAD_TABLE,
        dh_map_table: str = DH_MAP_TABLE,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            peak_load_table=peak_load_table,
            dh_map_table=dh_map_table,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH lost AS (
            SELECT building_id FROM {self.table}
            WHERE scenario = :predecessor
            EXCEPT
            SELECT building_id FROM {self.table}
            WHERE scenario = :scenario
        ),
        cells AS (
            SELECT l.building_id, c.zensus_population_id
            FROM lost l
            JOIN ({BUILDING_CELLS_SQL}) c ON c.building_id = l.building_id
        ),
        dh_cells AS (
            SELECT DISTINCT zensus_population_id
            FROM {self.params["dh_map_table"]}
            WHERE scenario = :dh_scenario
        ),
        building_dh AS (
            SELECT c.building_id,
                   bool_and(d.zensus_population_id IS NOT NULL) AS all_dh
            FROM cells c
            LEFT JOIN dh_cells d
                ON d.zensus_population_id = c.zensus_population_id
            GROUP BY 1
        ),
        peak AS (
            SELECT building_id FROM {self.params["peak_load_table"]}
            WHERE scenario = :scenario
        ),
        classified AS (
            SELECT
                l.building_id,
                b.building_id IS NOT NULL AS has_cell,
                coalesce(b.all_dh, false) AS all_dh,
                p.building_id IS NOT NULL AS has_peak
            FROM lost l
            LEFT JOIN building_dh b ON b.building_id = l.building_id
            LEFT JOIN peak p ON p.building_id = l.building_id
        )
        SELECT
            (SELECT count(*) FROM {self.table}
             WHERE scenario = :predecessor) AS n_predecessor,
            (SELECT count(*) FROM {self.table}
             WHERE scenario = :scenario) AS n_scenario,
            count(*) AS n_lost,
            count(*) FILTER (WHERE all_dh) AS n_moved_to_dh,
            count(*) FILTER (WHERE NOT all_dh AND NOT has_peak)
                AS n_no_peak_load,
            count(*) FILTER (WHERE NOT all_dh AND has_peak AND NOT has_cell)
                AS n_no_zensus_cell,
            count(*) FILTER (WHERE NOT all_dh AND has_peak AND has_cell)
                AS n_unexplained,
            (array_agg(building_id ORDER BY building_id) FILTER (
                WHERE NOT all_dh AND has_peak
            ))[1:5] AS examples
        FROM classified
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_predecessor) == 0:
            return self._skip(
                f"Predecessor '{self.predecessor}' has no heat pumps; "
                f"check skipped"
            )
        if int(row.n_scenario) == 0:
            return self._no_rows()

        n_lost = int(row.n_lost)
        counts = {
            "moved to district heating": int(row.n_moved_to_dh),
            "no peak load": int(row.n_no_peak_load),
            "no zensus cell": int(row.n_no_zensus_cell),
            "unexplained": int(row.n_unexplained),
        }
        n_bad = counts["unexplained"] + counts["no zensus cell"]
        complete = sum(counts.values()) == n_lost

        message = (
            f"{self.predecessor} -> {self.scenario}: "
            f"{int(row.n_predecessor) - n_lost:,} of "
            f"{int(row.n_predecessor):,} heat pumps kept, {n_lost:,} lost ("
            + ", ".join(f"{v:,} {k}" for k, v in counts.items())
            + ")"
        )
        if n_bad:
            message += f"; e.g. buildings {_examples(row.examples)}"
        if not complete:
            message += "; classification does not add up to the lost total"
        return self._result(
            success=n_bad == 0 and complete,
            observed=n_bad,
            expected=0,
            message=message,
        )


class HeatPumpFloorNotReduced(_FloorChainRule):
    """
    Check that inherited heat pumps never shrink along the floor chain.

    A building with a heat pump in both the predecessor and the scenario
    must have at least the predecessor's capacity: the inherited capacity
    is a lower bound that may only be raised where demand has grown.
    """

    def get_params(self, ctx):
        params = super().get_params(ctx)
        params["rtol"] = RTOL_ROUNDING
        return params

    def get_query(self, ctx):
        return f"""
        SELECT
            (SELECT count(*) FROM {self.table}
             WHERE scenario = :scenario) AS n_scenario,
            count(*) AS n_kept,
            count(*) FILTER (
                WHERE b.hp_capacity < a.hp_capacity * (1 - :rtol)
            ) AS n_reduced,
            count(*) FILTER (
                WHERE b.hp_capacity > a.hp_capacity * (1 + :rtol)
            ) AS n_raised,
            (array_agg(a.building_id ORDER BY a.building_id) FILTER (
                WHERE b.hp_capacity < a.hp_capacity * (1 - :rtol)
            ))[1:5] AS examples
        FROM {self.table} a
        JOIN {self.table} b ON b.building_id = a.building_id
        WHERE a.scenario = :predecessor
        AND b.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_scenario) == 0:
            return self._no_rows()
        n_reduced = int(row.n_reduced)
        message = (
            f"{self.predecessor} -> {self.scenario}: of {int(row.n_kept):,} "
            f"inherited heat pumps, {n_reduced} reduced and "
            f"{int(row.n_raised):,} raised"
            + (
                f"; e.g. buildings {_examples(row.examples)}"
                if n_reduced
                else ""
            )
        )
        return self._result(
            success=n_reduced == 0,
            observed=n_reduced,
            expected=0,
            message=message,
        )


class HeatPumpNotInDistrictHeating(_HeatPumpRule):
    """
    Check that no building with a heat pump lies in a district heating area.

    Mirrors the candidate selection of the dataset: a building is supplied
    by district heating if all of its zensus cells lie in a district
    heating area of the scenario. Such a building must not have a heat
    pump as well.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        dh_map_table: str = DH_MAP_TABLE,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            dh_map_table=dh_map_table,
            **kwargs,
        )

    def get_params(self, ctx):
        from egon.data.datasets.scenario_parameters import (
            demand_source_scenario,
        )

        return {
            "scenario": self.scenario,
            "dh_scenario": demand_source_scenario(self.scenario),
        }

    def get_query(self, ctx):
        return f"""
        WITH hp AS (
            SELECT building_id FROM {self.table}
            WHERE scenario = :scenario
        ),
        dh_cells AS (
            SELECT DISTINCT zensus_population_id
            FROM {self.params["dh_map_table"]}
            WHERE scenario = :dh_scenario
        ),
        building_dh AS (
            SELECT h.building_id,
                   bool_and(d.zensus_population_id IS NOT NULL) AS all_dh
            FROM hp h
            JOIN ({BUILDING_CELLS_SQL}) c ON c.building_id = h.building_id
            LEFT JOIN dh_cells d
                ON d.zensus_population_id = c.zensus_population_id
            GROUP BY 1
        )
        SELECT
            (SELECT count(*) FROM hp) AS n_rows,
            count(*) FILTER (WHERE all_dh) AS n_in_dh,
            (array_agg(building_id ORDER BY building_id)
                FILTER (WHERE all_dh))[1:5] AS examples
        FROM building_dh
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        n_in_dh = int(row.n_in_dh)
        message = (
            f"Scenario '{self.scenario}': {n_in_dh} of {int(row.n_rows):,} "
            f"heat pump buildings lie in a district heating area"
            + (f" (e.g. {_examples(row.examples)})" if n_in_dh else "")
        )
        return self._result(
            success=n_in_dh == 0,
            observed=n_in_dh,
            expected=0,
            message=message,
        )


class HeatTimeseriesCoverage(_HeatPumpRule):
    """
    Check that every MV grid has one time series per expected carrier.

    The dataset writes a ``heat_pump`` series for every MV grid with heat
    demand, plus a ``CH4`` series for the gas boilers in scenarios other
    than the status quo -- also for grids without heat pumps or gas
    boilers, whose series are zero. The MV grids are selected like in
    :func:`~egon.data.datasets.heat_supply.individual_heating.
    split_mvgds_into_bulks`.
    """

    def _carriers(self):
        if "status" in self.scenario:
            return ["heat_pump"]
        return ["heat_pump", "CH4"]

    def get_query(self, ctx):
        return f"""
        SELECT 'grid' AS source, m.bus_id, NULL AS carrier
        FROM (
            SELECT DISTINCT g.bus_id
            FROM boundaries.egon_map_zensus_grid_districts g
            JOIN demand.egon_peta_heat p
                ON p.zensus_population_id = g.zensus_population_id
        ) m
        UNION ALL
        SELECT 'series', bus_id, carrier
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        grids = set(df.loc[df.source == "grid", "bus_id"])
        series = df.loc[df.source == "series"]
        if series.empty:
            return self._no_rows()

        problems = []
        for carrier in self._carriers():
            have = set(series.loc[series.carrier == carrier, "bus_id"])
            missing = sorted(grids - have)
            extra = sorted(have - grids)
            if missing:
                problems.append(
                    f"{len(missing)} grids without '{carrier}' series "
                    f"(e.g. {_examples(missing)})"
                )
            if extra:
                problems.append(
                    f"{len(extra)} '{carrier}' series for unknown grids "
                    f"(e.g. {_examples(extra)})"
                )
        unexpected = sorted(set(series.carrier) - set(self._carriers()))
        if unexpected:
            problems.append(f"unexpected carriers {', '.join(unexpected)}")

        message = (
            f"Scenario '{self.scenario}': {len(grids):,} MV grids, carriers "
            f"{', '.join(self._carriers())}"
        )
        if problems:
            return self._result(
                success=False,
                observed=len(problems),
                expected=0,
                message=f"{message}; {'; '.join(problems)}",
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=f"{message}, one series per grid and carrier",
        )


class HeatTimeseriesValues(_HeatPumpRule):
    """
    Check the values inside the time series arrays.

    No value may be NULL, NaN or negative. The array length is checked
    separately by an ``ArrayCardinalityValidation``.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM unnest(dist_aggregated_mw) v
                WHERE v IS NULL OR v = 'NaN'
            )) AS n_nan,
            count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM unnest(dist_aggregated_mw) v WHERE v < 0
            )) AS n_negative
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        n_bad = int(row.n_nan) + int(row.n_negative)
        message = (
            f"Scenario '{self.scenario}': {int(row.n_nan)} series with "
            f"NULL/NaN values and {int(row.n_negative)} with negative "
            f"values out of {int(row.n_rows):,}"
        )
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class HeatTimeseriesHeatPumpCapacity(_HeatPumpRule):
    """
    Check the heat pump time series against the heat pump capacity.

    Per MV grid, the ``heat_pump`` series must be non-zero exactly if the
    grid has heat pump capacity, and its peak heat load must not exceed
    what the installed heat pumps can supply according to the sizing rule,
    i.e. ``sum(hp_capacity) / factor`` of
    :func:`~egon.data.datasets.heat_supply.individual_heating.
    determine_minimum_hp_capacity_per_building`.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        hp_capacity_table: str = HP_CAPACITY_TABLE,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            hp_capacity_table=hp_capacity_table,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH building_grid AS (
            SELECT DISTINCT building_id, bus_id
            FROM boundaries.egon_map_zensus_mvgd_buildings
        ),
        capacity AS (
            SELECT g.bus_id, sum(h.hp_capacity) AS capacity
            FROM {self.params["hp_capacity_table"]} h
            JOIN building_grid g ON g.building_id = h.building_id
            WHERE h.scenario = :scenario
            GROUP BY 1
        ),
        series AS (
            SELECT
                bus_id,
                (SELECT max(v) FROM unnest(dist_aggregated_mw) v) AS peak,
                (SELECT sum(v) FROM unnest(dist_aggregated_mw) v) AS energy
            FROM {self.table}
            WHERE scenario = :scenario
            AND carrier = 'heat_pump'
        )
        SELECT bus_id, s.peak, s.energy, c.capacity
        FROM series s
        FULL JOIN capacity c USING (bus_id)
        """

    def evaluate_df(self, df, ctx):
        df = df.fillna(0.0)
        if df.empty:
            return self._no_rows()

        factor = _hp_sizing_factor()
        series_without_capacity = df[(df.energy > 0) & (df.capacity <= 0)]
        capacity_without_series = df[(df.capacity > 0) & (df.energy <= 0)]
        over_capacity = df[
            (df.capacity > 0)
            & (df.peak > df.capacity / factor * (1 + RTOL_ROUNDING))
        ]

        problems = []
        for label, rows in [
            (
                "grids with a heat pump series but no heat pumps",
                series_without_capacity,
            ),
            (
                "grids with heat pumps but no heat pump series",
                capacity_without_series,
            ),
            ("grids whose peak exceeds the heat pump capacity", over_capacity),
        ]:
            if len(rows):
                problems.append(
                    f"{len(rows)} {label} (e.g. {_examples(rows.bus_id)})"
                )

        with_hp = df[df.capacity > 0]
        max_ratio = (
            float((with_hp.peak / (with_hp.capacity / factor)).max())
            if len(with_hp)
            else 0.0
        )
        message = (
            f"Scenario '{self.scenario}': {len(with_hp):,} grids with heat "
            f"pumps, max peak / supplyable heat = {max_ratio:.3f}"
        )
        if problems:
            return self._result(
                success=False,
                observed=len(problems),
                expected=0,
                message=f"{message}; {'; '.join(problems)}",
            )
        return self._result(
            success=True, observed=0, expected=0, message=message
        )


class HeatTimeseriesEnergyBalance(_HeatPumpRule):
    """
    Compare the annual energy of the time series with the heat demand.

    The ``heat_pump`` and ``CH4`` series of a scenario together must add up
    to the decentral heat demand: residential and CTS demand of
    ``demand.egon_peta_heat`` outside district heating areas. Demand of
    cells without buildings or mappings is lost when the building profiles
    are built, so the series stay slightly below the demand.

    Only meaningful for scenarios that write both carriers, i.e. not for
    the status quo.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        min_share: float = 0.95,
        max_share: float = 1.001,
        dh_map_table: str = DH_MAP_TABLE,
        **kwargs,
    ):
        """
        Parameters
        ----------
        min_share, max_share : float
            Accepted range of series energy / decentral heat demand.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            min_share=min_share,
            max_share=max_share,
            dh_map_table=dh_map_table,
            **kwargs,
        )

    def get_params(self, ctx):
        from egon.data.datasets.scenario_parameters import (
            demand_source_scenario,
        )

        return {
            "scenario": self.scenario,
            "demand_scenario": demand_source_scenario(self.scenario),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            (SELECT count(*) FROM {self.table}
             WHERE scenario = :scenario) AS n_rows,
            (SELECT sum((SELECT sum(v) FROM unnest(dist_aggregated_mw) v))
             FROM {self.table}
             WHERE scenario = :scenario) AS series_mwh,
            (SELECT sum(p.demand)
             FROM demand.egon_peta_heat p
             WHERE p.scenario = :demand_scenario
             AND p.sector IN ('residential', 'service')
             AND NOT EXISTS (
                 SELECT 1 FROM {self.params["dh_map_table"]} d
                 WHERE d.zensus_population_id = p.zensus_population_id
                 AND d.scenario = :demand_scenario
             )) AS demand_mwh
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        series = float(row.series_mwh or 0.0)
        demand = float(row.demand_mwh or 0.0)
        if demand <= 0:
            return self._result(
                success=False,
                message=(
                    f"Scenario '{self.scenario}': no decentral heat demand "
                    f"found to compare with"
                ),
            )
        share = series / demand
        min_share = float(self.params["min_share"])
        max_share = float(self.params["max_share"])
        success = min_share <= share <= max_share
        return self._result(
            success=success,
            observed=round(share, 4),
            expected=min_share,
            message=(
                f"Scenario '{self.scenario}': time series hold "
                f"{series / 1e6:.2f} TWh, decentral heat demand is "
                f"{demand / 1e6:.2f} TWh ({share:.2%}, accepted "
                f"{min_share:.0%} to {max_share:.1%})"
            ),
        )
