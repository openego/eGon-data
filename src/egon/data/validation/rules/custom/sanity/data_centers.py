"""
Sanity check validation rules for the data centers.

Validates everything :class:`~egon.data.datasets.data_centers_etrago.
DataCenters` writes into the eTraGo tables:

* ``grid.egon_etrago_load`` and ``grid.egon_etrago_load_timeseries`` -- one
  load per data center (``type = 'data_center'``) on an existing AC bus, with
  its hourly demand,
* ``grid.egon_etrago_generator`` and ``grid.egon_etrago_generator_timeseries``
  -- one ``data_center_waste_heat`` generator per data center close to a
  district heating area, whose availability follows the data center's load.

The allocated capacity of a data center is not stored. The waste heat rules
recover it from the generator, whose ``p_max_pu`` is the load profile divided
by the capacity and whose ``p_nom`` is ``REUSABLE_HEAT_FACTOR`` times the
capacity, so each generator is matched to the one load it reproduces.

Most rules check one scenario. A scenario that is not part of
``--scenarios`` is skipped (success, severity INFO). Reference values --
target capacity, full load hours, heat factor, size cap and voltages -- are
read from the dataset module when the rule runs, so they follow the code.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config

LOAD_TABLE = "grid.egon_etrago_load"
LOAD_TIMESERIES_TABLE = "grid.egon_etrago_load_timeseries"
GENERATOR_TABLE = "grid.egon_etrago_generator"
GENERATOR_TIMESERIES_TABLE = "grid.egon_etrago_generator_timeseries"

#: identify_voltage_level() puts a data center above this capacity on voltage
#: level 1, which assign_nearest_bus() maps to a 220 kV or 380 kV bus.
EHV_THRESHOLD_MW = 120

#: Relative tolerance for comparisons that only differ by float rounding
#: between the Python write path and the SQL checks.
RTOL_ROUNDING = 1e-6

#: Absolute tolerance in MW for the same comparisons on hourly values.
ATOL_ROUNDING_MW = 1e-6

#: SQL for the expected number of snapshots of a scenario, from its weather
#: year (leap years have 8784 hours).
EXPECTED_LENGTH_SQL = """
    (SELECT CASE
        WHEN ((global_parameters ->> 'weather_year')::int % 4 = 0
              AND (global_parameters ->> 'weather_year')::int % 100 <> 0)
          OR (global_parameters ->> 'weather_year')::int % 400 = 0
        THEN 8784 ELSE 8760 END
     FROM scenario.egon_scenario_parameters WHERE name = :scenario)
"""


def configured_scenarios():
    """Scenarios of the current run (``--scenarios``)."""
    return list(egon.data.config.settings()["egon-data"]["--scenarios"])


def _dataset():
    """The dataset module, imported late because it imports this module."""
    from egon.data.datasets import data_centers_etrago

    return data_centers_etrago


def _examples(values, n=5):
    """Up to ``n`` values as a comma-separated string."""
    values = [str(v) for v in list(values)[:n]]
    return ", ".join(values) if values else "-"


def _problems(counts):
    """Join the non-zero entries of ``{description: count}``."""
    return "; ".join(
        f"{count} {description}"
        for description, count in counts.items()
        if count
    )


class _DataCenterRule(DataFrameRule):
    """
    Common base for the data center rules.

    Queries return exactly one row, or one row per component, because the
    framework reports an empty result as a failure.
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
        # The eTraGo tables are shared by many datasets and never empty, so
        # the framework's full-table count only costs time.
        return None

    def evaluate(self, engine, ctx):
        if (
            self.scenario is not None
            and self.scenario not in configured_scenarios()
        ):
            return self._result(
                True,
                f"Scenario '{self.scenario}' is not part of --scenarios; "
                "check skipped",
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


class DataCenterScenarioCoverage(_DataCenterRule):
    """
    Validate in which scenarios data center components exist.

    Every configured scenario with a data center target capacity needs data
    center loads, the others must have no data center load and no waste heat
    generator.
    """

    def __init__(self, rule_id: str, **kwargs):
        super().__init__(table=LOAD_TABLE, rule_id=rule_id, **kwargs)

    def get_query(self, ctx):
        # The last row makes sure the result is never empty.
        return f"""
        SELECT scn_name, n_loads, n_generators
        FROM (
            SELECT s.scn_name,
                (SELECT count(*) FROM {LOAD_TABLE} l
                 WHERE l.scn_name = s.scn_name
                   AND l.type = 'data_center') AS n_loads,
                (SELECT count(*) FROM {GENERATOR_TABLE} g
                 WHERE g.scn_name = s.scn_name
                   AND g.carrier = 'data_center_waste_heat')
                    AS n_generators
            FROM (SELECT DISTINCT scn_name FROM grid.egon_etrago_bus) s
        ) AS per_scenario
        UNION ALL
        SELECT NULL, 0, 0
        """

    def get_params(self, ctx):
        return {}

    def evaluate_df(self, df, ctx):
        df = df.dropna(subset=["scn_name"]).set_index("scn_name")
        targets = set(_dataset().TARGET_CAPACITY_MW)
        expected = sorted(set(configured_scenarios()) & targets)

        missing = [
            scn
            for scn in expected
            if scn not in df.index or df.at[scn, "n_loads"] == 0
        ]
        unexpected = df[
            ~df.index.isin(expected)
            & ((df["n_loads"] > 0) | (df["n_generators"] > 0))
        ]

        problems = []
        if missing:
            problems.append(
                f"no data center loads in configured target scenario(s) "
                f"{_examples(missing)}"
            )
        if not unexpected.empty:
            problems.append(
                "data center loads or waste heat generators in scenario(s) "
                f"without a target or not configured: "
                f"{_examples(unexpected.index)}"
            )

        found = _examples(
            f"{scn}: {int(df.at[scn, 'n_loads'])} loads, "
            f"{int(df.at[scn, 'n_generators'])} waste heat generators"
            for scn in expected
            if scn in df.index
        )
        if problems:
            return self._result(
                False,
                "Data center coverage: " + "; ".join(problems),
                observed=float(len(problems)),
                expected=0.0,
            )
        return self._result(
            True,
            f"Data centers in exactly the configured target scenarios "
            f"({found})",
            observed=0.0,
            expected=0.0,
        )


class DataCenterLoads(_DataCenterRule):
    """
    Validate the data center loads of one scenario.

    Every load has carrier ``data_center``, ``sign`` -1 and no static
    ``p_set`` or ``q_set``, because the demand is only in the time series.
    It sits on an existing German AC bus at one of the connection voltages.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=LOAD_TABLE, rule_id=rule_id, scenario=scenario, **kwargs
        )

    def get_query(self, ctx):
        voltages = ", ".join(
            str(v) for v in _dataset().CONNECTION_VOLTAGES
        )
        return f"""
        SELECT
            count(*) AS n_loads,
            count(DISTINCT l.bus) AS n_buses,
            count(*) FILTER (WHERE l.carrier IS DISTINCT FROM 'data_center')
                AS wrong_carrier,
            count(*) FILTER (WHERE l.sign IS DISTINCT FROM -1) AS wrong_sign,
            count(*) FILTER (WHERE l.p_set IS NOT NULL) AS static_p_set,
            count(*) FILTER (WHERE l.q_set IS NOT NULL) AS static_q_set,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS bus_missing,
            count(*) FILTER (WHERE b.carrier IS DISTINCT FROM 'AC')
                AS bus_not_ac,
            count(*) FILTER (WHERE b.country IS DISTINCT FROM 'DE')
                AS bus_not_de,
            count(*) FILTER (WHERE b.v_nom IS NULL
                             OR b.v_nom NOT IN ({voltages}))
                AS bus_wrong_voltage,
            string_agg(DISTINCT b.v_nom::int::text, ', ') AS voltages,
            (array_agg(l.load_id ORDER BY l.load_id) FILTER (
                WHERE l.carrier IS DISTINCT FROM 'data_center'
                   OR l.sign IS DISTINCT FROM -1
                   OR l.p_set IS NOT NULL OR l.q_set IS NOT NULL
                   OR b.bus_id IS NULL
                   OR b.carrier IS DISTINCT FROM 'AC'
                   OR b.country IS DISTINCT FROM 'DE'
                   OR b.v_nom IS NULL OR b.v_nom NOT IN ({voltages})))[1:5]
                AS examples
        FROM {LOAD_TABLE} l
        LEFT JOIN grid.egon_etrago_bus b
          ON b.scn_name = l.scn_name AND b.bus_id = l.bus
        WHERE l.scn_name = :scenario AND l.type = 'data_center'
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n = int(row["n_loads"])
        if n == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' is configured but has no data "
                "center loads",
                observed=0.0,
            )

        problems = _problems(
            {
                "with a carrier other than 'data_center'": int(
                    row["wrong_carrier"]
                ),
                "with sign other than -1": int(row["wrong_sign"]),
                "with a static p_set": int(row["static_p_set"]),
                "with a static q_set": int(row["static_q_set"]),
                "on a bus that does not exist": int(row["bus_missing"]),
                "on a non-AC bus": int(row["bus_not_ac"]),
                "on a bus outside Germany": int(row["bus_not_de"]),
                "on a bus of another voltage": int(row["bus_wrong_voltage"]),
            }
        )
        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': of {n} data center loads, "
                f"{problems} (e.g. load_id {_examples(row['examples'])})",
                observed=float(n),
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': {n} data center loads with carrier "
            f"'data_center' and sign -1 on {int(row['n_buses'])} German AC "
            f"buses at {row['voltages']} kV",
            observed=float(n),
        )


class DataCenterLoadTimeseries(_DataCenterRule):
    """
    Validate the load time series of the data centers of one scenario.

    Every load has exactly one time series with ``temp_id`` 1 and no
    ``q_set``, and its ``p_set`` has one finite, non-negative value per
    snapshot of the scenario's weather year. No data center is idle all
    year.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=LOAD_TIMESERIES_TABLE,
            rule_id=rule_id,
            scenario=scenario,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH ts AS (
            SELECT l.load_id,
                count(t.load_id) AS n_series,
                bool_or(t.temp_id IS DISTINCT FROM 1) AS wrong_temp_id,
                bool_or(t.q_set IS NOT NULL) AS has_q_set,
                min(cardinality(t.p_set)) AS min_length,
                max(cardinality(t.p_set)) AS max_length,
                sum(v.n_null) AS n_null,
                sum(v.n_nan) AS n_nan,
                min(v.min_value) AS min_value,
                sum(v.total) AS total
            FROM {LOAD_TABLE} l
            LEFT JOIN {LOAD_TIMESERIES_TABLE} t
              ON t.scn_name = l.scn_name AND t.load_id = l.load_id
            LEFT JOIN LATERAL (
                SELECT count(*) FILTER (WHERE x IS NULL) AS n_null,
                    count(*) FILTER (WHERE x = 'NaN'::float8) AS n_nan,
                    min(x) FILTER (WHERE x <> 'NaN'::float8) AS min_value,
                    sum(x) FILTER (WHERE x <> 'NaN'::float8) AS total
                FROM unnest(t.p_set) AS x
            ) v ON true
            WHERE l.scn_name = :scenario AND l.type = 'data_center'
            GROUP BY l.load_id
        )
        SELECT
            count(*) AS n_loads,
            {EXPECTED_LENGTH_SQL} AS expected_length,
            count(*) FILTER (WHERE n_series <> 1) AS wrong_series_count,
            count(*) FILTER (WHERE wrong_temp_id) AS wrong_temp_id,
            count(*) FILTER (WHERE has_q_set) AS has_q_set,
            count(*) FILTER (WHERE min_length IS DISTINCT FROM
                                   {EXPECTED_LENGTH_SQL}
                             OR max_length IS DISTINCT FROM
                                   {EXPECTED_LENGTH_SQL})
                AS wrong_length,
            count(*) FILTER (WHERE n_null > 0) AS with_null,
            count(*) FILTER (WHERE n_nan > 0) AS with_nan,
            count(*) FILTER (WHERE min_value < 0) AS negative,
            count(*) FILTER (WHERE coalesce(total, 0) <= 0) AS idle,
            min(min_value) AS min_value,
            (array_agg(load_id ORDER BY load_id) FILTER (
                WHERE n_series <> 1 OR wrong_temp_id OR has_q_set
                   OR n_null > 0 OR n_nan > 0 OR min_value < 0
                   OR coalesce(total, 0) <= 0))[1:5] AS examples
        FROM ts
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n = int(row["n_loads"])
        if n == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' is configured but has no data "
                "center loads",
                observed=0.0,
            )

        expected_length = row["expected_length"]
        problems = _problems(
            {
                "without exactly one time series": int(
                    row["wrong_series_count"]
                ),
                "with temp_id other than 1": int(row["wrong_temp_id"]),
                "with a q_set time series": int(row["has_q_set"]),
                f"with a p_set length other than {expected_length}": int(
                    row["wrong_length"]
                ),
                "with NULL values": int(row["with_null"]),
                "with NaN values": int(row["with_nan"]),
                "with negative values": int(row["negative"]),
                "with zero annual demand": int(row["idle"]),
            }
        )
        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': of {n} data center loads, "
                f"{problems} (e.g. load_id {_examples(row['examples'])})",
                observed=float(n),
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': every one of {n} data center "
            f"loads has one time series of {expected_length} finite, "
            "non-negative values",
            observed=float(n),
        )


class DataCenterAnnualDemand(_DataCenterRule):
    """
    Validate the annual data center demand of one scenario.

    ``scale_data_center_profiles`` scales the profiles to the allocated
    capacity times ``FULL_LOAD_HOURS``, and ``cap_and_redistribute_profiles``
    conserves that energy. The allocation places exactly the target capacity
    (``get_target_capacity``, which also scales the national target down in
    test mode), so the annual demand is fixed by the target.
    """

    def __init__(
        self, rule_id: str, scenario: str, rtol=RTOL_ROUNDING, **kwargs
    ):
        super().__init__(
            table=LOAD_TIMESERIES_TABLE,
            rule_id=rule_id,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT count(DISTINCT l.load_id) AS n_loads,
            sum(x) FILTER (WHERE x <> 'NaN'::float8) AS total
        FROM {LOAD_TABLE} l
        LEFT JOIN {LOAD_TIMESERIES_TABLE} t
          ON t.scn_name = l.scn_name AND t.load_id = l.load_id
        LEFT JOIN LATERAL unnest(t.p_set) AS x ON true
        WHERE l.scn_name = :scenario AND l.type = 'data_center'
        """

    def evaluate_df(self, df, ctx):
        dataset = _dataset()
        row = df.iloc[0]
        if int(row["n_loads"]) == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' is configured but has no data "
                "center loads",
                observed=0.0,
            )

        capacity = float(dataset.get_target_capacity(self.scenario))
        expected = capacity * dataset.FULL_LOAD_HOURS
        observed = float(row["total"] or 0)
        rtol = float(self.params.get("rtol", RTOL_ROUNDING))
        deviation = abs(observed - expected) / expected
        success = deviation <= rtol

        return self._result(
            success,
            f"Scenario '{self.scenario}': data center demand "
            f"{observed / 1e6:.4f} TWh vs expected {expected / 1e6:.4f} TWh "
            f"({capacity:,.1f} MW target x {dataset.FULL_LOAD_HOURS} full "
            f"load hours, deviation {deviation:.2e}, tolerance {rtol:.0e})",
            observed=observed,
            expected=expected,
        )


class DataCenterPeakLoads(_DataCenterRule):
    """
    Validate the peaks of the data center demand of one scenario.

    * The coincident peak of all data centers stays below the target
      capacity, as no data center draws more than its own capacity.
    * No data center peaks above the size cap ``MAX_RZ_SIZE``.
    * No data center on a 110 kV bus peaks above the extra high voltage
      threshold, because its capacity is below that threshold.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=LOAD_TIMESERIES_TABLE,
            rule_id=rule_id,
            scenario=scenario,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH dc AS (
            SELECT l.load_id, b.v_nom, t.p_set
            FROM {LOAD_TABLE} l
            LEFT JOIN grid.egon_etrago_bus b
              ON b.scn_name = l.scn_name AND b.bus_id = l.bus
            LEFT JOIN {LOAD_TIMESERIES_TABLE} t
              ON t.scn_name = l.scn_name AND t.load_id = l.load_id
            WHERE l.scn_name = :scenario AND l.type = 'data_center'
        ),
        per_load AS (
            SELECT dc.load_id, dc.v_nom,
                (SELECT max(x) FILTER (WHERE x <> 'NaN'::float8)
                 FROM unnest(dc.p_set) AS x) AS peak
            FROM dc
        ),
        hourly AS (
            SELECT h.i, sum(h.x) FILTER (WHERE h.x <> 'NaN'::float8) AS total
            FROM dc, unnest(dc.p_set) WITH ORDINALITY AS h(x, i)
            GROUP BY h.i
        )
        SELECT
            (SELECT count(*) FROM per_load) AS n_loads,
            (SELECT max(total) FROM hourly) AS coincident_peak,
            (SELECT max(peak) FROM per_load) AS max_peak,
            (SELECT max(peak) FROM per_load WHERE v_nom = :hv_voltage)
                AS max_hv_peak,
            (SELECT array_agg(load_id ORDER BY peak DESC)
             FROM per_load WHERE peak > :size_cap) AS above_cap,
            (SELECT array_agg(load_id ORDER BY peak DESC)
             FROM per_load
             WHERE v_nom = :hv_voltage AND peak > :ehv_threshold)
                AS hv_above_threshold
        """

    def get_params(self, ctx):
        dataset = _dataset()
        return {
            "scenario": self.scenario,
            "hv_voltage": dataset.HV_VOLTAGE,
            "size_cap": dataset.MAX_RZ_SIZE * (1 + RTOL_ROUNDING),
            "ehv_threshold": EHV_THRESHOLD_MW * (1 + RTOL_ROUNDING),
        }

    def evaluate_df(self, df, ctx):
        dataset = _dataset()
        row = df.iloc[0]
        if int(row["n_loads"]) == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' is configured but has no data "
                "center loads",
                observed=0.0,
            )

        capacity = float(dataset.get_target_capacity(self.scenario))
        coincident = float(row["coincident_peak"] or 0)
        above_cap = list(row["above_cap"] or [])
        hv_above = list(row["hv_above_threshold"] or [])

        problems = []
        if coincident > capacity * (1 + RTOL_ROUNDING):
            problems.append(
                f"coincident peak {coincident:,.1f} MW exceeds the target "
                f"capacity of {capacity:,.1f} MW"
            )
        if above_cap:
            problems.append(
                f"{len(above_cap)} data centers peak above the "
                f"{dataset.MAX_RZ_SIZE:,.0f} MW size cap "
                f"(load_id {_examples(above_cap)})"
            )
        if hv_above:
            problems.append(
                f"{len(hv_above)} data centers on {dataset.HV_VOLTAGE} kV "
                f"peak above {EHV_THRESHOLD_MW} MW "
                f"(load_id {_examples(hv_above)})"
            )

        summary = (
            f"coincident peak {coincident:,.1f} MW "
            f"({100 * coincident / capacity:.1f} % of {capacity:,.1f} MW), "
            f"largest single peak {float(row['max_peak'] or 0):,.1f} MW, "
            f"largest {dataset.HV_VOLTAGE} kV peak "
            f"{float(row['max_hv_peak'] or 0):,.1f} MW"
        )
        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': " + "; ".join(problems),
                observed=coincident,
                expected=capacity,
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': {summary}",
            observed=coincident,
            expected=capacity,
        )


class DataCenterWasteHeatGenerators(_DataCenterRule):
    """
    Validate the waste heat generators of one scenario.

    Every generator sits on an existing German ``central_heat`` bus, has a
    positive, fixed ``p_nom`` of at most ``REUSABLE_HEAT_FACTOR`` times the
    size cap, a marginal cost of zero, ``sign`` 1 and ``p_min_pu`` 0, so the
    heat can be used but never has to be. There is at most one generator per
    data center.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=GENERATOR_TABLE, rule_id=rule_id, scenario=scenario, **kwargs
        )

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_generators,
            (SELECT count(*) FROM {LOAD_TABLE}
             WHERE scn_name = :scenario AND type = 'data_center') AS n_loads,
            count(DISTINCT g.bus) AS n_buses,
            sum(g.p_nom) AS total_p_nom,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS bus_missing,
            count(*) FILTER (WHERE b.carrier IS DISTINCT FROM 'central_heat')
                AS bus_not_heat,
            count(*) FILTER (WHERE b.country IS DISTINCT FROM 'DE')
                AS bus_not_de,
            count(*) FILTER (WHERE g.p_nom IS NULL OR g.p_nom <= 0)
                AS p_nom_not_positive,
            count(*) FILTER (WHERE g.p_nom > :max_p_nom) AS p_nom_above_cap,
            count(*) FILTER (WHERE g.p_nom_extendable IS DISTINCT FROM false)
                AS extendable,
            count(*) FILTER (WHERE coalesce(g.marginal_cost, 0) <> 0)
                AS marginal_cost,
            count(*) FILTER (WHERE g.sign IS DISTINCT FROM 1) AS wrong_sign,
            count(*) FILTER (WHERE coalesce(g.p_min_pu, 0) <> 0)
                AS must_run,
            (array_agg(g.generator_id ORDER BY g.generator_id) FILTER (
                WHERE b.bus_id IS NULL
                   OR b.carrier IS DISTINCT FROM 'central_heat'
                   OR b.country IS DISTINCT FROM 'DE'
                   OR g.p_nom IS NULL OR g.p_nom <= 0
                   OR g.p_nom > :max_p_nom
                   OR g.p_nom_extendable IS DISTINCT FROM false
                   OR coalesce(g.marginal_cost, 0) <> 0
                   OR g.sign IS DISTINCT FROM 1
                   OR coalesce(g.p_min_pu, 0) <> 0))[1:5] AS examples
        FROM {GENERATOR_TABLE} g
        LEFT JOIN grid.egon_etrago_bus b
          ON b.scn_name = g.scn_name AND b.bus_id = g.bus
        WHERE g.scn_name = :scenario AND g.carrier = 'data_center_waste_heat'
        """

    def get_params(self, ctx):
        dataset = _dataset()
        return {
            "scenario": self.scenario,
            "max_p_nom": dataset.MAX_RZ_SIZE
            * dataset.REUSABLE_HEAT_FACTOR
            * (1 + RTOL_ROUNDING),
        }

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n = int(row["n_generators"])
        n_loads = int(row["n_loads"])
        if n == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' has {n_loads} data centers but "
                "no waste heat generator",
                observed=0.0,
            )

        problems = _problems(
            {
                "on a bus that does not exist": int(row["bus_missing"]),
                "on a bus other than central_heat": int(row["bus_not_heat"]),
                "on a bus outside Germany": int(row["bus_not_de"]),
                "with p_nom <= 0": int(row["p_nom_not_positive"]),
                "with p_nom above the heat share of the size cap": int(
                    row["p_nom_above_cap"]
                ),
                "extendable": int(row["extendable"]),
                "with a marginal cost": int(row["marginal_cost"]),
                "with sign other than 1": int(row["wrong_sign"]),
                "with a p_min_pu above 0": int(row["must_run"]),
            }
        )
        if n > n_loads:
            problems = "; ".join(
                p
                for p in [
                    problems,
                    f"{n} generators for only {n_loads} data centers",
                ]
                if p
            )
        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': of {n} waste heat generators, "
                f"{problems} (e.g. generator_id {_examples(row['examples'])})",
                observed=float(n),
                expected=float(n_loads),
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': {n} of {n_loads} data centers feed "
            f"{float(row['total_p_nom']):,.1f} MW of waste heat into "
            f"{int(row['n_buses'])} central heat buses",
            observed=float(n),
            expected=float(n_loads),
        )


class DataCenterWasteHeatTimeseries(_DataCenterRule):
    """
    Validate the time series of the waste heat generators of one scenario.

    Every generator has exactly one time series with ``temp_id`` 1 and only
    a ``p_max_pu``, with one finite value in [0, 1] per snapshot.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=GENERATOR_TIMESERIES_TABLE,
            rule_id=rule_id,
            scenario=scenario,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH ts AS (
            SELECT g.generator_id,
                count(t.generator_id) AS n_series,
                bool_or(t.temp_id IS DISTINCT FROM 1) AS wrong_temp_id,
                bool_or(t.p_set IS NOT NULL OR t.q_set IS NOT NULL
                        OR t.p_min_pu IS NOT NULL
                        OR t.marginal_cost IS NOT NULL) AS other_series,
                min(cardinality(t.p_max_pu)) AS min_length,
                max(cardinality(t.p_max_pu)) AS max_length,
                sum(v.n_null) AS n_null,
                sum(v.n_nan) AS n_nan,
                min(v.min_value) AS min_value,
                max(v.max_value) AS max_value
            FROM {GENERATOR_TABLE} g
            LEFT JOIN {GENERATOR_TIMESERIES_TABLE} t
              ON t.scn_name = g.scn_name AND t.generator_id = g.generator_id
            LEFT JOIN LATERAL (
                SELECT count(*) FILTER (WHERE x IS NULL) AS n_null,
                    count(*) FILTER (WHERE x = 'NaN'::float8) AS n_nan,
                    min(x) FILTER (WHERE x <> 'NaN'::float8) AS min_value,
                    max(x) FILTER (WHERE x <> 'NaN'::float8) AS max_value
                FROM unnest(t.p_max_pu) AS x
            ) v ON true
            WHERE g.scn_name = :scenario
              AND g.carrier = 'data_center_waste_heat'
            GROUP BY g.generator_id
        )
        SELECT
            count(*) AS n_generators,
            {EXPECTED_LENGTH_SQL} AS expected_length,
            count(*) FILTER (WHERE n_series <> 1) AS wrong_series_count,
            count(*) FILTER (WHERE wrong_temp_id) AS wrong_temp_id,
            count(*) FILTER (WHERE other_series) AS other_series,
            count(*) FILTER (WHERE min_length IS DISTINCT FROM
                                   {EXPECTED_LENGTH_SQL}
                             OR max_length IS DISTINCT FROM
                                   {EXPECTED_LENGTH_SQL})
                AS wrong_length,
            count(*) FILTER (WHERE n_null > 0) AS with_null,
            count(*) FILTER (WHERE n_nan > 0) AS with_nan,
            count(*) FILTER (WHERE min_value < 0) AS below_zero,
            count(*) FILTER (WHERE max_value > 1 + :atol) AS above_one,
            max(max_value) AS max_value,
            (array_agg(generator_id ORDER BY generator_id) FILTER (
                WHERE n_series <> 1 OR wrong_temp_id OR other_series
                   OR n_null > 0 OR n_nan > 0 OR min_value < 0
                   OR max_value > 1 + :atol))[1:5] AS examples
        FROM ts
        """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "atol": 1e-9}

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n = int(row["n_generators"])
        if n == 0:
            return self._result(
                False,
                f"Scenario '{self.scenario}' has no waste heat generator",
                observed=0.0,
            )

        expected_length = row["expected_length"]
        problems = _problems(
            {
                "without exactly one time series": int(
                    row["wrong_series_count"]
                ),
                "with temp_id other than 1": int(row["wrong_temp_id"]),
                "with a time series other than p_max_pu": int(
                    row["other_series"]
                ),
                f"with a p_max_pu length other than {expected_length}": int(
                    row["wrong_length"]
                ),
                "with NULL values": int(row["with_null"]),
                "with NaN values": int(row["with_nan"]),
                "with p_max_pu below 0": int(row["below_zero"]),
                "with p_max_pu above 1": int(row["above_one"]),
            }
        )
        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': of {n} waste heat generators, "
                f"{problems} (e.g. generator_id {_examples(row['examples'])})",
                observed=float(n),
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': every one of {n} waste heat "
            f"generators has one p_max_pu series of {expected_length} values "
            f"in [0, {float(row['max_value']):.4f}]",
            observed=float(n),
        )


class DataCenterWasteHeatProfile(_DataCenterRule):
    """
    Validate that each waste heat generator follows its own data center.

    ``p_max_pu`` is the load profile divided by the allocated capacity and
    ``p_nom`` is ``REUSABLE_HEAT_FACTOR`` times that capacity, so
    ``p_max_pu * p_nom / REUSABLE_HEAT_FACTOR`` reproduces the data center's
    hourly load. Each generator has to reproduce exactly one load, and no
    load may be reproduced twice. Through the match the capacity of each
    data center is known, which allows these checks as well:

    * the load never exceeds the capacity (``cap_and_redistribute_profiles``),
    * the capacity is at most ``MAX_RZ_SIZE``,
    * the voltage level follows the capacity: above the extra high voltage
      threshold at 220 kV or 380 kV, below it at 110 kV,
    * the waste heat is ``REUSABLE_HEAT_FACTOR`` of the demand,
    * if every data center has a generator, the capacities add up to the
      target capacity.

    Candidates are paired by their annual energy first, and only those pairs
    are compared hour by hour.
    """

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            table=GENERATOR_TIMESERIES_TABLE,
            rule_id=rule_id,
            scenario=scenario,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH g AS (
            SELECT g.generator_id, g.p_nom, t.p_max_pu,
                (SELECT sum(x) FROM unnest(t.p_max_pu) AS x) * g.p_nom
                    / :factor AS implied_energy
            FROM {GENERATOR_TABLE} g
            JOIN {GENERATOR_TIMESERIES_TABLE} t
              ON t.scn_name = g.scn_name AND t.generator_id = g.generator_id
            WHERE g.scn_name = :scenario
              AND g.carrier = 'data_center_waste_heat'
        ),
        l AS (
            SELECT l.load_id, b.v_nom, t.p_set,
                (SELECT sum(x) FROM unnest(t.p_set) AS x) AS energy,
                (SELECT max(x) FROM unnest(t.p_set) AS x) AS peak
            FROM {LOAD_TABLE} l
            JOIN {LOAD_TIMESERIES_TABLE} t
              ON t.scn_name = l.scn_name AND t.load_id = l.load_id
            LEFT JOIN grid.egon_etrago_bus b
              ON b.scn_name = l.scn_name AND b.bus_id = l.bus
            WHERE l.scn_name = :scenario AND l.type = 'data_center'
        ),
        candidates AS (
            SELECT g.generator_id, l.load_id
            FROM g JOIN l
              ON abs(g.implied_energy - l.energy)
                 <= :rtol * greatest(abs(l.energy), 1)
        ),
        matches AS (
            SELECT c.generator_id, c.load_id,
                max(abs(x.h * g.p_nom / :factor - x.p)) AS max_deviation
            FROM candidates c
            JOIN g ON g.generator_id = c.generator_id
            JOIN l ON l.load_id = c.load_id
            CROSS JOIN LATERAL unnest(g.p_max_pu, l.p_set) AS x(h, p)
            GROUP BY c.generator_id, c.load_id
            HAVING max(abs(x.h * g.p_nom / :factor - x.p)) <= :atol
        )
        SELECT g.generator_id, g.p_nom, g.implied_energy,
            m.load_id, m.max_deviation, l.v_nom, l.energy, l.peak,
            (SELECT count(*) FROM l) AS n_loads,
            (SELECT sum(energy) FROM l) AS total_energy
        FROM g
        LEFT JOIN matches m ON m.generator_id = g.generator_id
        LEFT JOIN l ON l.load_id = m.load_id
        ORDER BY g.generator_id
        """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "factor": _dataset().REUSABLE_HEAT_FACTOR,
            "rtol": RTOL_ROUNDING,
            "atol": ATOL_ROUNDING_MW,
        }

    def evaluate_df(self, df, ctx):
        dataset = _dataset()
        factor = dataset.REUSABLE_HEAT_FACTOR
        n_generators = df["generator_id"].nunique()
        n_loads = int(df["n_loads"].iloc[0] or 0)

        matches = df.groupby("generator_id")["load_id"].count()
        unmatched = matches[matches == 0].index.tolist()
        ambiguous = matches[matches > 1].index.tolist()
        matched = df.dropna(subset=["load_id"])
        reused = (
            matched["load_id"][matched["load_id"].duplicated()]
            .astype(int)
            .tolist()
        )

        problems = []
        if unmatched:
            problems.append(
                f"{len(unmatched)} generators reproduce no data center load "
                f"(generator_id {_examples(unmatched)})"
            )
        if ambiguous:
            problems.append(
                f"{len(ambiguous)} generators reproduce more than one load "
                f"(generator_id {_examples(ambiguous)})"
            )
        if reused:
            problems.append(
                f"{len(reused)} loads are reproduced by more than one "
                f"generator (load_id {_examples(reused)})"
            )

        pairs = matched.drop_duplicates("generator_id").copy()
        pairs["capacity"] = pairs["p_nom"].astype(float) / factor
        over_capacity = pairs[
            pairs["peak"].astype(float)
            > pairs["capacity"] * (1 + RTOL_ROUNDING)
        ]
        above_cap = pairs[
            pairs["capacity"] > dataset.MAX_RZ_SIZE * (1 + RTOL_ROUNDING)
        ]
        wrong_level = pairs[
            (pairs["capacity"] > EHV_THRESHOLD_MW)
            != pairs["v_nom"].isin(dataset.EHV_VOLTAGES)
        ]
        if not over_capacity.empty:
            problems.append(
                f"{len(over_capacity)} data centers draw more than their "
                f"capacity (load_id "
                f"{_examples(over_capacity['load_id'].astype(int))})"
            )
        if not above_cap.empty:
            problems.append(
                f"{len(above_cap)} data centers are larger than the "
                f"{dataset.MAX_RZ_SIZE:,.0f} MW size cap (load_id "
                f"{_examples(above_cap['load_id'].astype(int))})"
            )
        if not wrong_level.empty:
            problems.append(
                f"{len(wrong_level)} data centers sit on the wrong voltage "
                f"level for their capacity (load_id "
                f"{_examples(wrong_level['load_id'].astype(int))})"
            )

        heat = float((pairs["implied_energy"] * factor).sum())
        demand = float(pairs["energy"].astype(float).sum())
        if demand and abs(heat - factor * demand) > RTOL_ROUNDING * demand:
            problems.append(
                f"waste heat {heat / 1e6:.4f} TWh is not {factor:.0%} of the "
                f"{demand / 1e6:.4f} TWh demand of the matched data centers"
            )

        capacity_note = ""
        if n_generators == n_loads and not problems:
            target = float(dataset.get_target_capacity(self.scenario))
            total = float(pairs["capacity"].sum())
            if abs(total - target) > RTOL_ROUNDING * target:
                problems.append(
                    f"the capacities add up to {total:,.1f} MW instead of "
                    f"the {target:,.1f} MW target"
                )
            capacity_note = (
                f"; capacities add up to {total:,.1f} MW, the target"
            )
        elif n_generators < n_loads:
            capacity_note = (
                f"; {n_loads - n_generators} data centers without waste heat, "
                "so the total capacity is not checked"
            )

        if problems:
            return self._result(
                False,
                f"Scenario '{self.scenario}': " + "; ".join(problems),
                observed=float(len(pairs)),
                expected=float(n_generators),
            )
        return self._result(
            True,
            f"Scenario '{self.scenario}': each of {n_generators} waste heat "
            f"generators reproduces its own data center load (largest "
            f"deviation {matched['max_deviation'].astype(float).max():.1e} "
            f"MW); {heat / 1e6:.4f} TWh of heat is {factor:.0%} of the "
            f"matched demand; capacities, size cap and voltage levels are "
            f"consistent" + capacity_note,
            observed=float(len(pairs)),
            expected=float(n_generators),
        )
