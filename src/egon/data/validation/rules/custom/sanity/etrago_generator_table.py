"""
Sanity check validation rules for the complete eTraGo generator tables.

Unlike the rules in :mod:`.etrago_generators`, which only look at what one
dataset writes, these rules check ``grid.egon_etrago_generator`` and
``grid.egon_etrago_generator_timeseries`` as eTraGo reads them: every
generator of every dataset, German and foreign. They run in
``FinalValidations`` and fall into three groups:

* all generators: bus, ``p_nom``, static limits and time series,
* Germany: capacity per carrier against the national targets, unit size,
* other countries: every neighbour has generation, and enough of it.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config
from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)

#: Carriers whose ``p_max_pu`` comes from the weather data and therefore
#: needs a time series.
WEATHER_DEPENDENT_CARRIERS = (
    "solar",
    "solar_rooftop",
    "solar_thermal_collector",
    "wind_offshore",
    "wind_onshore",
)

#: Bus carrier of every generator carrier that does not feed an AC bus.
#: Carriers missing here must sit on an AC bus; carriers ending in
#: ``_CHP_heat`` must sit on a ``central_heat`` bus.
NON_AC_BUS_CARRIERS = {
    "CH4": "CH4",
    "O2": "O2",
    "data_center_waste_heat": "central_heat",
    "geo_thermal": "central_heat",
    "rural_solar_thermal": "rural_heat",
    "solar_thermal_collector": "central_heat",
}

#: Generator carriers that count towards each carrier of
#: ``supply.egon_scenario_capacities``. Targets for other carriers (``gas``
#: and ``small_chp`` are links or mix several generator carriers) are not
#: compared.
NATIONAL_TARGET_CARRIERS = {
    "biomass": [
        "biomass",
        "central_biomass_CHP",
        "industrial_biomass_CHP",
    ],
    "oil": ["oil"],
    "others": ["others"],
    "reservoir": ["reservoir"],
    "run_of_river": ["run_of_river"],
    "solar": ["solar"],
    "solar_rooftop": ["solar_rooftop"],
    "urban_central_geo_thermal": ["geo_thermal"],
    "urban_central_solar_thermal_collector": ["solar_thermal_collector"],
    "wind_offshore": ["wind_offshore"],
    "wind_onshore": ["wind_onshore"],
}


class _GeneratorTableRule(DataFrameRule):
    """
    Common base for the per-scenario rules on the whole generator table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        timeseries_table: str = "grid.egon_etrago_generator_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Generator table, "grid.egon_etrago_generator".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        timeseries_table : str
            Generator time series table.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            bus_table=bus_table,
            timeseries_table=timeseries_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.bus_table = bus_table
        self.timeseries_table = timeseries_table

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario}

    def _result(self, success, message, observed=None, expected=None,
                severity=None):
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
        """A non-failure result for a legitimately uncheckable case."""
        return self._result(success=True, message=message)

    def _no_generators(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no generators in {self.table}; "
            f"check skipped"
        )


class GeneratorTableScenarioCoverage(DataFrameRule):
    """
    Check that the generator table holds exactly the run's scenarios.

    A configured scenario without generators means nothing was written for
    it; a scenario that is not configured means rows are left over from a
    run with a different ``--scenarios``. The configured scenarios are read
    when the rule runs, not when the DAG is built.
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        SELECT array_agg(DISTINCT scn_name) AS scenarios
        FROM {self.table}
        """

    def evaluate_df(self, df, ctx):
        observed = set(df["scenarios"].values[0] or [])
        expected = set(
            egon.data.config.settings()["egon-data"]["--scenarios"]
        )
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        success = not missing and not extra

        if success:
            message = (
                f"Generators exist for exactly the configured scenarios: "
                f"{', '.join(sorted(observed))}"
            )
        else:
            message = (
                f"Configured scenarios without generators: "
                f"{', '.join(missing) or 'none'}; scenarios with generators "
                f"that are not configured: {', '.join(extra) or 'none'}"
            )
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=len(observed),
            expected=len(expected),
            message=message,
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class GeneratorTableBus(_GeneratorTableRule):
    """
    Check that every generator is attached to a bus of its own scenario.

    The bus is looked up by ``(bus_id, scn_name)``: a generator pointing at
    a bus id that only exists in another scenario is as unusable for
    eTraGo as one pointing at no bus at all.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_generators,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS n_missing,
            (
                array_agg(g.generator_id ORDER BY g.generator_id)
                FILTER (WHERE b.bus_id IS NULL)
            )[1:10] AS examples
        FROM {self.table} g
        LEFT JOIN {self.bus_table} b
            ON b.bus_id = g.bus
            AND b.scn_name = g.scn_name
        WHERE g.scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_missing = int(df["n_missing"].values[0])
        if n_missing:
            return self._result(
                success=False,
                observed=n_missing,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_missing} of "
                    f"{n_generators} generators have no bus in "
                    f"{self.bus_table} for this scenario, e.g. generator_id "
                    f"{list(df['examples'].values[0])}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"are attached to a bus of the scenario"
            ),
        )


class GeneratorTableBusCarrier(_GeneratorTableRule):
    """
    Check that every generator feeds a bus of the matching energy carrier.

    A wrong bus id usually still points at an existing bus, so
    :class:`GeneratorTableBus` does not notice it; the carrier of that bus
    does. The expected bus carrier comes from ``bus_carriers``, carriers
    ending in ``_CHP_heat`` belong on ``central_heat`` and every other
    carrier on ``AC``.
    """

    def __init__(self, table, rule_id, scenario,
                 bus_carriers=None, **kwargs):
        """
        Parameters
        ----------
        bus_carriers : dict, optional
            Bus carrier per generator carrier for the generators that do
            not feed an AC bus (default: :data:`NON_AC_BUS_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers=dict(bus_carriers or NON_AC_BUS_CARRIERS),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH pairs AS (
            SELECT g.carrier, b.carrier AS bus_carrier, count(*) AS n
            FROM {self.table} g
            JOIN {self.bus_table} b
                ON b.bus_id = g.bus
                AND b.scn_name = g.scn_name
            WHERE g.scn_name = :scenario
            GROUP BY g.carrier, b.carrier
        )
        SELECT json_agg(
            json_build_object(
                'carrier', carrier,
                'bus_carrier', bus_carrier,
                'n', n
            )
            ORDER BY carrier, bus_carrier
        ) AS pairs
        FROM pairs
        """

    def _expected_bus_carrier(self, carrier):
        bus_carriers = self.params["bus_carriers"]
        if carrier in bus_carriers:
            return bus_carriers[carrier]
        if carrier.endswith("_CHP_heat"):
            return "central_heat"
        return "AC"

    def evaluate_df(self, df, ctx):
        pairs = _parse_json(df["pairs"].values[0])
        if not pairs:
            return self._no_generators()

        n_generators = sum(p["n"] for p in pairs)
        mismatches = [
            p for p in pairs
            if p["bus_carrier"] != self._expected_bus_carrier(p["carrier"])
        ]
        if mismatches:
            n_wrong = sum(p["n"] for p in mismatches)
            details = "; ".join(
                f"{p['n']} {p['carrier']} on {p['bus_carrier']} (expected "
                f"{self._expected_bus_carrier(p['carrier'])})"
                for p in mismatches
            )
            return self._result(
                success=False,
                observed=n_wrong,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_wrong} of "
                    f"{n_generators} generators sit on a bus of the wrong "
                    f"carrier: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"sit on a bus of the expected carrier"
            ),
        )


class GeneratorTablePNom(_GeneratorTableRule):
    """
    Check that ``p_nom`` and its expansion bounds are usable.

    An error is a ``p_nom`` that is negative, NaN or infinite, or an
    extendable generator whose ``p_nom_min`` exceeds ``p_nom_max``. A fixed
    generator with ``p_nom = 0`` is only a warning: it never produces, but
    does no harm. Extendable generators may start at 0 (the O2 outlets of
    the electrolysers do).

    NaN is a legal ``double precision`` that passes NOT NULL, so it has to
    be tested explicitly with ``= 'NaN'::float8``.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_generators,
            count(*) FILTER (
                WHERE p_nom = 'NaN'::float8
                OR p_nom IN ('Infinity'::float8, '-Infinity'::float8)
                OR p_nom < 0
            ) AS n_invalid,
            count(*) FILTER (
                WHERE p_nom_extendable
                AND (
                    p_nom_min = 'NaN'::float8
                    OR p_nom_max = 'NaN'::float8
                    OR p_nom_min > p_nom_max
                )
            ) AS n_bad_bounds,
            count(*) FILTER (
                WHERE p_nom = 0 AND NOT p_nom_extendable
            ) AS n_zero,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM {self.table}
                    WHERE scn_name = :scenario
                    AND p_nom = 0
                    AND NOT p_nom_extendable
                    GROUP BY carrier
                ) z
            ) AS zero_carriers
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_invalid = int(df["n_invalid"].values[0])
        n_bad_bounds = int(df["n_bad_bounds"].values[0])
        n_zero = int(df["n_zero"].values[0])

        if n_invalid or n_bad_bounds:
            return self._result(
                success=False,
                observed=n_invalid + n_bad_bounds,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_generators} "
                    f"generators, {n_invalid} have a negative, NaN or "
                    f"infinite p_nom and {n_bad_bounds} extendable ones "
                    f"have p_nom_min > p_nom_max or NaN bounds"
                ),
            )
        if n_zero:
            zero_carriers = _parse_json(df["zero_carriers"].values[0])
            details = ", ".join(
                f"{carrier}: {n}"
                for carrier, n in sorted(zero_carriers.items())
            )
            return self._result(
                success=False,
                observed=n_zero,
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {n_zero} of {n_generators} "
                    f"generators are not extendable and have p_nom = 0 "
                    f"({details})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"have a valid p_nom"
            ),
        )


class GeneratorTableStaticLimits(_GeneratorTableRule):
    """
    Check the static per-unit limits and the marginal cost.

    ``p_min_pu`` and ``p_max_pu`` must lie in [0, 1] with
    ``p_min_pu <= p_max_pu``, and ``marginal_cost`` must be finite and not
    negative. Whether the marginal cost matches the scenario parameters is
    checked per dataset (``EtragoGeneratorMarginalCost``).
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_generators,
            count(*) FILTER (
                WHERE p_min_pu = 'NaN'::float8
                OR p_max_pu = 'NaN'::float8
                OR p_min_pu < 0
                OR p_max_pu > 1
                OR p_min_pu > p_max_pu
            ) AS n_bad_pu,
            count(*) FILTER (
                WHERE marginal_cost = 'NaN'::float8
                OR marginal_cost IN ('Infinity'::float8, '-Infinity'::float8)
                OR marginal_cost < 0
            ) AS n_bad_cost
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_bad_pu = int(df["n_bad_pu"].values[0])
        n_bad_cost = int(df["n_bad_cost"].values[0])
        if n_bad_pu or n_bad_cost:
            return self._result(
                success=False,
                observed=n_bad_pu + n_bad_cost,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_generators} "
                    f"generators, {n_bad_pu} have p_min_pu/p_max_pu outside "
                    f"0 <= p_min_pu <= p_max_pu <= 1 and {n_bad_cost} a "
                    f"negative, NaN or infinite marginal cost"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"have valid per-unit limits and marginal costs"
            ),
        )


class GeneratorTableTimeseriesCoverage(_GeneratorTableRule):
    """
    Check that weather dependent generators have one time series each.

    Every generator of a carrier in ``carriers``, German or foreign, needs
    exactly one row in the time series table with a ``p_max_pu`` array;
    without it eTraGo runs it at its static ``p_max_pu`` of 1 in every
    hour. Time series rows whose generator does not exist in the same
    scenario are reported as orphans.
    """

    def __init__(self, table, rule_id, scenario, carriers=None, **kwargs):
        """
        Parameters
        ----------
        carriers : Sequence[str], optional
            Weather dependent carriers (default:
            :data:`WEATHER_DEPENDENT_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers or WEATHER_DEPENDENT_CARRIERS),
            **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": list(self.params["carriers"]),
        }

    def get_query(self, ctx):
        return f"""
        WITH weather_dependent AS (
            SELECT
                g.generator_id,
                g.carrier,
                count(t.generator_id) FILTER (
                    WHERE t.p_max_pu IS NOT NULL
                ) AS n_series
            FROM {self.table} g
            LEFT JOIN {self.timeseries_table} t
                ON t.generator_id = g.generator_id
                AND t.scn_name = g.scn_name
            WHERE g.scn_name = :scenario
            AND g.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY g.generator_id, g.carrier
        )
        SELECT
            (SELECT count(*) FROM weather_dependent) AS n_generators,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM weather_dependent
                    WHERE n_series = 0
                    GROUP BY carrier
                ) m
            ) AS missing,
            (
                SELECT count(*) FROM weather_dependent WHERE n_series > 1
            ) AS n_duplicated,
            (
                SELECT count(*)
                FROM {self.timeseries_table} t
                WHERE t.scn_name = :scenario
                AND NOT EXISTS (
                    SELECT 1
                    FROM {self.table} g
                    WHERE g.generator_id = t.generator_id
                    AND g.scn_name = t.scn_name
                )
            ) AS n_orphaned
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        n_orphaned = int(df["n_orphaned"].values[0])
        if n_generators == 0 and n_orphaned == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no weather dependent "
                f"generators in {self.table}; check skipped"
            )

        missing = _parse_json(df["missing"].values[0]) or {}
        n_missing = sum(missing.values())
        n_duplicated = int(df["n_duplicated"].values[0])

        if n_missing or n_duplicated or n_orphaned:
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(missing.items())
            )
            return self._result(
                success=False,
                observed=n_missing + n_duplicated + n_orphaned,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_generators} weather "
                    f"dependent generators, {n_missing} have no p_max_pu "
                    f"series ({details or 'none'}) and {n_duplicated} have "
                    f"more than one; {n_orphaned} time series rows belong to "
                    f"no generator of the scenario"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} weather "
                f"dependent generators have exactly one p_max_pu series, "
                f"and no time series row is orphaned"
            ),
        )


class GeneratorTableTimeseriesValues(_GeneratorTableRule):
    """
    Check every ``p_max_pu`` series of the scenario for unusable values.

    A series fails if it contains NULL or NaN, leaves [0, 1] by more than
    ``atol``, or is zero in every hour -- the typical result of a failed
    join to the weather data, which would silently switch the generator
    off. The length is checked by ``ArrayCardinalityValidation``.

    ``atol`` exists because the foreign wind profiles reach
    1.0000000000000007 through floating point arithmetic.
    """

    def __init__(self, table, rule_id, scenario, atol: float = 1e-9,
                 **kwargs):
        """
        Parameters
        ----------
        atol : float
            Absolute tolerance on the [0, 1] bounds (default: 1e-9).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, atol=atol,
            **kwargs,
        )

    def get_query(self, ctx):
        atol = float(self.params.get("atol", 1e-9))
        return f"""
        WITH profiles AS (
            SELECT g.carrier, s.*
            FROM {self.timeseries_table} t
            JOIN {self.table} g
                ON g.generator_id = t.generator_id
                AND g.scn_name = t.scn_name
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE v IS NULL OR v = 'NaN'::float8
                    ) AS n_nan,
                    min(v) FILTER (WHERE v <> 'NaN'::float8) AS min_pu,
                    max(v) FILTER (WHERE v <> 'NaN'::float8) AS max_pu
                FROM unnest(t.p_max_pu) AS v
            ) s
            WHERE t.scn_name = :scenario
            AND t.p_max_pu IS NOT NULL
        )
        SELECT
            count(*) AS n_series,
            count(*) FILTER (WHERE n_nan > 0) AS n_with_nan,
            count(*) FILTER (
                WHERE min_pu < -{atol} OR max_pu > 1 + {atol}
            ) AS n_out_of_range,
            count(*) FILTER (WHERE max_pu = 0) AS n_all_zero,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM profiles
                    WHERE n_nan > 0
                    OR min_pu < -{atol}
                    OR max_pu > 1 + {atol}
                    OR max_pu = 0
                    GROUP BY carrier
                ) f
            ) AS failing_carriers,
            min(min_pu) AS min_pu,
            max(max_pu) AS max_pu
        FROM profiles
        """

    def evaluate_df(self, df, ctx):
        n_series = int(df["n_series"].values[0])
        if n_series == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no p_max_pu series in "
                f"{self.timeseries_table}; check skipped"
            )

        n_with_nan = int(df["n_with_nan"].values[0])
        n_out_of_range = int(df["n_out_of_range"].values[0])
        n_all_zero = int(df["n_all_zero"].values[0])
        value_range = (
            f"{df['min_pu'].values[0]:g}..{df['max_pu'].values[0]:g}"
            if n_with_nan < n_series
            else "no values"
        )

        if n_with_nan or n_out_of_range or n_all_zero:
            failing = _parse_json(df["failing_carriers"].values[0]) or {}
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(failing.items())
            )
            return self._result(
                success=False,
                observed=n_with_nan + n_out_of_range + n_all_zero,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_series} p_max_pu "
                    f"series, {n_with_nan} contain NULL or NaN, "
                    f"{n_out_of_range} leave [0, 1] and {n_all_zero} are "
                    f"zero in every hour (range {value_range}; per carrier "
                    f"{details})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_series} p_max_pu series "
                f"are complete, inside [0, 1] and not constantly zero "
                f"(range {value_range})"
            ),
        )


class GeneratorTableNationalCapacity(_GeneratorTableRule):
    """
    Compare the German capacity per carrier with the national targets.

    For each carrier of ``supply.egon_scenario_capacities`` listed in
    ``target_carriers``, the ``p_nom`` of the generators on German buses
    with the mapped carriers must add up to the target. Unlike
    ``EtragoGeneratorCapacity``, which compares one dataset with its source
    table, this covers every dataset writing German generators (power
    plants, rooftop PV, CHP, district heating).

    A deviation above ``rtol`` is an error; one above ``warn_rtol`` is a
    warning. Scenarios without generator targets (status2024) are skipped.

    ``nuts`` restricts the targets to the federal states of the dataset
    boundary; national rows (``nuts = 'DE'``) are always included.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        capacities_table: str = "supply.egon_scenario_capacities",
        target_carriers=None,
        nuts=None,
        rtol: float = 0.05,
        warn_rtol: float = 1e-3,
        **kwargs,
    ):
        """
        Parameters
        ----------
        capacities_table : str
            Table with the national capacity targets.
        target_carriers : dict, optional
            Generator carriers per target carrier (default:
            :data:`NATIONAL_TARGET_CARRIERS`).
        nuts : list of str, optional
            NUTS-1 codes of the federal states to compare; None compares
            all of them.
        rtol : float
            Relative deviation per carrier that is an error (default: 5 %).
        warn_rtol : float
            Relative deviation per carrier that is a warning (default:
            0.1 %).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            capacities_table=capacities_table,
            target_carriers=dict(target_carriers or NATIONAL_TARGET_CARRIERS),
            nuts=nuts,
            rtol=rtol,
            warn_rtol=warn_rtol,
            **kwargs,
        )
        self.capacities_table = capacities_table

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        nuts = self.params.get("nuts")
        return {
            "scenario": self.scenario,
            "all_nuts": nuts is None,
            "nuts": list(nuts or []),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT json_object_agg(carrier, capacity)
                FROM (
                    SELECT carrier, SUM(capacity) AS capacity
                    FROM {self.capacities_table}
                    WHERE scenario_name = :scenario
                    AND component = 'generator'
                    AND (
                        :all_nuts
                        OR nuts = 'DE'
                        OR nuts = ANY(CAST(:nuts AS text[]))
                    )
                    GROUP BY carrier
                ) c
            ) AS targets,
            (
                SELECT json_object_agg(carrier, p_nom)
                FROM (
                    SELECT g.carrier, SUM(g.p_nom) AS p_nom
                    FROM {self.table} g
                    JOIN {self.bus_table} b
                        ON b.bus_id = g.bus
                        AND b.scn_name = g.scn_name
                    WHERE g.scn_name = :scenario
                    AND b.country = 'DE'
                    GROUP BY g.carrier
                ) p
            ) AS generators
        """

    def evaluate_df(self, df, ctx):
        targets = _parse_json(df["targets"].values[0]) or {}
        if not targets:
            return self._skip(
                f"Scenario '{self.scenario}' has no generator targets in "
                f"{self.capacities_table}; check skipped"
            )
        generators = _parse_json(df["generators"].values[0]) or {}

        mapping = self.params["target_carriers"]
        rtol = float(self.params.get("rtol", 0.05))
        warn_rtol = float(self.params.get("warn_rtol", 1e-3))

        compared = []
        for target_carrier, expected in sorted(targets.items()):
            if target_carrier not in mapping:
                continue
            observed = sum(
                generators.get(c, 0.0) for c in mapping[target_carrier]
            )
            deviation = abs(observed - expected) / max(abs(expected), 1.0)
            compared.append((target_carrier, observed, expected, deviation))
        not_compared = sorted(set(targets) - set(mapping))

        def describe(rows):
            return "; ".join(
                f"{c}: {o:.0f} MW vs {e:.0f} MW ({(o - e) / max(e, 1.0):+.1%})"
                for c, o, e, _ in rows
            )

        errors = [r for r in compared if r[3] > rtol]
        warnings = [r for r in compared if warn_rtol < r[3] <= rtol]
        observed = sum(r[1] for r in compared)
        expected = sum(r[2] for r in compared)
        skipped = (
            f"; not compared: {', '.join(not_compared)}"
            if not_compared else ""
        )

        if errors:
            return self._result(
                success=False,
                observed=observed,
                expected=expected,
                message=(
                    f"Scenario '{self.scenario}': German generator capacity "
                    f"deviates by more than {rtol:.0%} from the national "
                    f"target for {len(errors)} of {len(compared)} carriers: "
                    f"{describe(errors)}"
                    + (f"; within tolerance: {describe(warnings)}"
                       if warnings else "")
                    + skipped
                ),
            )
        if warnings:
            return self._result(
                success=False,
                observed=observed,
                expected=expected,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': German generator capacity "
                    f"is within {rtol:.0%} of the national target for all "
                    f"{len(compared)} carriers, but deviates for "
                    f"{describe(warnings)}{skipped}"
                ),
            )
        return self._result(
            success=True,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': German generator capacity "
                f"matches the national target for all {len(compared)} "
                f"carriers ({observed / 1e3:.1f} GW){skipped}"
            ),
        )


class GeneratorTableMaxUnitSize(_GeneratorTableRule):
    """
    Check that no single German generator is implausibly large.

    A generator above ``max_p_nom`` usually means a unit error in the
    source data or capacities summed up on one bus. Some generators are
    deliberately large (status2024 has one 100 GW CH4 supply per gas
    bus), so this is a warning, and the limit is inclusive.
    """

    def __init__(self, table, rule_id, scenario, max_p_nom: float = 1e5,
                 **kwargs):
        """
        Parameters
        ----------
        max_p_nom : float
            Largest plausible p_nom of one generator in MW (default:
            100 GW).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario,
            max_p_nom=max_p_nom, **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "max_p_nom": float(self.params.get("max_p_nom", 1e5)),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_generators,
            (
                json_agg(
                    json_build_object(
                        'generator_id', g.generator_id,
                        'carrier', g.carrier,
                        'bus', g.bus,
                        'p_nom', g.p_nom
                    )
                    ORDER BY g.p_nom DESC
                ) FILTER (WHERE g.p_nom > :max_p_nom)
            ) AS too_large
        FROM {self.table} g
        JOIN {self.bus_table} b
            ON b.bus_id = g.bus
            AND b.scn_name = g.scn_name
        WHERE g.scn_name = :scenario
        AND b.country = 'DE'
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        max_p_nom = float(self.params.get("max_p_nom", 1e5))
        too_large = _parse_json(df["too_large"].values[0]) or []
        if too_large:
            details = "; ".join(
                f"{g['carrier']} {g['generator_id']} on bus {g['bus']}: "
                f"{g['p_nom'] / 1e3:.1f} GW"
                for g in too_large[:10]
            )
            return self._result(
                success=False,
                observed=len(too_large),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {len(too_large)} German "
                    f"generators exceed {max_p_nom / 1e3:.0f} GW: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': none of the {n_generators} "
                f"German generators exceeds {max_p_nom / 1e3:.0f} GW"
            ),
        )


class _ForeignGenerationRule(_GeneratorTableRule):
    """
    Common query for the rules on the neighbouring countries.

    Returns, for every foreign country with an AC bus in the scenario, the
    peak of its summed hourly AC load and the ``p_nom`` of its generators
    on AC buses.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        load_table: str = "grid.egon_etrago_load",
        load_timeseries_table: str = "grid.egon_etrago_load_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        load_table : str
            eTraGo load table.
        load_timeseries_table : str
            eTraGo load time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            load_table=load_table,
            load_timeseries_table=load_timeseries_table,
            **kwargs,
        )
        self.load_table = load_table
        self.load_timeseries_table = load_timeseries_table

    def get_query(self, ctx):
        return f"""
        WITH countries AS (
            SELECT DISTINCT country
            FROM {self.bus_table}
            WHERE scn_name = :scenario
            AND carrier = 'AC'
            AND country <> 'DE'
        ),
        hourly_load AS (
            SELECT b.country, u.hour, SUM(u.p_set) AS p_set
            FROM {self.load_table} l
            JOIN {self.bus_table} b
                ON b.bus_id = l.bus
                AND b.scn_name = l.scn_name
            JOIN {self.load_timeseries_table} t
                ON t.load_id = l.load_id
                AND t.scn_name = l.scn_name
            CROSS JOIN LATERAL unnest(t.p_set)
                WITH ORDINALITY AS u(p_set, hour)
            WHERE l.scn_name = :scenario
            AND b.carrier = 'AC'
            AND b.country <> 'DE'
            GROUP BY b.country, u.hour
        ),
        peak_load AS (
            SELECT country, MAX(p_set) AS peak_load
            FROM hourly_load
            GROUP BY country
        ),
        generation AS (
            SELECT b.country, SUM(g.p_nom) AS p_nom
            FROM {self.table} g
            JOIN {self.bus_table} b
                ON b.bus_id = g.bus
                AND b.scn_name = g.scn_name
            WHERE g.scn_name = :scenario
            AND b.carrier = 'AC'
            AND b.country <> 'DE'
            GROUP BY b.country
        )
        SELECT json_agg(
            json_build_object(
                'country', c.country,
                'peak_load', COALESCE(pl.peak_load, 0),
                'p_nom', COALESCE(gen.p_nom, 0)
            )
            ORDER BY c.country
        ) AS countries
        FROM countries c
        LEFT JOIN peak_load pl USING (country)
        LEFT JOIN generation gen USING (country)
        """

    def _countries(self, df):
        return _parse_json(df["countries"].values[0]) or []

    def _no_neighbours(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no foreign AC buses in "
            f"{self.bus_table}; check skipped"
        )


class GeneratorTableForeignCoverage(_ForeignGenerationRule):
    """
    Check that every neighbouring country with a load has generation.

    A foreign country with AC load but no AC generator can only be supplied
    through its interconnectors, i.e. mostly from Germany, which distorts
    the German dispatch. A country with an AC bus but neither load nor
    generation is reported as a warning: it can only act as a transit
    node.
    """

    def evaluate_df(self, df, ctx):
        countries = self._countries(df)
        if not countries:
            return self._no_neighbours()

        without_generation = [
            c for c in countries if c["peak_load"] > 0 and c["p_nom"] == 0
        ]
        empty = [
            c["country"] for c in countries
            if c["peak_load"] == 0 and c["p_nom"] == 0
        ]

        if without_generation:
            details = ", ".join(
                f"{c['country']} ({c['peak_load']:.0f} MW peak load)"
                for c in without_generation
            )
            return self._result(
                success=False,
                observed=len(without_generation),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {len(without_generation)} "
                    f"of {len(countries)} neighbouring countries have AC "
                    f"load but no AC generator: {details}"
                    + (f"; without load and generation: {', '.join(empty)}"
                       if empty else "")
                ),
            )
        if empty:
            return self._result(
                success=False,
                observed=len(empty),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': neighbouring countries "
                    f"with an AC bus but neither AC load nor AC generator: "
                    f"{', '.join(empty)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {len(countries)} "
                f"neighbouring countries have AC generators "
                f"({', '.join(c['country'] for c in countries)})"
            ),
        )


class GeneratorTableForeignAdequacy(_ForeignGenerationRule):
    """
    Check that each neighbour's generation is in proportion to its load.

    The installed AC generator capacity of a country divided by its peak
    AC load should be at least ``min_ratio``. Countries far below it
    depend on imports for most of their load, which usually means
    generators were lost while mapping the TYNDP or ENTSO-E data to the
    buses. This is a plausibility check, so it reports a warning.
    Countries without load are left to
    :class:`GeneratorTableForeignCoverage`.
    """

    def __init__(self, table, rule_id, scenario, min_ratio: float = 0.8,
                 **kwargs):
        """
        Parameters
        ----------
        min_ratio : float
            Smallest plausible ratio of installed capacity to peak load
            (default: 0.8).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario,
            min_ratio=min_ratio, **kwargs,
        )

    def evaluate_df(self, df, ctx):
        countries = [
            c for c in self._countries(df)
            if c["peak_load"] > 0 and c["p_nom"] > 0
        ]
        if not countries:
            return self._no_neighbours()

        min_ratio = float(self.params.get("min_ratio", 0.8))
        for c in countries:
            c["ratio"] = c["p_nom"] / c["peak_load"]
        low = [c for c in countries if c["ratio"] < min_ratio]
        lowest = min(countries, key=lambda c: c["ratio"])

        if low:
            details = ", ".join(
                f"{c['country']}: {c['p_nom']:.0f} MW for "
                f"{c['peak_load']:.0f} MW ({c['ratio']:.2f})"
                for c in low
            )
            return self._result(
                success=False,
                observed=len(low),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {len(low)} neighbouring "
                    f"countries have less than {min_ratio:.0%} of their peak "
                    f"load as installed capacity: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': installed capacity covers at "
                f"least {min_ratio:.0%} of the peak load in all "
                f"{len(countries)} neighbouring countries (lowest: "
                f"{lowest['country']}, {lowest['ratio']:.2f})"
            ),
        )
