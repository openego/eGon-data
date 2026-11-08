"""
Sanity check validation rules for the eTraGo generators dataset.

Validates what :class:`~egon.data.datasets.fill_etrago_gen.Egon_etrago_gen`
writes into ``grid.egon_etrago_generator`` and
``grid.egon_etrago_generator_timeseries``: the power plants of
``supply.egon_power_plants`` (all carriers but gas), grouped per bus and
carrier.

Other datasets write generators with the same carriers into the same table
-- the neighbouring countries' plants on foreign buses, and in the future
scenarios the rooftop PV of ``PowerPlants``, which never passes through
``supply.egon_power_plants``. The per-scenario rules therefore only look at
"this dataset's generators": generators on a German AC bus whose carrier
occurs in ``supply.egon_power_plants`` for that scenario.
"""

import json

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config

#: Carriers that get a ``p_max_pu`` time series from the weather data.
WEATHER_DEPENDENT_CARRIERS = (
    "solar",
    "solar_rooftop",
    "wind_onshore",
    "wind_offshore",
)


def _parse_json(value):
    """JSON aggregates arrive parsed or as a string, depending on the driver."""
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return value


class _EtragoGeneratorsRule(DataFrameRule):
    """
    Common base for the per-scenario rules of the eTraGo generators dataset.

    Subclasses write their query on top of the ``dataset_generators`` CTE
    returned by :meth:`_dataset_generators_cte` and return exactly one row,
    because the framework reports an empty query result as a failure.

    A scenario that is absent from the table is **not** an error: the rule
    reports success with severity INFO, because which scenarios a run
    produces depends on ``--scenarios``.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        power_plants_table: str = "supply.egon_power_plants",
        bus_table: str = "grid.egon_etrago_bus",
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
        power_plants_table : str
            Source table the dataset groups into generators.
        bus_table : str
            eTraGo bus table, used to restrict the check to German AC buses.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            power_plants_table=power_plants_table,
            bus_table=bus_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.power_plants_table = power_plants_table
        self.bus_table = bus_table

    def _dataset_generators_cte(self):
        """CTEs ``dataset_carriers`` and ``dataset_generators``.

        Gas is excluded because ``fill_etrago_generators`` does not load it
        from the power plants table.
        """
        return f"""
        dataset_carriers AS (
            SELECT DISTINCT carrier
            FROM {self.power_plants_table}
            WHERE scenario = :scenario
            AND carrier <> 'gas'
        ),
        dataset_generators AS (
            SELECT g.*
            FROM {self.table} g
            JOIN {self.bus_table} b
                ON b.bus_id = g.bus
                AND b.scn_name = g.scn_name
            WHERE g.scn_name = :scenario
            AND b.country = 'DE'
            AND b.carrier = 'AC'
            AND g.carrier IN (SELECT carrier FROM dataset_carriers)
        )
        """

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
        """A non-failure result for cases that are legitimately not checkable."""
        return self._result(success=True, message=message)

    def _no_generators(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no generators of this dataset "
            f"in {self.table}; check skipped"
        )


class EtragoGeneratorCapacity(_EtragoGeneratorsRule):
    """
    Compare the generator capacity per carrier with its source table.

    ``fill_etrago_generators`` sums ``el_capacity`` of the power plants per
    bus and carrier, so for every carrier of the scenario the ``p_nom`` of
    the generators on German AC buses must add up to the capacity in
    ``supply.egon_power_plants``. A deviation means capacity was lost or
    duplicated while grouping, or that generators of an earlier run were
    not deleted.
    """

    def __init__(self, table, rule_id, scenario, rtol: float = 1e-6,
                 **kwargs):
        """
        Parameters
        ----------
        rtol : float
            Relative tolerance per carrier (default: 1e-6, i.e. rounding
            only).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, rtol=rtol,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH power_plants AS (
            SELECT carrier, SUM(el_capacity) AS capacity
            FROM {self.power_plants_table}
            WHERE scenario = :scenario
            AND carrier <> 'gas'
            GROUP BY carrier
        ),
        generators AS (
            SELECT g.carrier, SUM(g.p_nom) AS p_nom
            FROM {self.table} g
            JOIN {self.bus_table} b
                ON b.bus_id = g.bus
                AND b.scn_name = g.scn_name
            WHERE g.scn_name = :scenario
            AND b.country = 'DE'
            AND b.carrier = 'AC'
            AND g.carrier IN (SELECT carrier FROM power_plants)
            GROUP BY g.carrier
        )
        SELECT json_agg(
            json_build_object(
                'carrier', pp.carrier,
                'expected', pp.capacity,
                'observed', COALESCE(gen.p_nom, 0)
            )
            ORDER BY pp.carrier
        ) AS carriers
        FROM power_plants pp
        LEFT JOIN generators gen USING (carrier)
        """

    def evaluate_df(self, df, ctx):
        carriers = _parse_json(df["carriers"].values[0])
        if not carriers:
            return self._skip(
                f"Scenario '{self.scenario}' has no power plants in "
                f"{self.power_plants_table}; check skipped"
            )

        rtol = float(self.params.get("rtol", 1e-6))
        deviations = [
            c for c in carriers
            if abs(c["observed"] - c["expected"])
            > rtol * max(abs(c["expected"]), 1.0)
        ]
        observed = sum(c["observed"] for c in carriers)
        expected = sum(c["expected"] for c in carriers)

        if deviations:
            details = "; ".join(
                f"{c['carrier']}: {c['observed']:.1f} MW vs "
                f"{c['expected']:.1f} MW"
                for c in deviations
            )
            return self._result(
                success=False,
                observed=observed,
                expected=expected,
                message=(
                    f"Scenario '{self.scenario}': generator capacity differs "
                    f"from {self.power_plants_table} for "
                    f"{len(deviations)} of {len(carriers)} carriers: "
                    f"{details}"
                ),
            )
        return self._result(
            success=True,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': generator capacity matches "
                f"{self.power_plants_table} for all {len(carriers)} carriers "
                f"({observed / 1e3:.1f} GW)"
            ),
        )


class EtragoGeneratorUniquePerBusCarrier(_EtragoGeneratorsRule):
    """
    Check that there is one generator per bus and carrier.

    ``fill_etrago_generators`` groups the power plants per bus and carrier,
    so a second generator with the same bus and carrier can only be a
    leftover of an earlier run that ``delete_previuos_gen`` missed.
    """

    def get_query(self, ctx):
        return f"""
        WITH {self._dataset_generators_cte()},
        duplicates AS (
            SELECT bus, carrier, count(*) AS n
            FROM dataset_generators
            GROUP BY bus, carrier
            HAVING count(*) > 1
        )
        SELECT
            (SELECT count(*) FROM dataset_generators) AS n_generators,
            (SELECT count(*) FROM duplicates) AS n_duplicates,
            (
                SELECT string_agg(carrier || ' at bus ' || bus, ', '
                                  ORDER BY carrier, bus)
                FROM (SELECT * FROM duplicates LIMIT 10) d
            ) AS examples
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_duplicates = int(df["n_duplicates"].values[0])
        if n_duplicates:
            return self._result(
                success=False,
                observed=n_duplicates,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_duplicates} bus/carrier "
                    f"pairs have more than one generator, e.g. "
                    f"{df['examples'].values[0]}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"are unique per bus and carrier"
            ),
        )


class EtragoGeneratorPositiveCapacity(_EtragoGeneratorsRule):
    """
    Check that every generator has a positive, finite ``p_nom``.

    A group of power plants that adds up to zero, or a NaN capacity, would
    give eTraGo a generator that can never produce anything.
    """

    def get_query(self, ctx):
        return f"""
        WITH {self._dataset_generators_cte()}
        SELECT
            count(*) AS n_generators,
            count(*) FILTER (
                WHERE p_nom IS NULL
                OR p_nom = 'NaN'::float8
                OR p_nom <= 0
            ) AS n_invalid,
            string_agg(DISTINCT carrier, ', ') FILTER (
                WHERE p_nom IS NULL
                OR p_nom = 'NaN'::float8
                OR p_nom <= 0
            ) AS carriers
        FROM dataset_generators
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_invalid = int(df["n_invalid"].values[0])
        if n_invalid:
            return self._result(
                success=False,
                observed=n_invalid,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_invalid} of "
                    f"{n_generators} generators have a p_nom that is not "
                    f"positive (carriers: {df['carriers'].values[0]})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} generators "
                f"have a positive p_nom"
            ),
        )


class EtragoGeneratorMarginalCost(_EtragoGeneratorsRule):
    """
    Compare the marginal costs with the scenario parameters.

    ``add_marginal_costs`` takes them from
    ``electricity.marginal_cost`` of the scenario parameters and silently
    sets 0 for any carrier without a value there. A value that differs from
    the parameter is an error; a carrier without a parameter is reported as
    a warning, because 0 may or may not be the intended cost.

    The parameters are read in the rule's own query rather than through
    ``get_sector_parameters``, so they come from the database under
    validation.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        parameters_table: str = "scenario.egon_scenario_parameters",
        rtol: float = 1e-9,
        **kwargs,
    ):
        """
        Parameters
        ----------
        parameters_table : str
            Scenario parameters table.
        rtol : float
            Relative tolerance against the parameter (default: 1e-9).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            parameters_table=parameters_table,
            rtol=rtol,
            **kwargs,
        )
        self.parameters_table = parameters_table

    def get_query(self, ctx):
        return f"""
        WITH {self._dataset_generators_cte()}
        SELECT
            json_agg(
                json_build_object(
                    'carrier', carrier,
                    'n', n,
                    'min', min_cost,
                    'max', max_cost
                )
                ORDER BY carrier
            ) AS carriers,
            (
                SELECT electricity_parameters -> 'marginal_cost'
                FROM {self.parameters_table}
                WHERE name = :scenario
            ) AS parameters
        FROM (
            SELECT
                carrier,
                count(*) AS n,
                min(marginal_cost) AS min_cost,
                max(marginal_cost) AS max_cost
            FROM dataset_generators
            GROUP BY carrier
        ) c
        """

    def evaluate_df(self, df, ctx):
        carriers = _parse_json(df["carriers"].values[0])
        if not carriers:
            return self._no_generators()

        parameters = df["parameters"].values[0]
        if parameters is None:
            return self._skip(
                f"No marginal cost parameters available for scenario "
                f"'{self.scenario}'; check skipped"
            )
        if isinstance(parameters, str):
            parameters = json.loads(parameters)

        rtol = float(self.params.get("rtol", 1e-9))
        mismatches = []
        without_parameter = []
        for c in carriers:
            if c["carrier"] in parameters:
                expected = float(parameters[c["carrier"]])
            else:
                # add_marginal_costs falls back to 0
                expected = 0.0
                without_parameter.append(c["carrier"])
            tolerance = rtol * max(abs(expected), 1.0)
            if (
                abs(c["min"] - expected) > tolerance
                or abs(c["max"] - expected) > tolerance
            ):
                mismatches.append(
                    f"{c['carrier']}: {c['min']:g}..{c['max']:g} vs "
                    f"{expected:g}"
                )

        if mismatches:
            return self._result(
                success=False,
                observed=len(mismatches),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': marginal cost differs from "
                    f"the scenario parameters for {len(mismatches)} "
                    f"carriers: {'; '.join(mismatches)}"
                ),
            )
        if without_parameter:
            return self._result(
                success=False,
                observed=len(without_parameter),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': no marginal cost parameter "
                    f"for {', '.join(without_parameter)}; these generators "
                    f"have a marginal cost of 0"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': marginal cost matches the "
                f"scenario parameters for all {len(carriers)} carriers"
            ),
        )


class EtragoGeneratorTimeseriesCoverage(_EtragoGeneratorsRule):
    """
    Check that the weather dependent generators have one time series each.

    Every solar, rooftop PV and wind generator of the dataset needs exactly
    one row in the time series table; without it eTraGo treats the
    generator as always available. Time series rows of the scenario whose
    generator no longer exists are counted too: ``delete_previuos_gen`` is
    meant to remove them, but the table has no foreign key to enforce it.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        timeseries_table: str = "grid.egon_etrago_generator_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        timeseries_table : str
            Generator time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            timeseries_table=timeseries_table,
            **kwargs,
        )
        self.timeseries_table = timeseries_table

    def get_query(self, ctx):
        carriers = ", ".join(f"'{c}'" for c in WEATHER_DEPENDENT_CARRIERS)
        return f"""
        WITH {self._dataset_generators_cte()},
        generators AS (
            SELECT generator_id
            FROM dataset_generators
            WHERE carrier IN ({carriers})
        ),
        timeseries AS (
            SELECT generator_id, count(*) AS n
            FROM {self.timeseries_table}
            WHERE scn_name = :scenario
            GROUP BY generator_id
        )
        SELECT
            (SELECT count(*) FROM generators) AS n_generators,
            (
                SELECT count(*)
                FROM generators g
                LEFT JOIN timeseries t USING (generator_id)
                WHERE t.generator_id IS NULL
            ) AS n_missing,
            (
                SELECT count(*)
                FROM generators g
                JOIN timeseries t USING (generator_id)
                WHERE t.n > 1
            ) AS n_multiple,
            (
                SELECT count(*)
                FROM {self.timeseries_table} t
                WHERE t.scn_name = :scenario
                AND NOT EXISTS (
                    SELECT 1
                    FROM {self.table} g
                    WHERE g.scn_name = t.scn_name
                    AND g.generator_id = t.generator_id
                )
            ) AS n_orphans
        """

    def evaluate_df(self, df, ctx):
        n_generators = int(df["n_generators"].values[0])
        if n_generators == 0:
            return self._no_generators()

        n_missing = int(df["n_missing"].values[0])
        n_multiple = int(df["n_multiple"].values[0])
        n_orphans = int(df["n_orphans"].values[0])
        n_invalid = n_missing + n_multiple + n_orphans

        if n_invalid:
            return self._result(
                success=False,
                observed=n_invalid,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_generators} weather "
                    f"dependent generators, {n_missing} have no time series "
                    f"and {n_multiple} have more than one; {n_orphans} time "
                    f"series rows of the scenario have no generator"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_generators} weather "
                f"dependent generators have exactly one time series, and "
                f"no time series row is orphaned"
            ),
        )


class EtragoGeneratorTimeseriesRange(EtragoGeneratorTimeseriesCoverage):
    """
    Check that every ``p_max_pu`` series is complete and inside [0, 1].

    NULL and NaN values are counted separately from values out of range.
    NaN is a legal ``double precision`` that passes NOT NULL, and PostgreSQL
    sorts it above every float, so ``min``/``max`` do not reveal it; it has
    to be tested explicitly with ``= 'NaN'::float8``. The series length is
    already checked by ``FinalValidations``.
    """

    def get_query(self, ctx):
        carriers = ", ".join(f"'{c}'" for c in WEATHER_DEPENDENT_CARRIERS)
        return f"""
        WITH {self._dataset_generators_cte()},
        profiles AS (
            SELECT
                t.p_max_pu IS NULL AS is_null,
                s.n_nan,
                s.min_pu,
                s.max_pu
            FROM dataset_generators g
            JOIN {self.timeseries_table} t
                ON t.generator_id = g.generator_id
                AND t.scn_name = g.scn_name
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE v IS NULL OR v = 'NaN'::float8
                    ) AS n_nan,
                    min(v) FILTER (WHERE v <> 'NaN'::float8) AS min_pu,
                    max(v) FILTER (WHERE v <> 'NaN'::float8) AS max_pu
                FROM unnest(t.p_max_pu) AS v
            ) s
            WHERE g.carrier IN ({carriers})
        )
        SELECT
            count(*) AS n_series,
            count(*) FILTER (WHERE is_null OR n_nan > 0) AS n_with_nan,
            count(*) FILTER (
                WHERE min_pu < 0 OR max_pu > 1
            ) AS n_out_of_range,
            min(min_pu) AS min_pu,
            max(max_pu) AS max_pu
        FROM profiles
        """

    def evaluate_df(self, df, ctx):
        n_series = int(df["n_series"].values[0])
        if n_series == 0:
            return self._no_generators()

        n_with_nan = int(df["n_with_nan"].values[0])
        n_out_of_range = int(df["n_out_of_range"].values[0])
        value_range = (
            f"{df['min_pu'].values[0]:g}..{df['max_pu'].values[0]:g}"
            if n_with_nan < n_series
            else "no values"
        )

        if n_with_nan or n_out_of_range:
            return self._result(
                success=False,
                observed=n_with_nan + n_out_of_range,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_series} p_max_pu "
                    f"series, {n_with_nan} contain NULL or NaN and "
                    f"{n_out_of_range} leave [0, 1] (range {value_range})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_series} p_max_pu series "
                f"are complete and inside [0, 1] (range {value_range})"
            ),
        )


class EtragoGeneratorScenarioCoverage(DataFrameRule):
    """
    Check that the dataset's generators cover exactly the run's scenarios.

    A configured scenario without generators means the dataset wrote
    nothing for it; a scenario that is not configured means rows are left
    over from a run with a different ``--scenarios``. The configured
    scenarios are read when the rule runs, not when the DAG is built.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        carriers,
        bus_table: str = "grid.egon_etrago_bus",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Generator table, "grid.egon_etrago_generator".
        rule_id : str
            Unique identifier for this validation rule.
        carriers : Sequence[str]
            Carriers the dataset writes.
        bus_table : str
            eTraGo bus table, used to restrict the check to German AC buses.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            carriers=list(carriers),
            bus_table=bus_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.bus_table = bus_table

    def get_query(self, ctx):
        return f"""
        SELECT array_agg(DISTINCT g.scn_name) AS scenarios
        FROM {self.table} g
        JOIN {self.bus_table} b
            ON b.bus_id = g.bus
            AND b.scn_name = g.scn_name
        WHERE b.country = 'DE'
        AND b.carrier = 'AC'
        AND g.carrier = ANY(:carriers)
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"carriers": list(self.params["carriers"])}

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
