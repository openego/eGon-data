"""
Sanity check validation rules for the complete eTraGo load tables.

These rules check ``grid.egon_etrago_load`` and
``grid.egon_etrago_load_timeseries`` as eTraGo reads them: every load of
every dataset, German and foreign. They run in ``FinalValidations`` and
fall into three groups:

* all loads: bus, demand definition and time series,
* Germany: annual demand against the scenario parameters, and loads on
  every heat bus and MV grid district,
* other countries: every neighbour has an AC demand with a plausible
  profile.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config
from egon.data.validation.rules.custom.sanity.etrago_generator_table import (
    _GeneratorTableRule,
)
from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)

#: Bus carriers each load carrier may sit on. Carriers missing here must
#: sit on an AC bus.
LOAD_BUS_CARRIERS = {
    "CH4": ["CH4"],
    "CH4_for_industry": ["CH4"],
    # The foreign industrial hydrogen demand is attached to the AC buses on
    # purpose, see gas_neighbours.eGon2035.insert_ind_H2_demand().
    "H2_for_industry": ["H2", "H2_grid", "AC"],
    "O2": ["O2"],
    "central_heat": ["central_heat"],
    "land_transport_EV": ["AC", "Li_ion"],
    "land_transport_HGV": ["AC", "HGV_charger"],
    "rural_gas_boiler": ["CH4"],
    "rural_heat": ["rural_heat"],
}

#: Load carriers of the rail transport demand.
RAIL_CARRIERS = ("rail_sbahn_dc", "rail_traction", "rail_transit_dc")

#: Heat bus carriers whose buses are expected to carry a load of the same
#: carrier.
HEAT_CARRIERS = ("central_heat", "rural_heat")


def _energy_cte(table, timeseries_table, bus_table):
    """CTE ``energy``: annual energy and country of every load of a scenario.

    A load's energy is the sum of its ``p_set`` series, or its static
    ``p_set`` times 8760 h when it has no series. NaN values are left out
    of the sum (they are reported by :class:`LoadTableTimeseriesValues`).
    """
    return f"""
    energy AS (
        SELECT
            l.load_id,
            l.carrier,
            b.country,
            COALESCE(s.energy, l.p_set * 8760, 0) AS energy
        FROM {table} l
        JOIN {bus_table} b
            ON b.bus_id = l.bus
            AND b.scn_name = l.scn_name
        LEFT JOIN {timeseries_table} t
            ON t.load_id = l.load_id
            AND t.scn_name = l.scn_name
        LEFT JOIN LATERAL (
            SELECT SUM(v) FILTER (WHERE v <> 'NaN'::float8) AS energy
            FROM unnest(t.p_set) AS v
        ) s ON TRUE
        WHERE l.scn_name = :scenario
    )
    """


class _LoadTableRule(_GeneratorTableRule):
    """
    Common base for the per-scenario rules on the whole load table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        timeseries_table: str = "grid.egon_etrago_load_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Load table, "grid.egon_etrago_load".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        timeseries_table : str
            Load time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_table=bus_table,
            timeseries_table=timeseries_table,
            **kwargs,
        )

    def _no_loads(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no loads in {self.table}; "
            f"check skipped"
        )


class LoadTableScenarioCoverage(DataFrameRule):
    """
    Check that every configured scenario has loads.

    A configured scenario without loads is an error. A scenario in the
    table that is not configured is only a warning: it may be left over
    from a run with a different ``--scenarios``, or be derived from a
    configured one (the ``_lowflex`` scenarios). The configured scenarios
    are read when the rule runs, not when the DAG is built.
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

        if missing:
            severity = Severity.ERROR
            message = (
                f"Configured scenarios without loads: {', '.join(missing)}"
                + (f"; scenarios with loads that are not configured: "
                   f"{', '.join(extra)}" if extra else "")
            )
        elif extra:
            severity = Severity.WARNING
            message = (
                f"Loads exist for all configured scenarios, and also for "
                f"scenarios that are not configured: {', '.join(extra)}"
            )
        else:
            severity = Severity.INFO
            message = (
                f"Loads exist for exactly the configured scenarios: "
                f"{', '.join(sorted(observed))}"
            )
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=not missing and not extra,
            observed=len(observed),
            expected=len(expected),
            message=message,
            severity=severity,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class LoadTableBus(_LoadTableRule):
    """
    Check that every load is attached to a bus of its own scenario.

    The bus is looked up by ``(bus_id, scn_name)``: a load pointing at a
    bus id that only exists in another scenario is as unusable for eTraGo
    as one pointing at no bus at all.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_loads,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS n_missing,
            (
                array_agg(l.load_id ORDER BY l.load_id)
                FILTER (WHERE b.bus_id IS NULL)
            )[1:10] AS examples
        FROM {self.table} l
        LEFT JOIN {self.bus_table} b
            ON b.bus_id = l.bus
            AND b.scn_name = l.scn_name
        WHERE l.scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_loads = int(df["n_loads"].values[0])
        if n_loads == 0:
            return self._no_loads()

        n_missing = int(df["n_missing"].values[0])
        if n_missing:
            return self._result(
                success=False,
                observed=n_missing,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_missing} of {n_loads} "
                    f"loads have no bus in {self.bus_table} for this "
                    f"scenario, e.g. load_id "
                    f"{list(df['examples'].values[0])}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_loads} loads are "
                f"attached to a bus of the scenario"
            ),
        )


class LoadTableBusCarrier(_LoadTableRule):
    """
    Check that every load draws from a bus of a matching energy carrier.

    A wrong bus id usually still points at an existing bus, so
    :class:`LoadTableBus` does not notice it; the carrier of that bus does.
    The permitted bus carriers come from ``bus_carriers``; every other load
    carrier must sit on an ``AC`` bus.
    """

    def __init__(self, table, rule_id, scenario,
                 bus_carriers=None, **kwargs):
        """
        Parameters
        ----------
        bus_carriers : dict, optional
            Permitted bus carriers per load carrier for the loads that may
            sit on other than AC buses (default: :data:`LOAD_BUS_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers={
                k: list(v)
                for k, v in (bus_carriers or LOAD_BUS_CARRIERS).items()
            },
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH pairs AS (
            SELECT l.carrier, b.carrier AS bus_carrier, count(*) AS n
            FROM {self.table} l
            JOIN {self.bus_table} b
                ON b.bus_id = l.bus
                AND b.scn_name = l.scn_name
            WHERE l.scn_name = :scenario
            GROUP BY l.carrier, b.carrier
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

    def _permitted(self, carrier):
        return self.params["bus_carriers"].get(carrier, ["AC"])

    def evaluate_df(self, df, ctx):
        pairs = _parse_json(df["pairs"].values[0])
        if not pairs:
            return self._no_loads()

        n_loads = sum(p["n"] for p in pairs)
        mismatches = [
            p for p in pairs
            if p["bus_carrier"] not in self._permitted(p["carrier"])
        ]
        if mismatches:
            n_wrong = sum(p["n"] for p in mismatches)
            details = "; ".join(
                f"{p['n']} {p['carrier']} on {p['bus_carrier']} (expected "
                f"{' or '.join(self._permitted(p['carrier']))})"
                for p in mismatches
            )
            return self._result(
                success=False,
                observed=n_wrong,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_wrong} of {n_loads} "
                    f"loads sit on a bus of the wrong carrier: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_loads} loads sit on a "
                f"bus of a permitted carrier"
            ),
        )


class LoadTableDemandDefinition(_LoadTableRule):
    """
    Check that every load defines its demand exactly once.

    eTraGo takes a load's demand from its ``p_set`` time series if there is
    one, and from the static ``p_set`` otherwise. A load with both is
    ambiguous, one with neither has no demand at all. A static ``p_set``
    must be finite and not negative, and ``sign`` must be -1 (consumption).
    """

    def get_query(self, ctx):
        return f"""
        WITH loads AS (
            SELECT
                l.carrier,
                l.p_set,
                l.sign,
                EXISTS (
                    SELECT 1
                    FROM {self.timeseries_table} t
                    WHERE t.load_id = l.load_id
                    AND t.scn_name = l.scn_name
                    AND t.p_set IS NOT NULL
                ) AS has_series
            FROM {self.table} l
            WHERE l.scn_name = :scenario
        )
        SELECT
            count(*) AS n_loads,
            count(*) FILTER (
                WHERE sign IS DISTINCT FROM -1
            ) AS n_bad_sign,
            count(*) FILTER (
                WHERE has_series AND p_set IS NOT NULL
            ) AS n_both,
            count(*) FILTER (
                WHERE NOT has_series AND p_set IS NULL
            ) AS n_neither,
            count(*) FILTER (
                WHERE p_set = 'NaN'::float8
                OR p_set IN ('Infinity'::float8, '-Infinity'::float8)
                OR p_set < 0
            ) AS n_bad_static,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM loads
                    WHERE NOT has_series AND p_set IS NULL
                    GROUP BY carrier
                ) z
            ) AS neither_carriers
        FROM loads
        """

    def evaluate_df(self, df, ctx):
        n_loads = int(df["n_loads"].values[0])
        if n_loads == 0:
            return self._no_loads()

        counts = {
            k: int(df[k].values[0])
            for k in ("n_bad_sign", "n_both", "n_neither", "n_bad_static")
        }
        if any(counts.values()):
            neither = _parse_json(df["neither_carriers"].values[0]) or {}
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(neither.items())
            )
            return self._result(
                success=False,
                observed=sum(counts.values()),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_loads} loads, "
                    f"{counts['n_bad_sign']} have a sign other than -1, "
                    f"{counts['n_both']} have both a static p_set and a "
                    f"series, {counts['n_neither']} have neither"
                    + (f" ({details})" if details else "")
                    + f", and {counts['n_bad_static']} have a negative, NaN "
                    f"or infinite static p_set"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_loads} loads consume "
                f"(sign -1) and define their demand exactly once"
            ),
        )


class LoadTableTimeseriesCoverage(_LoadTableRule):
    """
    Check that no load has more than one series and none is orphaned.

    Loads without any demand are reported by
    :class:`LoadTableDemandDefinition`.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT count(*)
                FROM {self.table}
                WHERE scn_name = :scenario
            ) AS n_loads,
            (
                SELECT count(*)
                FROM (
                    SELECT load_id
                    FROM {self.timeseries_table}
                    WHERE scn_name = :scenario
                    GROUP BY load_id
                    HAVING count(*) > 1
                ) d
            ) AS n_duplicated,
            (
                SELECT count(*)
                FROM {self.timeseries_table} t
                WHERE t.scn_name = :scenario
                AND NOT EXISTS (
                    SELECT 1
                    FROM {self.table} l
                    WHERE l.load_id = t.load_id
                    AND l.scn_name = t.scn_name
                )
            ) AS n_orphaned
        """

    def evaluate_df(self, df, ctx):
        n_loads = int(df["n_loads"].values[0])
        n_duplicated = int(df["n_duplicated"].values[0])
        n_orphaned = int(df["n_orphaned"].values[0])
        if n_loads == 0 and n_orphaned == 0:
            return self._no_loads()

        if n_duplicated or n_orphaned:
            return self._result(
                success=False,
                observed=n_duplicated + n_orphaned,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_duplicated} loads have "
                    f"more than one time series row and {n_orphaned} time "
                    f"series rows belong to no load of the scenario"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': no load has more than one time "
                f"series row, and no time series row is orphaned"
            ),
        )


class LoadTableTimeseriesValues(_LoadTableRule):
    """
    Check every ``p_set`` series of the scenario for unusable values.

    A series containing NULL or NaN, or any negative value, is an error:
    a load never feeds in, however small the value.
    NaN is a legal ``double precision`` that passes NOT NULL and that
    PostgreSQL sorts above every float, so it is tested explicitly with
    ``= 'NaN'::float8``. A series that is zero in every hour is a warning:
    the load exists but never consumes. The length is checked by
    ``ArrayCardinalityValidation``.
    """

    def get_query(self, ctx):
        return f"""
        WITH profiles AS (
            SELECT l.carrier, s.*
            FROM {self.timeseries_table} t
            JOIN {self.table} l
                ON l.load_id = t.load_id
                AND l.scn_name = t.scn_name
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE v IS NULL OR v = 'NaN'::float8
                    ) AS n_nan,
                    min(v) FILTER (WHERE v <> 'NaN'::float8) AS min_p,
                    max(v) FILTER (WHERE v <> 'NaN'::float8) AS max_p
                FROM unnest(t.p_set) AS v
            ) s
            WHERE t.scn_name = :scenario
            AND t.p_set IS NOT NULL
        ),
        flagged AS (
            SELECT
                carrier,
                n_nan > 0 AS has_nan,
                min_p < 0 AS negative,
                max_p = 0 AS all_zero
            FROM profiles
        )
        SELECT
            count(*) AS n_series,
            count(*) FILTER (WHERE has_nan) AS n_with_nan,
            count(*) FILTER (WHERE negative) AS n_negative,
            count(*) FILTER (WHERE all_zero) AS n_all_zero,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM flagged
                    WHERE has_nan OR negative
                    GROUP BY carrier
                ) e
            ) AS error_carriers,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM flagged
                    WHERE all_zero
                    GROUP BY carrier
                ) z
            ) AS zero_carriers
        FROM flagged
        """

    @staticmethod
    def _per_carrier(value):
        counts = _parse_json(value) or {}
        return ", ".join(
            f"{carrier}: {n}" for carrier, n in sorted(counts.items())
        )

    def evaluate_df(self, df, ctx):
        n_series = int(df["n_series"].values[0])
        if n_series == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no p_set series in "
                f"{self.timeseries_table}; check skipped"
            )

        n_with_nan = int(df["n_with_nan"].values[0])
        n_negative = int(df["n_negative"].values[0])
        n_all_zero = int(df["n_all_zero"].values[0])
        zero_note = (
            f"; {n_all_zero} are zero in every hour "
            f"({self._per_carrier(df['zero_carriers'].values[0])})"
            if n_all_zero else ""
        )

        if n_with_nan or n_negative:
            return self._result(
                success=False,
                observed=n_with_nan + n_negative,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_series} p_set "
                    f"series, {n_with_nan} contain NULL or NaN and "
                    f"{n_negative} contain negative values ("
                    f"{self._per_carrier(df['error_carriers'].values[0])})"
                    f"{zero_note}"
                ),
            )
        if n_all_zero:
            return self._result(
                success=False,
                observed=n_all_zero,
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': all {n_series} p_set "
                    f"series are complete and not negative{zero_note}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_series} p_set series "
                f"are complete, not negative and not constantly zero"
            ),
        )


class LoadTableAnnualDemand(_LoadTableRule):
    """
    Compare the annual German demand of some carriers with a parameter.

    The energy of the loads with a carrier in ``carriers`` on German buses
    is compared with a value from ``scenario.egon_scenario_parameters``:
    column ``parameter_column``, then the keys in ``parameter_path``. If
    that value is a dict, its values are summed, so
    ``electricity_parameters -> annual_demand`` yields households + CTS +
    industry.

    The targets are national. On a run with a smaller dataset boundary,
    pass ``enabled=False`` (through ``resolve_boundary_dependence``) and
    the rule reports INFO instead.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        carriers,
        parameter_column: str,
        parameter_path,
        parameters_table: str = "scenario.egon_scenario_parameters",
        rtol: float = 0.01,
        enabled=True,
        **kwargs,
    ):
        """
        Parameters
        ----------
        carriers : Sequence[str]
            Load carriers whose energy is summed.
        parameter_column : str
            Column of the parameters table, e.g. "electricity_parameters".
        parameter_path : Sequence[str]
            Keys leading to the target in that column, in MWh.
        parameters_table : str
            Scenario parameters table.
        rtol : float
            Relative deviation that is an error (default: 1 %).
        enabled : bool
            False skips the rule, e.g. for a regional dataset boundary.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            parameter_column=parameter_column,
            parameter_path=list(parameter_path),
            parameters_table=parameters_table,
            rtol=rtol,
            enabled=enabled,
            **kwargs,
        )
        self.parameters_table = parameters_table

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": list(self.params["carriers"]),
            "path": list(self.params["parameter_path"]),
        }

    def get_query(self, ctx):
        column = self.params["parameter_column"]
        return f"""
        WITH {_energy_cte(self.table, self.timeseries_table,
                          self.bus_table)}
        SELECT
            (
                SELECT count(*)
                FROM energy
                WHERE country = 'DE'
                AND carrier = ANY(CAST(:carriers AS text[]))
            ) AS n_loads,
            (
                SELECT SUM(energy)
                FROM energy
                WHERE country = 'DE'
                AND carrier = ANY(CAST(:carriers AS text[]))
            ) AS energy,
            (
                SELECT {column} #> CAST(:path AS text[])
                FROM {self.parameters_table}
                WHERE name = :scenario
            ) AS target
        """

    def evaluate_df(self, df, ctx):
        carriers = ", ".join(self.params["carriers"])
        if not self.params.get("enabled", True):
            return self._skip(
                f"Scenario '{self.scenario}': the national demand target "
                f"for {carriers} is not comparable on this dataset "
                f"boundary; check skipped"
            )
        target = _parse_json(df["target"].values[0])
        path = " -> ".join(
            [self.params["parameter_column"]] + self.params["parameter_path"]
        )
        if target is None:
            return self._skip(
                f"Scenario '{self.scenario}' has no {path} in "
                f"{self.parameters_table}; check skipped"
            )
        n_loads = int(df["n_loads"].values[0])
        if n_loads == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no German loads of carrier "
                f"{carriers}; check skipped"
            )

        if isinstance(target, dict):
            detail = " + ".join(
                f"{k} {float(v) / 1e6:.1f}" for k, v in sorted(target.items())
            )
            expected = sum(float(v) for v in target.values())
        else:
            expected = float(target)
            detail = f"{expected / 1e6:.2f}"
        observed = float(df["energy"].values[0] or 0.0)
        deviation = (observed - expected) / max(abs(expected), 1.0)
        rtol = float(self.params.get("rtol", 0.01))

        message = (
            f"Scenario '{self.scenario}': German {carriers} demand is "
            f"{observed / 1e6:.2f} TWh vs {expected / 1e6:.2f} TWh from "
            f"{path} ({detail} TWh), deviation {deviation:+.2%}"
        )
        return self._result(
            success=abs(deviation) <= rtol,
            observed=observed,
            expected=expected,
            message=message,
        )


class LoadTableHeatCoverage(_LoadTableRule):
    """
    Check that the German heat buses carry heat demand.

    For each carrier in ``carriers``, the share of German buses of that
    carrier with a load of the same carrier is computed. No such load at
    all while such buses exist means the heat demand was not written: an
    error. A share below ``min_share`` is a warning, because some buses
    legitimately have no demand.
    """

    def __init__(self, table, rule_id, scenario, carriers=None,
                 min_share: float = 0.95, **kwargs):
        """
        Parameters
        ----------
        carriers : Sequence[str], optional
            Heat bus carriers (default: :data:`HEAT_CARRIERS`).
        min_share : float
            Smallest share of buses with a load that is not a warning
            (default: 0.95).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers or HEAT_CARRIERS),
            min_share=min_share,
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
        SELECT json_agg(
            json_build_object(
                'carrier', carrier,
                'buses', buses,
                'with_load', with_load
            )
            ORDER BY carrier
        ) AS carriers
        FROM (
            SELECT
                b.carrier,
                count(*) AS buses,
                count(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                        FROM {self.table} l
                        WHERE l.bus = b.bus_id
                        AND l.scn_name = b.scn_name
                        AND l.carrier = b.carrier
                    )
                ) AS with_load
            FROM {self.bus_table} b
            WHERE b.scn_name = :scenario
            AND b.country = 'DE'
            AND b.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY b.carrier
        ) c
        """

    def evaluate_df(self, df, ctx):
        carriers = _parse_json(df["carriers"].values[0]) or []
        if not carriers:
            return self._skip(
                f"Scenario '{self.scenario}' has no German "
                f"{' or '.join(self.params['carriers'])} buses; check "
                f"skipped"
            )

        min_share = float(self.params.get("min_share", 0.95))
        summary = "; ".join(
            f"{c['carrier']}: {c['with_load']} of {c['buses']} buses"
            for c in carriers
        )
        empty = [c["carrier"] for c in carriers if c["with_load"] == 0]
        low = [
            c["carrier"] for c in carriers
            if 0 < c["with_load"] < min_share * c["buses"]
        ]

        if empty:
            return self._result(
                success=False,
                observed=len(empty),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': no German "
                    f"{' or '.join(empty)} bus carries a heat load, the heat "
                    f"demand is missing ({summary})"
                ),
            )
        if low:
            return self._result(
                success=False,
                observed=len(low),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': less than "
                    f"{min_share:.0%} of the German {' and '.join(low)} "
                    f"buses carry a heat load ({summary})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=f"Scenario '{self.scenario}': heat loads on {summary}",
        )


class LoadTableMvGridCoverage(_LoadTableRule):
    """
    Check that every MV grid district has an AC load.

    The households, CTS and industry demand is assigned to the bus of the
    MV grid district (or HV/MV substation) it lies in. A district without
    an AC load usually has no demand data, which is possible for very
    small districts, so this is a warning listing them.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        mv_grid_table: str = "grid.egon_mv_grid_district",
        **kwargs,
    ):
        """
        Parameters
        ----------
        mv_grid_table : str
            MV grid district table with a ``bus_id`` per district.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            mv_grid_table=mv_grid_table,
            **kwargs,
        )
        self.mv_grid_table = mv_grid_table

    def get_query(self, ctx):
        return f"""
        WITH districts AS (
            SELECT
                m.bus_id,
                EXISTS (
                    SELECT 1
                    FROM {self.table} l
                    WHERE l.bus = m.bus_id
                    AND l.scn_name = :scenario
                    AND l.carrier = 'AC'
                ) AS has_load
            FROM {self.mv_grid_table} m
        )
        SELECT
            (
                SELECT count(*)
                FROM {self.table}
                WHERE scn_name = :scenario
                AND carrier = 'AC'
            ) AS n_ac_loads,
            count(*) AS n_districts,
            count(*) FILTER (WHERE NOT has_load) AS n_without,
            (
                array_agg(bus_id ORDER BY bus_id)
                FILTER (WHERE NOT has_load)
            )[1:20] AS examples
        FROM districts
        """

    def evaluate_df(self, df, ctx):
        if int(df["n_ac_loads"].values[0]) == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no AC loads in "
                f"{self.table}; check skipped"
            )

        n_districts = int(df["n_districts"].values[0])
        n_without = int(df["n_without"].values[0])
        if n_without:
            return self._result(
                success=False,
                observed=n_without,
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {n_without} of "
                    f"{n_districts} MV grid districts have no AC load, bus_id "
                    f"{list(df['examples'].values[0])}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_districts} MV grid "
                f"districts have an AC load"
            ),
        )


class _ForeignLoadRule(_LoadTableRule):
    """
    Common query for the rules on the neighbouring countries' AC demand.

    Returns, for every foreign country with an AC bus in the scenario, the
    number of AC loads and the mean and peak of their summed hourly series.
    """

    def get_query(self, ctx):
        return f"""
        WITH countries AS (
            SELECT DISTINCT country
            FROM {self.bus_table}
            WHERE scn_name = :scenario
            AND carrier = 'AC'
            AND country <> 'DE'
        ),
        foreign_loads AS (
            SELECT l.load_id, b.country
            FROM {self.table} l
            JOIN {self.bus_table} b
                ON b.bus_id = l.bus
                AND b.scn_name = l.scn_name
            WHERE l.scn_name = :scenario
            AND l.carrier = 'AC'
            AND b.country <> 'DE'
        ),
        hourly AS (
            SELECT fl.country, u.hour, SUM(u.p_set) AS p_set
            FROM foreign_loads fl
            JOIN {self.timeseries_table} t
                ON t.load_id = fl.load_id
                AND t.scn_name = :scenario
            CROSS JOIN LATERAL unnest(t.p_set)
                WITH ORDINALITY AS u(p_set, hour)
            GROUP BY fl.country, u.hour
        ),
        profiles AS (
            SELECT country, AVG(p_set) AS mean_load, MAX(p_set) AS peak_load
            FROM hourly
            GROUP BY country
        ),
        counts AS (
            SELECT country, count(*) AS n_loads
            FROM foreign_loads
            GROUP BY country
        )
        SELECT json_agg(
            json_build_object(
                'country', c.country,
                'n_loads', COALESCE(n.n_loads, 0),
                'mean_load', p.mean_load,
                'peak_load', p.peak_load
            )
            ORDER BY c.country
        ) AS countries
        FROM countries c
        LEFT JOIN counts n USING (country)
        LEFT JOIN profiles p USING (country)
        """

    def _countries(self, df):
        return _parse_json(df["countries"].values[0]) or []

    def _no_neighbours(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no foreign AC buses in "
            f"{self.bus_table}; check skipped"
        )


class LoadTableForeignCoverage(_ForeignLoadRule):
    """
    Check that every neighbouring country with an AC bus has AC demand.

    A neighbour without demand only passes power through, which is
    possible but usually means its load was not mapped to its buses, so
    this is a warning.
    """

    def evaluate_df(self, df, ctx):
        countries = self._countries(df)
        if not countries:
            return self._no_neighbours()

        without = [c["country"] for c in countries if c["n_loads"] == 0]
        if without:
            return self._result(
                success=False,
                observed=len(without),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {len(without)} of "
                    f"{len(countries)} neighbouring countries have an AC bus "
                    f"but no AC load: {', '.join(without)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {len(countries)} "
                f"neighbouring countries have AC loads "
                f"({', '.join(c['country'] for c in countries)})"
            ),
        )


class LoadTableForeignProfile(_ForeignLoadRule):
    """
    Check that each neighbour's AC demand has a plausible load factor.

    The load factor is the mean of the country's summed hourly AC load
    divided by its peak. National load curves lie well inside
    [``min_factor``, ``max_factor``]; a value near 1 means a flat profile,
    a very low one a profile with spikes, both signs of a broken series.
    This is a plausibility check, so it reports a warning.
    """

    def __init__(self, table, rule_id, scenario, min_factor: float = 0.3,
                 max_factor: float = 0.95, **kwargs):
        """
        Parameters
        ----------
        min_factor : float
            Smallest plausible load factor (default: 0.3).
        max_factor : float
            Largest plausible load factor (default: 0.95).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario,
            min_factor=min_factor, max_factor=max_factor, **kwargs,
        )

    def evaluate_df(self, df, ctx):
        countries = [
            c for c in self._countries(df)
            if c["peak_load"] is not None and c["peak_load"] > 0
        ]
        if not countries:
            return self._no_neighbours()

        min_factor = float(self.params.get("min_factor", 0.3))
        max_factor = float(self.params.get("max_factor", 0.95))
        for c in countries:
            c["factor"] = c["mean_load"] / c["peak_load"]
        outside = [
            c for c in countries
            if not min_factor <= c["factor"] <= max_factor
        ]
        low = min(countries, key=lambda c: c["factor"])
        high = max(countries, key=lambda c: c["factor"])

        if outside:
            details = ", ".join(
                f"{c['country']}: {c['factor']:.2f}" for c in outside
            )
            return self._result(
                success=False,
                observed=len(outside),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': the AC load factor of "
                    f"{len(outside)} neighbouring countries lies outside "
                    f"[{min_factor}, {max_factor}]: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': the AC load factor of all "
                f"{len(countries)} neighbouring countries lies in "
                f"[{min_factor}, {max_factor}] ({low['country']} "
                f"{low['factor']:.2f} to {high['country']} "
                f"{high['factor']:.2f})"
            ),
        )
