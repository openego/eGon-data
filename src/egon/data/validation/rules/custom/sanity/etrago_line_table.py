"""
Sanity check validation rules for the complete eTraGo line tables.

These rules check ``grid.egon_etrago_line`` and
``grid.egon_etrago_line_timeseries`` as eTraGo reads them: every AC line of
every dataset, German and foreign. They run in ``FinalValidations`` and
fall into two groups:

* all lines: buses, electrical parameters, capacity limits and costs, and
  the time series rows,
* Germany: the capital cost against the scenario parameters.

The values of the ``s_max_pu`` series are checked by ``DLRValues`` in
``Calculate_dlr``; :class:`LineTableTimeseries` only adds what that rule
does not cover.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon_validation.rules.base import Severity

from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)
from egon.data.validation.rules.custom.sanity.etrago_link_table import (
    _LinkTableRule,
)
from egon.data.validation.rules.custom.sanity.etrago_load_table import (
    LoadTableScenarioCoverage,
)


def _not_finite(column):
    """SQL condition that is true for a NULL, NaN or infinite value.

    The column is cast to ``double precision`` first, because ``x``, ``r``,
    ``b``, ``g`` and ``s_nom`` are ``numeric``. NaN is a legal value of
    both types and passes NOT NULL, so it has to be tested explicitly.
    """
    value = f"({column})::float8"
    return (
        f"({column} IS NULL OR {value} = 'NaN'::float8 "
        f"OR {value} IN ('Infinity'::float8, '-Infinity'::float8))"
    )


class _LineTableRule(_LinkTableRule):
    """
    Common base for the per-scenario rules on the whole line table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        timeseries_table: str = "grid.egon_etrago_line_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Line table, "grid.egon_etrago_line".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        timeseries_table : str
            Line time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_table=bus_table,
            timeseries_table=timeseries_table,
            **kwargs,
        )

    def _lines_with_buses(self):
        """FROM clause of the lines joined to their two buses as b0 and b1."""
        return self._links_with_buses()

    def _no_lines(self, which="lines"):
        return self._skip(
            f"Scenario '{self.scenario}' has no {which} in {self.table}; "
            f"check skipped"
        )


class LineTableScenarioCoverage(LoadTableScenarioCoverage):
    """
    Check that every configured scenario has lines.

    A configured scenario without lines is an error, a scenario that is
    not configured is a warning (see :class:`LoadTableScenarioCoverage`).
    """

    component = "lines"


class LineTableBus(_LineTableRule):
    """
    Check that every line connects two AC buses of its voltage level.

    The buses are looked up by ``(bus_id, scn_name)``: a line pointing at a
    bus id that only exists in another scenario is as unusable for eTraGo
    as one pointing at no bus at all. A line from a bus to itself carries
    nothing; a line to a bus of another carrier belongs in the link table;
    and a line whose ``v_nom`` differs from that of its buses gets the wrong
    per-unit impedance.
    """

    def get_query(self, ctx):
        return f"""
        WITH lines AS (
            SELECT
                l.line_id,
                l.bus0,
                l.bus1,
                b0.bus_id IS NULL OR b1.bus_id IS NULL AS missing,
                l.bus0 = l.bus1 AS self_loop,
                b0.carrier <> 'AC' OR b1.carrier <> 'AC' AS not_ac,
                l.v_nom IS DISTINCT FROM b0.v_nom
                    OR l.v_nom IS DISTINCT FROM b1.v_nom AS wrong_v_nom
            FROM {self.table} l
            LEFT JOIN {self.bus_table} b0
                ON b0.bus_id = l.bus0
                AND b0.scn_name = l.scn_name
            LEFT JOIN {self.bus_table} b1
                ON b1.bus_id = l.bus1
                AND b1.scn_name = l.scn_name
            WHERE l.scn_name = :scenario
        )
        SELECT
            count(*) AS n_lines,
            count(*) FILTER (WHERE missing) AS n_missing,
            (
                array_agg(line_id ORDER BY line_id)
                FILTER (WHERE missing)
            )[1:10] AS missing_examples,
            (
                SELECT array_agg(DISTINCT m.bus_id ORDER BY m.bus_id)
                FROM lines,
                LATERAL unnest(ARRAY[bus0, bus1]) AS m(bus_id)
                WHERE missing
                AND NOT EXISTS (
                    SELECT 1
                    FROM {self.bus_table} b
                    WHERE b.bus_id = m.bus_id
                    AND b.scn_name = :scenario
                )
            ) AS missing_buses,
            count(*) FILTER (WHERE self_loop) AS n_self_loops,
            count(*) FILTER (WHERE not_ac) AS n_not_ac,
            count(*) FILTER (WHERE wrong_v_nom AND NOT missing)
                AS n_wrong_v_nom,
            (
                array_agg(line_id ORDER BY line_id)
                FILTER (WHERE self_loop OR not_ac
                        OR (wrong_v_nom AND NOT missing))
            )[1:10] AS other_examples
        FROM lines
        """

    def evaluate_df(self, df, ctx):
        n_lines = int(df["n_lines"].values[0])
        if n_lines == 0:
            return self._no_lines()

        counts = {
            column: int(df[column].values[0])
            for column in [
                "n_missing",
                "n_self_loops",
                "n_not_ac",
                "n_wrong_v_nom",
            ]
        }
        if any(counts.values()):
            problems = []
            if counts["n_missing"]:
                problems.append(
                    f"{counts['n_missing']} have bus0 or bus1 missing from "
                    f"{self.bus_table} for this scenario (buses "
                    f"{list(df['missing_buses'].values[0] or [])}, e.g. "
                    f"line_id {list(df['missing_examples'].values[0])})"
                )
            if counts["n_self_loops"]:
                problems.append(
                    f"{counts['n_self_loops']} connect a bus to itself"
                )
            if counts["n_not_ac"]:
                problems.append(
                    f"{counts['n_not_ac']} end at a bus that is not AC"
                )
            if counts["n_wrong_v_nom"]:
                problems.append(
                    f"{counts['n_wrong_v_nom']} have a v_nom different from "
                    f"that of their buses"
                )
            other = df["other_examples"].values[0]
            return self._result(
                success=False,
                observed=sum(counts.values()),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_lines} lines, "
                    f"{'; '.join(problems)}"
                    + (f" (e.g. line_id {list(other)})"
                       if other is not None else "")
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_lines} lines connect two "
                f"different AC buses of the scenario at their own voltage"
            ),
        )


class LineTableElectrical(_LineTableRule):
    """
    Check that the electrical parameters of every line are usable.

    ``s_nom``, ``length`` and ``num_parallel`` must be finite and positive,
    ``b`` and ``g`` finite. ``x`` must be positive and ``r`` not negative,
    unless the line has a ``type``: PyPSA then derives both from the line
    type and ``num_parallel`` and ignores the columns, which is why the
    PyPSA-Eur lines have ``x = r = 0``. A zero or missing reactance on a
    line without a type makes the admittance matrix singular.
    """

    def get_query(self, ctx):
        no_type = "coalesce(type, '') = ''"
        return f"""
        WITH lines AS (
            SELECT
                line_id,
                {_not_finite('s_nom')} OR s_nom <= 0 AS bad_s_nom,
                {_not_finite('length')} OR length <= 0 AS bad_length,
                {_not_finite('num_parallel')} OR num_parallel <= 0
                    AS bad_num_parallel,
                {no_type} AND ({_not_finite('x')} OR x <= 0) AS bad_x,
                {no_type} AND ({_not_finite('r')} OR r < 0) AS bad_r,
                {_not_finite('b')} OR {_not_finite('g')} AS bad_b_g
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_lines,
            count(*) FILTER (WHERE bad_s_nom) AS n_bad_s_nom,
            count(*) FILTER (WHERE bad_length) AS n_bad_length,
            count(*) FILTER (WHERE bad_num_parallel) AS n_bad_num_parallel,
            count(*) FILTER (WHERE bad_x) AS n_bad_x,
            count(*) FILTER (WHERE bad_r) AS n_bad_r,
            count(*) FILTER (WHERE bad_b_g) AS n_bad_b_g,
            (
                array_agg(line_id ORDER BY line_id)
                FILTER (
                    WHERE bad_s_nom OR bad_length OR bad_num_parallel
                    OR bad_x OR bad_r OR bad_b_g
                )
            )[1:10] AS examples
        FROM lines
        """

    def evaluate_df(self, df, ctx):
        n_lines = int(df["n_lines"].values[0])
        if n_lines == 0:
            return self._no_lines()

        labels = {
            "n_bad_s_nom": "a missing, NaN or non-positive s_nom",
            "n_bad_length": "a missing, NaN or non-positive length",
            "n_bad_num_parallel": "a missing or non-positive num_parallel",
            "n_bad_x": "no type and a missing, NaN or non-positive x",
            "n_bad_r": "no type and a missing, NaN or negative r",
            "n_bad_b_g": "a missing or NaN b or g",
        }
        problems = [
            f"{int(df[column].values[0])} have {label}"
            for column, label in labels.items()
            if int(df[column].values[0])
        ]
        if problems:
            return self._result(
                success=False,
                observed=sum(int(df[c].values[0]) for c in labels),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_lines} lines, "
                    f"{'; '.join(problems)} (e.g. line_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_lines} lines have usable "
                f"electrical parameters"
            ),
        )


class LineTableLimits(_LineTableRule):
    """
    Check the capacity bounds, the static ``s_max_pu`` and the costs.

    An error is ``s_nom_min > s_nom`` or ``s_nom > s_nom_max`` beyond
    ``rtol`` (the PyPSA-Eur lines carry ``s_nom_min`` a rounding error above
    ``s_nom``), a NaN bound, a static ``s_max_pu`` outside (0, 1], a missing,
    NaN, infinite or negative capital cost, or an extendable line with no
    capital cost, which eTraGo could expand for free. Values of
    ``s_max_pu`` above 1 belong in the time series (dynamic line rating).
    """

    def __init__(self, table, rule_id, scenario, rtol: float = 1e-9,
                 **kwargs):
        """
        Parameters
        ----------
        rtol : float
            Relative tolerance on ``s_nom_min <= s_nom <= s_nom_max``
            (default: 1e-9).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, rtol=rtol,
            **kwargs,
        )

    def get_query(self, ctx):
        rtol = float(self.params.get("rtol", 1e-9))
        tolerance = f"{rtol} * greatest(abs(s_nom), 1)"
        return f"""
        WITH lines AS (
            SELECT
                line_id,
                s_nom_min = 'NaN'::float8 OR s_nom_max = 'NaN'::float8
                    OR s_nom_min > s_nom + {tolerance}
                    OR s_nom > s_nom_max + {tolerance} AS bad_bounds,
                s_max_pu IS NULL OR s_max_pu = 'NaN'::float8
                    OR s_max_pu <= 0 OR s_max_pu > 1 AS bad_s_max_pu,
                {_not_finite('capital_cost')} OR capital_cost < 0
                    AS bad_cost,
                s_nom_extendable AND capital_cost = 0 AS free
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_lines,
            count(*) FILTER (WHERE bad_bounds) AS n_bad_bounds,
            count(*) FILTER (WHERE bad_s_max_pu) AS n_bad_s_max_pu,
            count(*) FILTER (WHERE bad_cost) AS n_bad_cost,
            count(*) FILTER (WHERE free) AS n_free,
            (
                array_agg(line_id ORDER BY line_id)
                FILTER (WHERE bad_bounds OR bad_s_max_pu OR bad_cost OR free)
            )[1:10] AS examples
        FROM lines
        """

    def evaluate_df(self, df, ctx):
        n_lines = int(df["n_lines"].values[0])
        if n_lines == 0:
            return self._no_lines()

        labels = {
            "n_bad_bounds": (
                "NaN bounds or s_nom outside [s_nom_min, s_nom_max]"
            ),
            "n_bad_s_max_pu": "a static s_max_pu outside (0, 1]",
            "n_bad_cost": "a missing, NaN, infinite or negative capital cost",
            "n_free": "s_nom_extendable and no capital cost",
        }
        problems = [
            f"{int(df[column].values[0])} have {label}"
            for column, label in labels.items()
            if int(df[column].values[0])
        ]
        if problems:
            return self._result(
                success=False,
                observed=sum(int(df[c].values[0]) for c in labels),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_lines} lines, "
                    f"{'; '.join(problems)} (e.g. line_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_lines} lines have "
                f"consistent capacity bounds, a static s_max_pu in (0, 1] and "
                f"a valid capital cost"
            ),
        )


class LineTableTimeseries(_LineTableRule):
    """
    Check the line time series rows that ``DLRValues`` does not cover.

    ``DLRValues`` checks that every line has a series of 8760 values
    between 1.0 and 1.5. This rule adds two errors, a line with more than
    one row and a row whose line does not exist in the scenario, and one
    warning: a cross-border line (one bus in Germany, the other abroad)
    whose series exceeds its static ``s_max_pu``. ``Calculate_dlr`` does not
    rate cross-border lines, but it decides by geometry, so a line whose
    route lies inside the German regions gets a rating anyway. Lines with
    both buses abroad are not checked.
    """

    def __init__(self, table, rule_id, scenario, atol: float = 1e-9,
                 **kwargs):
        """
        Parameters
        ----------
        atol : float
            Absolute tolerance on the series against the static
            ``s_max_pu`` (default: 1e-9).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, atol=atol,
            **kwargs,
        )

    def get_query(self, ctx):
        atol = float(self.params.get("atol", 1e-9))
        return f"""
        SELECT
            (
                SELECT count(*)
                FROM {self.timeseries_table}
                WHERE scn_name = :scenario
            ) AS n_series,
            (
                SELECT count(*)
                FROM (
                    SELECT line_id
                    FROM {self.timeseries_table}
                    WHERE scn_name = :scenario
                    GROUP BY line_id
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
                    WHERE l.line_id = t.line_id
                    AND l.scn_name = t.scn_name
                )
            ) AS n_orphaned,
            (
                SELECT json_agg(
                    json_build_object(
                        'line_id', l.line_id,
                        'countries', b0.country || '-' || b1.country,
                        'max', (
                            SELECT max(v)
                            FROM unnest(t.s_max_pu) v
                            WHERE v <> 'NaN'::float8
                        )
                    )
                    ORDER BY l.line_id
                )
                FROM {self._lines_with_buses()}
                JOIN {self.timeseries_table} t
                    ON t.line_id = l.line_id
                    AND t.scn_name = l.scn_name
                WHERE l.scn_name = :scenario
                AND (b0.country = 'DE') <> (b1.country = 'DE')
                AND EXISTS (
                    SELECT 1
                    FROM unnest(t.s_max_pu) v
                    WHERE v > l.s_max_pu + {atol}
                    AND v <> 'NaN'::float8
                )
            ) AS rated_cross_border
        """

    def evaluate_df(self, df, ctx):
        n_series = int(df["n_series"].values[0])
        if n_series == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no series in "
                f"{self.timeseries_table}; check skipped"
            )

        n_duplicated = int(df["n_duplicated"].values[0])
        n_orphaned = int(df["n_orphaned"].values[0])
        rated = _parse_json(df["rated_cross_border"].values[0]) or []

        if n_duplicated or n_orphaned:
            return self._result(
                success=False,
                observed=n_duplicated + n_orphaned,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_series} line time "
                    f"series rows, {n_duplicated} lines have more than one "
                    f"row and {n_orphaned} rows belong to no line of the "
                    f"scenario"
                    + (f"; {len(rated)} cross-border lines have a dynamic "
                       f"rating" if rated else "")
                ),
            )
        if rated:
            details = ", ".join(
                f"{r['line_id']} ({r['countries']}, up to {r['max']:g})"
                for r in rated[:10]
            )
            return self._result(
                success=False,
                observed=len(rated),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {len(rated)} cross-border "
                    f"lines have an s_max_pu series above their static "
                    f"s_max_pu, i.e. a dynamic line rating that "
                    f"Calculate_dlr does not intend for them: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': each of the {n_series} line "
                f"time series rows belongs to exactly one line of the "
                f"scenario, and no cross-border line has a dynamic rating"
            ),
        )


class LineTableCapitalCost(_LineTableRule):
    """
    Compare the capital cost of German lines with the scenario parameters.

    Every line with both buses in Germany must cost
    ``capital_cost.ac_hv_overhead_line`` (110 kV) or
    ``capital_cost.ac_ehv_overhead_line`` (220 and 380 kV) of the
    scenario's ``electricity_parameters`` times its ``length``, within
    ``rtol``. The capital cost is per MVA, so it does not depend on the
    number of circuits. The parameters are read in the rule's own query, so
    they come from the database under validation.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        parameters_table: str = "scenario.egon_scenario_parameters",
        rtol: float = 1e-6,
        **kwargs,
    ):
        """
        Parameters
        ----------
        parameters_table : str
            Scenario parameters table.
        rtol : float
            Relative tolerance against the parameters (default: 1e-6).
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
        rtol = float(self.params.get("rtol", 1e-6))
        return f"""
        WITH parameters AS (
            SELECT
                (electricity_parameters -> 'capital_cost'
                    ->> 'ac_hv_overhead_line')::float8 AS hv,
                (electricity_parameters -> 'capital_cost'
                    ->> 'ac_ehv_overhead_line')::float8 AS ehv
            FROM {self.parameters_table}
            WHERE name = :scenario
        ),
        lines AS (
            SELECT
                l.line_id,
                l.v_nom,
                l.capital_cost,
                l.length * CASE WHEN l.v_nom > 110 THEN p.ehv ELSE p.hv END
                    AS expected
            FROM {self._lines_with_buses()}
            LEFT JOIN parameters p ON true
            WHERE l.scn_name = :scenario
            AND b0.country = 'DE'
            AND b1.country = 'DE'
        ),
        mismatches AS (
            SELECT *
            FROM lines
            WHERE expected IS NULL
            OR NOT abs(capital_cost - expected)
                <= {rtol} * greatest(abs(expected), 1)
        )
        SELECT
            (SELECT count(*) FROM lines) AS n_lines,
            (SELECT hv FROM parameters) AS hv,
            (SELECT ehv FROM parameters) AS ehv,
            (SELECT count(*) FROM mismatches) AS n_mismatches,
            (
                SELECT json_agg(
                    json_build_object(
                        'line_id', line_id,
                        'v_nom', v_nom,
                        'ratio', capital_cost / nullif(expected, 0)
                    )
                    ORDER BY line_id
                )
                FROM (SELECT * FROM mismatches ORDER BY line_id LIMIT 10) m
            ) AS examples
        """

    def evaluate_df(self, df, ctx):
        n_lines = int(df["n_lines"].values[0])
        if n_lines == 0:
            return self._no_lines("German lines")

        hv, ehv = df["hv"].values[0], df["ehv"].values[0]
        if hv is None or ehv is None or hv != hv or ehv != ehv:
            return self._skip(
                f"Scenario '{self.scenario}': no ac_hv_overhead_line or "
                f"ac_ehv_overhead_line capital cost in "
                f"{self.parameters_table}; check skipped"
            )

        n_mismatches = int(df["n_mismatches"].values[0])
        if n_mismatches:
            examples = _parse_json(df["examples"].values[0]) or []
            details = ", ".join(
                f"{e['line_id']} ({e['v_nom']:g} kV, "
                + (f"{e['ratio']:.3g}x" if e["ratio"] is not None
                   else "no expected value")
                + ")"
                for e in examples
            )
            return self._result(
                success=False,
                observed=n_mismatches,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_mismatches} of {n_lines} "
                    f"German lines have a capital cost other than "
                    f"{hv:g} (110 kV) or {ehv:g} (220/380 kV) per km times "
                    f"their length, e.g. line_id {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_lines} German lines cost "
                f"{hv:g} (110 kV) or {ehv:g} (220/380 kV) per km times their "
                f"length"
            ),
        )
