"""
Sanity check validation rules for the complete eTraGo transformer table.

These rules check ``grid.egon_etrago_transformer`` as eTraGo reads it:
every transformer of every dataset, German and foreign. They run in
``FinalValidations`` and fall into two groups:

* all transformers: buses, electrical parameters, capacity limits and
  costs,
* Germany: the capital cost against the scenario parameters.

Two things are deliberately not checked. ``x`` is only required to be
positive: osmtgmod and ``electrical_neighbours`` write it on different
scales, and which convention eTraGo expects is still open. And
``s_nom_min`` may exceed ``s_nom``: osmtgmod lowers ``s_nom`` to the
connected line capacity afterwards, and eTraGo sets ``s_nom_min`` to
``s_nom`` itself. ``grid.egon_etrago_transformer_timeseries`` is not
written by any dataset and has no rules.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)
from egon.data.validation.rules.custom.sanity.etrago_line_table import (
    _not_finite,
)
from egon.data.validation.rules.custom.sanity.etrago_link_table import (
    _LinkTableRule,
)
from egon.data.validation.rules.custom.sanity.etrago_load_table import (
    LoadTableScenarioCoverage,
)

#: Capital cost parameter of ``electricity_parameters -> 'capital_cost'``
#: per pair of (lower, higher) nominal voltages in kV.
CAPITAL_COST_PARAMETERS = {
    (110, 220): "transformer_220_110",
    (110, 380): "transformer_380_110",
    (220, 380): "transformer_380_220",
}


class _TransformerTableRule(_LinkTableRule):
    """
    Common base for the per-scenario rules on the whole transformer table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Transformer table, "grid.egon_etrago_transformer".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_table=bus_table,
            timeseries_table=None,
            **kwargs,
        )

    def _transformers_with_buses(self, join="JOIN"):
        """FROM clause of the transformers joined to their buses b0 and b1."""
        return f"""
        {self.table} t
        {join} {self.bus_table} b0
            ON b0.bus_id = t.bus0
            AND b0.scn_name = t.scn_name
        {join} {self.bus_table} b1
            ON b1.bus_id = t.bus1
            AND b1.scn_name = t.scn_name
        """

    def _no_transformers(self, which="transformers"):
        return self._skip(
            f"Scenario '{self.scenario}' has no {which} in {self.table}; "
            f"check skipped"
        )


class TransformerTableScenarioCoverage(LoadTableScenarioCoverage):
    """
    Check that every configured scenario has transformers.

    A configured scenario without transformers is an error, a scenario that
    is not configured is a warning (see :class:`LoadTableScenarioCoverage`).
    """

    component = "transformers"


class TransformerTableBus(_TransformerTableRule):
    """
    Check that every transformer connects two voltage levels of one grid.

    Both buses must exist in the scenario, be different and AC, have
    different nominal voltages (two buses of one voltage are connected by a
    line), and lie in the same country (a border is crossed by lines and
    links, not by transformers).
    """

    def get_query(self, ctx):
        return f"""
        WITH transformers AS (
            SELECT
                t.trafo_id,
                b0.bus_id IS NULL OR b1.bus_id IS NULL AS missing,
                t.bus0 = t.bus1 AS self_loop,
                b0.carrier <> 'AC' OR b1.carrier <> 'AC' AS not_ac,
                b0.v_nom = b1.v_nom AS same_v_nom,
                b0.country IS DISTINCT FROM b1.country AS two_countries
            FROM {self._transformers_with_buses(join="LEFT JOIN")}
            WHERE t.scn_name = :scenario
        )
        SELECT
            count(*) AS n_transformers,
            count(*) FILTER (WHERE missing) AS n_missing,
            count(*) FILTER (WHERE self_loop) AS n_self_loops,
            count(*) FILTER (WHERE not_ac) AS n_not_ac,
            count(*) FILTER (WHERE same_v_nom) AS n_same_v_nom,
            count(*) FILTER (WHERE two_countries AND NOT missing)
                AS n_two_countries,
            (
                array_agg(trafo_id ORDER BY trafo_id)
                FILTER (
                    WHERE missing OR self_loop OR not_ac OR same_v_nom
                    OR two_countries
                )
            )[1:10] AS examples
        FROM transformers
        """

    def evaluate_df(self, df, ctx):
        n_transformers = int(df["n_transformers"].values[0])
        if n_transformers == 0:
            return self._no_transformers()

        labels = {
            "n_missing": (
                f"have bus0 or bus1 missing from {self.bus_table} for this "
                f"scenario"
            ),
            "n_self_loops": "connect a bus to itself",
            "n_not_ac": "end at a bus that is not AC",
            "n_same_v_nom": "connect two buses of the same voltage",
            "n_two_countries": "connect buses of two countries",
        }
        problems = [
            f"{int(df[column].values[0])} {label}"
            for column, label in labels.items()
            if int(df[column].values[0])
        ]
        if problems:
            return self._result(
                success=False,
                observed=sum(int(df[c].values[0]) for c in labels),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_transformers} "
                    f"transformers, {'; '.join(problems)} (e.g. trafo_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_transformers} "
                f"transformers connect two AC buses of different voltage in "
                f"one country"
            ),
        )


class TransformerTableElectrical(_TransformerTableRule):
    """
    Check that the electrical parameters of every transformer are usable.

    ``s_nom``, ``x`` and ``tap_ratio`` must be finite and positive, ``r``
    finite and not negative, ``b``, ``g`` and ``phase_shift`` finite, and the
    static ``s_max_pu`` in (0, 1]. ``x`` has no range: osmtgmod and
    ``electrical_neighbours`` write it on scales about 1000 times apart, and
    which one eTraGo expects is still open.
    """

    def get_query(self, ctx):
        return f"""
        WITH transformers AS (
            SELECT
                trafo_id,
                {_not_finite('s_nom')} OR s_nom <= 0 AS bad_s_nom,
                {_not_finite('x')} OR x <= 0 AS bad_x,
                {_not_finite('r')} OR r < 0 AS bad_r,
                {_not_finite('b')} OR {_not_finite('g')} AS bad_b_g,
                {_not_finite('tap_ratio')} OR tap_ratio <= 0
                    OR {_not_finite('phase_shift')} AS bad_tap,
                {_not_finite('s_max_pu')} OR s_max_pu <= 0 OR s_max_pu > 1
                    AS bad_s_max_pu
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_transformers,
            count(*) FILTER (WHERE bad_s_nom) AS n_bad_s_nom,
            count(*) FILTER (WHERE bad_x) AS n_bad_x,
            count(*) FILTER (WHERE bad_r) AS n_bad_r,
            count(*) FILTER (WHERE bad_b_g) AS n_bad_b_g,
            count(*) FILTER (WHERE bad_tap) AS n_bad_tap,
            count(*) FILTER (WHERE bad_s_max_pu) AS n_bad_s_max_pu,
            (
                array_agg(trafo_id ORDER BY trafo_id)
                FILTER (
                    WHERE bad_s_nom OR bad_x OR bad_r OR bad_b_g OR bad_tap
                    OR bad_s_max_pu
                )
            )[1:10] AS examples
        FROM transformers
        """

    def evaluate_df(self, df, ctx):
        n_transformers = int(df["n_transformers"].values[0])
        if n_transformers == 0:
            return self._no_transformers()

        labels = {
            "n_bad_s_nom": "a missing, NaN or non-positive s_nom",
            "n_bad_x": "a missing, NaN or non-positive x",
            "n_bad_r": "a missing, NaN or negative r",
            "n_bad_b_g": "a missing or NaN b or g",
            "n_bad_tap": (
                "a missing, NaN or non-positive tap_ratio or an unusable "
                "phase_shift"
            ),
            "n_bad_s_max_pu": "a static s_max_pu outside (0, 1]",
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
                    f"Scenario '{self.scenario}': of {n_transformers} "
                    f"transformers, {'; '.join(problems)} (e.g. trafo_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_transformers} "
                f"transformers have usable electrical parameters"
            ),
        )


class TransformerTableLimits(_TransformerTableRule):
    """
    Check the expansion bounds and the capital cost.

    An error is ``s_nom_min > s_nom_max`` or a NaN bound, a missing, NaN,
    infinite or negative capital cost, or an extendable transformer with no
    capital cost, which eTraGo could expand for free. ``s_nom_min`` may
    exceed ``s_nom``: osmtgmod lowers ``s_nom`` to the connected line
    capacity after setting ``s_nom_min``, and eTraGo sets ``s_nom_min`` to
    ``s_nom`` before the optimisation.
    """

    def get_query(self, ctx):
        return f"""
        WITH transformers AS (
            SELECT
                trafo_id,
                s_nom_min IS NULL OR s_nom_max IS NULL
                    OR s_nom_min = 'NaN'::float8
                    OR s_nom_max = 'NaN'::float8
                    OR s_nom_min > s_nom_max AS bad_bounds,
                {_not_finite('capital_cost')} OR capital_cost < 0
                    AS bad_cost,
                s_nom_extendable AND capital_cost = 0 AS free
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_transformers,
            count(*) FILTER (WHERE bad_bounds) AS n_bad_bounds,
            count(*) FILTER (WHERE bad_cost) AS n_bad_cost,
            count(*) FILTER (WHERE free) AS n_free,
            (
                array_agg(trafo_id ORDER BY trafo_id)
                FILTER (WHERE bad_bounds OR bad_cost OR free)
            )[1:10] AS examples
        FROM transformers
        """

    def evaluate_df(self, df, ctx):
        n_transformers = int(df["n_transformers"].values[0])
        if n_transformers == 0:
            return self._no_transformers()

        labels = {
            "n_bad_bounds": "missing or NaN bounds or s_nom_min > s_nom_max",
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
                    f"Scenario '{self.scenario}': of {n_transformers} "
                    f"transformers, {'; '.join(problems)} (e.g. trafo_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_transformers} "
                f"transformers have consistent expansion bounds and a valid "
                f"capital cost"
            ),
        )


class TransformerTableCapitalCost(_TransformerTableRule):
    """
    Compare the capital cost of German transformers with the parameters.

    Every transformer with both buses in Germany must cost the parameter of
    its voltage pair in ``electricity_parameters -> 'capital_cost'`` (see
    :data:`CAPITAL_COST_PARAMETERS`), within ``rtol``. The cost is per MVA,
    so it depends neither on ``s_nom`` nor on a length. A voltage pair
    without a parameter counts as a mismatch. The parameters are read in the
    rule's own query, so they come from the database under validation.
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
        # Nested CASE on the lower, then the higher voltage of the pair
        branches = {}
        for (low, high), key in CAPITAL_COST_PARAMETERS.items():
            branches.setdefault(low, []).append(
                f"WHEN {high} THEN (p.costs ->> '{key}')::float8"
            )
        case = "\n".join(
            f"WHEN {low} THEN CASE tr.high {' '.join(whens)} END"
            for low, whens in sorted(branches.items())
        )
        return f"""
        WITH parameters AS (
            SELECT electricity_parameters -> 'capital_cost' AS costs
            FROM {self.parameters_table}
            WHERE name = :scenario
        ),
        transformers AS (
            SELECT
                t.trafo_id,
                least(b0.v_nom, b1.v_nom) AS low,
                greatest(b0.v_nom, b1.v_nom) AS high,
                t.capital_cost
            FROM {self._transformers_with_buses()}
            WHERE t.scn_name = :scenario
            AND b0.country = 'DE'
            AND b1.country = 'DE'
        ),
        compared AS (
            SELECT
                tr.*,
                CASE tr.low {case} END AS expected
            FROM transformers tr
            LEFT JOIN parameters p ON true
        ),
        mismatches AS (
            SELECT *
            FROM compared
            WHERE expected IS NULL
            OR NOT abs(capital_cost - expected)
                <= {rtol} * greatest(abs(expected), 1)
        )
        SELECT
            (SELECT count(*) FROM compared) AS n_transformers,
            (SELECT count(*) FROM parameters) AS n_parameters,
            (
                SELECT json_object_agg(pair, cost)
                FROM (
                    SELECT DISTINCT low || '/' || high AS pair,
                        expected AS cost
                    FROM compared
                    WHERE expected IS NOT NULL
                ) c
            ) AS parameters,
            (SELECT count(*) FROM mismatches) AS n_mismatches,
            (
                SELECT json_agg(
                    json_build_object(
                        'trafo_id', trafo_id,
                        'pair', low || '/' || high,
                        'capital_cost', capital_cost,
                        'expected', expected
                    )
                    ORDER BY trafo_id
                )
                FROM (
                    SELECT * FROM mismatches ORDER BY trafo_id LIMIT 10
                ) m
            ) AS examples
        """

    def evaluate_df(self, df, ctx):
        n_transformers = int(df["n_transformers"].values[0])
        if n_transformers == 0:
            return self._no_transformers("German transformers")
        if int(df["n_parameters"].values[0]) == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no row in "
                f"{self.parameters_table}; check skipped"
            )

        parameters = _parse_json(df["parameters"].values[0]) or {}
        costs = ", ".join(
            f"{pair} kV: {cost:g}" for pair, cost in sorted(parameters.items())
        )
        n_mismatches = int(df["n_mismatches"].values[0])
        if n_mismatches:
            examples = _parse_json(df["examples"].values[0]) or []
            details = ", ".join(
                f"{e['trafo_id']} ({e['pair']} kV: {e['capital_cost']:g} vs "
                + (f"{e['expected']:g}" if e["expected"] is not None
                   else "no parameter")
                + ")"
                for e in examples
            )
            return self._result(
                success=False,
                observed=n_mismatches,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_mismatches} of "
                    f"{n_transformers} German transformers have a capital "
                    f"cost other than the parameter of their voltage pair, "
                    f"e.g. trafo_id {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_transformers} German "
                f"transformers cost the parameter of their voltage pair "
                f"({costs})"
            ),
        )
