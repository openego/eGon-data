"""
Sanity check validation rules for the complete eTraGo store tables.

These rules check ``grid.egon_etrago_store`` and
``grid.egon_etrago_store_timeseries`` as eTraGo reads them: every store of
every dataset, German and foreign. They run in ``FinalValidations`` and
fall into two groups:

* all stores: buses, static parameters and time series,
* Germany: the capital cost of the extendable stores against the scenario
  parameters.

``e_cyclic`` is not checked: eGon-data writes the table default for every
store, and eTraGo sets it per carrier itself.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon_validation.rules.base import Severity

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

#: Permitted bus carriers per store carrier. The e-mobility batteries sit
#: on the Li_ion buses of the cars and the HGV_charger buses of the trucks;
#: the H2 grid uses both ``H2`` and ``H2_grid`` buses.
STORE_BUS_CARRIERS = {
    "CH4": ["CH4"],
    "H2_overground": ["H2", "H2_grid"],
    "H2_underground": ["H2_saltcavern"],
    "battery_storage": ["Li_ion", "HGV_charger"],
    "central_heat_store": ["central_heat_store"],
    "dsm": ["dsm"],
    "rural_heat_store": ["rural_heat_store"],
}

#: Time series columns every store of a carrier needs. The DSM band and the
#: state of charge band of the vehicle batteries follow their demand
#: profiles; with only the static values eTraGo would treat them as
#: constant.
REQUIRED_TIMESERIES = {
    "battery_storage": ["e_min_pu", "e_max_pu"],
    "dsm": ["e_min_pu", "e_max_pu"],
}

#: Scenario parameter (column, key under ``capital_cost``) per extendable
#: store carrier.
CAPITAL_COST_PARAMETERS = {
    "H2_overground": ["gas_parameters", "H2_overground"],
    "H2_underground": ["gas_parameters", "H2_underground"],
    "central_heat_store": ["heat_parameters", "central_water_tank"],
    "rural_heat_store": ["heat_parameters", "rural_water_tank"],
}


class _StoreTableRule(_LinkTableRule):
    """
    Common base for the per-scenario rules on the whole store table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        timeseries_table: str = "grid.egon_etrago_store_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Store table, "grid.egon_etrago_store".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        timeseries_table : str
            Store time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_table=bus_table,
            timeseries_table=timeseries_table,
            **kwargs,
        )

    def _stores_with_bus(self, join="JOIN"):
        """FROM clause of the stores joined to their bus as b."""
        return f"""
        {self.table} s
        {join} {self.bus_table} b
            ON b.bus_id = s.bus
            AND b.scn_name = s.scn_name
        """

    def _no_stores(self, which="stores"):
        return self._skip(
            f"Scenario '{self.scenario}' has no {which} in {self.table}; "
            f"check skipped"
        )


class StoreTableScenarioCoverage(LoadTableScenarioCoverage):
    """
    Check that every configured scenario has stores.

    A configured scenario without stores is an error, a scenario that is
    not configured is a warning (see :class:`LoadTableScenarioCoverage`).
    """

    component = "stores"


class StoreTableBus(_StoreTableRule):
    """
    Check that every store sits alone on an existing bus of its carrier.

    The bus is looked up by ``(bus_id, scn_name)``. A store on a bus
    carrier not listed for it in ``bus_carriers`` is an error, and so is a
    second store of the same carrier on one bus: every dataset writes one
    store per bus, so a duplicate means rows written twice. A store carrier
    missing from ``bus_carriers`` altogether is a warning, because it is
    either new, and the list needs extending, or misspelt.
    """

    def __init__(self, table, rule_id, scenario, bus_carriers=None,
                 **kwargs):
        """
        Parameters
        ----------
        bus_carriers : dict, optional
            Permitted bus carriers per store carrier (default:
            :data:`STORE_BUS_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers=dict(bus_carriers or STORE_BUS_CARRIERS),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT json_agg(
                    json_build_object(
                        'carrier', carrier,
                        'bus_carrier', bus_carrier,
                        'n', n,
                        'examples', examples
                    )
                    ORDER BY carrier, bus_carrier
                )
                FROM (
                    SELECT
                        s.carrier,
                        b.carrier AS bus_carrier,
                        count(*) AS n,
                        (array_agg(s.store_id ORDER BY s.store_id))[1:10]
                            AS examples
                    FROM {self._stores_with_bus(join="LEFT JOIN")}
                    WHERE s.scn_name = :scenario
                    GROUP BY s.carrier, b.carrier
                ) p
            ) AS pairs,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM (
                        SELECT carrier, bus
                        FROM {self.table}
                        WHERE scn_name = :scenario
                        GROUP BY carrier, bus
                        HAVING count(*) > 1
                    ) d
                    GROUP BY carrier
                ) c
            ) AS duplicated
        """

    def evaluate_df(self, df, ctx):
        pairs = _parse_json(df["pairs"].values[0])
        if not pairs:
            return self._no_stores()

        bus_carriers = self.params["bus_carriers"]
        duplicated = _parse_json(df["duplicated"].values[0]) or {}
        n_stores = sum(p["n"] for p in pairs)
        missing = [p for p in pairs if p["bus_carrier"] is None]
        unknown = [
            p for p in pairs
            if p["bus_carrier"] is not None
            and p["carrier"] not in bus_carriers
        ]
        wrong = [
            p for p in pairs
            if p["bus_carrier"] is not None
            and p["carrier"] in bus_carriers
            and p["bus_carrier"] not in bus_carriers[p["carrier"]]
        ]

        if missing or wrong or duplicated:
            problems = []
            if missing:
                problems.append(
                    f"on a bus missing from {self.bus_table} for this "
                    f"scenario: "
                    + "; ".join(
                        f"{p['n']} {p['carrier']} (e.g. store_id "
                        f"{p['examples']})"
                        for p in missing
                    )
                )
            if wrong:
                problems.append(
                    "on a bus of an unexpected carrier: "
                    + "; ".join(
                        f"{p['n']} {p['carrier']} on {p['bus_carrier']}"
                        for p in wrong
                    )
                )
            if duplicated:
                problems.append(
                    "on a bus with another store of the same carrier: "
                    + ", ".join(
                        f"{carrier} on {n} buses"
                        for carrier, n in sorted(duplicated.items())
                    )
                )
            return self._result(
                success=False,
                observed=(
                    sum(p["n"] for p in missing + wrong)
                    + sum(duplicated.values())
                ),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_stores} stores, some "
                    f"sit {'; and '.join(problems)}"
                ),
            )
        if unknown:
            return self._result(
                success=False,
                observed=sum(p["n"] for p in unknown),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': store carriers without "
                    f"permitted bus carriers, extend the list if they are "
                    f"intended: "
                    + "; ".join(
                        f"{p['n']} {p['carrier']} on {p['bus_carrier']}"
                        for p in unknown
                    )
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_stores} stores sit "
                f"alone on an existing bus of the expected carrier"
            ),
        )


class StoreTableParameters(_StoreTableRule):
    """
    Check that the static parameters of every store are usable.

    ``e_nom`` must be finite and not negative, and an extendable store
    needs ``e_nom_min <= e_nom_max`` and a capital cost (otherwise eTraGo
    could build it for free). The static ``e_min_pu`` must not exceed
    ``e_max_pu``; their range is not limited, because the DSM and vehicle
    bands legitimately leave [0, 1]. ``standing_loss`` must lie in [0, 1),
    the costs must be finite and not negative, and a fixed store must not
    start fuller than its capacity.
    """

    def __init__(self, table, rule_id, scenario, rtol: float = 1e-6,
                 **kwargs):
        """
        Parameters
        ----------
        rtol : float
            Relative tolerance on ``e_initial <= e_nom`` (default: 1e-6).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, rtol=rtol,
            **kwargs,
        )

    def get_query(self, ctx):
        rtol = float(self.params.get("rtol", 1e-6))
        return f"""
        WITH stores AS (
            SELECT
                store_id,
                {_not_finite('e_nom')} OR e_nom < 0
                    OR (e_nom_extendable AND (
                        {_not_finite('e_nom_min')}
                        OR e_nom_max IS NULL
                        OR e_nom_max = 'NaN'::float8
                        OR e_nom_min > e_nom_max
                    )) AS bad_e_nom,
                {_not_finite('e_min_pu')} OR {_not_finite('e_max_pu')}
                    OR e_min_pu > e_max_pu AS bad_pu,
                {_not_finite('standing_loss')} OR standing_loss < 0
                    OR standing_loss >= 1 AS bad_standing_loss,
                {_not_finite('capital_cost')}
                    OR {_not_finite('marginal_cost')}
                    OR capital_cost < 0 OR marginal_cost < 0
                    OR (e_nom_extendable AND capital_cost = 0) AS bad_cost,
                {_not_finite('e_initial')} OR e_initial < 0
                    OR (NOT e_nom_extendable
                        AND e_initial > e_nom * (1 + {rtol}) + {rtol})
                    AS bad_initial
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_stores,
            count(*) FILTER (WHERE bad_e_nom) AS n_bad_e_nom,
            count(*) FILTER (WHERE bad_pu) AS n_bad_pu,
            count(*) FILTER (WHERE bad_standing_loss) AS n_bad_standing_loss,
            count(*) FILTER (WHERE bad_cost) AS n_bad_cost,
            count(*) FILTER (WHERE bad_initial) AS n_bad_initial,
            (
                array_agg(store_id ORDER BY store_id)
                FILTER (
                    WHERE bad_e_nom OR bad_pu OR bad_standing_loss
                    OR bad_cost OR bad_initial
                )
            )[1:10] AS examples
        FROM stores
        """

    def evaluate_df(self, df, ctx):
        n_stores = int(df["n_stores"].values[0])
        if n_stores == 0:
            return self._no_stores()

        labels = {
            "n_bad_e_nom": (
                "an invalid e_nom or, if extendable, invalid bounds"
            ),
            "n_bad_pu": "a missing or NaN e_min_pu/e_max_pu or "
                        "e_min_pu > e_max_pu",
            "n_bad_standing_loss": "a standing loss outside [0, 1)",
            "n_bad_cost": (
                "an invalid capital or marginal cost, or are extendable "
                "with no capital cost"
            ),
            "n_bad_initial": (
                "an invalid e_initial or one above the fixed e_nom"
            ),
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
                    f"Scenario '{self.scenario}': of {n_stores} stores, "
                    f"{'; '.join(problems)} (e.g. store_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_stores} stores have "
                f"usable static parameters"
            ),
        )


class StoreTableTimeseries(_StoreTableRule):
    """
    Check the store time series: coverage, uniqueness and values.

    Every store of a carrier in ``required`` needs one row with each listed
    column set. A store with more than one row is ambiguous, and a row
    whose store does not exist in the scenario is an orphan. In every
    series, no value may be NULL or NaN, and ``e_min_pu`` must not exceed
    ``e_max_pu`` in any hour where both are given (beyond ``atol``). The
    length is checked by ``ArrayCardinalityValidation`` and the range of
    the bands by the ``SANITY_TIMESERIES_STORE_*`` rules.
    """

    def __init__(self, table, rule_id, scenario, required=None,
                 atol: float = 1e-9, **kwargs):
        """
        Parameters
        ----------
        required : dict, optional
            Time series columns per store carrier (default:
            :data:`REQUIRED_TIMESERIES`).
        atol : float
            Absolute tolerance on ``e_min_pu <= e_max_pu`` (default: 1e-9).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            required=dict(required or REQUIRED_TIMESERIES),
            atol=atol,
            **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": sorted(self.params["required"]),
        }

    def get_query(self, ctx):
        atol = float(self.params.get("atol", 1e-9))
        return f"""
        WITH coverage AS (
            SELECT
                s.carrier,
                count(*) AS n,
                count(*) FILTER (WHERE NOT coalesce(t.e_min_pu, false))
                    AS e_min_pu,
                count(*) FILTER (WHERE NOT coalesce(t.e_max_pu, false))
                    AS e_max_pu,
                count(*) FILTER (WHERE NOT coalesce(t.p_set, false))
                    AS p_set,
                count(*) FILTER (WHERE NOT coalesce(t.marginal_cost, false))
                    AS marginal_cost
            FROM {self.table} s
            LEFT JOIN (
                SELECT
                    store_id,
                    bool_or(e_min_pu IS NOT NULL) AS e_min_pu,
                    bool_or(e_max_pu IS NOT NULL) AS e_max_pu,
                    bool_or(p_set IS NOT NULL) AS p_set,
                    bool_or(marginal_cost IS NOT NULL) AS marginal_cost
                FROM {self.timeseries_table}
                WHERE scn_name = :scenario
                GROUP BY store_id
            ) t ON t.store_id = s.store_id
            WHERE s.scn_name = :scenario
            AND s.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY s.carrier
        ),
        series AS (
            SELECT s.carrier, v.*
            FROM {self.timeseries_table} t
            JOIN {self.table} s
                ON s.store_id = t.store_id
                AND s.scn_name = t.scn_name
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE (t.e_min_pu IS NOT NULL
                               AND (u.e_min_pu IS NULL
                                    OR u.e_min_pu = 'NaN'::float8))
                        OR (t.e_max_pu IS NOT NULL
                            AND (u.e_max_pu IS NULL
                                 OR u.e_max_pu = 'NaN'::float8))
                        OR (t.p_set IS NOT NULL
                            AND (u.p_set IS NULL
                                 OR u.p_set = 'NaN'::float8))
                        OR (t.marginal_cost IS NOT NULL
                            AND (u.marginal_cost IS NULL
                                 OR u.marginal_cost = 'NaN'::float8))
                    ) AS n_nan,
                    count(*) FILTER (
                        WHERE u.e_min_pu <> 'NaN'::float8
                        AND u.e_max_pu <> 'NaN'::float8
                        AND u.e_min_pu > u.e_max_pu + {atol}
                    ) AS n_crossed
                FROM unnest(
                    t.e_min_pu, t.e_max_pu, t.p_set, t.marginal_cost
                ) AS u(e_min_pu, e_max_pu, p_set, marginal_cost)
            ) v
            WHERE t.scn_name = :scenario
        )
        SELECT
            (
                SELECT json_object_agg(carrier, row_to_json(coverage))
                FROM coverage
            ) AS coverage,
            (SELECT count(*) FROM series) AS n_series,
            (SELECT count(*) FROM series WHERE n_nan > 0) AS n_with_nan,
            (SELECT count(*) FROM series WHERE n_crossed > 0) AS n_crossed,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM series
                    WHERE n_nan > 0 OR n_crossed > 0
                    GROUP BY carrier
                ) f
            ) AS failing_carriers,
            (
                SELECT count(*)
                FROM (
                    SELECT store_id
                    FROM {self.timeseries_table}
                    WHERE scn_name = :scenario
                    GROUP BY store_id
                    HAVING count(*) > 1
                ) d
            ) AS n_duplicated,
            (
                SELECT count(*)
                FROM {self.timeseries_table} t
                WHERE t.scn_name = :scenario
                AND NOT EXISTS (
                    SELECT 1
                    FROM {self.table} s
                    WHERE s.store_id = t.store_id
                    AND s.scn_name = t.scn_name
                )
            ) AS n_orphaned
        """

    def evaluate_df(self, df, ctx):
        coverage = _parse_json(df["coverage"].values[0]) or {}
        n_series = int(df["n_series"].values[0])
        n_duplicated = int(df["n_duplicated"].values[0])
        n_orphaned = int(df["n_orphaned"].values[0])
        if not coverage and not n_series and not n_orphaned:
            return self._skip(
                f"Scenario '{self.scenario}' has no stores with time "
                f"dependent attributes and no store time series; check "
                f"skipped"
            )

        required = self.params["required"]
        missing = [
            f"{carrier} without {column}: {counts[column]}"
            for carrier, counts in sorted(coverage.items())
            for column in required.get(carrier, [])
            if counts[column]
        ]
        n_with_nan = int(df["n_with_nan"].values[0])
        n_crossed = int(df["n_crossed"].values[0])

        if missing or n_duplicated or n_orphaned or n_with_nan or n_crossed:
            failing = _parse_json(df["failing_carriers"].values[0]) or {}
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(failing.items())
            )
            coverage_text = (
                "; ".join(missing) or "every store that needs a series has one"
            )
            return self._result(
                success=False,
                observed=(
                    len(missing) + n_duplicated + n_orphaned + n_with_nan
                    + n_crossed
                ),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': "
                    f"{coverage_text}; of {n_series} series, "
                    f"{n_with_nan} contain NULL or NaN and {n_crossed} have "
                    f"e_min_pu > e_max_pu in some hour"
                    + (f" (per carrier {details})" if details else "")
                    + f"; {n_duplicated} stores have more than one row and "
                    f"{n_orphaned} rows belong to no store of the scenario"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': every "
                f"{', '.join(sorted(coverage)) or 'listed'} store has one "
                f"complete series, and all {n_series} series are free of "
                f"NaN with e_min_pu <= e_max_pu"
            ),
        )


class StoreTableCapitalCost(_StoreTableRule):
    """
    Compare the capital cost of German extendable stores with the parameters.

    For each carrier in :data:`CAPITAL_COST_PARAMETERS`, every store on a
    German bus must have ``capital_cost`` equal to ``<column> ->
    'capital_cost' -> <key>`` of the scenario parameters, within ``rtol``.
    A carrier whose parameter is missing in the scenario is not compared
    and named in the message. The parameters are read in the rule's own
    query, so they come from the database under validation.
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

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": sorted(CAPITAL_COST_PARAMETERS),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT json_agg(
                    json_build_object(
                        'carrier', carrier,
                        'n', n,
                        'min', min_cost,
                        'max', max_cost
                    )
                    ORDER BY carrier
                )
                FROM (
                    SELECT
                        s.carrier,
                        count(*) AS n,
                        min(s.capital_cost) AS min_cost,
                        max(s.capital_cost) AS max_cost
                    FROM {self._stores_with_bus()}
                    WHERE s.scn_name = :scenario
                    AND b.country = 'DE'
                    AND s.carrier = ANY(CAST(:carriers AS text[]))
                    GROUP BY s.carrier
                ) c
            ) AS stores,
            (
                SELECT json_build_object(
                    'gas_parameters', gas_parameters -> 'capital_cost',
                    'heat_parameters', heat_parameters -> 'capital_cost'
                )
                FROM {self.parameters_table}
                WHERE name = :scenario
            ) AS parameters
        """

    def evaluate_df(self, df, ctx):
        stores = _parse_json(df["stores"].values[0]) or []
        if not stores:
            return self._no_stores("German stores of the listed carriers")

        parameters = _parse_json(df["parameters"].values[0]) or {}
        rtol = float(self.params.get("rtol", 1e-6))

        compared, mismatches, not_compared = [], [], []
        for store in stores:
            column, key = CAPITAL_COST_PARAMETERS[store["carrier"]]
            expected = (parameters.get(column) or {}).get(key)
            if expected is None:
                not_compared.append(store["carrier"])
                continue
            expected = float(expected)
            tolerance = rtol * max(abs(expected), 1.0)
            low, high = float(store["min"]), float(store["max"])
            compared.append(store["carrier"])
            # NaN compares false, so it has to count as a difference
            if not (
                abs(low - expected) <= tolerance
                and abs(high - expected) <= tolerance
            ):
                mismatches.append(
                    f"{store['carrier']} ({store['n']} stores): "
                    f"{low:g}..{high:g} vs {column}.capital_cost.{key} = "
                    f"{expected:g}"
                )
        skipped = (
            f"; no parameter for {', '.join(not_compared)}"
            if not_compared else ""
        )

        if mismatches:
            return self._result(
                success=False,
                observed=len(mismatches),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': the capital cost of "
                    f"{len(mismatches)} of {len(compared)} German store "
                    f"carriers differs from the scenario parameters: "
                    f"{'; '.join(mismatches)}{skipped}"
                ),
            )
        if not compared:
            return self._skip(
                f"Scenario '{self.scenario}': no capital cost parameter for "
                f"any German store carrier; check skipped{skipped}"
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': the capital cost of all "
                f"{len(compared)} compared German store carriers matches "
                f"the scenario parameters{skipped}"
            ),
        )
