"""
Sanity check validation rules for the complete eTraGo storage unit table.

These rules check ``grid.egon_etrago_storage`` as eTraGo reads it: every
storage unit of every dataset, German and foreign. They run in
``FinalValidations`` and fall into two groups:

* all storage units: buses, static parameters and a cyclic state of
  charge,
* Germany: efficiencies, capital costs and capacities against the scenario
  parameters and ``supply.egon_storages``.

Home batteries with ``p_nom = 0`` are not reported: one row is written per
substation bus whether or not it has a home battery, which is harmless.
``grid.egon_etrago_storage_timeseries`` has no rules.

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

#: Permitted bus carriers per storage carrier.
STORAGE_BUS_CARRIERS = {
    "BESS": ["AC"],
    "battery": ["AC"],
    "home_battery": ["AC"],
    "pumped_hydro": ["AC"],
}

#: Key under ``electricity_parameters -> 'efficiency'`` per storage carrier.
EFFICIENCY_PARAMETERS = {
    "BESS": "battery",
    "battery": "battery",
    "home_battery": "battery",
    "pumped_hydro": "pumped_hydro",
}

#: Storage unit column per key of the efficiency parameters.
EFFICIENCY_COLUMNS = {
    "max_hours": "max_hours",
    "efficiency_store": "store",
    "efficiency_dispatch": "dispatch",
    "standing_loss": "standing_loss",
}

#: Keys under ``electricity_parameters -> 'capital_cost'`` and
#: ``-> 'lifetime'`` per battery carrier.
COST_PARAMETERS = {
    "BESS": ("BESS", "BESS storage"),
    "home_battery": ("home_battery", "home battery storage"),
}


def _fmt(value):
    """Format a JSON number, or the string PostgreSQL writes for NaN."""
    try:
        return f"{float(value):g}"
    except (TypeError, ValueError):
        return str(value)


class _StorageTableRule(_LinkTableRule):
    """
    Common base for the per-scenario rules on the whole storage unit table.

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
            Storage unit table, "grid.egon_etrago_storage".
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

    def _storage_with_bus(self, join="JOIN"):
        """FROM clause of the storage units joined to their bus as b."""
        return f"""
        {self.table} s
        {join} {self.bus_table} b
            ON b.bus_id = s.bus
            AND b.scn_name = s.scn_name
        """

    def _no_storage(self, which="storage units"):
        return self._skip(
            f"Scenario '{self.scenario}' has no {which} in {self.table}; "
            f"check skipped"
        )


class StorageTableScenarioCoverage(LoadTableScenarioCoverage):
    """
    Check that every configured scenario has storage units.

    A configured scenario without storage units is an error, a scenario
    that is not configured is a warning (see
    :class:`LoadTableScenarioCoverage`).
    """

    component = "storage units"


class StorageTableBus(_StorageTableRule):
    """
    Check that every storage unit sits on an existing bus of its carrier.

    The bus is looked up by ``(bus_id, scn_name)``. A storage carrier
    connected to a bus carrier not listed for it in ``bus_carriers`` is an
    error; a storage carrier missing from ``bus_carriers`` altogether is a
    warning, because it is either new, and the list needs extending, or
    misspelt.
    """

    def __init__(self, table, rule_id, scenario, bus_carriers=None,
                 **kwargs):
        """
        Parameters
        ----------
        bus_carriers : dict, optional
            Permitted bus carriers per storage carrier (default:
            :data:`STORAGE_BUS_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers=dict(bus_carriers or STORAGE_BUS_CARRIERS),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT json_agg(
            json_build_object(
                'carrier', carrier,
                'bus_carrier', bus_carrier,
                'n', n,
                'examples', examples
            )
            ORDER BY carrier, bus_carrier
        ) AS pairs
        FROM (
            SELECT
                s.carrier,
                b.carrier AS bus_carrier,
                count(*) AS n,
                (array_agg(s.storage_id ORDER BY s.storage_id))[1:10]
                    AS examples
            FROM {self._storage_with_bus(join="LEFT JOIN")}
            WHERE s.scn_name = :scenario
            GROUP BY s.carrier, b.carrier
        ) p
        """

    def evaluate_df(self, df, ctx):
        pairs = _parse_json(df["pairs"].values[0])
        if not pairs:
            return self._no_storage()

        bus_carriers = self.params["bus_carriers"]
        n_storage = sum(p["n"] for p in pairs)
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

        if missing or wrong:
            problems = []
            if missing:
                problems.append(
                    "on a bus missing from "
                    f"{self.bus_table} for this scenario: "
                    + "; ".join(
                        f"{p['n']} {p['carrier']} (e.g. storage_id "
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
            n_bad = sum(p["n"] for p in missing + wrong)
            return self._result(
                success=False,
                observed=n_bad,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_bad} of {n_storage} "
                    f"storage units sit {'; and '.join(problems)}"
                ),
            )
        if unknown:
            return self._result(
                success=False,
                observed=sum(p["n"] for p in unknown),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': storage carriers without "
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
                f"Scenario '{self.scenario}': all {n_storage} storage units "
                f"sit on an existing bus of the expected carrier"
            ),
        )


class StorageTableParameters(_StorageTableRule):
    """
    Check that the static parameters of every storage unit are usable.

    ``p_nom`` must be finite and not negative, and an extendable unit needs
    ``p_nom_min <= p_nom_max`` and a capital cost (otherwise eTraGo could
    build it for free). ``max_hours`` must be positive, both efficiencies
    in (0, 1], ``standing_loss`` in [0, 1), ``p_min_pu`` in [-1, 0] and
    ``p_max_pu`` in [0, 1]. Capital and marginal cost, the initial state of
    charge and the static inflow must be finite and not negative.
    """

    def get_query(self, ctx):
        return f"""
        WITH storage AS (
            SELECT
                storage_id,
                {_not_finite('p_nom')} OR p_nom < 0
                    OR (p_nom_extendable AND (
                        {_not_finite('p_nom_min')}
                        OR p_nom_max IS NULL
                        OR p_nom_max = 'NaN'::float8
                        OR p_nom_min > p_nom_max
                    )) AS bad_p_nom,
                {_not_finite('max_hours')} OR max_hours <= 0
                    AS bad_max_hours,
                {_not_finite('efficiency_store')}
                    OR {_not_finite('efficiency_dispatch')}
                    OR efficiency_store <= 0 OR efficiency_store > 1
                    OR efficiency_dispatch <= 0 OR efficiency_dispatch > 1
                    OR {_not_finite('standing_loss')}
                    OR standing_loss < 0 OR standing_loss >= 1
                    AS bad_efficiency,
                {_not_finite('p_min_pu')} OR {_not_finite('p_max_pu')}
                    OR p_min_pu < -1 OR p_min_pu > 0
                    OR p_max_pu < 0 OR p_max_pu > 1 AS bad_pu,
                {_not_finite('capital_cost')}
                    OR {_not_finite('marginal_cost')}
                    OR capital_cost < 0 OR marginal_cost < 0
                    OR (p_nom_extendable AND capital_cost = 0) AS bad_cost,
                {_not_finite('state_of_charge_initial')}
                    OR state_of_charge_initial < 0
                    OR {_not_finite('inflow')} OR inflow < 0 AS bad_soc
            FROM {self.table}
            WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_storage,
            count(*) FILTER (WHERE bad_p_nom) AS n_bad_p_nom,
            count(*) FILTER (WHERE bad_max_hours) AS n_bad_max_hours,
            count(*) FILTER (WHERE bad_efficiency) AS n_bad_efficiency,
            count(*) FILTER (WHERE bad_pu) AS n_bad_pu,
            count(*) FILTER (WHERE bad_cost) AS n_bad_cost,
            count(*) FILTER (WHERE bad_soc) AS n_bad_soc,
            (
                array_agg(storage_id ORDER BY storage_id)
                FILTER (
                    WHERE bad_p_nom OR bad_max_hours OR bad_efficiency
                    OR bad_pu OR bad_cost OR bad_soc
                )
            )[1:10] AS examples
        FROM storage
        """

    def evaluate_df(self, df, ctx):
        n_storage = int(df["n_storage"].values[0])
        if n_storage == 0:
            return self._no_storage()

        labels = {
            "n_bad_p_nom": (
                "an invalid p_nom or, if extendable, invalid bounds"
            ),
            "n_bad_max_hours": "a missing, NaN or non-positive max_hours",
            "n_bad_efficiency": (
                "an efficiency outside (0, 1] or a standing loss outside "
                "[0, 1)"
            ),
            "n_bad_pu": "p_min_pu outside [-1, 0] or p_max_pu outside [0, 1]",
            "n_bad_cost": (
                "an invalid capital or marginal cost, or are extendable "
                "with no capital cost"
            ),
            "n_bad_soc": (
                "an invalid initial state of charge or static inflow"
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
                    f"Scenario '{self.scenario}': of {n_storage} storage "
                    f"units, {'; '.join(problems)} (e.g. storage_id "
                    f"{list(df['examples'].values[0])})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_storage} storage units "
                f"have usable static parameters"
            ),
        )


class StorageTableCyclic(_StorageTableRule):
    """
    Check that every storage unit has a cyclic state of charge.

    All storage units of the models are cyclic: a non-cyclic unit starts
    from ``state_of_charge_initial`` and may end the year at any level, so
    its energy balance over the year is not closed. The column defaults to
    false, so a dataset that does not set it produces non-cyclic units.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_storage,
            (
                SELECT json_agg(
                    json_build_object(
                        'carrier', carrier,
                        'country', country,
                        'n', n
                    )
                    ORDER BY carrier, country
                )
                FROM (
                    SELECT
                        s.carrier,
                        coalesce(b.country, '?') AS country,
                        count(*) AS n
                    FROM {self._storage_with_bus(join="LEFT JOIN")}
                    WHERE s.scn_name = :scenario
                    AND s.cyclic_state_of_charge IS NOT TRUE
                    GROUP BY s.carrier, coalesce(b.country, '?')
                ) c
            ) AS not_cyclic
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_storage = int(df["n_storage"].values[0])
        if n_storage == 0:
            return self._no_storage()

        not_cyclic = _parse_json(df["not_cyclic"].values[0]) or []
        if not_cyclic:
            n_bad = sum(c["n"] for c in not_cyclic)
            details = ", ".join(
                f"{c['n']} {c['carrier']} in {c['country']}"
                for c in not_cyclic
            )
            return self._result(
                success=False,
                observed=n_bad,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_bad} of {n_storage} "
                    f"storage units have no cyclic state of charge: "
                    f"{details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_storage} storage units "
                f"have a cyclic state of charge"
            ),
        )


class _StorageParameterRule(_StorageTableRule):
    """
    Base for the German rules that compare columns with the parameters.

    The query returns the minimum and maximum of each compared column per
    German storage carrier, and the scenario's ``electricity_parameters``.
    Subclasses define ``columns`` and :meth:`expected`, which returns the
    expected value per column for a carrier, or None if the carrier is not
    compared.
    """

    #: Storage unit columns compared with the parameters.
    columns = ()

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
        aggregates = ",\n".join(
            f"'{c}', json_build_array(min(s.{c}), max(s.{c}))"
            for c in self.columns
        )
        return f"""
        SELECT
            (
                SELECT json_object_agg(carrier, value)
                FROM (
                    SELECT
                        s.carrier,
                        json_build_object(
                            'n', count(*),
                            {aggregates}
                        ) AS value
                    FROM {self._storage_with_bus()}
                    WHERE s.scn_name = :scenario
                    AND b.country = 'DE'
                    GROUP BY s.carrier
                ) c
            ) AS carriers,
            (
                SELECT electricity_parameters
                FROM {self.parameters_table}
                WHERE name = :scenario
            ) AS parameters
        """

    def expected(self, carrier, parameters):
        raise NotImplementedError

    #: What the rule compares, for the messages.
    subject = "parameters"

    def evaluate_df(self, df, ctx):
        carriers = _parse_json(df["carriers"].values[0]) or {}
        if not carriers:
            return self._no_storage("German storage units")
        parameters = _parse_json(df["parameters"].values[0]) or {}
        rtol = float(self.params.get("rtol", 1e-6))

        compared, mismatches, not_compared = [], [], []
        for carrier, values in sorted(carriers.items()):
            expected = self.expected(carrier, parameters)
            if expected is None:
                not_compared.append(carrier)
                continue
            compared.append(carrier)
            for column in self.columns:
                low, high = values[column]
                target = expected.get(column)
                if target is None:
                    mismatches.append(
                        f"{carrier}: no parameter for {column}"
                    )
                    continue
                target = float(target)
                tolerance = rtol * max(abs(target), 1.0)
                # NaN and NULL compare false, so they count as a difference
                if not (
                    low is not None and high is not None
                    and abs(float(low) - target) <= tolerance
                    and abs(float(high) - target) <= tolerance
                ):
                    mismatches.append(
                        f"{carrier} {column} {_fmt(low)}..{_fmt(high)} vs "
                        f"{target:g}"
                    )
        skipped = (
            f"; not compared: {', '.join(not_compared)}"
            if not_compared else ""
        )

        if mismatches:
            return self._result(
                success=False,
                observed=len(mismatches),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': German storage "
                    f"{self.subject} differ from the scenario parameters: "
                    f"{'; '.join(mismatches)}{skipped}"
                ),
            )
        if not compared:
            return self._skip(
                f"Scenario '{self.scenario}': no German storage carrier with "
                f"{self.subject} in the parameters; check skipped{skipped}"
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': the {self.subject} of "
                f"{', '.join(compared)} match the scenario "
                f"parameters{skipped}"
            ),
        )


class StorageTableEfficiency(_StorageParameterRule):
    """
    Compare the efficiencies of German storage units with the parameters.

    ``max_hours``, ``efficiency_store``, ``efficiency_dispatch`` and
    ``standing_loss`` of every German unit must equal the values under
    ``electricity_parameters -> 'efficiency' -> <key>``, with the key per
    carrier from :data:`EFFICIENCY_PARAMETERS`. Whether the state of charge
    is cyclic is checked for all units by :class:`StorageTableCyclic`.
    """

    columns = tuple(EFFICIENCY_COLUMNS)
    subject = "efficiencies"

    def expected(self, carrier, parameters):
        key = EFFICIENCY_PARAMETERS.get(carrier)
        if key is None:
            return None
        values = (parameters.get("efficiency") or {}).get(key) or {}
        return {
            column: values.get(parameter)
            for column, parameter in EFFICIENCY_COLUMNS.items()
        }


class StorageTableCapitalCost(_StorageParameterRule):
    """
    Compare capital cost and lifetime of German batteries with the parameters.

    ``capital_cost`` and ``lifetime`` of every German unit of a carrier in
    :data:`COST_PARAMETERS` must equal ``electricity_parameters ->
    'capital_cost'`` and ``-> 'lifetime'``. Pumped hydro is not built by
    eTraGo and carries no capital cost.
    """

    columns = ("capital_cost", "lifetime")
    subject = "capital costs and lifetimes"

    def expected(self, carrier, parameters):
        if carrier not in COST_PARAMETERS:
            return None
        cost_key, lifetime_key = COST_PARAMETERS[carrier]
        return {
            "capital_cost": (parameters.get("capital_cost") or {}).get(
                cost_key
            ),
            "lifetime": (parameters.get("lifetime") or {}).get(lifetime_key),
        }


class StorageTableCapacity(_StorageTableRule):
    """
    Compare the German storage capacity per carrier with its source table.

    Per carrier, ``p_nom`` of the fixed units plus ``p_nom_min`` of the
    extendable ones on German buses must add up to ``el_capacity`` in
    ``supply.egon_storages`` on the same buses, within ``rtol``. A carrier
    present on only one side is a mismatch. Whether the source matches the
    national targets is checked on ``supply.egon_storages`` itself.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        source_table: str = "supply.egon_storages",
        rtol: float = 1e-6,
        **kwargs,
    ):
        """
        Parameters
        ----------
        source_table : str
            Storage units per bus that ``StorageEtrago`` reads.
        rtol : float
            Relative tolerance per carrier (default: 1e-6).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            source_table=source_table,
            rtol=rtol,
            **kwargs,
        )
        self.source_table = source_table

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT json_object_agg(carrier, capacity)
                FROM (
                    SELECT
                        s.carrier,
                        sum(
                            CASE WHEN s.p_nom_extendable THEN s.p_nom_min
                            ELSE s.p_nom END
                        ) AS capacity
                    FROM {self._storage_with_bus()}
                    WHERE s.scn_name = :scenario
                    AND b.country = 'DE'
                    GROUP BY s.carrier
                ) e
            ) AS etrago,
            (
                SELECT json_object_agg(carrier, capacity)
                FROM (
                    SELECT st.carrier, sum(st.el_capacity) AS capacity
                    FROM {self.source_table} st
                    JOIN {self.bus_table} b
                        ON b.bus_id = st.bus_id
                        AND b.scn_name = st.scenario
                    WHERE st.scenario = :scenario
                    AND b.country = 'DE'
                    GROUP BY st.carrier
                ) src
            ) AS source
        """

    def evaluate_df(self, df, ctx):
        etrago = _parse_json(df["etrago"].values[0]) or {}
        source = _parse_json(df["source"].values[0]) or {}
        if not etrago and not source:
            return self._no_storage("German storage units")
        rtol = float(self.params.get("rtol", 1e-6))

        rows = []
        for carrier in sorted(set(etrago) | set(source)):
            observed = float(etrago.get(carrier) or 0.0)
            expected = float(source.get(carrier) or 0.0)
            deviation = abs(observed - expected) / max(abs(expected), 1.0)
            rows.append((carrier, observed, expected, deviation))
        mismatches = [r for r in rows if r[3] > rtol]

        def describe(selection):
            return "; ".join(
                f"{c}: {o:.0f} MW vs {e:.0f} MW" for c, o, e, _ in selection
            )

        if mismatches:
            return self._result(
                success=False,
                observed=sum(r[1] for r in rows),
                expected=sum(r[2] for r in rows),
                message=(
                    f"Scenario '{self.scenario}': German storage capacity "
                    f"differs from {self.source_table} for "
                    f"{len(mismatches)} of {len(rows)} carriers: "
                    f"{describe(mismatches)}"
                ),
            )
        return self._result(
            success=True,
            observed=sum(r[1] for r in rows),
            expected=sum(r[2] for r in rows),
            message=(
                f"Scenario '{self.scenario}': German storage capacity matches "
                f"{self.source_table} for all {len(rows)} carriers "
                f"({describe(rows)})"
            ),
        )
