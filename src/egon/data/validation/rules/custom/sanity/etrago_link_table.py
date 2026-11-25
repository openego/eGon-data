"""
Sanity check validation rules for the complete eTraGo link tables.

These rules check ``grid.egon_etrago_link`` and
``grid.egon_etrago_link_timeseries`` as eTraGo reads them: every link of
every dataset, German and foreign. They run in ``FinalValidations`` and
fall into two groups:

* all links: buses, carriers, ``p_nom``, static limits, capital cost and
  time series,
* Germany: efficiencies against the scenario parameters, capacity per
  carrier against the national targets, one link per bus pair.

There are no rules for the foreign links alone: most cross-border
connections are AC lines, so whether a neighbour is connected is checked
on the bus table (``BusTableConnected``).

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config
from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)

#: Permitted (bus0 carrier, bus1 carrier) pairs per link carrier. The H2
#: grid uses both ``H2`` and ``H2_grid`` buses, so the electrolysers, fuel
#: cells, hydrogen power plants, methanisation and SMR links and the
#: pipelines appear with both.
LINK_BUS_CARRIERS = {
    "BEV_charger": [["AC", "Li_ion"]],
    "BEV_charger_transmission": [["AC", "Li_ion"]],
    "CH4": [["CH4", "CH4"]],
    "CH4_to_H2": [["CH4", "H2"], ["CH4", "H2_grid"]],
    "DC": [["AC", "AC"]],
    "H2_OCGT_conversion": [["H2", "AC"], ["H2_grid", "AC"]],
    "H2_OCGT_load_near": [["H2", "AC"], ["H2_grid", "AC"]],
    "H2_OCGT_new": [["H2", "AC"], ["H2_grid", "AC"]],
    "H2_grid": [["H2_grid", "H2_grid"], ["H2_grid", "H2"]],
    "H2_saltcavern": [["H2_grid", "H2_saltcavern"]],
    "H2_to_CH4": [["H2", "CH4"], ["H2_grid", "CH4"]],
    "H2_to_power": [["H2", "AC"], ["H2_grid", "AC"]],
    "HGV_charger": [["AC", "HGV_charger"]],
    "OCGT": [["CH4", "AC"]],
    "PtH2_O2": [["AC", "O2"]],
    "PtH2_waste_heat": [["AC", "central_heat"]],
    "central_gas_CHP": [["CH4", "AC"]],
    "central_gas_CHP_heat": [["CH4", "central_heat"]],
    "central_gas_boiler": [["CH4", "central_heat"]],
    "central_heat_pump": [["AC", "central_heat"]],
    "central_heat_store_charger": [["central_heat", "central_heat_store"]],
    "central_heat_store_discharger": [
        ["central_heat_store", "central_heat"]
    ],
    "central_resistive_heater": [["AC", "central_heat"]],
    "dsm": [["AC", "dsm"]],
    "industrial_gas_CHP": [["CH4", "AC"]],
    "power_to_H2": [["AC", "H2"], ["AC", "H2_grid"]],
    "rural_gas_boiler": [["CH4", "rural_heat"]],
    "rural_heat_pump": [["AC", "rural_heat"]],
    "rural_heat_store_charger": [["rural_heat", "rural_heat_store"]],
    "rural_heat_store_discharger": [["rural_heat_store", "rural_heat"]],
    "rural_resistive_heater": [["AC", "rural_heat"]],
}

#: Extendable link carriers that may have no capital cost, because the
#: investment is carried by the store they charge and discharge.
FREE_EXTENSION_CARRIERS = (
    "central_heat_store_charger",
    "central_heat_store_discharger",
    "rural_heat_store_charger",
    "rural_heat_store_discharger",
)

#: Time series columns every link of a carrier needs. The heat pump COP
#: follows the temperature, the DSM band and the charger availability
#: follow their demand profiles; with a static value eTraGo would treat
#: them as constant.
REQUIRED_TIMESERIES = {
    "BEV_charger": ["p_max_pu"],
    "HGV_charger": ["p_max_pu"],
    "central_heat_pump": ["efficiency"],
    "dsm": ["p_min_pu", "p_max_pu"],
    "rural_heat_pump": ["efficiency"],
}

#: Scenario parameter (column, key under ``efficiency``) that sets the
#: static efficiency of each link carrier.
EFFICIENCY_PARAMETERS = {
    "CH4_to_H2": ["gas_parameters", "CH4_to_H2"],
    "H2_to_CH4": ["gas_parameters", "H2_to_CH4"],
    "H2_to_power": ["gas_parameters", "H2_to_power"],
    "OCGT": ["gas_parameters", "OCGT"],
    "power_to_H2": ["gas_parameters", "power_to_H2"],
    "central_gas_boiler": ["heat_parameters", "central_gas_boiler"],
    "central_heat_store_charger": ["heat_parameters", "water_tank_charger"],
    "central_heat_store_discharger": [
        "heat_parameters",
        "water_tank_discharger",
    ],
    "central_resistive_heater": [
        "heat_parameters",
        "central_resistive_heater",
    ],
    "rural_gas_boiler": ["heat_parameters", "rural_gas_boiler"],
    "rural_heat_store_charger": ["heat_parameters", "water_tank_charger"],
    "rural_heat_store_discharger": [
        "heat_parameters",
        "water_tank_discharger",
    ],
    "rural_resistive_heater": ["heat_parameters", "rural_resistive_heater"],
}

#: Gas-fired links whose ``efficiency`` is the electrical efficiency.
#: eTraGo derives the heat output of the CHP from it, and no parameter
#: holds it, so only the physical bound can be checked.
GAS_TO_POWER_CARRIERS = ("central_gas_CHP", "industrial_gas_CHP")

#: Link carriers that count towards each link carrier of
#: ``supply.egon_scenario_capacities``.
NATIONAL_TARGET_CARRIERS = {
    "residential_rural_heat_pump": ["rural_heat_pump"],
    "rural_heat_pump": ["rural_heat_pump"],
    "urban_central_heat_pump": ["central_heat_pump"],
    "urban_central_resistive_heater": ["central_resistive_heater"],
}

#: Link carriers written at most once per bus pair. Parallel CHP, OCGT,
#: pipeline and DC links between the same buses are legitimate (several
#: plants at one substation, parallel pipes and cables), so they are not
#: listed.
ONE_PER_BUS_PAIR_CARRIERS = (
    "BEV_charger",
    "CH4_to_H2",
    "H2_saltcavern",
    "H2_to_CH4",
    "H2_to_power",
    "HGV_charger",
    "PtH2_O2",
    "PtH2_waste_heat",
    "central_gas_boiler",
    "central_heat_pump",
    "central_heat_store_charger",
    "central_heat_store_discharger",
    "central_resistive_heater",
    "dsm",
    "power_to_H2",
    "rural_gas_boiler",
    "rural_heat_pump",
    "rural_heat_store_charger",
    "rural_heat_store_discharger",
    "rural_resistive_heater",
)


class _LinkTableRule(DataFrameRule):
    """
    Common base for the per-scenario rules on the whole link table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        timeseries_table: str = "grid.egon_etrago_link_timeseries",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Link table, "grid.egon_etrago_link".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table.
        timeseries_table : str
            Link time series table.
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

    def _links_with_buses(self):
        """FROM clause of the links joined to their two buses as b0 and b1."""
        return f"""
        {self.table} l
        JOIN {self.bus_table} b0
            ON b0.bus_id = l.bus0
            AND b0.scn_name = l.scn_name
        JOIN {self.bus_table} b1
            ON b1.bus_id = l.bus1
            AND b1.scn_name = l.scn_name
        """

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

    def _no_links(self, which="links"):
        return self._skip(
            f"Scenario '{self.scenario}' has no {which} in {self.table}; "
            f"check skipped"
        )


class LinkTableScenarioCoverage(DataFrameRule):
    """
    Check that the link table holds exactly the run's scenarios.

    A configured scenario without links means nothing was written for it;
    a scenario that is not configured means rows are left over from a run
    with a different ``--scenarios``. The configured scenarios are read
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
                f"Links exist for exactly the configured scenarios: "
                f"{', '.join(sorted(observed))}"
            )
        else:
            message = (
                f"Configured scenarios without links: "
                f"{', '.join(missing) or 'none'}; scenarios with links that "
                f"are not configured: {', '.join(extra) or 'none'}"
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


class LinkTableBus(_LinkTableRule):
    """
    Check that both ends of every link are distinct buses of its scenario.

    The buses are looked up by ``(bus_id, scn_name)``: a link pointing at a
    bus id that only exists in another scenario is as unusable for eTraGo
    as one pointing at no bus at all. A link from a bus to itself carries
    nothing.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_links,
            count(*) FILTER (
                WHERE b0.bus_id IS NULL OR b1.bus_id IS NULL
            ) AS n_missing,
            (
                array_agg(l.link_id ORDER BY l.link_id)
                FILTER (WHERE b0.bus_id IS NULL OR b1.bus_id IS NULL)
            )[1:10] AS missing_examples,
            count(*) FILTER (WHERE l.bus0 = l.bus1) AS n_self_loops,
            (
                array_agg(l.link_id ORDER BY l.link_id)
                FILTER (WHERE l.bus0 = l.bus1)
            )[1:10] AS self_loop_examples
        FROM {self.table} l
        LEFT JOIN {self.bus_table} b0
            ON b0.bus_id = l.bus0
            AND b0.scn_name = l.scn_name
        LEFT JOIN {self.bus_table} b1
            ON b1.bus_id = l.bus1
            AND b1.scn_name = l.scn_name
        WHERE l.scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_links = int(df["n_links"].values[0])
        if n_links == 0:
            return self._no_links()

        n_missing = int(df["n_missing"].values[0])
        n_self_loops = int(df["n_self_loops"].values[0])
        if n_missing or n_self_loops:
            problems = []
            if n_missing:
                problems.append(
                    f"{n_missing} have bus0 or bus1 missing from "
                    f"{self.bus_table} for this scenario, e.g. link_id "
                    f"{list(df['missing_examples'].values[0])}"
                )
            if n_self_loops:
                problems.append(
                    f"{n_self_loops} connect a bus to itself, e.g. link_id "
                    f"{list(df['self_loop_examples'].values[0])}"
                )
            return self._result(
                success=False,
                observed=n_missing + n_self_loops,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_links} links, "
                    f"{'; '.join(problems)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} links connect two "
                f"different buses of the scenario"
            ),
        )


class LinkTableBusCarrier(_LinkTableRule):
    """
    Check that every link connects buses of the expected energy carriers.

    A wrong bus id usually still points at an existing bus, so
    :class:`LinkTableBus` does not notice it; the carriers of the two
    buses do. A link carrier connecting a pair not listed for it in
    ``bus_carriers`` is an error. A link carrier missing from
    ``bus_carriers`` altogether is a warning: it is either new, and the
    list needs extending, or misspelt.
    """

    def __init__(self, table, rule_id, scenario,
                 bus_carriers=None, **kwargs):
        """
        Parameters
        ----------
        bus_carriers : dict, optional
            Permitted ``[bus0 carrier, bus1 carrier]`` pairs per link
            carrier (default: :data:`LINK_BUS_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers=dict(bus_carriers or LINK_BUS_CARRIERS),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        WITH pairs AS (
            SELECT
                l.carrier,
                b0.carrier AS carrier0,
                b1.carrier AS carrier1,
                count(*) AS n
            FROM {self._links_with_buses()}
            WHERE l.scn_name = :scenario
            GROUP BY l.carrier, b0.carrier, b1.carrier
        )
        SELECT json_agg(
            json_build_object(
                'carrier', carrier,
                'carrier0', carrier0,
                'carrier1', carrier1,
                'n', n
            )
            ORDER BY carrier, carrier0, carrier1
        ) AS pairs
        FROM pairs
        """

    def evaluate_df(self, df, ctx):
        pairs = _parse_json(df["pairs"].values[0])
        if not pairs:
            return self._no_links()

        bus_carriers = self.params["bus_carriers"]
        n_links = sum(p["n"] for p in pairs)
        unknown = [p for p in pairs if p["carrier"] not in bus_carriers]
        mismatches = [
            p for p in pairs
            if p["carrier"] in bus_carriers
            and [p["carrier0"], p["carrier1"]]
            not in bus_carriers[p["carrier"]]
        ]

        def describe(rows):
            return "; ".join(
                f"{p['n']} {p['carrier']} from {p['carrier0']} to "
                f"{p['carrier1']}"
                for p in rows
            )

        if mismatches:
            n_wrong = sum(p["n"] for p in mismatches)
            return self._result(
                success=False,
                observed=n_wrong,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_wrong} of {n_links} "
                    f"links connect buses of unexpected carriers: "
                    f"{describe(mismatches)}"
                    + (f"; carriers not in the list: {describe(unknown)}"
                       if unknown else "")
                ),
            )
        if unknown:
            return self._result(
                success=False,
                observed=sum(p["n"] for p in unknown),
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': link carriers without "
                    f"permitted bus carriers, extend the list if they are "
                    f"intended: {describe(unknown)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} links connect "
                f"buses of the expected carriers"
            ),
        )


class LinkTablePNom(_LinkTableRule):
    """
    Check that ``p_nom`` and its expansion bounds are usable.

    An error is a ``p_nom`` that is NULL, negative, NaN or infinite, or an
    extendable link whose ``p_nom_min`` exceeds ``p_nom_max``. A fixed link
    with ``p_nom = 0`` is only a warning: it never carries anything, but
    does no harm. Extendable links may start at 0 (the electrolysers and
    the heat store chargers do).

    NaN is a legal ``double precision`` that passes NOT NULL, so it has to
    be tested explicitly with ``= 'NaN'::float8``.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_links,
            count(*) FILTER (
                WHERE p_nom IS NULL
                OR p_nom = 'NaN'::float8
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
        n_links = int(df["n_links"].values[0])
        if n_links == 0:
            return self._no_links()

        n_invalid = int(df["n_invalid"].values[0])
        n_bad_bounds = int(df["n_bad_bounds"].values[0])
        n_zero = int(df["n_zero"].values[0])

        if n_invalid or n_bad_bounds:
            return self._result(
                success=False,
                observed=n_invalid + n_bad_bounds,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_links} links, "
                    f"{n_invalid} have a missing, negative, NaN or infinite "
                    f"p_nom and {n_bad_bounds} extendable ones have "
                    f"p_nom_min > p_nom_max or NaN bounds"
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
                    f"Scenario '{self.scenario}': {n_zero} of {n_links} links "
                    f"are not extendable and have p_nom = 0 ({details})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} links have a "
                f"valid p_nom"
            ),
        )


class LinkTableStaticLimits(_LinkTableRule):
    """
    Check the static efficiency, per-unit limits and costs.

    ``efficiency`` must be set, finite and positive; values above 1 are
    allowed, since heat pump COPs and the condensing gas boilers exceed it.
    ``p_min_pu`` and ``p_max_pu`` must lie in [-1, 1] with
    ``p_min_pu <= p_max_pu``: unlike generators, a link may run backwards,
    and pipelines and DC links do. ``capital_cost`` and ``marginal_cost``
    must be set, finite and not negative. Whether an efficiency matches the
    scenario parameters is checked by :class:`LinkTableEfficiency`.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_links,
            count(*) FILTER (
                WHERE efficiency IS NULL
                OR efficiency = 'NaN'::float8
                OR efficiency = 'Infinity'::float8
                OR efficiency <= 0
            ) AS n_bad_efficiency,
            count(*) FILTER (
                WHERE p_min_pu IS NULL
                OR p_max_pu IS NULL
                OR p_min_pu = 'NaN'::float8
                OR p_max_pu = 'NaN'::float8
                OR p_min_pu < -1
                OR p_max_pu > 1
                OR p_min_pu > p_max_pu
            ) AS n_bad_pu,
            count(*) FILTER (
                WHERE capital_cost IS NULL
                OR marginal_cost IS NULL
                OR capital_cost = 'NaN'::float8
                OR marginal_cost = 'NaN'::float8
                OR capital_cost IN ('Infinity'::float8, '-Infinity'::float8)
                OR marginal_cost IN ('Infinity'::float8, '-Infinity'::float8)
                OR capital_cost < 0
                OR marginal_cost < 0
            ) AS n_bad_cost
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_links = int(df["n_links"].values[0])
        if n_links == 0:
            return self._no_links()

        n_bad_efficiency = int(df["n_bad_efficiency"].values[0])
        n_bad_pu = int(df["n_bad_pu"].values[0])
        n_bad_cost = int(df["n_bad_cost"].values[0])
        if n_bad_efficiency or n_bad_pu or n_bad_cost:
            return self._result(
                success=False,
                observed=n_bad_efficiency + n_bad_pu + n_bad_cost,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_links} links, "
                    f"{n_bad_efficiency} have a missing, NaN, infinite or "
                    f"non-positive efficiency, {n_bad_pu} have p_min_pu/"
                    f"p_max_pu outside -1 <= p_min_pu <= p_max_pu <= 1 and "
                    f"{n_bad_cost} a missing, negative, NaN or infinite "
                    f"capital or marginal cost"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} links have valid "
                f"efficiencies, per-unit limits and costs"
            ),
        )


class LinkTableCapitalCost(_LinkTableRule):
    """
    Check that every extendable link pays for its expansion.

    An extendable link with ``capital_cost = 0`` lets eTraGo build
    capacity for free, up to ``p_nom_max`` or the expansion bounds eTraGo
    sets itself (four times ``p_nom`` for foreign DC links by default).
    Carriers in ``exempt_carriers`` are allowed, because their cost is
    carried by another component.

    The reversed copies of the lossy PyPSA-Eur DC links fail here: they
    have ``length = 0`` and therefore no cost, and the constraint that ties
    them to their forward link in PyPSA-Eur does not exist in eTraGo.
    """

    def __init__(self, table, rule_id, scenario, exempt_carriers=None,
                 **kwargs):
        """
        Parameters
        ----------
        exempt_carriers : Sequence[str], optional
            Extendable carriers that may have no capital cost (default:
            :data:`FREE_EXTENSION_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            exempt_carriers=list(
                exempt_carriers
                if exempt_carriers is not None
                else FREE_EXTENSION_CARRIERS
            ),
            **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "exempt_carriers": list(self.params["exempt_carriers"]),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) FILTER (WHERE p_nom_extendable) AS n_extendable,
            (
                json_agg(
                    json_build_object(
                        'link_id', link_id,
                        'carrier', carrier,
                        'bus0', bus0,
                        'bus1', bus1,
                        'length', length
                    )
                    ORDER BY carrier, link_id
                ) FILTER (
                    WHERE p_nom_extendable
                    AND capital_cost = 0
                    AND NOT carrier = ANY(CAST(:exempt_carriers AS text[]))
                )
            ) AS free
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_extendable = int(df["n_extendable"].values[0])
        if n_extendable == 0:
            return self._no_links("extendable links")

        free = _parse_json(df["free"].values[0]) or []
        if free:
            per_carrier = {}
            for link in free:
                per_carrier.setdefault(link["carrier"], []).append(link)
            details = "; ".join(
                f"{carrier}: {len(links)} (link_id "
                f"{[link['link_id'] for link in links][:10]}, "
                f"{sum(link['length'] == 0 for link in links)} with "
                f"length 0)"
                for carrier, links in sorted(per_carrier.items())
            )
            return self._result(
                success=False,
                observed=len(free),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {len(free)} of "
                    f"{n_extendable} extendable links have no capital cost "
                    f"and can be expanded for free: {details}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_extendable} extendable "
                f"links have a capital cost, apart from the exempt carriers"
            ),
        )


class LinkTableTimeseriesCoverage(_LinkTableRule):
    """
    Check that links with time dependent attributes have their series.

    Every link of a carrier in ``required`` needs one time series row with
    each listed column set; without it eTraGo uses the static value in
    every hour, e.g. a heat pump COP of 1. A link with more than one row
    is ambiguous, and rows whose link does not exist in the same scenario
    are reported as orphans.
    """

    def __init__(self, table, rule_id, scenario, required=None, **kwargs):
        """
        Parameters
        ----------
        required : dict, optional
            Time series columns per link carrier (default:
            :data:`REQUIRED_TIMESERIES`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            required=dict(required or REQUIRED_TIMESERIES),
            **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": sorted(self.params["required"]),
        }

    def get_query(self, ctx):
        return f"""
        WITH links AS (
            SELECT
                l.link_id,
                l.carrier,
                bool_or(t.p_min_pu IS NOT NULL) AS p_min_pu,
                bool_or(t.p_max_pu IS NOT NULL) AS p_max_pu,
                bool_or(t.efficiency IS NOT NULL) AS efficiency,
                bool_or(t.marginal_cost IS NOT NULL) AS marginal_cost,
                bool_or(t.p_set IS NOT NULL) AS p_set
            FROM {self.table} l
            LEFT JOIN {self.timeseries_table} t
                ON t.link_id = l.link_id
                AND t.scn_name = l.scn_name
            WHERE l.scn_name = :scenario
            AND l.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY l.link_id, l.carrier
        ),
        per_carrier AS (
            SELECT
                carrier,
                count(*) AS n,
                count(*) FILTER (WHERE NOT coalesce(p_min_pu, false))
                    AS p_min_pu,
                count(*) FILTER (WHERE NOT coalesce(p_max_pu, false))
                    AS p_max_pu,
                count(*) FILTER (WHERE NOT coalesce(efficiency, false))
                    AS efficiency,
                count(*) FILTER (WHERE NOT coalesce(marginal_cost, false))
                    AS marginal_cost,
                count(*) FILTER (WHERE NOT coalesce(p_set, false)) AS p_set
            FROM links
            GROUP BY carrier
        )
        SELECT
            (
                SELECT json_object_agg(carrier, row_to_json(per_carrier))
                FROM per_carrier
            ) AS missing,
            (
                SELECT count(*)
                FROM (
                    SELECT link_id
                    FROM {self.timeseries_table}
                    WHERE scn_name = :scenario
                    GROUP BY link_id
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
                    WHERE l.link_id = t.link_id
                    AND l.scn_name = t.scn_name
                )
            ) AS n_orphaned
        """

    def evaluate_df(self, df, ctx):
        per_carrier = _parse_json(df["missing"].values[0]) or {}
        n_duplicated = int(df["n_duplicated"].values[0])
        n_orphaned = int(df["n_orphaned"].values[0])
        if not per_carrier and not n_duplicated and not n_orphaned:
            return self._skip(
                f"Scenario '{self.scenario}' has no links with time "
                f"dependent attributes and no link time series; check "
                f"skipped"
            )

        required = self.params["required"]
        n_links = sum(c["n"] for c in per_carrier.values())
        missing = [
            f"{carrier} without {column}: {counts[column]}"
            for carrier, counts in sorted(per_carrier.items())
            for column in required.get(carrier, [])
            if counts[column]
        ]

        if missing or n_duplicated or n_orphaned:
            return self._result(
                success=False,
                observed=len(missing) + n_duplicated + n_orphaned,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_links} links with "
                    f"time dependent attributes, "
                    f"{'; '.join(missing) or 'none lack a series'}; "
                    f"{n_duplicated} links have more than one time series "
                    f"row and {n_orphaned} rows belong to no link of the "
                    f"scenario"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} links with time "
                f"dependent attributes have their series "
                f"({', '.join(sorted(per_carrier))}), no link has more than "
                f"one row and no row is orphaned"
            ),
        )


class LinkTableTimeseriesValues(_LinkTableRule):
    """
    Check every link time series of the scenario for unusable values.

    A series fails if it contains NULL or NaN; if ``p_min_pu`` or
    ``p_max_pu`` leave [-1, 1] by more than ``atol``, or ``p_min_pu``
    exceeds ``p_max_pu`` in an hour where both are given; if an efficiency
    is infinite or not positive; or if a marginal cost is negative or
    infinite. The length is checked by ``ArrayCardinalityValidation``.

    All columns are unnested together in one pass per row, which keeps
    the query to one scan of the table.
    """

    def __init__(self, table, rule_id, scenario, atol: float = 1e-9,
                 **kwargs):
        """
        Parameters
        ----------
        atol : float
            Absolute tolerance on the [-1, 1] bounds and on
            ``p_min_pu <= p_max_pu`` (default: 1e-9).
        """
        super().__init__(
            table=table, rule_id=rule_id, scenario=scenario, atol=atol,
            **kwargs,
        )

    def get_query(self, ctx):
        atol = float(self.params.get("atol", 1e-9))
        return f"""
        WITH series AS (
            SELECT l.carrier, s.*
            FROM {self.timeseries_table} t
            JOIN {self.table} l
                ON l.link_id = t.link_id
                AND l.scn_name = t.scn_name
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE (
                            t.p_min_pu IS NOT NULL
                            AND (u.p_min_pu IS NULL
                                 OR u.p_min_pu = 'NaN'::float8)
                        )
                        OR (
                            t.p_max_pu IS NOT NULL
                            AND (u.p_max_pu IS NULL
                                 OR u.p_max_pu = 'NaN'::float8)
                        )
                        OR (
                            t.efficiency IS NOT NULL
                            AND (u.efficiency IS NULL
                                 OR u.efficiency = 'NaN'::float8)
                        )
                        OR (
                            t.marginal_cost IS NOT NULL
                            AND (u.marginal_cost IS NULL
                                 OR u.marginal_cost = 'NaN'::float8)
                        )
                        OR (
                            t.p_set IS NOT NULL
                            AND (u.p_set IS NULL OR u.p_set = 'NaN'::float8)
                        )
                    ) AS n_nan,
                    count(*) FILTER (
                        WHERE u.p_min_pu < -1 - {atol}
                        OR u.p_min_pu > 1 + {atol}
                        OR u.p_max_pu < -1 - {atol}
                        OR u.p_max_pu > 1 + {atol}
                    ) AS n_pu_out_of_range,
                    count(*) FILTER (
                        WHERE u.p_min_pu <> 'NaN'::float8
                        AND u.p_max_pu <> 'NaN'::float8
                        AND u.p_min_pu > u.p_max_pu + {atol}
                    ) AS n_pu_crossed,
                    count(*) FILTER (
                        WHERE u.efficiency <= 0
                        OR u.efficiency = 'Infinity'::float8
                    ) AS n_bad_efficiency,
                    count(*) FILTER (
                        WHERE u.marginal_cost < 0
                        OR u.marginal_cost = 'Infinity'::float8
                        OR u.p_set IN ('Infinity'::float8, '-Infinity'::float8)
                    ) AS n_bad_other,
                    min(u.efficiency) FILTER (
                        WHERE u.efficiency <> 'NaN'::float8
                    ) AS min_efficiency,
                    max(u.efficiency) FILTER (
                        WHERE u.efficiency <> 'NaN'::float8
                    ) AS max_efficiency
                FROM unnest(
                    t.p_min_pu, t.p_max_pu, t.efficiency, t.marginal_cost,
                    t.p_set
                ) AS u(p_min_pu, p_max_pu, efficiency, marginal_cost, p_set)
            ) s
            WHERE t.scn_name = :scenario
        )
        SELECT
            count(*) AS n_series,
            count(*) FILTER (WHERE n_nan > 0) AS n_with_nan,
            count(*) FILTER (WHERE n_pu_out_of_range > 0) AS n_out_of_range,
            count(*) FILTER (WHERE n_pu_crossed > 0) AS n_crossed,
            count(*) FILTER (WHERE n_bad_efficiency > 0) AS n_bad_efficiency,
            count(*) FILTER (WHERE n_bad_other > 0) AS n_bad_other,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM series
                    WHERE n_nan > 0
                    OR n_pu_out_of_range > 0
                    OR n_pu_crossed > 0
                    OR n_bad_efficiency > 0
                    OR n_bad_other > 0
                    GROUP BY carrier
                ) f
            ) AS failing_carriers,
            min(min_efficiency) AS min_efficiency,
            max(max_efficiency) AS max_efficiency
        FROM series
        """

    def evaluate_df(self, df, ctx):
        n_series = int(df["n_series"].values[0])
        if n_series == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has no series in "
                f"{self.timeseries_table}; check skipped"
            )

        counts = {
            column: int(df[column].values[0])
            for column in [
                "n_with_nan",
                "n_out_of_range",
                "n_crossed",
                "n_bad_efficiency",
                "n_bad_other",
            ]
        }
        efficiency_range = (
            f"{df['min_efficiency'].values[0]:g}.."
            f"{df['max_efficiency'].values[0]:g}"
            if df["min_efficiency"].values[0] is not None
            and df["min_efficiency"].values[0]
            == df["min_efficiency"].values[0]
            else "none"
        )

        if any(counts.values()):
            failing = _parse_json(df["failing_carriers"].values[0]) or {}
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(failing.items())
            )
            return self._result(
                success=False,
                observed=sum(counts.values()),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_series} link time "
                    f"series rows, {counts['n_with_nan']} contain NULL or "
                    f"NaN, {counts['n_out_of_range']} have p_min_pu/p_max_pu "
                    f"outside [-1, 1], {counts['n_crossed']} have p_min_pu > "
                    f"p_max_pu in some hour, {counts['n_bad_efficiency']} an "
                    f"infinite or non-positive efficiency and "
                    f"{counts['n_bad_other']} a negative or infinite "
                    f"marginal cost or an infinite p_set (per carrier "
                    f"{details}; efficiency range {efficiency_range})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_series} link time "
                f"series rows are complete and inside their bounds "
                f"(efficiency range {efficiency_range})"
            ),
        )


class LinkTableEfficiency(_LinkTableRule):
    """
    Compare the static efficiency of German links with the parameters.

    For each carrier in ``efficiency_parameters``, every link with both
    buses in Germany must have the efficiency from
    ``<column> -> 'efficiency' -> <key>`` of the scenario parameters,
    within ``rtol``. A carrier whose parameter is missing in the scenario
    is not compared and named in the message. The parameters are read in
    the rule's own query, so they come from the database under validation.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        parameters_table: str = "scenario.egon_scenario_parameters",
        efficiency_parameters=None,
        rtol: float = 1e-6,
        **kwargs,
    ):
        """
        Parameters
        ----------
        parameters_table : str
            Scenario parameters table.
        efficiency_parameters : dict, optional
            ``[parameter column, key]`` per link carrier (default:
            :data:`EFFICIENCY_PARAMETERS`).
        rtol : float
            Relative tolerance against the parameters (default: 1e-6).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            parameters_table=parameters_table,
            efficiency_parameters=dict(
                efficiency_parameters or EFFICIENCY_PARAMETERS
            ),
            rtol=rtol,
            **kwargs,
        )
        self.parameters_table = parameters_table

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "carriers": sorted(self.params["efficiency_parameters"]),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT json_agg(
                    json_build_object(
                        'carrier', carrier,
                        'n', n,
                        'min', min_efficiency,
                        'max', max_efficiency
                    )
                    ORDER BY carrier
                )
                FROM (
                    SELECT
                        l.carrier,
                        count(*) AS n,
                        min(l.efficiency) AS min_efficiency,
                        max(l.efficiency) AS max_efficiency
                    FROM {self._links_with_buses()}
                    WHERE l.scn_name = :scenario
                    AND b0.country = 'DE'
                    AND b1.country = 'DE'
                    AND l.carrier = ANY(CAST(:carriers AS text[]))
                    GROUP BY l.carrier
                ) c
            ) AS links,
            (
                SELECT json_build_object(
                    'gas_parameters', gas_parameters -> 'efficiency',
                    'heat_parameters', heat_parameters -> 'efficiency'
                )
                FROM {self.parameters_table}
                WHERE name = :scenario
            ) AS parameters
        """

    def evaluate_df(self, df, ctx):
        links = _parse_json(df["links"].values[0]) or []
        if not links:
            return self._no_links("German links of the listed carriers")

        parameters = _parse_json(df["parameters"].values[0]) or {}
        mapping = self.params["efficiency_parameters"]
        rtol = float(self.params.get("rtol", 1e-6))

        compared, mismatches, not_compared = [], [], []
        for link in links:
            column, key = mapping[link["carrier"]]
            expected = (parameters.get(column) or {}).get(key)
            if expected is None:
                not_compared.append(link["carrier"])
                continue
            expected = float(expected)
            tolerance = rtol * max(abs(expected), 1.0)
            low, high = float(link["min"]), float(link["max"])
            compared.append(link["carrier"])
            # NaN compares false, so it has to count as a difference
            if not (
                abs(low - expected) <= tolerance
                and abs(high - expected) <= tolerance
            ):
                mismatches.append(
                    f"{link['carrier']} ({link['n']} links): "
                    f"{low:g}..{high:g} vs {column}.efficiency.{key} = "
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
                    f"Scenario '{self.scenario}': the efficiency of "
                    f"{len(mismatches)} of {len(compared)} German link "
                    f"carriers differs from the scenario parameters: "
                    f"{'; '.join(mismatches)}{skipped}"
                ),
            )
        if not compared:
            return self._skip(
                f"Scenario '{self.scenario}': no efficiency parameter for "
                f"any German link carrier; check skipped{skipped}"
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': the efficiency of all "
                f"{len(compared)} compared German link carriers matches the "
                f"scenario parameters{skipped}"
            ),
        )


class LinkTableGasToPowerEfficiency(_LinkTableRule):
    """
    Check that gas CHP links do not convert gas to power without losses.

    The ``efficiency`` of a gas CHP link is its electrical efficiency, and
    eTraGo derives the heat output from it (``efficiency / c_v`` in the
    CHP constraints), so it must be below 1. A value of exactly 1 is the
    table default, i.e. no efficiency was written at all. No scenario
    parameter holds these efficiencies, so only this physical bound is
    checked.
    """

    def __init__(self, table, rule_id, scenario, carriers=None, **kwargs):
        """
        Parameters
        ----------
        carriers : Sequence[str], optional
            Gas-to-power link carriers (default:
            :data:`GAS_TO_POWER_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers or GAS_TO_POWER_CARRIERS),
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
                'n', n,
                'n_lossless', n_lossless,
                'min', min_efficiency,
                'max', max_efficiency
            )
            ORDER BY carrier
        ) AS carriers
        FROM (
            SELECT
                l.carrier,
                count(*) AS n,
                count(*) FILTER (
                    WHERE NOT l.efficiency < 1
                ) AS n_lossless,
                min(l.efficiency) AS min_efficiency,
                max(l.efficiency) AS max_efficiency
            FROM {self._links_with_buses()}
            WHERE l.scn_name = :scenario
            AND b0.country = 'DE'
            AND b1.country = 'DE'
            AND l.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY l.carrier
        ) c
        """

    def evaluate_df(self, df, ctx):
        carriers = _parse_json(df["carriers"].values[0]) or []
        if not carriers:
            return self._no_links("German gas CHP links")

        n_links = sum(c["n"] for c in carriers)
        lossless = [c for c in carriers if c["n_lossless"]]
        if lossless:
            n_lossless = sum(c["n_lossless"] for c in lossless)
            details = "; ".join(
                f"{c['carrier']}: {c['n_lossless']} of {c['n']} (efficiency "
                f"{float(c['min']):g}..{float(c['max']):g})"
                for c in lossless
            )
            return self._result(
                success=False,
                observed=n_lossless,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_lossless} of {n_links} "
                    f"German gas CHP links have an electrical efficiency of "
                    f"1 or more: {details}"
                ),
            )
        details = ", ".join(
            f"{c['carrier']} {float(c['min']):g}..{float(c['max']):g}"
            for c in carriers
        )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} German gas CHP "
                f"links have an electrical efficiency below 1 ({details})"
            ),
        )


class LinkTableNationalCapacity(_LinkTableRule):
    """
    Compare the German link capacity per carrier with the national targets.

    For each link carrier of ``supply.egon_scenario_capacities`` listed in
    ``target_carriers``, the ``p_nom`` of the links with both buses in
    Germany and the mapped carriers must add up to the target. A deviation
    above ``rtol`` is an error; one above ``warn_rtol`` is a warning.

    ``nuts`` restricts the comparison to the targets of these federal
    states. National targets (``nuts = 'DE'``) are then left out, because
    a run on part of Germany only holds a share of them.
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
            Link carriers per target carrier (default:
            :data:`NATIONAL_TARGET_CARRIERS`).
        nuts : list of str, optional
            NUTS-1 codes of the federal states to compare; None compares
            all targets, national ones included.
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
                    AND component = 'link'
                    AND (
                        :all_nuts
                        OR nuts = ANY(CAST(:nuts AS text[]))
                    )
                    GROUP BY carrier
                ) c
            ) AS targets,
            (
                SELECT json_object_agg(carrier, p_nom)
                FROM (
                    SELECT l.carrier, SUM(l.p_nom) AS p_nom
                    FROM {self._links_with_buses()}
                    WHERE l.scn_name = :scenario
                    AND b0.country = 'DE'
                    AND b1.country = 'DE'
                    GROUP BY l.carrier
                ) p
            ) AS links
        """

    def evaluate_df(self, df, ctx):
        targets = _parse_json(df["targets"].values[0]) or {}
        if not targets:
            return self._skip(
                f"Scenario '{self.scenario}' has no link targets in "
                f"{self.capacities_table} for the dataset boundary; check "
                f"skipped"
            )
        links = _parse_json(df["links"].values[0]) or {}

        mapping = self.params["target_carriers"]
        rtol = float(self.params.get("rtol", 0.05))
        warn_rtol = float(self.params.get("warn_rtol", 1e-3))

        compared = []
        for target_carrier, expected in sorted(targets.items()):
            if target_carrier not in mapping:
                continue
            expected = float(expected)
            observed = sum(
                float(links.get(c, 0.0)) for c in mapping[target_carrier]
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
                    f"Scenario '{self.scenario}': German link capacity "
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
                    f"Scenario '{self.scenario}': German link capacity is "
                    f"within {rtol:.0%} of the national target for all "
                    f"{len(compared)} carriers, but deviates for "
                    f"{describe(warnings)}{skipped}"
                ),
            )
        if not compared:
            return self._skip(
                f"Scenario '{self.scenario}': no link target is mapped to "
                f"a link carrier; check skipped{skipped}"
            )
        return self._result(
            success=True,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': German link capacity matches "
                f"the national target for all {len(compared)} carriers "
                f"({describe(compared)}){skipped}"
            ),
        )


class LinkTableUniquePerBusPair(_LinkTableRule):
    """
    Check that per-bus technologies are written once per bus pair.

    Heat pumps, boilers, store chargers, DSM, chargers and the hydrogen
    conversion links are written once per pair of German buses. A second
    link with the same ``(bus0, bus1, carrier)`` means a dataset wrote its
    links twice, e.g. after a rerun that did not delete the old rows, and
    doubles the capacity.
    """

    def __init__(self, table, rule_id, scenario, carriers=None, **kwargs):
        """
        Parameters
        ----------
        carriers : Sequence[str], optional
            Link carriers written at most once per bus pair (default:
            :data:`ONE_PER_BUS_PAIR_CARRIERS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers or ONE_PER_BUS_PAIR_CARRIERS),
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
        WITH pairs AS (
            SELECT l.carrier, l.bus0, l.bus1, count(*) AS n
            FROM {self._links_with_buses()}
            WHERE l.scn_name = :scenario
            AND b0.country = 'DE'
            AND b1.country = 'DE'
            AND l.carrier = ANY(CAST(:carriers AS text[]))
            GROUP BY l.carrier, l.bus0, l.bus1
        )
        SELECT
            count(*) AS n_pairs,
            (
                SELECT json_object_agg(carrier, n)
                FROM (
                    SELECT carrier, count(*) AS n
                    FROM pairs
                    WHERE n > 1
                    GROUP BY carrier
                ) d
            ) AS duplicated
        FROM pairs
        """

    def evaluate_df(self, df, ctx):
        n_pairs = int(df["n_pairs"].values[0])
        if n_pairs == 0:
            return self._no_links("German links of the listed carriers")

        duplicated = _parse_json(df["duplicated"].values[0]) or {}
        if duplicated:
            n_duplicated = sum(duplicated.values())
            details = ", ".join(
                f"{carrier}: {n}" for carrier, n in sorted(duplicated.items())
            )
            return self._result(
                success=False,
                observed=n_duplicated,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_duplicated} of {n_pairs} "
                    f"German bus pairs have more than one link of a carrier "
                    f"that is written once per bus pair ({details})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': each of the {n_pairs} German "
                f"bus pairs has at most one link per carrier"
            ),
        )
