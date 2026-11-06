"""
Sanity check validation rules for the gas datasets.

Each gas dataset lists its rules in the static method ``validation_rules``;
the cross-cutting rules are in :func:`final_validation_rules`. Most rules
need no expected value; the few totals of :class:`GasComponentTotal` come
from the scenario parameters or are given for both dataset boundaries. The
eTraGo tables are shared with other sectors, so the rules always filter by
scenario and carrier.

"""

import re

from egon_validation.rules.base import DataFrameRule, Severity
import pandas as pd

from egon.data import config
from egon.data.datasets.power_etrago.match_ocgt import OCGT_CARRIERS
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)
from egon.data.validation import resolve_boundary_dependence

# Scenarios the gas rules are defined for. A rule for a scenario that is
# not in ``--scenarios`` of the run is skipped.
GAS_SCENARIOS = ("status2024", "reGon2037", "reGon2045")

# Scenarios with a gas network. status2024 models the German methane
# system as a single CH4 bus without pipes and has no hydrogen.
TARGET_SCENARIOS = ("reGon2037", "reGon2045")

# Dataset boundaries an expected value has to be given for
BOUNDARIES = ("Schleswig-Holstein", "Everything")

# Expected value taken from the scenario parameters (``parameter`` of the
# rule). The model scales the gas sector to national totals and only then
# cuts it to the dataset boundary, so this is valid for "Everything" only.
FROM_PARAMETERS = "FROM_PARAMETERS"

# Absolute tolerance, used when the expected value is 0
ATOL = 1e-6

# Bus columns of the eTraGo component tables
BUS_COLUMNS = {
    "grid.egon_etrago_bus": (),
    "grid.egon_etrago_link": ("bus0", "bus1"),
    "grid.egon_etrago_generator": ("bus",),
    "grid.egon_etrago_load": ("bus",),
    "grid.egon_etrago_store": ("bus",),
}

# Id columns of the eTraGo component tables
ID_COLUMNS = {
    "grid.egon_etrago_bus": "bus_id",
    "grid.egon_etrago_link": "link_id",
    "grid.egon_etrago_generator": "generator_id",
    "grid.egon_etrago_load": "load_id",
    "grid.egon_etrago_store": "store_id",
}

_NO_EXPECTED = object()


def for_each_scenario(
    rule_class,
    rule_id,
    expected=_NO_EXPECTED,
    scenarios=GAS_SCENARIOS,
    **params,
):
    """
    Create one rule per scenario (the scenario is appended to the rule id)

    ``expected`` is a number or FROM_PARAMETERS, optionally
    per boundary (``{"Schleswig-Holstein": ..., "Everything": ...}``) and/or
    per scenario (a dict naming every scenario of ``scenarios``).

    Parameters
    ----------
    rule_class : type
        One of the rule classes of this module
    rule_id : str
        Rule id without the scenario
    expected : optional
        Expected value, see above
    scenarios : tuple of str
        Scenarios to create a rule for
    **params
        Further parameters of the rule

    Returns
    -------
    list
        One rule per scenario

    """
    rules = []
    for scenario in scenarios:
        if expected is not _NO_EXPECTED:
            params["expected"] = _per_boundary(
                _of_scenario(expected, scenario, scenarios)
            )
        rules.append(
            rule_class(
                rule_id=f"{rule_id}.{scenario}", scenario=scenario, **params
            )
        )
    return rules


def _of_scenario(expected, scenario, scenarios):
    """Expected value of one scenario, see :func:`for_each_scenario`"""
    if not isinstance(expected, dict) or set(expected) <= set(BOUNDARIES):
        return expected

    if set(expected) != set(scenarios):
        raise ValueError(
            f"Expected values are given for {sorted(expected)}, but the rule "
            f"is created for {sorted(scenarios)}."
        )
    return expected[scenario]


def _per_boundary(value):
    """Wrap a value into a boundary dependent value for both boundaries"""
    if not isinstance(value, dict):
        value = {boundary: value for boundary in BOUNDARIES}

    if set(value) != set(BOUNDARIES):
        raise ValueError(
            f"Expected values are given for {sorted(value)}, but both "
            f"boundaries {list(BOUNDARIES)} are needed."
        )
    for boundary, boundary_value in value.items():
        if boundary_value == FROM_PARAMETERS and boundary != "Everything":
            raise ValueError(
                f"FROM_PARAMETERS is only valid for the boundary "
                f"'Everything', not for '{boundary}'."
            )
    return resolve_boundary_dependence(value)


def _identifier(name):
    """Return a column name, checked because it is written into the SQL"""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"Invalid column name: {name!r}")
    return name


def _location_sql(table, location):
    """
    Return the joins and the condition that restrict a component table
    (alias ``c``) to a location

    Parameters
    ----------
    table : str
        eTraGo component table, a key of :data:`BUS_COLUMNS`
    location : str or None
        "DE", "abroad", "cross-border" or None (everywhere)

    Returns
    -------
    tuple of str
        Joins and condition

    """
    if table not in BUS_COLUMNS:
        raise ValueError(f"Not an eTraGo component table: {table}")

    if location is None:
        return "", "TRUE"

    columns = BUS_COLUMNS[table]
    if columns:
        joins = "".join(
            f" JOIN grid.egon_etrago_bus b{i}"
            f" ON b{i}.bus_id = c.{column} AND b{i}.scn_name = c.scn_name"
            for i, column in enumerate(columns)
        )
        countries = [f"b{i}.country" for i in range(len(columns))]
    else:
        joins = ""
        countries = ["c.country"]

    if location == "DE":
        condition = " AND ".join(f"{c} = 'DE'" for c in countries)
    elif location == "abroad":
        condition = " AND ".join(f"{c} <> 'DE'" for c in countries)
    elif location == "cross-border" and len(countries) == 2:
        condition = f"({countries[0]} = 'DE') <> ({countries[1]} = 'DE')"
    else:
        raise ValueError(f"Invalid location {location!r} for {table}")

    return joins, condition


def _parameter(scenario, path):
    """
    Return a value of the scenario parameters

    Parameters
    ----------
    scenario : str
        Name of the scenario
    path : tuple of str
        Sector and keys, e.g. ("gas", "industrial_gas_demand", "CH4")

    Returns
    -------
    float
        The value; a dict of values (e.g. per federal state) is summed

    """
    sector, *keys = path
    value = get_sector_parameters(sector, scenario=scenario)
    for key in keys:
        value = value[key]
    if isinstance(value, dict):
        return float(sum(value.values()))
    return float(value)


def _compare(observed, expected, comparison, rtol):
    """Check an observed value against the expected one"""
    tolerance = rtol * abs(expected) + ATOL
    if comparison == "equal":
        return abs(observed - expected) <= tolerance
    if comparison == "at_most":
        return observed <= expected + tolerance
    if comparison == "at_least":
        return observed >= expected - tolerance
    raise ValueError(f"Invalid comparison: {comparison!r}")


def _examples(values):
    """Up to ten ids of the components that fail a rule, for the message"""
    if not isinstance(values, list) or not values:
        return ""
    return f" (e.g. {', '.join(str(v) for v in values[:10])})"


class _GasRule(DataFrameRule):
    """
    Common part of the gas rules: one scenario, skipped if not built

    Every query of a gas rule returns exactly one row (aggregates), as
    :class:`DataFrameRule` fails on an empty query result.
    """

    def __init__(self, table, rule_id, scenario, **kwargs):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Skip scenarios that the run does not build"""
        if self.scenario not in config.settings()["egon-data"]["--scenarios"]:
            return self.create_result(
                success=True,
                message=(
                    f"Scenario '{self.scenario}' is not part of this run; "
                    "check skipped"
                ),
                severity=Severity.INFO,
            )
        return super().evaluate(engine, ctx)

    def get_params(self, ctx):
        """Return the query parameters"""
        return {"scenario": self.scenario}

    def result(
        self,
        success,
        message,
        observed=None,
        expected=None,
        severity=Severity.ERROR,
    ):
        """Result with the scenario in the message; INFO if successful"""
        return self.create_result(
            success=success,
            message=f"{self.scenario}: {message}",
            observed=observed,
            expected=expected,
            severity=Severity.INFO if success else severity,
        )

    def judge(self, observed, description):
        """
        Compare an observed value with the rule's expected value

        Parameters
        ----------
        observed : float
            Observed value
        description : str
            What was observed, for the message

        Returns
        -------
        RuleResult

        """
        expected = self.params["expected"]
        comparison = self.params.get("comparison", "equal")
        rtol = self.params.get("rtol", 0.01)

        if expected == FROM_PARAMETERS:
            paths = self.params["parameter"]
            if isinstance(paths, tuple):
                paths = [paths]
            expected = sum(_parameter(self.scenario, path) for path in paths)
            source = "scenario parameters"
        else:
            expected = float(expected)
            source = "given value"

        return self.result(
            _compare(observed, expected, comparison, rtol),
            f"{description} = {observed:,.3f}, expected {comparison} "
            f"{expected:,.3f} ({source}, rtol {rtol})",
            observed=observed,
            expected=expected,
        )


class GasComponentTotal(_GasRule):
    """
    Number or total of the gas components of one scenario

    Parameters
    ----------
    table : str
        eTraGo component table, e.g. "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the components
    location : str or None
        "DE", "abroad", "cross-border" or None, see :func:`_location_sql`
    measure : str or tuple of str
        "count", "annual_energy" (loads only: sum of the time series
        [MWh]) or (function, column) with the function "sum", "max" or
        "min", e.g. ("sum", "p_nom")
    expected : float or str
        Expected value or FROM_PARAMETERS
    parameter : tuple or list of tuple, optional
        Path in the scenario parameters for FROM_PARAMETERS (see
        :func:`_parameter`); the values of several paths are summed
    comparison : str
        "equal" (default), "at_most" or "at_least"
    rtol : float
        Relative tolerance; default 0 for "count", else 0.01

    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        carriers,
        expected,
        location="DE",
        measure="count",
        **kwargs,
    ):
        if measure == "count":
            kwargs.setdefault("rtol", 0)
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            expected=expected,
            location=location,
            measure=measure,
            **kwargs,
        )

    def get_query(self, ctx):
        joins, condition = _location_sql(self.table, self.params["location"])
        measure = self.params["measure"]

        if measure == "count":
            value, aggregate = "1", "count(*)"
        elif measure == "annual_energy":
            if self.table != "grid.egon_etrago_load":
                raise ValueError("annual_energy is only defined for loads")
            joins += (
                " LEFT JOIN grid.egon_etrago_load_timeseries t"
                " ON t.load_id = c.load_id AND t.scn_name = c.scn_name"
            )
            value = (
                "COALESCE((SELECT sum(v) FROM unnest(t.p_set) AS v), "
                "c.p_set * 8760)"
            )
            aggregate = "sum(value)"
        else:
            function, column = measure
            if function not in ("sum", "max", "min"):
                raise ValueError(f"Invalid function: {function!r}")
            value = f"c.{_identifier(column)}"
            aggregate = f"{function}(value)"

        return f"""
            SELECT {aggregate} AS value, count(*) AS n_components
            FROM (
                SELECT {value} AS value
                FROM {self.table} c{joins}
                WHERE c.scn_name = :scenario
                AND c.carrier = ANY(:carriers)
                AND {condition}
            ) AS components
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": self.params["carriers"]}

    def evaluate_df(self, df, ctx):
        value = df["value"].values[0]
        observed = 0.0 if value is None or pd.isna(value) else float(value)
        measure = self.params["measure"]
        if not isinstance(measure, str):
            measure = "({}, {})".format(*measure)
        return self.judge(
            observed,
            f"{measure} of {'/'.join(self.params['carriers'])} "
            f"({self.params['location'] or 'everywhere'}, "
            f"{int(df['n_components'].values[0])} components)",
        )


class GasBusReferences(_GasRule):
    """
    Gas components whose bus does not exist in the scenario

    Parameters
    ----------
    table : str
        eTraGo component table with bus columns, e.g.
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the components

    """

    def __init__(self, table, rule_id, scenario, carriers, **kwargs):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            **kwargs,
        )

    def get_query(self, ctx):
        if not BUS_COLUMNS.get(self.table):
            raise ValueError(f"No bus columns known for {self.table}")

        missing = " OR ".join(
            f"NOT EXISTS (SELECT 1 FROM grid.egon_etrago_bus b"
            f" WHERE b.bus_id = c.{column} AND b.scn_name = c.scn_name)"
            for column in BUS_COLUMNS[self.table]
        )
        return f"""
            SELECT
                count(*) AS n_components,
                count(*) FILTER (WHERE {missing}) AS n_dangling,
                (array_agg(c.{ID_COLUMNS[self.table]}
                    ORDER BY c.{ID_COLUMNS[self.table]})
                    FILTER (WHERE {missing}))[1:10] AS examples
            FROM {self.table} c
            WHERE c.scn_name = :scenario
            AND c.carrier = ANY(:carriers)
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": self.params["carriers"]}

    def evaluate_df(self, df, ctx):
        n_dangling = int(df["n_dangling"].values[0])
        return self.result(
            n_dangling == 0,
            f"{n_dangling} of {int(df['n_components'].values[0])} "
            f"{'/'.join(self.params['carriers'])} components refer to a "
            f"bus that does not exist{_examples(df['examples'].values[0])}",
            observed=n_dangling,
            expected=0,
        )


class GasVoronoiCoverage(_GasRule):
    """
    Gas voronoi cells of one carrier: tied to buses, area of the boundary

    Parameters
    ----------
    table : str
        "grid.egon_gas_voronoi"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carrier : str
        Carrier of the cells
    bus_carriers : list of str
        Carriers of the buses of the cells
    rtol : float
        Relative tolerance of the area, default 0.01

    """

    def __init__(
        self, table, rule_id, scenario, carrier, bus_carriers, **kwargs
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carrier=carrier,
            bus_carriers=list(bus_carriers),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
            SELECT
                (SELECT count(*) FROM grid.egon_etrago_bus
                    WHERE scn_name = :scenario
                    AND carrier = ANY(:bus_carriers)
                    AND country = 'DE') AS n_buses,
                count(v.bus_id) AS n_cells,
                count(v.bus_id) FILTER (WHERE NOT EXISTS (
                    SELECT 1 FROM grid.egon_etrago_bus b
                    WHERE b.bus_id = v.bus_id
                    AND b.scn_name = v.scn_name
                    AND b.carrier = ANY(:bus_carriers)
                    AND b.country = 'DE'
                )) AS n_orphans,
                COALESCE(sum(ST_Area(ST_Transform(v.geom, 3035))), 0)
                    AS cell_area,
                (SELECT sum(ST_Area(ST_Transform(geometry, 3035)))
                    FROM boundaries.vg250_sta_union) AS boundary_area
            FROM {self.table} v
            WHERE v.scn_name = :scenario
            AND v.carrier = :carrier
            """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "carrier": self.params["carrier"],
            "bus_carriers": self.params["bus_carriers"],
        }

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_buses, n_cells = int(row["n_buses"]), int(row["n_cells"])
        carrier = self.params["carrier"]

        if n_buses == 0:
            return self.result(
                n_cells == 0,
                f"no {carrier} buses in Germany and {n_cells} {carrier} cells",
                observed=n_cells,
                expected=0,
            )

        n_orphans = int(row["n_orphans"])
        cell_area = float(row["cell_area"])
        boundary_area = float(row["boundary_area"])
        rtol = self.params.get("rtol", 0.01)
        covered = _compare(cell_area, boundary_area, "equal", rtol)

        return self.result(
            n_orphans == 0 and covered,
            f"{n_cells} {carrier} cells for {n_buses} buses, {n_orphans} "
            f"without a bus; cell area {cell_area / 1e6:,.0f} km² vs boundary "
            f"{boundary_area / 1e6:,.0f} km² (rtol {rtol})",
            observed=cell_area,
            expected=boundary_area,
        )


class GasBuildYearWithinScenario(_GasRule):
    """
    Gas components without a build year or built after the scenario year

    E.g. H2 pipelines of the core network with a commissioning year after
    the scenario year must not be part of the scenario.

    Parameters
    ----------
    table : str
        eTraGo component table with a build_year column
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the components

    """

    def __init__(self, table, rule_id, scenario, carriers, **kwargs):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            **kwargs,
        )

    def get_query(self, ctx):
        id_column = ID_COLUMNS[self.table]
        return f"""
            SELECT
                count(*) AS n_components,
                count(*) FILTER (
                    WHERE build_year IS NULL OR build_year > :year
                ) AS n_later,
                max(build_year) AS max_build_year,
                (array_agg({id_column} ORDER BY {id_column})
                    FILTER (
                        WHERE build_year IS NULL OR build_year > :year
                    ))[1:10] AS examples
            FROM {self.table}
            WHERE scn_name = :scenario
            AND carrier = ANY(:carriers)
            """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "carriers": self.params["carriers"],
            "year": get_scenario_year(self.scenario),
        }

    def evaluate_df(self, df, ctx):
        n_later = int(df["n_later"].values[0])
        return self.result(
            n_later == 0,
            f"{n_later} of {int(df['n_components'].values[0])} "
            f"{'/'.join(self.params['carriers'])} components built after "
            f"{get_scenario_year(self.scenario)} (latest: "
            f"{df['max_build_year'].values[0]})"
            f"{_examples(df['examples'].values[0])}",
            observed=n_later,
            expected=0,
        )


def _nearest_state_sql(bus_column):
    """
    Lateral join of the federal state (NUTS 1 code) nearest to a bus

    The nearest state is taken, so that substations at the coast or the
    border are not lost.
    """
    return f"""
        JOIN grid.egon_etrago_bus b
            ON b.bus_id = {bus_column} AND b.scn_name = :scenario
        CROSS JOIN LATERAL (
            SELECT s.nuts
            FROM boundaries.vg250_lan s
            WHERE s.gf = 4
            ORDER BY ST_Distance(
                s.geometry, ST_Transform(b.geom, ST_SRID(s.geometry))
            )
            LIMIT 1
        ) AS nearest
        """


class ElectrolyserCapPerState(_GasRule):
    """
    Electrolyser potential per federal state vs the NEP capacity

    The electrolysers are extendable up to their connection limit; eTraGo
    builds the NEP capacity per federal state (``power_to_H2`` in
    ``supply.egon_scenario_capacities``). Every state needs at least that
    much ``p_nom_max``.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check

    """

    def get_query(self, ctx):
        return f"""
            WITH electrolysers AS (
                SELECT nearest.nuts, sum(l.p_nom_max) AS p_nom_max
                FROM grid.egon_etrago_link l
                {_nearest_state_sql("l.bus0")}
                WHERE l.scn_name = :scenario
                AND l.carrier = 'power_to_H2'
                GROUP BY nearest.nuts
            )
            SELECT json_agg(states) AS states
            FROM (
                SELECT
                    c.nuts,
                    c.capacity,
                    COALESCE(e.p_nom_max, 0) AS p_nom_max
                FROM supply.egon_scenario_capacities c
                LEFT JOIN electrolysers e ON e.nuts = c.nuts
                WHERE c.scenario_name = :scenario
                AND c.component = 'link'
                AND c.carrier = 'power_to_H2'
            ) AS states
            """

    def evaluate_df(self, df, ctx):
        states = df["states"].values[0] or []
        if not states:
            return self.result(
                False,
                "no NEP electrolyser capacity (power_to_H2) in "
                "supply.egon_scenario_capacities",
            )

        below = [
            f"{s['nuts']} {s['p_nom_max']:,.0f} < {s['capacity']:,.0f} MW"
            for s in states
            if not _compare(s["p_nom_max"], s["capacity"], "at_least", 0)
        ]
        observed = sum(s["p_nom_max"] for s in states)
        expected = sum(s["capacity"] for s in states)
        return self.result(
            not below,
            (
                f"electrolyser potential below the NEP capacity: "
                f"{'; '.join(below)}"
                if below
                else f"{len(states)} federal states: electrolyser potential "
                f"{observed:,.0f} MW, NEP capacity {expected:,.0f} MW"
            ),
            observed=observed,
            expected=expected,
        )


def _connected_parts(nodes, edges):
    """
    Connected part of every node of a graph (union-find)

    Parameters
    ----------
    nodes : iterable
        Nodes, e.g. bus ids
    edges : iterable of tuple
        Pairs of nodes; nodes missing in ``nodes`` are added

    Returns
    -------
    dict
        Representative node of the part of every node

    """
    edges = list(edges)
    parent = {node: node for node in nodes}
    for a, b in edges:
        parent.setdefault(a, a)
        parent.setdefault(b, b)

    def root(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for a, b in edges:
        parent[root(a)] = root(b)
    return {node: root(node) for node in parent}


#: Buses and pipes of the two gas networks and the links that consume gas
#: from them, see :class:`GasNetworkSupply`
GAS_NETWORKS = {
    "CH4": {
        "buses": ["CH4"],
        "pipes": ["CH4"],
        "consumers": [
            "central_gas_CHP",
            "industrial_gas_CHP",
            "central_gas_boiler",
            "rural_gas_boiler",
            "OCGT",
        ],
    },
    "H2": {
        "buses": ["H2", "H2_grid", "H2_saltcavern"],
        "pipes": ["H2_grid", "H2_saltcavern"],
        "consumers": [
            *[c for c in OCGT_CARRIERS if c.startswith("H2_")],
            "H2_to_power",
        ],
    },
}


class GasNetworkSupply(_GasRule):
    """
    Loads and gas consumers in parts of a gas network without supply

    The network is split into its connected parts (buses joined by pipes).
    A CH4 part has a supply if it holds a generator or a methanation link
    from an H2 bus with an electrolyser or on the H2 grid.
    An H2 part has a supply if it holds an electrolyser, an import or an SMR
    link from a CH4 part with a supply. A load or a consumer link (CHP,
    boiler, gas turbine, fuel cell) in a part without supply cannot be
    served. A part without a bus in Germany is cut off from the model.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_bus"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    network : str
        "CH4" or "H2"

    """

    def __init__(self, table, rule_id, scenario, network, **kwargs):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            network=network,
            **kwargs,
        )

    def get_query(self, ctx):
        return """
            WITH gas_bus AS (
                SELECT bus_id, carrier, country
                FROM grid.egon_etrago_bus
                WHERE scn_name = :scenario
                AND carrier IN ('CH4', 'H2', 'H2_grid', 'H2_saltcavern')
            )
            SELECT
                'bus' AS kind, bus_id AS bus0, NULL::bigint AS bus1,
                carrier, country, NULL::float AS value
            FROM gas_bus
            UNION ALL
            SELECT 'link', bus0, bus1, carrier, NULL, p_nom * efficiency
            FROM grid.egon_etrago_link
            WHERE scn_name = :scenario
            AND (
                bus0 IN (SELECT bus_id FROM gas_bus)
                OR bus1 IN (SELECT bus_id FROM gas_bus)
            )
            UNION ALL
            SELECT 'generator', bus, NULL, carrier, NULL, p_nom
            FROM grid.egon_etrago_generator
            WHERE scn_name = :scenario
            AND bus IN (SELECT bus_id FROM gas_bus)
            UNION ALL
            SELECT
                'load', c.bus, NULL, c.carrier, NULL,
                COALESCE(
                    (SELECT sum(v) FROM unnest(t.p_set) AS v),
                    c.p_set * 8760
                )
            FROM grid.egon_etrago_load c
            LEFT JOIN grid.egon_etrago_load_timeseries t
                ON t.load_id = c.load_id AND t.scn_name = c.scn_name
            WHERE c.scn_name = :scenario
            AND c.bus IN (SELECT bus_id FROM gas_bus)
            """

    def evaluate_df(self, df, ctx):
        buses = df[df["kind"] == "bus"]
        links = df[df["kind"] == "link"]
        generators = df[df["kind"] == "generator"]
        loads = df[df["kind"] == "load"]

        part = {}
        for name, network in GAS_NETWORKS.items():
            ids = buses.loc[buses["carrier"].isin(network["buses"]), "bus0"]
            pipes = links[
                links["carrier"].isin(network["pipes"])
                & links["bus0"].isin(ids)
                & links["bus1"].isin(ids)
            ]
            part[name] = _connected_parts(
                ids, zip(pipes["bus0"], pipes["bus1"])
            )

        def parts_of(bus_ids, name):
            return set(bus_ids.map(part[name]).dropna())

        electrolysers = links[links["carrier"] == "power_to_H2"]
        h2_with_source = set(electrolysers["bus1"]) | set(
            buses.loc[
                buses["carrier"].isin(["H2_grid", "H2_saltcavern"]), "bus0"
            ]
        )
        methanation = links[
            (links["carrier"] == "H2_to_CH4")
            & links["bus0"].isin(h2_with_source)
        ]
        supplied = {
            "CH4": parts_of(generators["bus0"], "CH4")
            | parts_of(methanation["bus1"], "CH4")
        }
        smr = links[
            (links["carrier"] == "CH4_to_H2")
            & links["bus0"].map(part["CH4"]).isin(supplied["CH4"])
        ]
        supplied["H2"] = (
            parts_of(electrolysers["bus1"], "H2")
            | parts_of(generators["bus0"], "H2")
            | parts_of(smr["bus1"], "H2")
        )

        name = self.params["network"]
        all_parts = set(part[name].values())
        dry = all_parts - supplied[name]
        german = buses.loc[buses["country"] == "DE", "bus0"]
        cut_off = sorted(all_parts - parts_of(german, name))
        load_part = loads["bus0"].map(part[name])
        dry_loads = loads[load_part.isin(dry)]
        consumers = links[
            links["carrier"].isin(GAS_NETWORKS[name]["consumers"])
            & links["bus0"].map(part[name]).isin(dry)
        ]

        message = (
            f"{len(dry)} of {len(all_parts)} {name} network parts without "
            f"supply, with {len(dry_loads)} loads "
            f"({dry_loads['value'].sum() / 1e6:,.1f} of "
            f"{loads.loc[load_part.notna(), 'value'].sum() / 1e6:,.1f} TWh/a)"
        )
        for carrier, group in consumers.groupby("carrier"):
            message += (
                f", {len(group)} {carrier} links "
                f"({group['value'].sum() / 1e3:,.1f} GW)"
            )
        stranded = sorted(set(dry_loads["bus0"]) | set(consumers["bus0"]))
        if cut_off:
            message += f", {len(cut_off)} parts without a bus in Germany"
        return self.result(
            not stranded and not cut_off,
            message + _examples([int(bus) for bus in stranded + cut_off]),
            observed=len(stranded) + len(cut_off),
            expected=0,
        )


class OcgtMatchesPowerPlants(_GasRule):
    """
    Gas turbine links in Germany vs the gas and hydrogen power plants

    match_ocgt gives every plant of ``supply.egon_power_plants`` one link
    from the nearest CH4 (gas, carrier OCGT) or H2 bus (hydrogen, carriers
    H2_OCGT_*), with p_nom = el_capacity / efficiency: number and
    electrical capacity must match. Links at a bus of another fuel do not
    count.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    rtol : float
        Relative tolerance of the capacity, default 0.001

    """

    def get_query(self, ctx):
        return """
            WITH links AS (
                SELECT
                    CASE WHEN l.carrier = 'OCGT' THEN 'gas' ELSE 'hydrogen'
                    END AS fuel,
                    l.p_nom * l.efficiency AS el
                FROM grid.egon_etrago_link l
                JOIN grid.egon_etrago_bus b
                    ON b.bus_id = l.bus0 AND b.scn_name = l.scn_name
                WHERE l.scn_name = :scenario
                AND l.carrier = ANY(:carriers)
                AND b.country = 'DE'
                AND b.carrier = ANY(
                    CASE WHEN l.carrier = 'OCGT' THEN ARRAY['CH4']
                    ELSE ARRAY['H2', 'H2_grid'] END
                )
            ), plants AS (
                SELECT carrier AS fuel, el_capacity AS el
                FROM supply.egon_power_plants
                WHERE scenario = :scenario
                AND carrier IN ('gas', 'hydrogen')
            )
            SELECT
                fuel,
                count(*) FILTER (WHERE source = 'link') AS n_links,
                COALESCE(sum(el) FILTER (WHERE source = 'link'), 0)
                    AS el_links,
                count(*) FILTER (WHERE source = 'plant') AS n_plants,
                COALESCE(sum(el) FILTER (WHERE source = 'plant'), 0)
                    AS el_plants
            FROM (
                SELECT 'link' AS source, * FROM links
                UNION ALL
                SELECT 'plant', * FROM plants
                UNION ALL
                SELECT NULL, 'gas', 0
            ) AS components
            GROUP BY fuel
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": OCGT_CARRIERS}

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.001)
        wrong = [
            row.fuel
            for row in df.itertuples()
            if row.n_links != row.n_plants
            or not _compare(row.el_links, row.el_plants, "equal", rtol)
        ]
        return self.result(
            not wrong,
            "; ".join(
                f"{row.fuel}: {row.n_links} links / {row.el_links:,.1f} MW "
                f"vs {row.n_plants} plants / {row.el_plants:,.1f} MW"
                for row in df.itertuples()
            ),
            observed=float(df["el_links"].sum()),
            expected=float(df["el_plants"].sum()),
        )


def final_validation_rules():
    """
    Return the cross-cutting gas rules of ``FinalValidations``: bus
    references of all gas components and of the links of other datasets
    (CHP, boilers, gas turbines), supply of every gas network part with
    loads or consumers, and the gas turbines vs the power plants

    Returns
    -------
    dict
        Validation rules per validation task

    """
    return {
        "gas": [
            *[
                rule
                for rule_id, table, carriers in [
                    (
                        "SANITY_GAS_FINAL_LINKS_AT_GAS_BUSES",
                        "grid.egon_etrago_link",
                        [
                            "central_gas_CHP",
                            "central_gas_CHP_heat",
                            "industrial_gas_CHP",
                            "central_gas_boiler",
                            "rural_gas_boiler",
                            *OCGT_CARRIERS,
                        ],
                    ),
                    (
                        "SANITY_GAS_FINAL_GAS_LINKS",
                        "grid.egon_etrago_link",
                        [
                            "CH4",
                            "H2_grid",
                            "H2_saltcavern",
                            "CH4_to_H2",
                            "H2_to_CH4",
                            "power_to_H2",
                            "H2_to_power",
                            "PtH2_waste_heat",
                        ],
                    ),
                    (
                        "SANITY_GAS_FINAL_GENERATORS_AT_GAS_BUSES",
                        "grid.egon_etrago_generator",
                        ["CH4", "H2"],
                    ),
                    (
                        "SANITY_GAS_FINAL_LOADS_AT_GAS_BUSES",
                        "grid.egon_etrago_load",
                        [
                            "CH4",
                            "CH4_for_industry",
                            "H2_for_industry",
                            "rural_gas_boiler",
                        ],
                    ),
                    (
                        "SANITY_GAS_FINAL_STORES_AT_GAS_BUSES",
                        "grid.egon_etrago_store",
                        ["CH4", "H2_overground", "H2_underground"],
                    ),
                ]
                for rule in for_each_scenario(
                    GasBusReferences, rule_id, table=table, carriers=carriers
                )
            ],
            *[
                rule
                for network in GAS_NETWORKS
                for rule in for_each_scenario(
                    GasNetworkSupply,
                    f"SANITY_GAS_FINAL_{network}_SUPPLY",
                    table="grid.egon_etrago_bus",
                    network=network,
                )
            ],
            *for_each_scenario(
                OcgtMatchesPowerPlants,
                "SANITY_GAS_FINAL_OCGT_POWER_PLANTS",
                table="grid.egon_etrago_link",
            ),
        ],
    }
