"""
Sanity check validation rules for the gas datasets (#1526)

Each gas dataset lists its rules in the static method ``validation_rules``;
the cross-cutting rules are in :func:`final_validation_rules`. Most rules
need no expected value; the totals of :class:`GasComponentTotal` and
:class:`H2GridComponents` are measured per dataset boundary. The eTraGo
tables are shared with other sectors, so the rules always filter by
scenario and carrier.

"""

import re

from egon_validation.rules.base import DataFrameRule, Severity
import pandas as pd

from egon.data import config
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)
from egon.data.validation import resolve_boundary_dependence

#: Scenarios the gas rules are defined for. A rule for a scenario that is
#: not in ``--scenarios`` of the run is skipped.
GAS_SCENARIOS = ("status2024", "eGon2035", "reGon2037", "reGon2045")

#: Scenarios with a gas network. status2024 models the German methane
#: system as a single CH4 bus without pipes and has no hydrogen.
TARGET_SCENARIOS = ("eGon2035", "reGon2037", "reGon2045")

#: Dataset boundaries an expected value has to be given for
BOUNDARIES = ("Schleswig-Holstein", "Everything")

#: Placeholder for an expected value that is not measured yet. A rule that
#: still holds it fails and reports the observed value.
NOT_MEASURED = "NOT_MEASURED"

#: Expected value taken from the scenario parameters (``parameter`` of the
#: rule). The model scales the gas sector to national totals and only then
#: cuts it to the dataset boundary, so this is valid for "Everything" only.
FROM_PARAMETERS = "FROM_PARAMETERS"

#: Absolute tolerance, used when the expected value is 0
ATOL = 1e-6

#: Bus columns of the eTraGo component tables
BUS_COLUMNS = {
    "grid.egon_etrago_bus": (),
    "grid.egon_etrago_link": ("bus0", "bus1"),
    "grid.egon_etrago_generator": ("bus",),
    "grid.egon_etrago_load": ("bus",),
    "grid.egon_etrago_store": ("bus",),
}

#: Id columns of the eTraGo component tables
ID_COLUMNS = {
    "grid.egon_etrago_bus": "bus_id",
    "grid.egon_etrago_link": "link_id",
    "grid.egon_etrago_generator": "generator_id",
    "grid.egon_etrago_load": "load_id",
    "grid.egon_etrago_store": "store_id",
}

#: Time series tables of the eTraGo component tables
TIMESERIES_TABLES = {
    "grid.egon_etrago_generator": "grid.egon_etrago_generator_timeseries",
    "grid.egon_etrago_load": "grid.egon_etrago_load_timeseries",
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

    ``expected`` is a number, NOT_MEASURED or FROM_PARAMETERS, optionally
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
        # Unlike DemandRegioScenarioDemand, a missing scenario is decided
        # by --scenarios and not by the data: a scenario that was built but
        # has no components of a carrier is a result the rule has to judge.
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

        if expected == NOT_MEASURED:
            return self.result(
                False,
                f"{description} = {observed:,.3f}; the expected value is "
                "not measured yet (#1526)",
                observed=observed,
            )

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
        Expected value, NOT_MEASURED or FROM_PARAMETERS
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
            # A load without a time series has a constant p_set
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


class GasBusesWithoutLink(_GasRule):
    """
    German gas buses without a link of their carrier

    Such a bus is cut off from its network: the loads, stores and
    generators at it can only be balanced locally.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_bus"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    bus_carrier : str
        Carrier of the buses, e.g. "H2_grid"
    link_carriers : list of str
        Carriers of the links that connect such a bus, e.g. ["H2_grid"]

    """

    def __init__(
        self, table, rule_id, scenario, bus_carrier, link_carriers, **kwargs
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carrier=bus_carrier,
            link_carriers=list(link_carriers),
            **kwargs,
        )

    def get_query(self, ctx):
        return """
            SELECT
                count(*) AS n_buses,
                count(*) FILTER (WHERE NOT linked) AS n_unlinked,
                (array_agg(bus_id ORDER BY bus_id)
                    FILTER (WHERE NOT linked))[1:10] AS examples
            FROM (
                SELECT b.bus_id, EXISTS (
                    SELECT 1 FROM grid.egon_etrago_link l
                    WHERE l.scn_name = b.scn_name
                    AND l.carrier = ANY(:link_carriers)
                    AND b.bus_id IN (l.bus0, l.bus1)
                ) AS linked
                FROM grid.egon_etrago_bus b
                WHERE b.scn_name = :scenario
                AND b.carrier = :bus_carrier
                AND b.country = 'DE'
            ) AS buses
            """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "bus_carrier": self.params["bus_carrier"],
            "link_carriers": self.params["link_carriers"],
        }

    def evaluate_df(self, df, ctx):
        n_unlinked = int(df["n_unlinked"].values[0])
        return self.result(
            n_unlinked == 0,
            f"{n_unlinked} of {int(df['n_buses'].values[0])} "
            f"{self.params['bus_carrier']} buses in Germany without a "
            f"{'/'.join(self.params['link_carriers'])} link"
            f"{_examples(df['examples'].values[0])}",
            observed=n_unlinked,
            expected=0,
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


class GasTimeseriesComplete(_GasRule):
    """
    Gas loads or generators without a complete time series

    A time series is incomplete if its row is missing, the column is NULL,
    it does not have ``length`` values, or it holds NULL or NaN values.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_load" or "grid.egon_etrago_generator"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the components
    location : str or None
        "DE", "abroad" or None, see :func:`_location_sql`
    column : str
        Array column of the time series table, default "p_set"
    length : int
        Number of time steps, default 8760

    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        carriers,
        location="DE",
        column="p_set",
        length=8760,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            location=location,
            column=column,
            length=length,
            **kwargs,
        )

    def get_query(self, ctx):
        if self.table not in TIMESERIES_TABLES:
            raise ValueError(f"No time series table known for {self.table}")

        joins, condition = _location_sql(self.table, self.params["location"])
        id_column = ID_COLUMNS[self.table]
        series = f"t.{_identifier(self.params['column'])}"
        incomplete = (
            f"t.{id_column} IS NULL OR {series} IS NULL"
            f" OR cardinality({series}) <> :length"
            f" OR array_position({series}, NULL) IS NOT NULL"
            f" OR 'NaN'::double precision = ANY({series})"
        )
        return f"""
            SELECT
                count(*) AS n_components,
                count(*) FILTER (WHERE {incomplete}) AS n_incomplete,
                (array_agg(c.{id_column} ORDER BY c.{id_column})
                    FILTER (WHERE {incomplete}))[1:10] AS examples
            FROM {self.table} c{joins}
            LEFT JOIN {TIMESERIES_TABLES[self.table]} t
                ON t.{id_column} = c.{id_column}
                AND t.scn_name = c.scn_name
            WHERE c.scn_name = :scenario
            AND c.carrier = ANY(:carriers)
            AND {condition}
            """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "carriers": self.params["carriers"],
            "length": self.params["length"],
        }

    def evaluate_df(self, df, ctx):
        n_incomplete = int(df["n_incomplete"].values[0])
        return self.result(
            n_incomplete == 0,
            f"{n_incomplete} of {int(df['n_components'].values[0])} "
            f"{'/'.join(self.params['carriers'])} components without a "
            f"complete {self.params['column']} series of "
            f"{self.params['length']} values"
            f"{_examples(df['examples'].values[0])}",
            observed=n_incomplete,
            expected=0,
        )


class GasDuplicateLinks(_GasRule):
    """
    Parallel links of the same carrier between the same buses

    Meant for conversion links (e.g. SMR and methanation), which exist
    once per pair of buses. Not for pipelines: two pipelines between the
    same nodes can both be real.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the links

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
        return """
            SELECT
                count(*) AS n_pairs,
                COALESCE(sum(n_links - 1), 0) AS n_duplicates,
                (array_agg(first_link ORDER BY first_link))[1:10] AS examples
            FROM (
                SELECT count(*) AS n_links, min(link_id) AS first_link
                FROM grid.egon_etrago_link
                WHERE scn_name = :scenario
                AND carrier = ANY(:carriers)
                GROUP BY carrier, bus0, bus1
                HAVING count(*) > 1
            ) AS duplicates
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": self.params["carriers"]}

    def evaluate_df(self, df, ctx):
        n_duplicates = int(df["n_duplicates"].values[0])
        return self.result(
            n_duplicates == 0,
            f"{n_duplicates} duplicate {'/'.join(self.params['carriers'])} "
            f"links at {int(df['n_pairs'].values[0])} pairs of buses"
            f"{_examples(df['examples'].values[0])}",
            observed=n_duplicates,
            expected=0,
        )


class GasDuplicateBusLocations(_GasRule):
    """
    German gas buses at the same coordinates

    Two buses at one place split the components that belong together,
    because pipelines are mapped to buses by their coordinates.

    Parameters
    ----------
    table : str
        "grid.egon_etrago_bus"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the buses that must not share a place, e.g.
        ["H2", "H2_grid"]

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
        return """
            SELECT
                count(*) AS n_places,
                COALESCE(sum(n_buses - 1), 0) AS n_duplicates,
                (array_agg(first_bus ORDER BY first_bus))[1:10] AS examples
            FROM (
                SELECT count(*) AS n_buses, min(bus_id) AS first_bus
                FROM grid.egon_etrago_bus
                WHERE scn_name = :scenario
                AND carrier = ANY(:carriers)
                AND country = 'DE'
                GROUP BY x, y
                HAVING count(*) > 1
            ) AS duplicates
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": self.params["carriers"]}

    def evaluate_df(self, df, ctx):
        n_duplicates = int(df["n_duplicates"].values[0])
        return self.result(
            n_duplicates == 0,
            f"{n_duplicates} {'/'.join(self.params['carriers'])} buses share "
            f"their coordinates with another one at "
            f"{int(df['n_places'].values[0])} places"
            f"{_examples(df['examples'].values[0])}",
            observed=n_duplicates,
            expected=0,
        )


class GasOnePortsPerBus(_GasRule):
    """
    German gas buses without the expected number of one-port components

    E.g. one H2_overground store at every German H2 and H2_grid bus.

    Parameters
    ----------
    table : str
        One-port table, e.g. "grid.egon_etrago_store"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    bus_carriers : list of str
        Carriers of the buses
    carriers : list of str
        Carriers of the one-port components
    per_bus : int
        Number of such components per bus, default 1

    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        bus_carriers,
        carriers,
        per_bus=1,
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_carriers=list(bus_carriers),
            carriers=list(carriers),
            per_bus=per_bus,
            **kwargs,
        )

    def get_query(self, ctx):
        if BUS_COLUMNS.get(self.table) != ("bus",):
            raise ValueError(f"Not a one-port table: {self.table}")

        return f"""
            SELECT
                count(*) AS n_buses,
                count(*) FILTER (WHERE n <> :per_bus) AS n_wrong,
                (array_agg(bus_id ORDER BY bus_id)
                    FILTER (WHERE n <> :per_bus))[1:10] AS examples
            FROM (
                SELECT b.bus_id, (
                    SELECT count(*) FROM {self.table} c
                    WHERE c.bus = b.bus_id
                    AND c.scn_name = b.scn_name
                    AND c.carrier = ANY(:carriers)
                ) AS n
                FROM grid.egon_etrago_bus b
                WHERE b.scn_name = :scenario
                AND b.carrier = ANY(:bus_carriers)
                AND b.country = 'DE'
            ) AS buses
            """

    def get_params(self, ctx):
        return {
            "scenario": self.scenario,
            "bus_carriers": self.params["bus_carriers"],
            "carriers": self.params["carriers"],
            "per_bus": self.params["per_bus"],
        }

    def evaluate_df(self, df, ctx):
        n_wrong = int(df["n_wrong"].values[0])
        return self.result(
            n_wrong == 0,
            f"{n_wrong} of {int(df['n_buses'].values[0])} "
            f"{'/'.join(self.params['bus_carriers'])} buses in Germany "
            f"without exactly {self.params['per_bus']} "
            f"{'/'.join(self.params['carriers'])} component(s)"
            f"{_examples(df['examples'].values[0])}",
            observed=n_wrong,
            expected=0,
        )


class GasVoronoiCoverage(_GasRule):
    """
    Gas voronoi cells of one carrier: tied to buses, covering the boundary once

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
    Gas components built after the year of the scenario

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
                count(*) FILTER (WHERE build_year > :year) AS n_later,
                max(build_year) AS max_build_year,
                (array_agg({id_column} ORDER BY {id_column})
                    FILTER (WHERE build_year > :year))[1:10] AS examples
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


class GasParameterMatch(_GasRule):
    """
    Gas components whose attribute differs from the scenario parameters
    (with several paths, each component has to match one of them)

    Parameters
    ----------
    table : str
        eTraGo component table
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    carriers : list of str
        Carriers of the components
    column : str
        Column to check, e.g. "efficiency"
    parameter : tuple or list of tuple
        Path(s) in the scenario parameters, see :func:`_parameter`; paths a
        scenario does not have are left out
    location : str or None
        "DE", "abroad", "cross-border" or None, see :func:`_location_sql`
    rtol : float
        Relative tolerance, default 1e-6

    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        carriers,
        column,
        parameter,
        location="DE",
        **kwargs,
    ):
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carriers=list(carriers),
            column=column,
            parameter=parameter,
            location=location,
            **kwargs,
        )

    def get_query(self, ctx):
        joins, condition = _location_sql(self.table, self.params["location"])
        column = _identifier(self.params["column"])
        return f"""
            SELECT
                count(*) AS n_components,
                array_agg(DISTINCT c.{column}) AS values
            FROM {self.table} c{joins}
            WHERE c.scn_name = :scenario
            AND c.carrier = ANY(:carriers)
            AND {condition}
            """

    def get_params(self, ctx):
        return {"scenario": self.scenario, "carriers": self.params["carriers"]}

    def evaluate_df(self, df, ctx):
        n_components = int(df["n_components"].values[0])
        carriers = "/".join(self.params["carriers"])
        column = self.params["column"]

        if n_components == 0:
            return self.result(
                True, f"no {carriers} components to check {column} of"
            )

        paths = self.params["parameter"]
        if isinstance(paths, tuple):
            paths = [paths]
        allowed = []
        for path in paths:
            try:
                allowed.append(_parameter(self.scenario, path))
            except KeyError:
                continue
        if not allowed:
            raise KeyError(f"None of the parameters {paths} exists")
        rtol = self.params.get("rtol", 1e-6)

        values = df["values"].values[0]
        wrong = [
            value
            for value in values
            if value is None
            or pd.isna(value)
            or not any(
                _compare(float(value), a, "equal", rtol) for a in allowed
            )
        ]
        return self.result(
            not wrong,
            (
                f"{column} of {n_components} {carriers} components: values "
                f"{sorted(str(v) for v in wrong)} differ from the parameters "
                f"{allowed}"
                if wrong
                else f"{column} of {n_components} {carriers} components as in "
                f"the parameters {allowed}"
            ),
            observed=len(wrong),
            expected=0,
        )


def _nearest_state_sql(bus_column):
    """
    Lateral join of the federal state nearest to a bus

    As in ``hydrogen_etrago.power_to_h2.scale_electrolysis_to_nep``, the
    nearest state is taken, so that substations at the coast or the
    border are not lost.
    """
    return f"""
        JOIN grid.egon_etrago_bus b
            ON b.bus_id = {bus_column} AND b.scn_name = :scenario
        CROSS JOIN LATERAL (
            SELECT s.gen AS state
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
    Electrolyser capacity per federal state within the NEP capacity
    ("power_to_H2_capacity"); scenarios without the parameter are skipped

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    rtol : float
        Relative tolerance, default 0.001

    """

    def get_query(self, ctx):
        return f"""
            SELECT json_agg(states) AS states
            FROM (
                SELECT nearest.state, sum(l.p_nom_max) AS p_nom_max
                FROM grid.egon_etrago_link l
                {_nearest_state_sql("l.bus0")}
                WHERE l.scn_name = :scenario
                AND l.carrier = 'power_to_H2'
                GROUP BY nearest.state
            ) AS states
            """

    def evaluate_df(self, df, ctx):
        capacity = get_sector_parameters("gas", scenario=self.scenario).get(
            "power_to_H2_capacity"
        )
        if not capacity:
            return self.result(
                True, "no electrolyser capacity per federal state; skipped"
            )

        states = df["states"].values[0] or []
        rtol = self.params.get("rtol", 0.001)
        above = [
            f"{s['state']} {s['p_nom_max']:,.0f} > "
            f"{capacity.get(s['state'], 0):,.0f} MW"
            for s in states
            if not _compare(
                s["p_nom_max"], capacity.get(s["state"], 0), "at_most", rtol
            )
        ]
        observed = sum(s["p_nom_max"] for s in states)
        expected = sum(capacity.get(s["state"], 0) for s in states)
        return self.result(
            not above,
            (
                f"electrolysers above the NEP capacity: {'; '.join(above)}"
                if above
                else f"{len(states)} federal states within the NEP capacity "
                f"({observed:,.0f} of {expected:,.0f} MW)"
            ),
            observed=observed,
            expected=expected,
        )


class H2DemandAboveElectrolyserCap(_GasRule):
    """
    Federal states whose industrial H2 demand exceeds the H2 output of their
    electrolysers (warning: such a state imports H2 through the grid)

    Parameters
    ----------
    table : str
        "grid.egon_etrago_load"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check

    """

    def get_query(self, ctx):
        return f"""
            SELECT json_agg(states) AS states
            FROM (
                SELECT
                    nearest.state,
                    sum((SELECT sum(v) FROM unnest(t.p_set) AS v)) / 8760
                        AS mean_demand
                FROM grid.egon_etrago_load c
                JOIN grid.egon_etrago_load_timeseries t
                    ON t.load_id = c.load_id AND t.scn_name = c.scn_name
                {_nearest_state_sql("c.bus")}
                WHERE c.scn_name = :scenario
                AND c.carrier = 'H2_for_industry'
                AND b.country = 'DE'
                GROUP BY nearest.state
            ) AS states
            """

    def evaluate_df(self, df, ctx):
        parameters = get_sector_parameters("gas", scenario=self.scenario)
        capacity = parameters.get("power_to_H2_capacity")
        if not capacity:
            return self.result(
                True, "no electrolyser capacity per federal state; skipped"
            )

        efficiency = parameters["efficiency"]["power_to_H2"]
        states = df["states"].values[0] or []
        above = [
            f"{s['state']} {s['mean_demand']:,.0f} MW > "
            f"{capacity.get(s['state'], 0) * efficiency:,.0f} MW"
            for s in states
            if s["mean_demand"] > capacity.get(s["state"], 0) * efficiency
        ]
        return self.result(
            not above,
            (
                "industrial H2 demand above the H2 output of the NEP "
                f"electrolysers: {'; '.join(above)}"
                if above
                else f"{len(states)} federal states: industrial H2 demand "
                "within the H2 output of the NEP electrolysers"
            ),
            observed=len(above),
            expected=0,
            severity=Severity.WARNING,
        )


class WasteHeatBoundedByElectrolysis(_GasRule):
    """
    Waste heat links within the waste heat of the electrolysers at the same
    AC bus

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    rtol : float
        Relative tolerance, default 0.001

    """

    def get_query(self, ctx):
        return """
            WITH electrolysis AS (
                SELECT bus0, sum(p_nom_max) AS p_nom_max
                FROM grid.egon_etrago_link
                WHERE scn_name = :scenario AND carrier = 'power_to_H2'
                GROUP BY bus0
            ), heat AS (
                SELECT bus0, sum(p_nom_max) AS p_nom_max
                FROM grid.egon_etrago_link
                WHERE scn_name = :scenario AND carrier = 'PtH2_waste_heat'
                GROUP BY bus0
            )
            SELECT json_agg(json_build_object(
                'bus', heat.bus0,
                'heat', heat.p_nom_max,
                'electrolysis', COALESCE(electrolysis.p_nom_max, 0)
            )) AS buses
            FROM heat
            LEFT JOIN electrolysis USING (bus0)
            """

    def evaluate_df(self, df, ctx):
        share = get_sector_parameters("gas", scenario=self.scenario)[
            "efficiency"
        ]["power_to_Heat"]
        buses = df["buses"].values[0] or []
        rtol = self.params.get("rtol", 0.001)
        above = [
            b["bus"]
            for b in buses
            if not _compare(
                b["heat"], share * b["electrolysis"], "at_most", rtol
            )
        ]
        return self.result(
            not above,
            f"{len(above)} of {len(buses)} AC buses with more waste heat "
            f"links than {share} x electrolysis{_examples(above)}",
            observed=len(above),
            expected=0,
        )


class H2GridComponents(_GasRule):
    """
    Number of separate parts of the H2 grid (H2_grid buses and links)

    Parameters
    ----------
    table : str
        "grid.egon_etrago_link"
    rule_id : str
        Unique identifier for this validation rule
    scenario : str
        Scenario to check
    expected : int or str
        Expected number of parts, or NOT_MEASURED

    """

    def __init__(self, table, rule_id, scenario, expected, **kwargs):
        kwargs.setdefault("rtol", 0)
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            expected=expected,
            **kwargs,
        )

    def get_query(self, ctx):
        return """
            SELECT
                (SELECT array_agg(bus_id) FROM grid.egon_etrago_bus
                    WHERE scn_name = :scenario AND carrier = 'H2_grid')
                    AS buses,
                array_agg(bus0 ORDER BY link_id) AS bus0,
                array_agg(bus1 ORDER BY link_id) AS bus1
            FROM grid.egon_etrago_link
            WHERE scn_name = :scenario AND carrier = 'H2_grid'
            """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        bus0, bus1 = row["bus0"] or [], row["bus1"] or []

        parent = {bus: bus for bus in (row["buses"] or [])}
        parent.update({bus: bus for bus in bus0 + bus1})

        def root(bus):
            while parent[bus] != bus:
                parent[bus] = parent[parent[bus]]
                bus = parent[bus]
            return bus

        for a, b in zip(bus0, bus1):
            parent[root(a)] = root(b)

        n_parts = len({root(bus) for bus in parent})
        return self.judge(
            n_parts,
            f"parts of the H2 grid ({len(parent)} buses, {len(bus0)} links)",
        )


def final_validation_rules():
    """
    Return the cross-cutting gas rules of ``FinalValidations``: links of other
    datasets (CHP, boilers, gas turbines) that point to rebuilt gas buses

    Returns
    -------
    dict
        Validation rules per validation task

    """
    return {
        "gas": [
            *for_each_scenario(
                GasBusReferences,
                "SANITY_GAS_FINAL_LINKS_AT_GAS_BUSES",
                table="grid.egon_etrago_link",
                carriers=[
                    "central_gas_CHP",
                    "central_gas_CHP_heat",
                    "industrial_gas_CHP",
                    "central_gas_boiler",
                    "rural_gas_boiler",
                    "OCGT",
                ],
            ),
            # The gas links themselves, once all datasets have run
            *for_each_scenario(
                GasBusReferences,
                "SANITY_GAS_FINAL_GAS_LINKS",
                table="grid.egon_etrago_link",
                carriers=[
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
        ],
    }
