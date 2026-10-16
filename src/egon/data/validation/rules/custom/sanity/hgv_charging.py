"""
Sanity check validation rules for HGV charging (vehicle classes N2, N3, N3S).

Validates what :class:`~egon.data.datasets.emobility.hgv_charging.
HGVCharging` writes:

* ``demand.egon_ev_hgv_charging_site`` -- depots and highway sites with
  their annual energy, connection power and grid assignment,
* ``demand.egon_ev_hgv_charging_point`` -- charging points, one per site
  and sector,
* ``demand.egon_ev_hgv_charging_event`` -- charging and vacant events per
  physical slot,
* ``demand.egon_ev_hgv_profile`` -- one normalized profile per sector,
* the eTraGo tables: one flex model per MV grid district and per HV/EHV
  site (an ``HGV_charger`` bus with link, ``battery_storage`` store and
  driving load) and one highway-day fixed load per highway AC bus, all with
  their timeseries.

Most rules check one scenario. A scenario that is not part of
``--scenarios`` is skipped (success, severity INFO); a configured scenario
without rows fails.

The rules check what egon-data does with the input data -- import,
boundary filter, grid assignment and the eTraGo flex model -- not the
input data generation itself. They take their parameters from the dataset
code but compute the checked property on their own in SQL, so they do not
just repeat the code they check.

The event table has about 140 million rows per scenario on a full run. The
event rules therefore share one aggregate per scenario, grouped by physical
slot (see :func:`_event_aggregate`), instead of scanning the table each.
"""

import threading

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import pandas as pd

import egon.data.config

#: Relative tolerance for values that are copied or summed between the
#: Python write path and SQL. Measured deviations are 0 or float rounding.
RTOL_EXACT = 1e-4

#: Relative tolerance for float comparisons of single values.
RTOL_ROUNDING = 1e-9

#: Relative tolerance between the charging energy of the events and the
#: site energy. Year-boundary clipping of the events causes a small,
#: deterministic gap; measured at most 0.003 % on the Schleswig-Holstein
#: and the full Germany runs (reGon2037, reGon2045).
RTOL_EVENT_ENERGY = 0.005

#: Largest accepted connection power of a site in MW. Above 120 MW
#: ``spatial_assignment`` assigns voltage level 1 (EHV), but still connects
#: the site to its MV grid district bus. The largest site on the full
#: Germany run has 51 MW.
MAX_SITE_P_SET_MW = 120.0

#: Largest accepted distance in m between a site and the MV grid district
#: it is assigned to. Sites outside every district are assigned to the
#: nearest one; on the full Germany run 6 sites are 23-457 m outside.
MAX_DISTRICT_DISTANCE_M = 1000.0

#: Number of hourly time steps of all HGV series.
N_TIMESTEPS = 8760

#: Absolute tolerance for state of charge values, which are fractions.
ATOL_SOC = 1e-9

SITE_TABLE = "demand.egon_ev_hgv_charging_site"
CP_TABLE = "demand.egon_ev_hgv_charging_point"
EVENT_TABLE = "demand.egon_ev_hgv_charging_event"
PROFILE_TABLE = "demand.egon_ev_hgv_profile"
MV_GRID_DISTRICT_TABLE = "grid.egon_mv_grid_district"
BUS_TABLE = "grid.egon_etrago_bus"
LINK_TABLE = "grid.egon_etrago_link"
STORE_TABLE = "grid.egon_etrago_store"
LOAD_TABLE = "grid.egon_etrago_load"

#: eTraGo component tables and their timeseries table and id column.
COMPONENTS = {
    "link": (LINK_TABLE, "grid.egon_etrago_link_timeseries", "link_id"),
    "store": (STORE_TABLE, "grid.egon_etrago_store_timeseries", "store_id"),
    "load": (LOAD_TABLE, "grid.egon_etrago_load_timeseries", "load_id"),
}


def configured_scenarios():
    """Scenarios of the current run (``--scenarios``)."""
    return list(egon.data.config.settings()["egon-data"]["--scenarios"])


def _integration():
    """The eTraGo integration module, imported lazily (circular import)."""
    from egon.data.datasets.emobility.hgv_charging import etrago_integration

    return etrago_integration


def _carriers():
    """Carriers of the HGV bus, link and loads."""
    ei = _integration()
    return {
        "bus": ei.CARRIER_BUS,
        "link": ei.CARRIER_LINK,
        "store": ei.CARRIER_STORE,
        "load": ei.CARRIER_LOAD,
    }


def _examples(values, n=5):
    """Up to ``n`` values as a comma-separated string."""
    values = [str(v) for v in list(values)[:n]]
    return ", ".join(values) if values else "-"


def _hgv_component_sql(component):
    """
    SQL selecting the HGV rows of a component table for ``:scenario``.

    Returns the columns ``scn_name``, ``id`` and, for links and stores,
    ``hgv_bus`` (the HGV bus they belong to). Stores are selected by bus,
    since MIV writes ``battery_storage`` stores too.
    """
    c = _carriers()
    if component == "bus":
        return f"""
            SELECT scn_name, bus_id AS id, bus_id AS hgv_bus
            FROM {BUS_TABLE}
            WHERE scn_name = :scenario AND carrier = '{c["bus"]}'
        """
    if component == "link":
        return f"""
            SELECT scn_name, link_id AS id, bus1 AS hgv_bus
            FROM {LINK_TABLE}
            WHERE scn_name = :scenario AND carrier = '{c["link"]}'
        """
    if component == "store":
        return f"""
            SELECT s.scn_name, s.store_id AS id, s.bus AS hgv_bus
            FROM {STORE_TABLE} s
            JOIN {BUS_TABLE} b
                ON b.scn_name = s.scn_name AND b.bus_id = s.bus
            WHERE s.scn_name = :scenario AND b.carrier = '{c["bus"]}'
        """
    if component == "load":
        return f"""
            SELECT scn_name, load_id AS id, bus AS hgv_bus
            FROM {LOAD_TABLE}
            WHERE scn_name = :scenario AND carrier = '{c["load"]}'
        """
    raise ValueError(f"Unknown component '{component}'")


# ---------------------------------------------------------------------------
# Shared event aggregate
# ---------------------------------------------------------------------------

_EVENT_AGGREGATES = {}
_EVENT_AGGREGATES_LOCK = threading.Lock()
_EVENT_AGGREGATE_LOCKS = {}

#: One row per physical slot, location and use case of a scenario. The
#: physical slot is (cp_id, vehicle_class, slot_id): slot ids are only
#: unique per vehicle class on charging points shared by several classes.
EVENT_AGGREGATE_SQL = f"""
    SELECT
        site_id, cp_id, vehicle_class, slot_id, location, use_case,
        count(*) AS n_events,
        count(*) FILTER (WHERE
            bat_cap IS NULL OR bat_cap = 'NaN'
            OR soc_start IS NULL OR soc_start = 'NaN'
            OR soc_end IS NULL OR soc_end = 'NaN'
            OR charging_demand IS NULL OR charging_demand = 'NaN'
            OR consumption IS NULL OR consumption = 'NaN'
            OR charging_capacity_nominal IS NULL
            OR charging_capacity_nominal = 'NaN'
            OR charging_capacity_grid IS NULL
            OR charging_capacity_grid = 'NaN'
            OR park_start IS NULL OR park_end IS NULL
        ) AS n_null,
        min(park_start) AS park_start_min,
        max(park_end) AS park_end_max,
        count(*) FILTER (WHERE park_end < park_start) AS n_park_reversed,
        min(soc_start) AS soc_start_min,
        max(soc_end) AS soc_end_max,
        count(*) FILTER (
            WHERE soc_start > soc_end + {ATOL_SOC}
        ) AS n_soc_falling,
        min(bat_cap) AS bat_cap_min,
        max(bat_cap) AS bat_cap_max,
        min(charging_demand) AS demand_min,
        count(*) FILTER (
            WHERE abs(consumption - charging_demand)
                > {RTOL_ROUNDING} * greatest(abs(charging_demand), 1)
        ) AS n_consumption_differs,
        count(*) FILTER (
            WHERE location = 'charging' AND charging_demand > 0
            AND (charging_capacity_nominal <= 0
                 OR charging_capacity_grid <= 0)
        ) AS n_no_capacity,
        count(*) FILTER (
            WHERE location = 'vacant' AND (
                charging_demand <> 0
                OR abs(soc_start - 1) > {ATOL_SOC}
                OR abs(soc_end - 1) > {ATOL_SOC}
            )
        ) AS n_vacant_invalid,
        sum(charging_demand) AS demand_kwh,
        sum(park_end - park_start + 1) AS hours
    FROM {EVENT_TABLE}
    WHERE scenario = :scenario
    GROUP BY site_id, cp_id, vehicle_class, slot_id, location, use_case
"""


def _event_aggregate(engine, scenario):
    """
    The event aggregate of a scenario, queried once and then shared.

    The validation runner evaluates rules in parallel threads. The first
    rule that needs the aggregate of a scenario queries it; the others wait
    for it and reuse the result.
    """
    from egon_validation.db import fetch_dataframe

    with _EVENT_AGGREGATES_LOCK:
        lock = _EVENT_AGGREGATE_LOCKS.setdefault(scenario, threading.Lock())
    with lock:
        if scenario not in _EVENT_AGGREGATES:
            _EVENT_AGGREGATES[scenario] = fetch_dataframe(
                engine, EVENT_AGGREGATE_SQL, params={"scenario": scenario}
            )
        return _EVENT_AGGREGATES[scenario]


def clear_event_aggregates():
    """Forget the cached event aggregates, e.g. between test runs."""
    with _EVENT_AGGREGATES_LOCK:
        _EVENT_AGGREGATES.clear()


def _slots(agg):
    """The event aggregate reduced to one row per physical slot."""
    return (
        agg.groupby(
            ["site_id", "cp_id", "vehicle_class", "slot_id"], dropna=False
        )
        .agg(
            hours=("hours", "sum"),
            park_start_min=("park_start_min", "min"),
            park_end_max=("park_end_max", "max"),
            bat_cap_min=("bat_cap_min", "min"),
            bat_cap_max=("bat_cap_max", "max"),
        )
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class _HgvRule(DataFrameRule):
    """
    Common base for the HGV charging rules.

    Subclasses either return exactly one row per query, or evaluate the
    whole result in pandas, because the framework reports an empty result
    as a failure. Rules that need several queries override
    :meth:`evaluate_scenario` instead of ``get_query``/``evaluate_df``.
    """

    def __init__(self, table: str, rule_id: str, scenario=None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Table the rule is reported for.
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str or None
            Scenario to check, e.g. "reGon2037". ``None`` for rules that
            are not bound to a single scenario.
        """
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def _check_table_empty(self, engine, ctx):
        # The framework counts the whole table before every rule: a full
        # scan of the event table, and of the shared eTraGo tables. Empty
        # scenarios are detected per scenario by the rules instead.
        return None

    def evaluate(self, engine, ctx):
        if (
            self.scenario is not None
            and self.scenario not in configured_scenarios()
        ):
            return self._result(
                success=True,
                message=(
                    f"Scenario '{self.scenario}' is not part of --scenarios; "
                    f"check skipped"
                ),
            )
        if type(self).evaluate_scenario is not _HgvRule.evaluate_scenario:
            try:
                return self.evaluate_scenario(engine, ctx)
            except Exception as e:
                return self.error_result(
                    message=f"Rule execution failed: {str(e)}"
                )
        return super().evaluate(engine, ctx)

    def evaluate_scenario(self, engine, ctx):
        """Evaluate with own queries; overridden by multi-query rules."""
        raise NotImplementedError

    def get_params(self, ctx):
        return {"scenario": self.scenario}

    def _query(self, engine, sql):
        """A query result as DataFrame, with ``:scenario`` bound."""
        from egon_validation.db import fetch_dataframe

        return fetch_dataframe(engine, sql, params={"scenario": self.scenario})

    def _result(
        self, success, message, observed=None, expected=None, severity=None
    ):
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

    def _no_rows(self, table=None):
        """Failure for a configured scenario without rows."""
        return self._result(
            success=False,
            observed=0,
            message=(
                f"Scenario '{self.scenario}' is configured, but "
                f"{table or self.table} has no rows for it"
            ),
        )

    def _problems_result(self, problems, ok_message, observed=None):
        """Success if ``problems`` is empty, else a failure listing them."""
        if problems:
            return self._result(
                success=False,
                observed=len(problems) if observed is None else observed,
                expected=0,
                message=f"Scenario '{self.scenario}': " + "; ".join(problems),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=f"Scenario '{self.scenario}': {ok_message}",
        )


# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------


class HgvSiteValues(_HgvRule):
    """
    Check the site columns the standard rules cannot check.

    ``geom`` is NOT NULL and a POINT (the NOT NULL rule's ``<>`` operator
    is ambiguous for geometries), ``voltage_level`` is in the allowed set
    (the value set rule compares as text) and ``category_name`` is in the
    allowed set or NULL (NULL by design for highway sites).
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        voltage_levels,
        categories,
        **kwargs,
    ):
        """
        Parameters
        ----------
        voltage_levels : Sequence[int]
            Voltage levels ``spatial_assignment`` can assign.
        categories : Sequence[str]
            Allowed depot categories.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            voltage_levels=list(voltage_levels),
            categories=list(categories),
            **kwargs,
        )

    def get_params(self, ctx):
        return {
            "voltage_levels": list(self.params["voltage_levels"]),
            "categories": list(self.params["categories"]),
        }

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE geom IS NULL) AS n_geom_null,
            count(*) FILTER (
                WHERE geom IS NOT NULL AND GeometryType(geom) <> 'POINT'
            ) AS n_not_point,
            count(*) FILTER (
                WHERE voltage_level IS NOT NULL
                AND NOT (voltage_level = ANY(:voltage_levels))
            ) AS n_bad_voltage_level,
            count(*) FILTER (
                WHERE category_name IS NOT NULL
                AND NOT (category_name = ANY(:categories))
            ) AS n_bad_category,
            array_agg(DISTINCT category_name) FILTER (
                WHERE category_name IS NOT NULL
                AND NOT (category_name = ANY(:categories))
            ) AS bad_categories
        FROM {self.table}
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        problems = []
        if row["n_geom_null"]:
            problems.append(f"{row['n_geom_null']} sites without geometry")
        if row["n_not_point"]:
            problems.append(f"{row['n_not_point']} geometries are not points")
        if row["n_bad_voltage_level"]:
            problems.append(
                f"{row['n_bad_voltage_level']} voltage levels not in "
                f"{self.params['voltage_levels']}"
            )
        if row["n_bad_category"]:
            problems.append(
                f"{row['n_bad_category']} unknown categories: "
                f"{_examples(row['bad_categories'] or [])}"
            )
        bad = sum(
            int(row[c])
            for c in (
                "n_geom_null",
                "n_not_point",
                "n_bad_voltage_level",
                "n_bad_category",
            )
        )
        return self._result(
            success=not problems,
            observed=bad,
            expected=0,
            message=(
                "; ".join(problems)
                if problems
                else f"All {int(row['n_rows']):,} sites have a point "
                f"geometry, a valid voltage level and category"
            ),
        )


class HgvMinimumSites(_HgvRule):
    """
    Check that a scenario has at least a minimum number of sites per
    location type.

    Catches a broken boundary filter or an empty or wrong input file. The
    minimum is measured per dataset boundary and passed via
    :func:`~egon.data.validation.resolve_boundary_dependence`.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, min_sites, **kwargs
    ):
        """
        Parameters
        ----------
        min_sites : dict or BoundaryDependent
            Minimum number of sites per location type, e.g.
            ``{"depot": 179, "highway": 17}``.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            min_sites=min_sites,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT location_type, count(*) AS n_sites
        FROM {self.table}
        WHERE scenario = :scenario
        GROUP BY location_type
        """

    def evaluate_df(self, df, ctx):
        counts = df.set_index("location_type")["n_sites"].to_dict()
        min_sites = dict(self.params["min_sites"])
        short = {
            lt: (int(counts.get(lt, 0)), int(n))
            for lt, n in min_sites.items()
            if counts.get(lt, 0) < n
        }
        observed = ", ".join(f"{lt} {int(n):,}" for lt, n in counts.items())
        if short:
            return self._result(
                success=False,
                observed=sum(counts.values()),
                expected=sum(min_sites.values()),
                message=(
                    f"Scenario '{self.scenario}': fewer sites than the "
                    "minimum: "
                    + ", ".join(
                        f"{lt} {n:,} < {m:,}" for lt, (n, m) in short.items()
                    )
                ),
                severity=Severity.WARNING,
            )
        return self._result(
            success=True,
            observed=sum(counts.values()),
            expected=sum(min_sites.values()),
            message=(
                f"Scenario '{self.scenario}': {observed} sites (at least "
                f"the minimum)"
            ),
        )


class HgvAnnualEnergy(_HgvRule):
    """
    Check the annual energy per location type against a reference.

    Catches a mixed-up scenario mapping, lost or duplicated sites and
    scaling errors. The reference is measured per dataset boundary; on the
    full Germany run it equals the national targets of the input data.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        expected_mwh,
        rtol: float = 0.005,
        **kwargs,
    ):
        """
        Parameters
        ----------
        expected_mwh : dict or BoundaryDependent
            Reference energy in MWh per location type.
        rtol : float
            Accepted relative deviation per location type.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            expected_mwh=expected_mwh,
            rtol=rtol,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT location_type,
            sum("el_demand_N2_mwh" + "el_demand_N3_mwh"
                + "el_demand_N3S_mwh") AS energy_mwh
        FROM {self.table}
        WHERE scenario = :scenario
        GROUP BY location_type
        """

    def evaluate_df(self, df, ctx):
        actual = df.set_index("location_type")["energy_mwh"].to_dict()
        expected = dict(self.params["expected_mwh"])
        rtol = float(self.params["rtol"])
        parts, ok = [], True
        for lt, exp in expected.items():
            act = float(actual.get(lt, 0.0))
            dev = act / exp - 1 if exp else float("inf")
            ok &= abs(dev) <= rtol
            parts.append(f"{lt} {act:,.0f} MWh vs {exp:,.0f} MWh ({dev:+.2%})")
        return self._result(
            success=ok,
            observed=round(sum(actual.values()), 1),
            expected=round(sum(expected.values()), 1),
            message=(
                f"Scenario '{self.scenario}': "
                + "; ".join(parts)
                + f" (tolerance ±{rtol:.1%})"
            ),
            severity=None if ok else Severity.WARNING,
        )


class HgvSitePower(_HgvRule):
    """
    Check that every site has a connection power in (0, 120] MW.

    ``p_set_aggregated_mw`` sets the voltage level. Zero means a site
    without charging points; above 120 MW the site would be EHV, which
    ``spatial_assignment`` still connects to the MV grid district bus.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_sites,
            count(*) FILTER (WHERE NOT (
                p_set_aggregated_mw > 0
                AND p_set_aggregated_mw <= {MAX_SITE_P_SET_MW}
            )) AS n_bad,
            (array_agg(site_id ORDER BY site_id) FILTER (WHERE NOT (
                p_set_aggregated_mw > 0
                AND p_set_aggregated_mw <= {MAX_SITE_P_SET_MW}
            )))[1:5] AS examples,
            min(p_set_aggregated_mw) AS p_min,
            max(p_set_aggregated_mw) AS p_max
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_sites"]:
            return self._no_rows()
        n_bad = int(row["n_bad"])
        observed_range = f"range {row['p_min']:.3f}-{row['p_max']:.2f} MW"
        return self._problems_result(
            (
                [
                    f"{n_bad} of {int(row['n_sites']):,} sites with "
                    f"p_set_aggregated_mw outside (0, "
                    f"{MAX_SITE_P_SET_MW:g}] MW ({observed_range}; e.g. "
                    f"site_id {_examples(row['examples'] or [])})"
                ]
                if n_bad
                else []
            ),
            f"all {int(row['n_sites']):,} sites have p_set_aggregated_mw "
            f"in (0, {MAX_SITE_P_SET_MW:g}] MW ({observed_range})",
            observed=n_bad,
        )


class HgvSiteBusAssignment(_HgvRule):
    """
    Check the grid assignment of the sites.

    Every site's ``bus_id`` must be an AC bus of the scenario -- otherwise
    ``write_etrago`` skips its flex model with only a log message -- and
    the site must lie in its MV grid district or close to it.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_sites,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS n_no_ac_bus,
            (array_agg(s.site_id ORDER BY s.site_id)
                FILTER (WHERE b.bus_id IS NULL))[1:5] AS no_ac_bus,
            count(*) FILTER (WHERE d.bus_id IS NULL) AS n_no_district,
            count(*) FILTER (
                WHERE ST_Distance(s.geom, d.geom) > {MAX_DISTRICT_DISTANCE_M}
            ) AS n_far,
            (array_agg(s.site_id ORDER BY s.site_id) FILTER (
                WHERE ST_Distance(s.geom, d.geom) > {MAX_DISTRICT_DISTANCE_M}
            ))[1:5] AS far,
            max(ST_Distance(s.geom, d.geom)) AS max_distance_m
        FROM {self.table} s
        LEFT JOIN {BUS_TABLE} b
            ON b.bus_id = s.bus_id AND b.scn_name = s.scenario
            AND b.carrier = 'AC'
        LEFT JOIN {MV_GRID_DISTRICT_TABLE} d ON d.bus_id = s.mv_grid_id
        WHERE s.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_sites"]:
            return self._no_rows()
        problems = []
        if row["n_no_ac_bus"]:
            problems.append(
                f"{row['n_no_ac_bus']} sites without an AC bus in the "
                f"scenario (e.g. site_id {_examples(row['no_ac_bus'])})"
            )
        if row["n_no_district"]:
            problems.append(
                f"{row['n_no_district']} sites without an MV grid district"
            )
        if row["n_far"]:
            problems.append(
                f"{row['n_far']} sites more than "
                f"{MAX_DISTRICT_DISTANCE_M:g} m outside their MV grid "
                f"district (e.g. site_id {_examples(row['far'])})"
            )
        return self._problems_result(
            problems,
            f"all {int(row['n_sites']):,} sites have an AC bus and lie at "
            f"most {row['max_distance_m'] or 0:.0f} m outside their MV grid "
            f"district",
            observed=int(
                row["n_no_ac_bus"] + row["n_no_district"] + row["n_far"]
            ),
        )


# ---------------------------------------------------------------------------
# Charging points
# ---------------------------------------------------------------------------


class HgvChargingPointRange(_HgvRule):
    """
    Check that every charging point has at least one slot and a power.

    A charging point without power gives a flex model link with
    ``p_nom = 0``.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_cps,
            count(*) FILTER (WHERE NOT (num_cp >= 1)) AS n_bad_num,
            count(*) FILTER (WHERE NOT (p_set_mw > 0)) AS n_bad_power,
            (array_agg(cp_id ORDER BY cp_id) FILTER (
                WHERE NOT (num_cp >= 1) OR NOT (p_set_mw > 0)
            ))[1:5] AS examples
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_cps"]:
            return self._no_rows()
        problems = []
        if row["n_bad_num"]:
            problems.append(
                f"{row['n_bad_num']} charging points with num_cp < 1"
            )
        if row["n_bad_power"]:
            problems.append(
                f"{row['n_bad_power']} charging points with p_set_mw <= 0"
            )
        if problems:
            problems.append(f"e.g. cp_id {_examples(row['examples'])}")
        return self._problems_result(
            problems,
            f"all {int(row['n_cps']):,} charging points have num_cp >= 1 "
            f"and p_set_mw > 0",
            observed=int(row["n_bad_num"] + row["n_bad_power"]),
        )


class HgvChargingPointSites(_HgvRule):
    """
    Check that charging points and sites reference each other.

    Every charging point belongs to a site of the scenario, and every site
    has at least one charging point. ``fill_hgv_tables`` filters sites and
    charging points to the dataset boundary separately.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            (SELECT count(*) FROM {CP_TABLE} c
             WHERE c.scenario = :scenario) AS n_cps,
            (SELECT count(*) FROM {CP_TABLE} c
             WHERE c.scenario = :scenario AND NOT EXISTS (
                SELECT 1 FROM {SITE_TABLE} s
                WHERE s.scenario = c.scenario AND s.site_id = c.site_id
             )) AS n_cp_without_site,
            (SELECT count(*) FROM {SITE_TABLE} s
             WHERE s.scenario = :scenario) AS n_sites,
            (SELECT count(*) FROM {SITE_TABLE} s
             WHERE s.scenario = :scenario AND NOT EXISTS (
                SELECT 1 FROM {CP_TABLE} c
                WHERE c.scenario = s.scenario AND c.site_id = s.site_id
             )) AS n_site_without_cp
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_cps"]:
            return self._no_rows()
        problems = []
        if row["n_cp_without_site"]:
            problems.append(
                f"{row['n_cp_without_site']} charging points without a site"
            )
        if row["n_site_without_cp"]:
            problems.append(
                f"{row['n_site_without_cp']} sites without a charging point"
            )
        return self._problems_result(
            problems,
            f"all {int(row['n_cps']):,} charging points have a site and all "
            f"{int(row['n_sites']):,} sites have a charging point",
            observed=int(row["n_cp_without_site"] + row["n_site_without_cp"]),
        )


class HgvChargingPointProfiles(_HgvRule):
    """
    Check that every charging point's sector has a profile.

    ``write_etrago`` looks up the highway-day profile by sector; without it
    the highway-day load of a site is skipped with only a log message.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_cps,
            count(*) FILTER (WHERE p.sector IS NULL) AS n_missing,
            array_agg(DISTINCT c.sector)
                FILTER (WHERE p.sector IS NULL) AS missing_sectors
        FROM {self.table} c
        LEFT JOIN {PROFILE_TABLE} p
            ON p.scenario = c.scenario AND p.sector = c.sector
        WHERE c.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_cps"]:
            return self._no_rows()
        n_missing = int(row["n_missing"])
        return self._problems_result(
            (
                [
                    f"{n_missing} charging points in sectors without a "
                    f"profile: {_examples(row['missing_sectors'] or [])}"
                ]
                if n_missing
                else []
            ),
            f"the sectors of all {int(row['n_cps']):,} charging points have "
            f"a profile",
            observed=n_missing,
        )


# ---------------------------------------------------------------------------
# Events (shared aggregate)
# ---------------------------------------------------------------------------


class HgvEventContract(_HgvRule):
    """
    Check that the events meet what the flex model assumes.

    ``_generate_hgv_load_time_series`` indexes hourly arrays with
    ``park_start``/``park_end`` (negative values wrap around, values beyond
    the year are cut off) and only handles ``soc_start <= soc_end``; all of
    that would go unnoticed. Checks: no NULL/NaN values, time steps within
    the year, ``park_start <= park_end``, ``0 <= soc_start <= soc_end <=
    1``, ``bat_cap > 0``, ``charging_demand >= 0``, ``consumption ==
    charging_demand``, a charging capacity for charging events with demand,
    ``location`` in {charging, vacant}, and vacant events with no demand
    and a full battery.
    """

    def evaluate_scenario(self, engine, ctx):
        agg = _event_aggregate(engine, self.scenario)
        if agg.empty:
            return self._no_rows()
        keys = ["site_id", "cp_id", "vehicle_class", "slot_id", "location"]
        null_keys = agg[keys + ["use_case"]].isna().any(axis=1)
        checks = {
            "NULL/NaN values": int(agg["n_null"].sum())
            + int(agg.loc[null_keys, "n_events"].sum()),
            "park_end < park_start": int(agg["n_park_reversed"].sum()),
            "soc_start > soc_end": int(agg["n_soc_falling"].sum()),
            "consumption != charging_demand": int(
                agg["n_consumption_differs"].sum()
            ),
            "charging without charging capacity": int(
                agg["n_no_capacity"].sum()
            ),
            "invalid vacant events": int(agg["n_vacant_invalid"].sum()),
            "unknown location": int(
                agg.loc[
                    ~agg["location"].isin(["charging", "vacant"]), "n_events"
                ].sum()
            ),
        }
        problems = [f"{n:,} {what}" for what, n in checks.items() if n]
        valid = agg[~null_keys]
        ranges = [
            ("park_start", valid["park_start_min"].min(), 0, None),
            ("park_end", valid["park_end_max"].max(), None, N_TIMESTEPS - 1),
            ("soc_start", valid["soc_start_min"].min(), 0, None),
            ("soc_end", valid["soc_end_max"].max(), None, 1 + ATOL_SOC),
            ("charging_demand", valid["demand_min"].min(), 0, None),
        ]
        for column, value, lower, upper in ranges:
            if pd.isna(value):
                continue
            if lower is not None and value < lower:
                problems.append(f"{column} below {lower} (min {value})")
            if upper is not None and value > upper:
                problems.append(f"{column} above {upper:g} (max {value})")
        if not (valid["bat_cap_min"].min() > 0):
            problems.append(
                f"bat_cap not positive (min {valid['bat_cap_min'].min()})"
            )
        return self._problems_result(
            problems,
            f"all {int(agg['n_events'].sum()):,} events meet the flex model "
            f"contract",
        )


class HgvEventReferences(_HgvRule):
    """
    Check that every event belongs to a charging point of its site and
    sector.

    ``fill_hgv_tables`` filters events by ``site_id``, while the flex model
    assigns them to charging points by ``cp_id``.
    """

    def evaluate_scenario(self, engine, ctx):
        agg = _event_aggregate(engine, self.scenario)
        if agg.empty:
            return self._no_rows()
        cps = self._query(
            engine,
            f"""
            SELECT cp_id, site_id AS cp_site_id, sector
            FROM {CP_TABLE} WHERE scenario = :scenario
            """,
        )
        merged = agg.merge(cps, on="cp_id", how="left")
        no_cp = merged["cp_site_id"].isna()
        other_site = ~no_cp & (merged["cp_site_id"] != merged["site_id"])
        other_sector = ~no_cp & (merged["sector"] != merged["use_case"])
        problems = []
        for mask, what in (
            (no_cp, "events on unknown charging points"),
            (other_site, "events on a charging point of another site"),
            (other_sector, "events whose use_case is not the cp's sector"),
        ):
            n = int(merged.loc[mask, "n_events"].sum())
            if n:
                problems.append(
                    f"{n:,} {what} (e.g. cp_id "
                    f"{_examples(merged.loc[mask, 'cp_id'].unique())})"
                )
        return self._problems_result(
            problems,
            f"all {int(agg['n_events'].sum()):,} events belong to a charging "
            f"point of their site and sector",
        )


class HgvEventEnergy(_HgvRule):
    """
    Check the charging energy of the events against the site energy.

    Per location type, the energy of the charging events must match the
    sites' energy that has events: the whole energy of depots, the night
    energy of highway sites (daytime highway charging has no events).
    Catches an incomplete import of the event file and a wrong scenario
    filter.
    """

    def evaluate_scenario(self, engine, ctx):
        agg = _event_aggregate(engine, self.scenario)
        if agg.empty:
            return self._no_rows()
        sites = self._query(
            engine,
            f"""
            SELECT site_id, location_type,
                CASE WHEN location_type = 'highway'
                    THEN el_demand_night_mwh
                    ELSE "el_demand_N2_mwh" + "el_demand_N3_mwh"
                        + "el_demand_N3S_mwh"
                END AS event_energy_mwh
            FROM {SITE_TABLE} WHERE scenario = :scenario
            """,
        )
        charging = agg[agg["location"] == "charging"]
        events = (
            charging.groupby("site_id")["demand_kwh"].sum() / 1e3
        ).rename("events_mwh")
        merged = sites.set_index("site_id").join(events, how="outer")
        unknown = merged["location_type"].isna()
        problems = []
        if unknown.any():
            problems.append(
                f"{merged.loc[unknown, 'events_mwh'].sum():,.0f} MWh of "
                f"events on unknown sites"
            )
        parts = []
        for lt, grp in merged[~unknown].groupby("location_type"):
            ref = grp["event_energy_mwh"].sum()
            act = grp["events_mwh"].fillna(0).sum()
            dev = act / ref - 1 if ref else float("inf")
            text = f"{lt} {act:,.0f} vs {ref:,.0f} MWh ({dev:+.3%})"
            parts.append(text)
            if abs(dev) > RTOL_EVENT_ENERGY:
                problems.append(text)
        if problems:
            problems.append(f"tolerance ±{RTOL_EVENT_ENERGY:.1%}")
        return self._problems_result(problems, "; ".join(parts))


class HgvSlotChains(_HgvRule):
    """
    Check that every physical slot's events cover the year exactly once.

    Per physical slot (cp_id, vehicle_class, slot_id) the events must start
    at time step 0, end at the last one, last 8760 h in total, and all have
    the same battery capacity. ``e_nom``, ``e_initial`` and the SOC band of
    the flex model assume exactly this slot key and chain.
    """

    def evaluate_scenario(self, engine, ctx):
        agg = _event_aggregate(engine, self.scenario)
        if agg.empty:
            return self._no_rows()
        slots = _slots(agg)
        checks = {
            f"not {N_TIMESTEPS} h in total": slots["hours"] != N_TIMESTEPS,
            "not starting at time step 0": slots["park_start_min"] != 0,
            f"not ending at time step {N_TIMESTEPS - 1}": (
                slots["park_end_max"] != N_TIMESTEPS - 1
            ),
            "with more than one bat_cap": (
                slots["bat_cap_min"] != slots["bat_cap_max"]
            ),
        }
        problems = []
        for what, mask in checks.items():
            if mask.any():
                ex = slots.loc[
                    mask, ["cp_id", "vehicle_class", "slot_id"]
                ].head(3)
                problems.append(
                    f"{int(mask.sum()):,} slots {what} (e.g. "
                    + ", ".join(
                        f"({r.cp_id}, {r.vehicle_class}, {r.slot_id})"
                        for r in ex.itertuples()
                    )
                    + ")"
                )
        return self._problems_result(
            problems,
            f"all {len(slots):,} physical slots cover the year exactly once "
            f"with one battery capacity",
        )


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------


class HgvProfiles(_HgvRule):
    """
    Check the normalized profiles of a scenario.

    Exactly the expected sectors, each with 8760 values that are not
    negative and sum to 1. The highway-day fixed load is the highway-day
    profile times the site's day energy.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, sectors, **kwargs
    ):
        """
        Parameters
        ----------
        sectors : Sequence[str]
            Expected sectors.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            sectors=list(sectors),
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT p.sector,
            cardinality(p.profile) AS n_values,
            (SELECT count(*) FROM unnest(p.profile) v
             WHERE v IS NULL OR v = 'NaN') AS n_null,
            (SELECT min(v) FROM unnest(p.profile) v) AS v_min,
            (SELECT sum(v) FROM unnest(p.profile) v) AS v_sum
        FROM {self.table} p
        WHERE p.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        expected = set(self.params["sectors"])
        present = list(df["sector"])
        problems = []
        missing = expected - set(present)
        extra = set(present) - expected
        if missing:
            problems.append(f"missing sectors: {_examples(sorted(missing))}")
        if extra:
            problems.append(f"unexpected sectors: {_examples(sorted(extra))}")
        if len(present) != len(set(present)):
            problems.append("duplicate sectors")
        for row in df.itertuples():
            if row.n_values != N_TIMESTEPS:
                problems.append(f"{row.sector}: {row.n_values} values")
            if row.n_null:
                problems.append(f"{row.sector}: {row.n_null} NULL/NaN values")
            if row.v_min is not None and row.v_min < 0:
                problems.append(f"{row.sector}: negative values")
            if row.v_sum is None or abs(row.v_sum - 1) > 1e-6:
                problems.append(f"{row.sector}: sum {row.v_sum}")
        return self._problems_result(
            problems,
            f"{len(present)} profiles with {N_TIMESTEPS} non-negative values "
            f"summing to 1",
        )


# ---------------------------------------------------------------------------
# eTraGo
# ---------------------------------------------------------------------------


class HgvModelCount(_HgvRule):
    """
    Check that there is one flex model per MV grid district and HV site.

    ``write_etrago`` writes one HGV bus per MV grid district with MV/LV
    sites (voltage levels 4-7) and one per HV/EHV site (1-3). More buses
    mean leftovers of an earlier run, fewer mean models that were skipped
    with only a log message (no AC bus, zero charging power).
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            (SELECT count(DISTINCT mv_grid_id) FROM {SITE_TABLE}
             WHERE scenario = :scenario AND voltage_level BETWEEN 4 AND 7)
            + (SELECT count(*) FROM {SITE_TABLE}
               WHERE scenario = :scenario AND voltage_level BETWEEN 1 AND 3)
                AS n_expected,
            (SELECT count(*) FROM {BUS_TABLE}
             WHERE scn_name = :scenario
             AND carrier = '{_carriers()["bus"]}') AS n_buses
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        expected, actual = int(row["n_expected"]), int(row["n_buses"])
        if expected == 0:
            return self._no_rows(SITE_TABLE)
        return self._result(
            success=actual == expected,
            observed=actual,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': {actual:,} HGV buses, "
                f"{expected:,} expected (MV grid districts with MV/LV sites "
                f"plus HV/EHV sites)"
            ),
        )


class HgvModelComponents(_HgvRule):
    """
    Check that every HGV bus has exactly one link, store or driving load.

    One instance per component. For links, ``bus0`` must also be an AC bus
    of the same scenario, and no HGV link may point to a bus that is not an
    HGV bus. Catches the stores deleted by another dataset, orphaned buses
    and duplicates.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, component, **kwargs
    ):
        """
        Parameters
        ----------
        component : str
            "link", "store" or "load".
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            component=component,
            **kwargs,
        )

    def get_query(self, ctx):
        component = self.params["component"]
        extra = ""
        if component == "link":
            # Only links from an AC bus of the same scenario count.
            extra = f"""
                JOIN {LINK_TABLE} l
                    ON l.scn_name = c.scn_name AND l.link_id = c.id
                JOIN {BUS_TABLE} a
                    ON a.scn_name = c.scn_name AND a.bus_id = l.bus0
                    AND a.carrier = 'AC'
            """
        return f"""
        WITH hgv AS ({_hgv_component_sql("bus")}),
        comp AS (
            SELECT c.id, c.hgv_bus
            FROM ({_hgv_component_sql(component)}) c
            {extra}
        ),
        per_bus AS (
            SELECT h.id AS bus_id, count(comp.id) AS n
            FROM hgv h LEFT JOIN comp ON comp.hgv_bus = h.id
            GROUP BY h.id
        )
        SELECT
            (SELECT count(*) FROM hgv) AS n_buses,
            (SELECT count(*) FROM per_bus WHERE n <> 1) AS n_bad_buses,
            (SELECT (array_agg(bus_id ORDER BY bus_id))[1:5]
             FROM per_bus WHERE n <> 1) AS bad_buses,
            (SELECT count(*) FROM ({_hgv_component_sql(component)}) c
             WHERE NOT EXISTS (
                SELECT 1 FROM hgv h WHERE h.id = c.hgv_bus
             )) AS n_off_hgv
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        component = self.params["component"]
        if not row["n_buses"]:
            return self._no_rows(BUS_TABLE)
        problems = []
        if row["n_bad_buses"]:
            what = (
                "a link from an AC bus" if component == "link" else component
            )
            problems.append(
                f"{row['n_bad_buses']} HGV buses without exactly one "
                f"{what} (e.g. bus_id {_examples(row['bad_buses'])})"
            )
        if component != "load" and row["n_off_hgv"]:
            problems.append(
                f"{row['n_off_hgv']} HGV {component}s not on an HGV bus"
            )
        return self._problems_result(
            problems,
            f"all {int(row['n_buses']):,} HGV buses have exactly one "
            f"{component}",
            observed=int(row["n_bad_buses"])
            + (int(row["n_off_hgv"]) if component != "load" else 0),
        )


def _ac_bus_sql():
    """
    SQL mapping each HGV bus of ``:scenario`` to its AC bus.

    The AC bus of a flex model is ``bus0`` of its link. The district model
    and the HV/EHV site models of a district share that bus, so it is the
    finest level that maps back to the sites.
    """
    return f"""
        SELECT l.bus1 AS hgv_bus, l.bus0 AS ac_bus
        FROM {LINK_TABLE} l
        WHERE l.scn_name = :scenario AND l.carrier = '{_carriers()["link"]}'
    """


def _compare_per_bus(actual, expected, rtol, label, unit):
    """Problems of a per-AC-bus comparison of two Series."""
    both = pd.concat(
        [actual.rename("actual"), expected.rename("expected")], axis=1
    ).fillna(0.0)
    dev = (both["actual"] - both["expected"]).abs()
    bad = both[dev > rtol * both["expected"].abs().clip(lower=1e-9)]
    problems = []
    if not bad.empty:
        problems.append(
            f"{len(bad)} of {len(both)} AC buses with {label} off by more "
            f"than {rtol:.2%} (e.g. "
            + ", ".join(
                f"bus {i}: {r.actual:,.3f} vs {r.expected:,.3f} {unit}"
                for i, r in bad.head(3).iterrows()
            )
            + ")"
        )
    total_a, total_e = both["actual"].sum(), both["expected"].sum()
    summary = (
        f"{label} {total_a:,.1f} vs {total_e:,.1f} {unit} over "
        f"{len(both):,} AC buses"
    )
    return problems, summary


class HgvLinkPower(_HgvRule):
    """
    Check the charging power of the flex model links per AC bus.

    ``p_nom`` is the peak of the plugged-in charging power, so per AC bus
    it must be positive and at most the installed power of the charging
    points in the flex models (depot and highway night). Catches unit
    errors and events counted twice.
    """

    def evaluate_scenario(self, engine, ctx):
        links = self._query(
            engine,
            f"""
            SELECT bus0 AS ac_bus, p_nom::float AS p_nom
            FROM {LINK_TABLE}
            WHERE scn_name = :scenario
            AND carrier = '{_carriers()["link"]}'
            """,
        )
        if links.empty:
            return self._no_rows()
        installed = self._query(
            engine,
            f"""
            SELECT s.bus_id AS ac_bus, sum(c.p_set_mw) AS p_installed
            FROM {CP_TABLE} c
            JOIN {SITE_TABLE} s
                ON s.scenario = c.scenario AND s.site_id = c.site_id
            WHERE c.scenario = :scenario
            AND NOT (s.location_type = 'highway' AND c.time_of_day = 'day')
            GROUP BY s.bus_id
            """,
        ).set_index("ac_bus")["p_installed"]
        per_bus = links.groupby("ac_bus")["p_nom"].sum()
        problems = []
        n_zero = int((links["p_nom"] <= 0).sum())
        if n_zero:
            problems.append(f"{n_zero} links with p_nom <= 0")
        ratio = per_bus / installed.reindex(per_bus.index)
        no_cp = ratio.isna()
        if no_cp.any():
            problems.append(
                f"{int(no_cp.sum())} AC buses with HGV links but no flex "
                f"charging points (e.g. {_examples(ratio[no_cp].index)})"
            )
        over = ratio[ratio > 1 + RTOL_ROUNDING]
        if not over.empty:
            problems.append(
                f"{len(over)} AC buses with p_nom above the installed "
                f"charging power (max {over.max():.3f} x; e.g. bus "
                f"{_examples(over.index)})"
            )
        return self._problems_result(
            problems,
            f"p_nom of {len(links):,} links is positive and at most the "
            f"installed charging power per AC bus (ratio "
            f"{ratio.min():.2f}-{ratio.max():.3f})",
        )


class HgvStoreCapacity(_HgvRule):
    """
    Check the store capacity per AC bus against the batteries of the slots.

    ``e_nom`` must be the sum of the battery capacities of the physical
    slots (cp_id, vehicle_class, slot_id) behind the flex models of an AC
    bus. A wrong slot key merges slots of different vehicle classes and
    makes the stores too small.
    """

    def evaluate_scenario(self, engine, ctx):
        stores = self._query(
            engine,
            f"""
            WITH ac AS ({_ac_bus_sql()})
            SELECT ac.ac_bus, s.e_nom
            FROM ({_hgv_component_sql("store")}) c
            JOIN {STORE_TABLE} s
                ON s.scn_name = c.scn_name AND s.store_id = c.id
            LEFT JOIN ac ON ac.hgv_bus = c.hgv_bus
            """,
        )
        if stores.empty:
            return self._no_rows()
        agg = _event_aggregate(engine, self.scenario)
        sites = self._query(
            engine,
            f"""
            SELECT site_id, bus_id AS ac_bus FROM {SITE_TABLE}
            WHERE scenario = :scenario
            """,
        )
        slots = _slots(agg).merge(sites, on="site_id", how="left")
        expected = slots.groupby("ac_bus")["bat_cap_max"].sum() / 1e3
        actual = stores.groupby("ac_bus")["e_nom"].sum()
        problems, summary = _compare_per_bus(
            actual, expected, RTOL_EXACT, "e_nom", "MWh"
        )
        return self._problems_result(problems, summary)


class HgvStoreInitial(_HgvRule):
    """
    Check that every HGV store has a capacity and an initial energy within
    it.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_stores,
            count(*) FILTER (WHERE NOT (s.e_nom > 0)) AS n_no_capacity,
            count(*) FILTER (WHERE NOT (
                s.e_initial >= 0
                AND s.e_initial <= s.e_nom * (1 + {RTOL_ROUNDING})
            )) AS n_bad_initial,
            min(s.e_initial / NULLIF(s.e_nom, 0)) AS share_min,
            max(s.e_initial / NULLIF(s.e_nom, 0)) AS share_max
        FROM ({_hgv_component_sql("store")}) c
        JOIN {STORE_TABLE} s ON s.scn_name = c.scn_name AND s.store_id = c.id
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if not row["n_stores"]:
            return self._no_rows()
        problems = []
        if row["n_no_capacity"]:
            problems.append(f"{row['n_no_capacity']} stores with e_nom <= 0")
        if row["n_bad_initial"]:
            problems.append(
                f"{row['n_bad_initial']} stores with e_initial outside "
                f"[0, e_nom]"
            )
        return self._problems_result(
            problems,
            f"all {int(row['n_stores']):,} stores have e_nom > 0 and "
            f"e_initial within [0, e_nom] (share "
            f"{row['share_min']:.2f}-{row['share_max']:.2f})",
            observed=int(row["n_no_capacity"] + row["n_bad_initial"]),
        )


class HgvDrivingLoadEnergy(_HgvRule):
    """
    Check the driving load energy per AC bus against the charging events.

    The driving load of a flex model removes the energy its vehicles charge,
    so per AC bus its annual energy must equal the charging energy of the
    events behind it.
    """

    def evaluate_scenario(self, engine, ctx):
        loads = self._query(
            engine,
            f"""
            WITH ac AS ({_ac_bus_sql()}),
            hgv AS ({_hgv_component_sql("bus")})
            SELECT ac.ac_bus,
                (SELECT sum(v) FROM unnest(t.p_set) v) AS energy_mwh
            FROM {LOAD_TABLE} d
            JOIN hgv ON hgv.id = d.bus
            JOIN grid.egon_etrago_load_timeseries t
                ON t.scn_name = d.scn_name AND t.load_id = d.load_id
            LEFT JOIN ac ON ac.hgv_bus = d.bus
            WHERE d.scn_name = :scenario
            AND d.carrier = '{_carriers()["load"]}'
            """,
        )
        if loads.empty:
            return self._no_rows()
        agg = _event_aggregate(engine, self.scenario)
        sites = self._query(
            engine,
            f"""
            SELECT site_id, bus_id AS ac_bus FROM {SITE_TABLE}
            WHERE scenario = :scenario
            """,
        )
        charging = agg[agg["location"] == "charging"].merge(
            sites, on="site_id", how="left"
        )
        expected = charging.groupby("ac_bus")["demand_kwh"].sum() / 1e3
        actual = loads.groupby("ac_bus")["energy_mwh"].sum()
        problems, summary = _compare_per_bus(
            actual, expected, RTOL_EXACT, "driving load energy", "MWh"
        )
        return self._problems_result(problems, summary)


class HgvHighwayDayLoad(_HgvRule):
    """
    Check the highway-day fixed loads.

    Exactly one load per AC bus with highway sites, carrying the sites'
    daytime energy, and no such load on other AC buses. Catches loads
    written twice by a re-run.
    """

    def evaluate_scenario(self, engine, ctx):
        loads = self._query(
            engine,
            f"""
            SELECT d.bus AS ac_bus, count(*) AS n_loads,
                sum((SELECT sum(v) FROM unnest(t.p_set) v)) AS energy_mwh
            FROM {LOAD_TABLE} d
            JOIN {BUS_TABLE} b
                ON b.scn_name = d.scn_name AND b.bus_id = d.bus
                AND b.carrier = 'AC'
            LEFT JOIN grid.egon_etrago_load_timeseries t
                ON t.scn_name = d.scn_name AND t.load_id = d.load_id
            WHERE d.scn_name = :scenario
            AND d.carrier = '{_carriers()["load"]}'
            GROUP BY d.bus
            """,
        ).set_index("ac_bus")
        sites = self._query(
            engine,
            f"""
            SELECT bus_id AS ac_bus, sum(el_demand_day_mwh) AS day_mwh
            FROM {SITE_TABLE}
            WHERE scenario = :scenario AND location_type = 'highway'
            GROUP BY bus_id
            """,
        ).set_index("ac_bus")["day_mwh"]
        if sites.empty:
            return self._no_rows(SITE_TABLE)
        problems = []
        not_one = loads[loads["n_loads"] != 1]
        if not not_one.empty:
            problems.append(
                f"{len(not_one)} AC buses with more than one highway-day "
                f"load (e.g. bus {_examples(not_one.index)})"
            )
        missing = sites.index.difference(loads.index)
        if len(missing):
            problems.append(
                f"{len(missing)} highway AC buses without a highway-day "
                f"load (e.g. bus {_examples(missing)})"
            )
        energy_problems, summary = _compare_per_bus(
            loads["energy_mwh"], sites, RTOL_EXACT, "highway-day energy", "MWh"
        )
        return self._problems_result(problems + energy_problems, summary)


class HgvHighwayDayPeak(_HgvRule):
    """
    Check that the highway-day load does not exceed the connection power.

    Per AC bus, the peak of the highway-day fixed load must be at most the
    summed connection power of the highway sites at that bus.
    """

    def evaluate_scenario(self, engine, ctx):
        loads = self._query(
            engine,
            f"""
            SELECT d.bus AS ac_bus,
                max((SELECT max(v) FROM unnest(t.p_set) v)) AS peak_mw
            FROM {LOAD_TABLE} d
            JOIN {BUS_TABLE} b
                ON b.scn_name = d.scn_name AND b.bus_id = d.bus
                AND b.carrier = 'AC'
            JOIN grid.egon_etrago_load_timeseries t
                ON t.scn_name = d.scn_name AND t.load_id = d.load_id
            WHERE d.scn_name = :scenario
            AND d.carrier = '{_carriers()["load"]}'
            GROUP BY d.bus
            """,
        ).set_index("ac_bus")["peak_mw"]
        if loads.empty:
            return self._no_rows()
        sites = self._query(
            engine,
            f"""
            SELECT bus_id AS ac_bus, sum(p_set_aggregated_mw) AS p_set_mw
            FROM {SITE_TABLE}
            WHERE scenario = :scenario AND location_type = 'highway'
            GROUP BY bus_id
            """,
        ).set_index("ac_bus")["p_set_mw"]
        ratio = loads / sites.reindex(loads.index)
        problems = []
        if ratio.isna().any():
            problems.append(
                f"{int(ratio.isna().sum())} highway-day loads on AC buses "
                f"without highway sites"
            )
        over = ratio[ratio > 1 + RTOL_ROUNDING]
        if not over.empty:
            problems.append(
                f"{len(over)} AC buses with a highway-day peak above the "
                f"connection power (max {over.max():.3f} x; e.g. bus "
                f"{_examples(over.index)})"
            )
        return self._problems_result(
            problems,
            f"highway-day peak at most the connection power on "
            f"{len(loads):,} AC buses (max {ratio.max():.3f} x)",
        )


class HgvTimeseriesExists(_HgvRule):
    """
    Check that every HGV link, store or load has exactly one timeseries row.

    One instance per component; the row must be ``temp_id = 1`` of the
    same scenario.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, component, **kwargs
    ):
        """
        Parameters
        ----------
        component : str
            "link", "store" or "load".
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            component=component,
            **kwargs,
        )

    def get_query(self, ctx):
        component = self.params["component"]
        _, ts_table, id_col = COMPONENTS[component]
        return f"""
        WITH comp AS ({_hgv_component_sql(component)}),
        per_comp AS (
            SELECT c.id, count(t.{id_col}) AS n
            FROM comp c
            LEFT JOIN {ts_table} t
                ON t.scn_name = c.scn_name AND t.{id_col} = c.id
                AND t.temp_id = 1
            GROUP BY c.id
        )
        SELECT count(*) AS n_components,
            count(*) FILTER (WHERE n <> 1) AS n_bad,
            (array_agg(id ORDER BY id) FILTER (WHERE n <> 1))[1:5] AS bad
        FROM per_comp
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        component = self.params["component"]
        if not row["n_components"]:
            return self._no_rows()
        n_bad = int(row["n_bad"])
        return self._problems_result(
            (
                [
                    f"{n_bad} HGV {component}s without exactly one "
                    f"timeseries row (e.g. {_examples(row['bad'])})"
                ]
                if n_bad
                else []
            ),
            f"all {int(row['n_components']):,} HGV {component}s have "
            f"exactly one timeseries row",
            observed=n_bad,
        )


class HgvTimeseriesValues(_HgvRule):
    """
    Check the timeseries values of the HGV links, stores or loads.

    8760 values without NULL/NaN; ``0 <= p_max_pu <= 1`` for links,
    ``0 <= e_min_pu <= e_max_pu <= 1`` for stores and ``p_set >= 0`` for
    loads.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, component, **kwargs
    ):
        """
        Parameters
        ----------
        component : str
            "link", "store" or "load".
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            component=component,
            **kwargs,
        )

    def get_query(self, ctx):
        component = self.params["component"]
        _, ts_table, id_col = COMPONENTS[component]
        if component == "link":
            length = "cardinality(t.p_max_pu)"
            bad = f"""(SELECT count(*) FROM unnest(t.p_max_pu) v
                WHERE v IS NULL OR v = 'NaN' OR v < 0
                OR v > 1 + {RTOL_ROUNDING})"""
        elif component == "store":
            bad = f"""(SELECT count(*) FROM unnest(t.e_min_pu, t.e_max_pu)
                AS u(lo, hi)
                WHERE lo IS NULL OR hi IS NULL OR lo = 'NaN' OR hi = 'NaN'
                OR lo < 0 OR hi > 1 + {RTOL_ROUNDING}
                OR lo > hi + {RTOL_ROUNDING})"""
            length = (
                f"CASE WHEN cardinality(t.e_min_pu) = {N_TIMESTEPS} "
                f"AND cardinality(t.e_max_pu) = {N_TIMESTEPS} "
                f"THEN {N_TIMESTEPS} ELSE -1 END"
            )
        else:
            length = "cardinality(t.p_set)"
            bad = """(SELECT count(*) FROM unnest(t.p_set) v
                WHERE v IS NULL OR v = 'NaN' OR v < 0)"""
        return f"""
        WITH comp AS ({_hgv_component_sql(component)}),
        per_row AS (
            SELECT c.id, {length} AS n_values, {bad} AS n_bad_values
            FROM comp c
            JOIN {ts_table} t
                ON t.scn_name = c.scn_name AND t.{id_col} = c.id
        )
        SELECT count(*) AS n_rows,
            count(*) FILTER (
                WHERE n_values IS DISTINCT FROM {N_TIMESTEPS}
            ) AS n_bad_length,
            count(*) FILTER (WHERE n_bad_values > 0) AS n_bad_values,
            (array_agg(id ORDER BY id) FILTER (
                WHERE n_values IS DISTINCT FROM {N_TIMESTEPS}
                OR n_bad_values > 0
            ))[1:5] AS bad
        FROM per_row
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        component = self.params["component"]
        if not row["n_rows"]:
            return self._no_rows()
        problems = []
        if row["n_bad_length"]:
            problems.append(
                f"{row['n_bad_length']} rows without {N_TIMESTEPS} values"
            )
        if row["n_bad_values"]:
            problems.append(
                f"{row['n_bad_values']} rows with NULL/NaN or out-of-range "
                f"values"
            )
        if problems:
            problems.append(f"e.g. {component} {_examples(row['bad'])}")
        return self._problems_result(
            problems,
            f"all {int(row['n_rows']):,} {component} timeseries have "
            f"{N_TIMESTEPS} valid values",
            observed=int(row["n_bad_length"] + row["n_bad_values"]),
        )


class HgvScenarioCoverage(_HgvRule):
    """
    Check that HGV rows exist only in scenarios HGVCharging writes.

    Those are the scenarios of ``SCENARIO_MAP`` that are part of
    ``--scenarios``, plus their lowflex scenarios if lowflex is enabled.
    Rows elsewhere are leftovers of an earlier run or configuration. One
    instance per eTraGo table; timeseries rows are attributed via their
    component.
    """

    def __init__(self, table: str, rule_id: str, component, **kwargs):
        """
        Parameters
        ----------
        component : str
            "bus", "link", "store", "load", or one of the latter three with
            the suffix "_timeseries".
        """
        super().__init__(
            table=table, rule_id=rule_id, component=component, **kwargs
        )

    def get_params(self, ctx):
        return None

    def get_query(self, ctx):
        c = _carriers()
        component = self.params["component"]
        base = component.replace("_timeseries", "")
        where = {
            "bus": f"FROM {BUS_TABLE} x WHERE x.carrier = '{c['bus']}'",
            "link": f"FROM {LINK_TABLE} x WHERE x.carrier = '{c['link']}'",
            "store": f"""FROM {STORE_TABLE} x JOIN {BUS_TABLE} b
                ON b.scn_name = x.scn_name AND b.bus_id = x.bus
                WHERE b.carrier = '{c['bus']}'""",
            "load": f"FROM {LOAD_TABLE} x WHERE x.carrier = '{c['load']}'",
        }[base]
        if component.endswith("_timeseries"):
            table, ts_table, id_col = COMPONENTS[base]
            return f"""
            SELECT t.scn_name, count(*) AS n_rows
            FROM {ts_table} t
            WHERE EXISTS (
                SELECT 1 {where}
                AND x.scn_name = t.scn_name AND x.{id_col} = t.{id_col}
            )
            GROUP BY t.scn_name
            """
        return f"""
        SELECT x.scn_name, count(*) AS n_rows {where}
        GROUP BY x.scn_name
        """

    def evaluate(self, engine, ctx):
        # Not bound to one scenario; an empty result is a valid outcome.
        from egon_validation.db import fetch_dataframe

        try:
            df = fetch_dataframe(engine, self.get_query(ctx))
        except Exception as e:
            return self.error_result(message=f"Rule execution failed: {e}")
        from egon.data.datasets.emobility.hgv_charging.scenarios import (
            active_scenario_map,
        )

        allowed = set(active_scenario_map())
        lowflex = _integration()._lowflex_config()
        if lowflex["create"]:
            allowed |= {
                lowflex["names"][s] for s in allowed if s in lowflex["names"]
            }
        extra = df[~df["scn_name"].isin(allowed)]
        return self._result(
            success=extra.empty,
            observed=int(extra["n_rows"].sum()),
            expected=0,
            message=(
                f"HGV rows only in {_examples(sorted(allowed))}"
                if extra.empty
                else "HGV rows in scenarios HGVCharging does not write: "
                + ", ".join(
                    f"{r.scn_name} ({r.n_rows:,})" for r in extra.itertuples()
                )
            ),
        )


class HgvOrphanTimeseries(_HgvRule):
    """
    Check that no timeseries row of a scenario lacks its component.

    The timeseries tables have no carrier, so orphans cannot be attributed
    to HGV; the check covers all carriers and only warns.
    """

    def get_query(self, ctx):
        parts = []
        for name, (table, ts_table, id_col) in COMPONENTS.items():
            parts.append(f"""
            SELECT '{ts_table}' AS ts_table, count(*) AS n_orphans
            FROM {ts_table} t
            WHERE t.scn_name = :scenario AND NOT EXISTS (
                SELECT 1 FROM {table} x
                WHERE x.scn_name = t.scn_name AND x.{id_col} = t.{id_col}
            )""")
        return " UNION ALL ".join(parts)

    def evaluate_df(self, df, ctx):
        orphans = df[df["n_orphans"] > 0]
        if orphans.empty:
            return self._result(
                success=True,
                observed=0,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': no orphaned timeseries rows"
                ),
            )
        return self._result(
            success=False,
            observed=int(orphans["n_orphans"].sum()),
            expected=0,
            message=(
                f"Scenario '{self.scenario}': orphaned timeseries rows (any "
                "carrier): "
                + ", ".join(
                    f"{r.ts_table} {r.n_orphans:,}"
                    for r in orphans.itertuples()
                )
            ),
            severity=Severity.WARNING,
        )
