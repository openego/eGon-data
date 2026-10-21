"""
Sanity check validation rules for the M1/N1 eMobility model of the new
scenarios (motorized individual travel and light commercial vehicles).

Validates what :class:`~egon.data.datasets.emobility.
motorized_individual_travel.MotorizedIndividualTravel` writes for the
scenarios on the new methodology:

* ``demand.egon_ev_mit_lgv_charging_location`` -- the delivered charging
  locations with their grid connection, voltage level, MV grid district,
  AC bus and model type,
* ``demand.egon_ev_mit_lgv_location_events`` -- the placements, one array
  of charging event ids per location,
* ``demand.egon_ev_mit_lgv_trip`` and ``demand.egon_ev_mit_lgv_pool`` --
  the charging events and the profiles,
* the eTraGo tables: per flexible model a ``Li_ion`` bus with link,
  ``battery_storage`` store and driving load; dumb charging loads on the
  AC bus for lowflex and status2024,
* the three diagnostic exports.

Most rules check one scenario. A scenario that is not part of
``--scenarios`` is skipped (success, severity INFO); a configured scenario
without rows fails. The legacy scenario eGon2035 is not checked.

The rules check what egon-data does -- import, boundary filter, grid
assignment and the eTraGo model -- not the input data generation. They
take their parameters from the dataset code but recompute the checked
property in SQL, so they do not repeat the code they check.

The placements are ~5 bn per reGon scenario on a full run. The rules that
need them joined with their events share one aggregate per scenario,
grouped by model (:func:`_placement_aggregate`), instead of scanning them
each.
"""

import threading

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import numpy as np
import pandas as pd

import egon.data.config

#: Relative tolerance for energies summed in Python and SQL. Measured
#: deviations are float32 rounding of the stored timeseries.
RTOL_EXACT = 1e-4

#: Relative tolerance for the dumb charging energy, which loses what is
#: charged past the year end to the crop.
RTOL_DUMB = 1e-3

#: Largest accepted distance in m between a location assigned to the
#: nearest MV grid district and that district. To be confirmed on the
#: first Schleswig-Holstein run.
MAX_DISTRICT_DISTANCE_M = 1000.0

#: Absolute tolerance for per-unit values.
ATOL_PU = 1e-6

#: Hours of every timeseries.
N_TIMESTEPS = 8760

#: Quarter-hour index of the year end; placements arriving later are
#: cropped from the model.
LAST_TIMESTEP = 35040

LOCATION_TABLE = "demand.egon_ev_mit_lgv_charging_location"
LOCATION_EVENTS_TABLE = "demand.egon_ev_mit_lgv_location_events"
TRIP_TABLE = "demand.egon_ev_mit_lgv_trip"
POOL_TABLE = "demand.egon_ev_mit_lgv_pool"
METADATA_TABLE = "demand.egon_ev_mit_lgv_metadata"
FLEX_TIMESERIES_TABLE = "demand.egon_ev_mit_lgv_flex_timeseries"
ENERGY_BALANCE_TABLE = "demand.egon_ev_mit_lgv_energy_balance"
PROFILE_USE_CASE_TABLE = "demand.egon_ev_mit_lgv_charging_profile_use_case"
MV_GRID_DISTRICT_TABLE = "grid.egon_mv_grid_district"
EHV_VORONOI_TABLE = "grid.egon_ehv_substation_voronoi"
BUS_TABLE = "grid.egon_etrago_bus"
LINK_TABLE = "grid.egon_etrago_link"
STORE_TABLE = "grid.egon_etrago_store"
LOAD_TABLE = "grid.egon_etrago_load"
LINK_TS_TABLE = "grid.egon_etrago_link_timeseries"
STORE_TS_TABLE = "grid.egon_etrago_store_timeseries"
LOAD_TS_TABLE = "grid.egon_etrago_load_timeseries"


def configured_scenarios():
    """Scenarios of the current run (``--scenarios``)."""
    return list(egon.data.config.settings()["egon-data"]["--scenarios"])


def testmode_off():
    """Whether the run covers all of Germany."""
    return (
        egon.data.config.settings()["egon-data"]["--dataset-boundary"]
        == "Everything"
    )


def _helpers():
    """The dataset's helpers module, imported lazily (circular import)."""
    from egon.data.datasets.emobility.motorized_individual_travel import (
        helpers,
    )

    return helpers


def _carriers():
    """Link and load carriers per model type, bus and store carrier."""
    h = _helpers()
    return {
        "bus": h.BUS_CARRIER,
        "store": h.STORE_CARRIER,
        "link": dict(h.LINK_CARRIERS),
        "load": dict(h.LOAD_CARRIERS),
    }


def _sql_list(values):
    """Values as a SQL list of string literals."""
    return ", ".join(f"'{_}'" for _ in values)


def _model_type_sql(column, kind):
    """SQL mapping a link or load carrier to its model type."""
    cases = " ".join(
        f"WHEN '{carrier}' THEN '{model_type}'"
        for model_type, carrier in _carriers()[kind].items()
    )
    return f"(CASE {column} {cases} END)"


def _lowflex_name(scenario):
    """Name of the lowflex counterpart of a scenario, or None."""
    from egon.data.datasets.emobility.motorized_individual_travel import (
        MotorizedIndividualTravel,
    )

    lowflex = MotorizedIndividualTravel.sources.files["original_data"][
        "scenario"
    ]["lowflex"]
    if not lowflex["create_lowflex_scenario"]:
        return None
    return lowflex["names"].get(scenario)


def _is_flexible(scenario):
    """Whether the scenario models flexible charging."""
    from egon.data.datasets.emobility.motorized_individual_travel.model_timeseries import (  # noqa: E501
        is_flexible,
    )

    return is_flexible(scenario)


def _examples(values, n=5):
    """Up to ``n`` values as a comma-separated string."""
    values = [str(v) for v in list(values)[:n]]
    return ", ".join(values) if values else "-"


# ---------------------------------------------------------------------------
# Shared placement aggregate
# ---------------------------------------------------------------------------

_aggregates = {}
_aggregates_lock = threading.Lock()
_scenario_locks = {}


def _placement_aggregate(engine, scenario):
    """
    Placements of one scenario joined with their events, per model.

    One row per ``(model_type, bus_id)`` with the number of placements,
    those whose event exists and is a charging event, those whose event's
    use case differs from the location's (except ``urban_fast`` events at
    ``retail`` locations, which the delivery places there), and the
    deficit energy in kWh, in total and for placements arriving within the
    year. Computed once per scenario and shared by the rules.
    """
    with _aggregates_lock:
        lock = _scenario_locks.setdefault(scenario, threading.Lock())
    with lock:
        if scenario not in _aggregates:
            from egon_validation.db import fetch_dataframe

            _aggregates[scenario] = fetch_dataframe(
                engine,
                f"""
                SELECT
                    cl.model_type,
                    cl.bus_id,
                    COUNT(*) AS placements,
                    COUNT(t.event_id) AS referenced,
                    COUNT(*) FILTER (
                        WHERE t.event_id IS NOT NULL AND t.use_case IS NULL
                    ) AS not_charging,
                    COUNT(*) FILTER (
                        WHERE t.use_case <> cl.use_case
                            AND NOT (
                                t.use_case = 'urban_fast'
                                AND cl.use_case = 'retail'
                            )
                    ) AS use_case_mismatch,
                    COALESCE(
                        SUM(CAST(t.charging_demand AS double precision)), 0
                    ) AS deficit_kwh,
                    COALESCE(
                        SUM(CAST(t.charging_demand AS double precision))
                            FILTER (WHERE t.park_start < {LAST_TIMESTEP}),
                        0
                    ) AS deficit_in_year_kwh
                FROM {LOCATION_EVENTS_TABLE} le
                JOIN {LOCATION_TABLE} cl
                    ON cl.scenario = le.scenario
                    AND cl.location_id = le.location_id
                CROSS JOIN LATERAL UNNEST(le.event_ids) AS e(event_id)
                LEFT JOIN {TRIP_TABLE} t
                    ON t.scenario = le.scenario
                    AND t.event_id = e.event_id
                WHERE le.scenario = :scenario
                GROUP BY cl.model_type, cl.bus_id
                """,
                params={"scenario": scenario},
            )
        return _aggregates[scenario]


def clear_placement_aggregates():
    """Drop the cached aggregates, e.g. between test runs."""
    with _aggregates_lock:
        _aggregates.clear()


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class _MitRule(DataFrameRule):
    """
    Common base for the M1/N1 rules.

    Rules that need several queries override :meth:`evaluate_scenario`
    instead of ``get_query``/``evaluate_df``.
    """

    #: Severity of a failure.
    failure_severity = Severity.ERROR

    def __init__(self, table: str, rule_id: str, scenario=None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Table the rule is reported for.
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str or None
            Scenario to check. ``None`` for rules not bound to one.
        """
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def _check_table_empty(self, engine, ctx):
        # The framework counts the whole table before every rule: a full
        # scan of the shared eTraGo tables. Empty scenarios are detected
        # by the rules instead.
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
        try:
            return self.evaluate_scenario(engine, ctx)
        except Exception as e:
            return self.error_result(message=f"Rule execution failed: {e}")

    def evaluate_scenario(self, engine, ctx):
        """Evaluate the rule; overridden by every rule."""
        raise NotImplementedError

    def get_params(self, ctx):
        return {"scenario": self.scenario}

    def _query(self, engine, sql, **params):
        """A query result as DataFrame, with ``:scenario`` bound."""
        from egon_validation.db import fetch_dataframe

        return fetch_dataframe(
            engine, sql, params={"scenario": self.scenario, **params}
        )

    def _result(
        self, success, message, observed=None, expected=None, severity=None
    ):
        """A result with the common fields filled in."""
        if severity is None:
            severity = Severity.INFO if success else self.failure_severity
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

    def _skip(self, reason):
        """Success with a reason why nothing was checked."""
        return self._result(
            success=True, message=f"Scenario '{self.scenario}': {reason}"
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

    def _eta_cp(self, engine):
        """``eta_cp`` of the scenario from the stored run configuration."""
        value = self._query(
            engine,
            f"""
            SELECT CAST(
                simbev_config -> 'config' -> 'basic' ->> 'eta_cp' AS float
            ) AS eta_cp
            FROM {METADATA_TABLE}
            WHERE scenario = :scenario
            """,
        )
        if value.empty or value.eta_cp.isna().all():
            raise ValueError(f"No eta_cp stored in {METADATA_TABLE}")
        return float(value.eta_cp.iloc[0])


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


class MitImportCounts(_MitRule):
    """
    Check the imported row counts against the GeoLIS run metadata.

    Locations and profiles must equal ``n_locations`` and
    ``n_distinct_evs``, and the vehicles counted per profile
    (``n_vehicles``) must add up to ``n_vehicle_instances``. Catches a
    truncated import and an extraction of the wrong delivery. Full runs
    only: in test mode the imported tables are a subset by design.
    """

    def evaluate_scenario(self, engine, ctx):
        if not testmode_off():
            return self._skip("counts are only comparable on a full run")
        df = self._query(
            engine,
            f"""
            SELECT
                (SELECT COUNT(*) FROM {LOCATION_TABLE}
                 WHERE scenario = :scenario) AS locations,
                (SELECT COUNT(*) FROM {POOL_TABLE}
                 WHERE scenario = :scenario) AS profiles,
                (SELECT SUM(n_vehicles) FROM {POOL_TABLE}
                 WHERE scenario = :scenario) AS vehicles,
                (SELECT CAST(geolis_config ->> 'n_locations' AS bigint)
                 FROM {METADATA_TABLE} WHERE scenario = :scenario)
                    AS n_locations,
                (SELECT CAST(geolis_config ->> 'n_distinct_evs' AS bigint)
                 FROM {METADATA_TABLE} WHERE scenario = :scenario)
                    AS n_distinct_evs,
                (SELECT CAST(
                    geolis_config ->> 'n_vehicle_instances' AS bigint)
                 FROM {METADATA_TABLE} WHERE scenario = :scenario)
                    AS n_vehicle_instances
            """,
        ).iloc[0]
        problems = [
            f"{label}: {int(df[observed] or 0)} imported, "
            f"{df[stated]} stated by GeoLIS"
            for label, observed, stated in (
                ("locations", "locations", "n_locations"),
                ("profiles", "profiles", "n_distinct_evs"),
                ("vehicles", "vehicles", "n_vehicle_instances"),
            )
            if pd.isna(df[stated]) or int(df[observed] or 0) != df[stated]
        ]
        return self._problems_result(
            problems,
            f"{int(df.locations)} locations, {int(df.profiles)} profiles, "
            f"{int(df.vehicles)} vehicles as stated by GeoLIS",
        )


class MitEventArrayTotal(_MitRule):
    """
    Check that no placement was lost or duplicated while folding.

    Every charging event is placed once per vehicle of its profile, so the
    array lengths must add up to the sum of ``n_vehicles`` over the
    imported charging events. Full runs only: in test mode only the
    placements inside the boundary are kept.
    """

    def evaluate_scenario(self, engine, ctx):
        if not testmode_off():
            return self._skip("only checked on a full run")
        df = self._query(
            engine,
            f"""
            SELECT
                (SELECT SUM(CARDINALITY(event_ids))
                 FROM {LOCATION_EVENTS_TABLE}
                 WHERE scenario = :scenario) AS placements,
                (SELECT SUM(p.n_vehicles)
                 FROM {TRIP_TABLE} t
                 JOIN {POOL_TABLE} p
                    ON p.scenario = t.scenario AND p.ev_id = t.ev_id
                 WHERE t.scenario = :scenario) AS expected
            """,
        ).iloc[0]
        if pd.isna(df.placements):
            return self._no_rows(LOCATION_EVENTS_TABLE)
        placements, expected = int(df.placements), int(df.expected or 0)
        return self._result(
            success=placements == expected,
            observed=placements,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': {placements} placements "
                f"stored, {expected} expected from the vehicles per profile"
            ),
        )


class MitEventArrayReferences(_MitRule):
    """
    Check the references of the placements.

    Every array element must be a charging event of the event table, and
    every array must belong to an imported location.
    """

    def evaluate_scenario(self, engine, ctx):
        aggregate = _placement_aggregate(engine, self.scenario)
        if aggregate.empty:
            return self._no_rows(LOCATION_EVENTS_TABLE)
        orphan_arrays = int(
            self._query(
                engine,
                f"""
                SELECT COUNT(*) AS n
                FROM {LOCATION_EVENTS_TABLE} le
                LEFT JOIN {LOCATION_TABLE} cl
                    ON cl.scenario = le.scenario
                    AND cl.location_id = le.location_id
                WHERE le.scenario = :scenario AND cl.location_id IS NULL
                """,
            ).n.iloc[0]
        )
        unreferenced = int((aggregate.placements - aggregate.referenced).sum())
        not_charging = int(aggregate.not_charging.sum())
        problems = []
        if unreferenced:
            problems.append(f"{unreferenced} placements of unknown events")
        if not_charging:
            problems.append(
                f"{not_charging} placements of events without charging"
            )
        if orphan_arrays:
            problems.append(f"{orphan_arrays} arrays of unknown locations")
        return self._problems_result(
            problems,
            f"all {int(aggregate.placements.sum())} placements reference "
            f"charging events at imported locations",
        )


class MitEventArrayUseCase(_MitRule):
    """
    Check that a placement's event has its location's use case.

    The delivery places ``urban_fast`` events at ``retail`` locations as
    well; any other combination points to a mixed-up join between
    placements and locations.
    """

    def evaluate_scenario(self, engine, ctx):
        aggregate = _placement_aggregate(engine, self.scenario)
        if aggregate.empty:
            return self._no_rows(LOCATION_EVENTS_TABLE)
        mismatched = int(aggregate.use_case_mismatch.sum())
        return self._result(
            success=mismatched == 0,
            observed=mismatched,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': {mismatched} of "
                f"{int(aggregate.placements.sum())} placements have an "
                f"event use case other than their location's"
            ),
        )


class MitTechData(_MitRule):
    """
    Check that every vehicle type of the pool has a battery capacity in
    the stored simBEV run configuration. A missing one would give the
    store a NaN capacity.
    """

    def evaluate_scenario(self, engine, ctx):
        df = self._query(
            engine,
            f"""
            SELECT DISTINCT p.type
            FROM {POOL_TABLE} p
            LEFT JOIN {METADATA_TABLE} m ON m.scenario = p.scenario
            WHERE p.scenario = :scenario
                AND (
                    m.simbev_config -> 'tech_data' -> p.type
                        ->> 'battery_capacity'
                ) IS NULL
            """,
        )
        types = self._query(
            engine,
            f"SELECT COUNT(DISTINCT type) AS n FROM {POOL_TABLE} "
            f"WHERE scenario = :scenario",
        ).n.iloc[0]
        if not types:
            return self._no_rows(POOL_TABLE)
        return self._problems_result(
            (
                [f"no battery capacity for {_examples(df.type)}"]
                if len(df)
                else []
            ),
            f"all {int(types)} vehicle types have a battery capacity",
        )


# ---------------------------------------------------------------------------
# Spatial assignment
# ---------------------------------------------------------------------------


class MitLocationAssigned(_MitRule):
    """
    Check that every location is assigned: MV grid district, AC bus,
    voltage level and model type are set. An unassigned location would
    drop its placements, and their energy, from eTraGo silently.
    """

    def evaluate_scenario(self, engine, ctx):
        df = self._query(
            engine,
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (
                    WHERE mv_grid_id IS NULL OR bus_id IS NULL
                        OR voltage_level IS NULL OR model_type IS NULL
                        OR grid_connection_id IS NULL OR geom IS NULL
                ) AS unassigned
            FROM {LOCATION_TABLE}
            WHERE scenario = :scenario
            """,
        ).iloc[0]
        if not df.n:
            return self._no_rows()
        return self._result(
            success=int(df.unassigned) == 0,
            observed=int(df.unassigned),
            expected=0,
            message=(
                f"Scenario '{self.scenario}': {int(df.unassigned)} of "
                f"{int(df.n)} locations not fully assigned"
            ),
        )


class MitFallbackDistance(_MitRule):
    """
    Check that locations assigned to the nearest MV grid district lie
    close to it. Many far-away locations would point to a wrong CRS or
    swapped coordinates.
    """

    failure_severity = Severity.WARNING

    def __init__(self, table, rule_id, scenario=None, max_distance_m=None):
        super().__init__(table=table, rule_id=rule_id, scenario=scenario)
        self.max_distance_m = (
            MAX_DISTRICT_DISTANCE_M
            if max_distance_m is None
            else float(max_distance_m)
        )

    def evaluate_scenario(self, engine, ctx):
        df = self._query(
            engine,
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (
                    WHERE ST_Distance(m.geom, s.geom) > :max_m
                ) AS far,
                COALESCE(MAX(ST_Distance(m.geom, s.geom)), 0) AS max_m
            FROM {LOCATION_TABLE} s
            JOIN {MV_GRID_DISTRICT_TABLE} m ON m.bus_id = s.mv_grid_id
            WHERE s.scenario = :scenario AND s.assignment = 'nearest'
            """,
            max_m=self.max_distance_m,
        ).iloc[0]
        return self._result(
            success=int(df.far) == 0,
            observed=float(df.max_m),
            expected=self.max_distance_m,
            message=(
                f"Scenario '{self.scenario}': {int(df.n)} locations "
                f"assigned to the nearest district, at most "
                f"{float(df.max_m):.0f} m away; {int(df.far)} farther than "
                f"{self.max_distance_m:.0f} m"
            ),
        )


class MitVoltageLevel(_MitRule):
    """
    Check the grid connections and their voltage level.

    Locations at the same coordinates share one ``grid_connection_id`` and
    locations of one connection share its coordinates; the voltage level,
    recomputed from the summed installed capacity of the connection, must
    match the stored one, and so must the model type.
    """

    def evaluate_scenario(self, engine, ctx):
        h = _helpers()
        df = self._query(
            engine,
            f"""
            WITH connections AS (
                SELECT
                    grid_connection_id,
                    COUNT(DISTINCT (ST_X(geom), ST_Y(geom))) AS points,
                    COUNT(DISTINCT voltage_level) AS levels,
                    COUNT(DISTINCT model_type) AS models,
                    COUNT(DISTINCT bus_id) AS buses,
                    MIN(voltage_level) AS voltage_level,
                    MIN(model_type) AS model_type,
                    SUM(
                        CAST(charging_points AS double precision)
                        * average_charging_capacity
                    ) / 1000.0 AS capacity_mw
                FROM {LOCATION_TABLE}
                WHERE scenario = :scenario
                GROUP BY grid_connection_id
            ),
            points AS (
                SELECT COUNT(DISTINCT grid_connection_id) AS connections
                FROM {LOCATION_TABLE}
                WHERE scenario = :scenario
                GROUP BY ST_X(geom), ST_Y(geom)
            )
            SELECT
                (SELECT COUNT(*) FROM connections) AS n,
                (SELECT COUNT(*) FROM connections
                 WHERE points > 1 OR levels > 1 OR models > 1 OR buses > 1)
                    AS inconsistent,
                (SELECT COUNT(*) FROM points WHERE connections > 1)
                    AS split,
                (SELECT COUNT(*) FROM connections
                 WHERE voltage_level <> {h.voltage_level_sql("capacity_mw")})
                    AS wrong_level,
                (SELECT COUNT(*) FROM connections
                 WHERE model_type <> CASE
                    WHEN voltage_level >= {h.DISTRICT_MODEL_MIN_LEVEL}
                        THEN '{h.MODEL_DISTRICT}'
                    ELSE '{h.MODEL_TRANSMISSION}' END) AS wrong_model
            """,
        ).iloc[0]
        if not df.n:
            return self._no_rows()
        problems = [
            f"{int(df[key])} {label}"
            for key, label in (
                ("inconsistent", "connections with differing locations"),
                ("split", "points split over several connections"),
                ("wrong_level", "connections with a wrong voltage level"),
                ("wrong_model", "connections with a wrong model type"),
            )
            if int(df[key])
        ]
        return self._problems_result(
            problems, f"{int(df.n)} grid connections consistent"
        )


class MitBusByLevel(_MitRule):
    """
    Check the AC bus of every grid connection: an EHV substation's bus for
    voltage levels 1-2, the MV grid district's bus otherwise, and an
    existing AC bus of the scenario in either case.
    """

    def evaluate_scenario(self, engine, ctx):
        df = self._query(
            engine,
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (
                    WHERE s.voltage_level IN (1, 2)
                        AND s.bus_id NOT IN (
                            SELECT bus_id FROM {EHV_VORONOI_TABLE})
                ) AS not_ehv,
                COUNT(*) FILTER (
                    WHERE s.voltage_level >= 3 AND s.bus_id <> s.mv_grid_id
                ) AS not_district,
                COUNT(*) FILTER (WHERE b.bus_id IS NULL) AS missing_bus
            FROM {LOCATION_TABLE} s
            LEFT JOIN {BUS_TABLE} b
                ON b.scn_name = s.scenario AND b.bus_id = s.bus_id
                AND b.carrier = 'AC'
            WHERE s.scenario = :scenario
            """,
        ).iloc[0]
        if not df.n:
            return self._no_rows()
        problems = [
            f"{int(df[key])} {label}"
            for key, label in (
                ("not_ehv", "locations at levels 1-2 not on an EHV bus"),
                (
                    "not_district",
                    "locations at levels 3-7 not on their " "district's bus",
                ),
                (
                    "missing_bus",
                    "locations on a bus that is no AC bus of " "the scenario",
                ),
            )
            if int(df[key])
        ]
        return self._problems_result(
            problems, f"all {int(df.n)} locations on the right AC bus"
        )


class MitTransmissionModels(_MitRule):
    """
    Check that every transmission grid connection with placements has
    exactly one model in eTraGo, and no other transmission models exist.
    """

    def evaluate_scenario(self, engine, ctx):
        c = _carriers()
        expected = int(
            self._query(
                engine,
                f"""
                SELECT COUNT(DISTINCT cl.grid_connection_id) AS n
                FROM {LOCATION_TABLE} cl
                JOIN {LOCATION_EVENTS_TABLE} le
                    ON le.scenario = cl.scenario
                    AND le.location_id = cl.location_id
                WHERE cl.scenario = :scenario
                    AND cl.model_type = 'transmission'
                """,
            ).n.iloc[0]
        )
        if _is_flexible(self.scenario):
            sql = f"""
                SELECT COUNT(*) AS n FROM {LINK_TABLE}
                WHERE scn_name = :scenario
                    AND carrier = '{c["link"]["transmission"]}'
            """
        else:
            sql = f"""
                SELECT COUNT(*) AS n FROM {LOAD_TABLE}
                WHERE scn_name = :scenario
                    AND carrier = '{c["load"]["transmission"]}'
            """
        observed = int(self._query(engine, sql).n.iloc[0])
        return self._result(
            success=observed == expected,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': {observed} transmission "
                f"models for {expected} transmission grid connections"
            ),
        )


# ---------------------------------------------------------------------------
# eTraGo model
# ---------------------------------------------------------------------------


def _dumb_energy_sql(scenario_param):
    """SQL: dumb charging energy (MWh) per AC bus and model type.

    ``scenario_param`` names the scenario the loads belong to (the base
    scenario or its lowflex counterpart). The AC buses are looked up under
    the base scenario, bound as ``:scenario``: eTraGo has no AC buses of
    its own for the lowflex scenarios.
    """
    c = _carriers()
    return f"""
        SELECT
            ld.bus AS bus_id,
            {_model_type_sql("ld.carrier", "load")} AS model_type,
            SUM((SELECT SUM(v) FROM UNNEST(ts.p_set) v)) AS mwh
        FROM {LOAD_TABLE} ld
        JOIN {BUS_TABLE} b
            ON b.scn_name = :scenario AND b.bus_id = ld.bus
            AND b.carrier = 'AC'
        JOIN {LOAD_TS_TABLE} ts
            ON ts.scn_name = ld.scn_name AND ts.load_id = ld.load_id
        WHERE ld.scn_name = {scenario_param}
            AND ld.carrier IN ({_sql_list(c["load"].values())})
        GROUP BY 1, 2
    """


def _driving_energy_sql():
    """SQL: driving load energy (MWh) per AC bus and model type."""
    c = _carriers()
    return f"""
        SELECT
            l.bus0 AS bus_id,
            {_model_type_sql("l.carrier", "link")} AS model_type,
            SUM((SELECT SUM(v) FROM UNNEST(ts.p_set) v)) AS mwh
        FROM {LINK_TABLE} l
        JOIN {LOAD_TABLE} ld
            ON ld.scn_name = l.scn_name AND ld.bus = l.bus1
            AND ld.carrier IN ({_sql_list(c["load"].values())})
        JOIN {LOAD_TS_TABLE} ts
            ON ts.scn_name = ld.scn_name AND ts.load_id = ld.load_id
        WHERE l.scn_name = :scenario
            AND l.carrier IN ({_sql_list(c["link"].values())})
        GROUP BY 1, 2
    """


class MitModelEnergy(_MitRule):
    """
    Check the energy of every model against its placements.

    Flexible scenarios: the driving load equals the deficit of the
    placements arriving within the year. Dumb charging (status2024): the
    load on the AC bus equals that deficit divided by ``eta_cp``, up to
    what is charged past the year end.
    """

    def evaluate_scenario(self, engine, ctx):
        aggregate = _placement_aggregate(engine, self.scenario)
        if aggregate.empty:
            return self._no_rows(LOCATION_EVENTS_TABLE)
        expected = (
            aggregate.set_index(["bus_id", "model_type"])[
                "deficit_in_year_kwh"
            ]
            / 1e3
        )
        if _is_flexible(self.scenario):
            observed = self._query(engine, _driving_energy_sql())
            rtol, factor = RTOL_EXACT, 1.0
        else:
            observed = self._query(engine, _dumb_energy_sql(":scenario"))
            rtol, factor = RTOL_DUMB, 1 / self._eta_cp(engine)
        observed = observed.set_index(["bus_id", "model_type"]).mwh
        expected = expected * factor
        joined = pd.concat(
            [expected.rename("expected"), observed.rename("observed")],
            axis=1,
        ).fillna(0)
        joined = joined[(joined.expected > 0) | (joined.observed > 0)]
        deviation = (joined.observed - joined.expected).abs() / (
            joined.expected.where(joined.expected > 0, 1.0)
        )
        off = joined[deviation > rtol]
        return self._problems_result(
            (
                [
                    f"{len(off)} of {len(joined)} models off by more than "
                    f"{rtol:.2%} (e.g. bus/model "
                    f"{_examples(off.index)})"
                ]
                if len(off)
                else []
            ),
            f"{len(joined)} models match their placements "
            f"({joined.observed.sum():.0f} MWh)",
            observed=len(off),
        )


class MitTotalEnergy(_MitRule):
    """
    Check the scenario total: all models together carry the energy of all
    placements. Catches locations falling between district and
    transmission models.
    """

    def evaluate_scenario(self, engine, ctx):
        aggregate = _placement_aggregate(engine, self.scenario)
        if aggregate.empty:
            return self._no_rows(LOCATION_EVENTS_TABLE)
        expected = aggregate.deficit_in_year_kwh.sum() / 1e3
        if _is_flexible(self.scenario):
            observed = self._query(engine, _driving_energy_sql()).mwh.sum()
            rtol = RTOL_EXACT
        else:
            observed = self._query(
                engine, _dumb_energy_sql(":scenario")
            ).mwh.sum()
            expected /= self._eta_cp(engine)
            rtol = RTOL_DUMB
        deviation = abs(observed - expected) / expected if expected else 1.0
        return self._result(
            success=deviation <= rtol,
            observed=float(observed),
            expected=float(expected),
            message=(
                f"Scenario '{self.scenario}': models {observed:.1f} MWh, "
                f"placements {expected:.1f} MWh ({deviation:.4%})"
            ),
        )


class MitModelComponents(_MitRule):
    """
    Check the components of every flexible model.

    Every ``Li_ion`` bus has exactly one link, one store and one load, the
    link and load carriers of the same model type. Scenarios without
    flexible charging have no ``Li_ion`` bus at all. Catches duplicates
    after re-runs and mixed-up carriers.
    """

    def evaluate_scenario(self, engine, ctx):
        c = _carriers()
        df = self._query(
            engine,
            f"""
            SELECT
                b.bus_id,
                COUNT(DISTINCT l.link_id) AS links,
                COUNT(DISTINCT s.store_id) AS stores,
                COUNT(DISTINCT ld.load_id) AS loads,
                MIN({_model_type_sql("l.carrier", "link")}) AS link_model,
                MIN({_model_type_sql("ld.carrier", "load")}) AS load_model
            FROM {BUS_TABLE} b
            LEFT JOIN {LINK_TABLE} l
                ON l.scn_name = b.scn_name AND l.bus1 = b.bus_id
                AND l.carrier IN ({_sql_list(c["link"].values())})
            LEFT JOIN {STORE_TABLE} s
                ON s.scn_name = b.scn_name AND s.bus = b.bus_id
                AND s.carrier = '{c["store"]}'
            LEFT JOIN {LOAD_TABLE} ld
                ON ld.scn_name = b.scn_name AND ld.bus = b.bus_id
                AND ld.carrier IN ({_sql_list(c["load"].values())})
            WHERE b.scn_name = :scenario AND b.carrier = '{c["bus"]}'
            GROUP BY b.bus_id
            """,
        )
        if not _is_flexible(self.scenario):
            return self._problems_result(
                [f"{len(df)} Li_ion buses"] if len(df) else [],
                "no Li_ion buses, as expected for dumb charging",
            )
        if df.empty:
            return self._no_rows(BUS_TABLE)
        bad = df[
            (df.links != 1)
            | (df.stores != 1)
            | (df.loads != 1)
            | (df.link_model != df.load_model)
        ]
        return self._problems_result(
            (
                [
                    f"{len(bad)} of {len(df)} Li_ion buses without exactly "
                    f"one link, store and load of one model type (e.g. "
                    f"{_examples(bad.bus_id)})"
                ]
                if len(bad)
                else []
            ),
            f"{len(df)} flexible models complete",
        )


class MitStoreBand(_MitRule):
    """
    Check the stores: ``e_nom > 0``, ``e_initial = e_nom``, and 8760
    values of ``0 <= e_min_pu <= e_max_pu <= 1`` without NaN.
    """

    def evaluate_scenario(self, engine, ctx):
        if not _is_flexible(self.scenario):
            return self._skip("no stores for dumb charging")
        c = _carriers()
        df = self._query(
            engine,
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (WHERE s.e_nom IS NULL OR s.e_nom <= 0)
                    AS no_capacity,
                COUNT(*) FILTER (
                    WHERE ABS(s.e_initial - s.e_nom) > {RTOL_EXACT} * s.e_nom
                ) AS not_full,
                COUNT(*) FILTER (
                    WHERE ts.store_id IS NULL
                        OR CARDINALITY(ts.e_min_pu) <> {N_TIMESTEPS}
                        OR CARDINALITY(ts.e_max_pu) <> {N_TIMESTEPS}
                ) AS bad_length,
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                        FROM UNNEST(ts.e_min_pu, ts.e_max_pu) AS u(lo, hi)
                        WHERE lo = 'NaN' OR hi = 'NaN'
                            OR lo < -{ATOL_PU} OR hi > 1 + {ATOL_PU}
                            OR lo > hi + {ATOL_PU}
                    )
                ) AS bad_band
            FROM {STORE_TABLE} s
            JOIN {BUS_TABLE} b
                ON b.scn_name = s.scn_name AND b.bus_id = s.bus
                AND b.carrier = '{c["bus"]}'
            LEFT JOIN {STORE_TS_TABLE} ts
                ON ts.scn_name = s.scn_name AND ts.store_id = s.store_id
            WHERE s.scn_name = :scenario AND s.carrier = '{c["store"]}'
            """,
        ).iloc[0]
        if not df.n:
            return self._no_rows(STORE_TABLE)
        problems = [
            f"{int(df[key])} {label}"
            for key, label in (
                ("no_capacity", "stores without capacity"),
                ("not_full", "stores not starting full"),
                ("bad_length", "stores without 8760 band values"),
                ("bad_band", "stores with a band outside 0..1 or crossed"),
            )
            if int(df[key])
        ]
        return self._problems_result(
            problems, f"{int(df.n)} stores with a valid band"
        )


class MitLinkParams(_MitRule):
    """
    Check the links: ``p_nom > 0``, efficiency = ``eta_cp`` of the
    scenario, and 8760 values of ``0 <= p_max_pu <= 1``.
    """

    def evaluate_scenario(self, engine, ctx):
        if not _is_flexible(self.scenario):
            return self._skip("no links for dumb charging")
        c = _carriers()
        eta = self._eta_cp(engine)
        df = self._query(
            engine,
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (WHERE l.p_nom IS NULL OR l.p_nom <= 0)
                    AS no_power,
                COUNT(*) FILTER (
                    WHERE ABS(l.efficiency - :eta) > {ATOL_PU}
                ) AS wrong_efficiency,
                COUNT(*) FILTER (
                    WHERE ts.link_id IS NULL
                        OR CARDINALITY(ts.p_max_pu) <> {N_TIMESTEPS}
                        OR EXISTS (
                            SELECT 1 FROM UNNEST(ts.p_max_pu) v
                            WHERE v = 'NaN' OR v < -{ATOL_PU}
                                OR v > 1 + {ATOL_PU}
                        )
                ) AS bad_availability
            FROM {LINK_TABLE} l
            LEFT JOIN {LINK_TS_TABLE} ts
                ON ts.scn_name = l.scn_name AND ts.link_id = l.link_id
            WHERE l.scn_name = :scenario
                AND l.carrier IN ({_sql_list(c["link"].values())})
            """,
            eta=eta,
        ).iloc[0]
        if not df.n:
            return self._no_rows(LINK_TABLE)
        problems = [
            f"{int(df[key])} {label}"
            for key, label in (
                ("no_power", "links without capacity"),
                ("wrong_efficiency", f"links with efficiency != {eta}"),
                ("bad_availability", "links without valid p_max_pu"),
            )
            if int(df[key])
        ]
        return self._problems_result(
            problems, f"{int(df.n)} links with valid parameters"
        )


class MitBandReachable(_MitRule):
    """
    Check that the store band can be followed through the link.

    Charging as early as possible -- every hour as much as the link
    (``eta * p_nom * p_max_pu``) and the upper band allow -- keeps the store
    as full as it can be at every hour. The band is feasible exactly if
    that path never falls below the lower band. A band the optimiser
    cannot follow shows up as an infeasible eTraGo run. A warning, so the
    shortfall can be judged; it is reported relative to ``e_nom``.
    """

    failure_severity = Severity.WARNING

    #: Models fetched per query.
    BATCH = 300

    def evaluate_scenario(self, engine, ctx):
        if not _is_flexible(self.scenario):
            return self._skip("no stores for dumb charging")
        c = _carriers()
        buses = self._query(
            engine,
            f"""
            SELECT bus_id FROM {BUS_TABLE}
            WHERE scn_name = :scenario AND carrier = '{c["bus"]}'
            ORDER BY bus_id
            """,
        ).bus_id.to_list()
        if not buses:
            return self._no_rows(BUS_TABLE)
        failing = []
        for start in range(0, len(buses), self.BATCH):
            df = self._query(
                engine,
                f"""
                SELECT
                    b.bus_id, s.e_nom, s.e_initial, sts.e_min_pu,
                    sts.e_max_pu, l.p_nom, l.efficiency, lts.p_max_pu,
                    lds.p_set
                FROM {BUS_TABLE} b
                JOIN {STORE_TABLE} s
                    ON s.scn_name = b.scn_name AND s.bus = b.bus_id
                JOIN {STORE_TS_TABLE} sts
                    ON sts.scn_name = s.scn_name AND sts.store_id = s.store_id
                JOIN {LINK_TABLE} l
                    ON l.scn_name = b.scn_name AND l.bus1 = b.bus_id
                JOIN {LINK_TS_TABLE} lts
                    ON lts.scn_name = l.scn_name AND lts.link_id = l.link_id
                JOIN {LOAD_TABLE} ld
                    ON ld.scn_name = b.scn_name AND ld.bus = b.bus_id
                JOIN {LOAD_TS_TABLE} lds
                    ON lds.scn_name = ld.scn_name AND lds.load_id = ld.load_id
                WHERE b.scn_name = :scenario AND b.bus_id = ANY(:buses)
                """,
                buses=buses[start : start + self.BATCH],
            )
            for row in df.itertuples():
                shortfall = _band_shortfall(
                    e_initial=row.e_initial,
                    lower=np.asarray(row.e_min_pu, dtype=float) * row.e_nom,
                    upper=np.asarray(row.e_max_pu, dtype=float) * row.e_nom,
                    driving=np.asarray(row.p_set, dtype=float),
                    capacity=row.efficiency
                    * row.p_nom
                    * np.asarray(row.p_max_pu, dtype=float),
                )
                if shortfall > RTOL_EXACT * row.e_nom:
                    failing.append((row.bus_id, shortfall / row.e_nom))
        failing.sort(key=lambda _: -_[1])
        return self._problems_result(
            (
                [
                    f"{len(failing)} of {len(buses)} models cannot follow "
                    f"their band, at most {failing[0][1]:.2%} of e_nom short "
                    f"(e.g. Li_ion buses "
                    f"{_examples(_[0] for _ in failing)})"
                ]
                if failing
                else []
            ),
            f"all {len(buses)} models can follow their band",
        )


def _band_shortfall(e_initial, lower, upper, driving, capacity):
    """
    Largest shortfall below the lower band of the charge-as-early-as-
    possible path of a store, in its energy unit; 0 if the band is
    feasible. After a shortfall the path continues from the lower band, so
    later hours are judged on their own.
    """
    energy, shortfall = e_initial, 0.0
    for t in range(len(lower)):
        energy = min(energy - driving[t] + capacity[t], upper[t])
        if energy < lower[t]:
            shortfall = max(shortfall, lower[t] - energy)
            energy = lower[t]
    return shortfall


class MitLowflexPair(_MitRule):
    """
    Check the lowflex counterpart of a flexible scenario: every model has
    its dumb charging load on the same AC bus in the lowflex scenario, and
    that energy is the driving energy divided by ``eta_cp`` (D24).
    """

    def evaluate_scenario(self, engine, ctx):
        if not _is_flexible(self.scenario):
            return self._skip("no lowflex counterpart for dumb charging")
        lowflex = _lowflex_name(self.scenario)
        if lowflex is None:
            return self._skip("no lowflex scenario configured")
        driving = (
            self._query(engine, _driving_energy_sql())
            .set_index(["bus_id", "model_type"])
            .mwh
        )
        if driving.empty:
            return self._no_rows(LINK_TABLE)
        dumb = (
            self._query(engine, _dumb_energy_sql(":lowflex"), lowflex=lowflex)
            .set_index(["bus_id", "model_type"])
            .mwh
        )
        eta = self._eta_cp(engine)
        joined = pd.concat(
            [(driving / eta).rename("expected"), dumb.rename("observed")],
            axis=1,
        ).fillna(0)
        missing = joined[joined.observed == 0]
        deviation = (joined.observed - joined.expected).abs() / (
            joined.expected.where(joined.expected > 0, 1.0)
        )
        off = joined[(deviation > RTOL_DUMB) & (joined.observed > 0)]
        problems = []
        if len(missing):
            problems.append(
                f"{len(missing)} models without lowflex load (e.g. "
                f"{_examples(missing.index)})"
            )
        if len(off):
            problems.append(
                f"{len(off)} lowflex loads off the D24 identity by more "
                f"than {RTOL_DUMB:.1%}"
            )
        return self._problems_result(
            problems, f"{len(joined)} models paired with '{lowflex}'"
        )


class MitDiagnostics(_MitRule):
    """
    Check the diagnostic exports: per AC bus and model type, export A's
    charging load equals the sum of export D and its driving load equals
    the eTraGo driving load (flexible scenarios); export B's battery-side
    energy equals the placements' deficit and its grid side that divided
    by ``eta_cp``.
    """

    def evaluate_scenario(self, engine, ctx):
        aggregate = _placement_aggregate(engine, self.scenario)
        if aggregate.empty:
            return self._no_rows(LOCATION_EVENTS_TABLE)
        key = ["bus_id", "model_type"]
        a = self._query(
            engine,
            f"""
            SELECT
                bus_id, model_type,
                (SELECT SUM(v) FROM UNNEST(charging_load_grid) v) AS dumb,
                (SELECT SUM(v) FROM UNNEST(driving_load) v) AS driving
            FROM {FLEX_TIMESERIES_TABLE}
            WHERE scenario = :scenario
            """,
        ).set_index(key)
        if a.empty:
            return self._no_rows(FLEX_TIMESERIES_TABLE)
        d = self._query(
            engine,
            f"""
            SELECT bus_id, model_type,
                SUM((SELECT SUM(v) FROM UNNEST(charging_load_grid) v)) AS dumb
            FROM {PROFILE_USE_CASE_TABLE}
            WHERE scenario = :scenario
            GROUP BY bus_id, model_type
            """,
        ).set_index(key)
        b = self._query(
            engine,
            f"""
            SELECT bus_id, model_type,
                SUM(charging_demand_battery_mwh) AS battery,
                SUM(charging_demand_grid_mwh) AS grid,
                SUM(n_placements) AS placements
            FROM {ENERGY_BALANCE_TABLE}
            WHERE scenario = :scenario
            GROUP BY bus_id, model_type
            """,
        ).set_index(key)
        expected = aggregate.set_index(key)
        eta = self._eta_cp(engine)

        def off(observed, reference, rtol):
            joined = pd.concat(
                [observed.rename("o"), reference.rename("r")], axis=1
            ).fillna(0)
            deviation = (joined.o - joined.r).abs() / joined.r.where(
                joined.r.abs() > 0, 1.0
            )
            return int((deviation > rtol).sum())

        checks = {
            "A charging load != sum of D": off(a.dumb, d.dumb, RTOL_EXACT),
            "B battery side != deficit of the placements": off(
                b.battery, expected.deficit_kwh / 1e3, RTOL_EXACT
            ),
            "B grid side != battery side / eta_cp": off(
                b.grid, b.battery / eta, RTOL_EXACT
            ),
            "B placements != placements": off(
                b.placements, expected.placements, 0
            ),
        }
        if _is_flexible(self.scenario):
            driving = (
                self._query(engine, _driving_energy_sql()).set_index(key).mwh
            )
            checks["A driving load != eTraGo driving load"] = off(
                a.driving, driving, RTOL_EXACT
            )
        problems = [f"{n} models: {label}" for label, n in checks.items() if n]
        return self._problems_result(
            problems, f"exports A, B and D consistent for {len(a)} models"
        )


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


class MitScenarioCoverage(_MitRule):
    """
    Check which scenarios carry M1/N1 components: exactly the configured
    scenarios and the lowflex counterparts of the flexible ones. The
    transmission carriers occur in new-methodology scenarios only.
    Not bound to a scenario.
    """

    def evaluate_scenario(self, engine, ctx):
        c = _carriers()
        h = _helpers()
        configured = configured_scenarios()
        expected = set(configured) | {
            _lowflex_name(_)
            for _ in configured
            if _is_flexible(_) and _lowflex_name(_)
        }
        new = {_ for _ in configured if not h.is_legacy_scenario(_)}
        expected_transmission = new | {
            _lowflex_name(_)
            for _ in new
            if _is_flexible(_) and _lowflex_name(_)
        }
        df = self._query(
            engine,
            f"""
            SELECT DISTINCT scn_name, carrier FROM {LOAD_TABLE}
            WHERE carrier IN ({_sql_list(c["load"].values())})
            UNION
            SELECT DISTINCT scn_name, carrier FROM {LINK_TABLE}
            WHERE carrier IN ({_sql_list(c["link"].values())})
            """,
        )
        found = set(df.scn_name)
        transmission = set(
            df.scn_name[
                df.carrier.isin(
                    [c["load"]["transmission"], c["link"]["transmission"]]
                )
            ]
        )
        problems = []
        if found - expected:
            problems.append(
                f"components in unexpected scenarios "
                f"{sorted(found - expected)}"
            )
        if expected - found:
            problems.append(
                f"no components in scenarios {sorted(expected - found)}"
            )
        if transmission - expected_transmission:
            problems.append(
                f"transmission carriers in "
                f"{sorted(transmission - expected_transmission)}"
            )
        return self._problems_result(
            problems, f"components exactly in {sorted(expected)}"
        )


class MitOrphans(_MitRule):
    """
    Check for leftovers of crashed or repeated runs: no M1/N1 link whose
    ``Li_ion`` bus is missing and no ``Li_ion`` bus without a link. Also
    counts link, store and load timeseries rows whose component is gone;
    those cannot be attributed to a carrier any more, so that part covers
    every dataset. A warning: leftovers do not change the model, but they
    show a delete step missed something. Not bound to a scenario.
    """

    failure_severity = Severity.WARNING

    def evaluate_scenario(self, engine, ctx):
        c = _carriers()
        df = self._query(
            engine,
            f"""
            SELECT
                (SELECT COUNT(*) FROM {LINK_TABLE} l
                 LEFT JOIN {BUS_TABLE} b
                    ON b.scn_name = l.scn_name AND b.bus_id = l.bus1
                 WHERE l.carrier IN ({_sql_list(c["link"].values())})
                    AND b.bus_id IS NULL) AS links_without_bus,
                (SELECT COUNT(*) FROM {BUS_TABLE} b
                 LEFT JOIN {LINK_TABLE} l
                    ON l.scn_name = b.scn_name AND l.bus1 = b.bus_id
                 WHERE b.carrier = '{c["bus"]}'
                    AND l.link_id IS NULL) AS buses_without_link,
                (SELECT COUNT(*) FROM {LINK_TS_TABLE} ts
                 LEFT JOIN {LINK_TABLE} l
                    ON l.scn_name = ts.scn_name AND l.link_id = ts.link_id
                 WHERE l.link_id IS NULL) AS link_ts,
                (SELECT COUNT(*) FROM {STORE_TS_TABLE} ts
                 LEFT JOIN {STORE_TABLE} s
                    ON s.scn_name = ts.scn_name AND s.store_id = ts.store_id
                 WHERE s.store_id IS NULL) AS store_ts,
                (SELECT COUNT(*) FROM {LOAD_TS_TABLE} ts
                 LEFT JOIN {LOAD_TABLE} ld
                    ON ld.scn_name = ts.scn_name AND ld.load_id = ts.load_id
                 WHERE ld.load_id IS NULL) AS load_ts
            """,
        ).iloc[0]
        problems = [
            f"{int(df[key])} {label}"
            for key, label in (
                ("links_without_bus", "M1/N1 links without their bus"),
                ("buses_without_link", "Li_ion buses without a link"),
                ("link_ts", "link timeseries without a link (any dataset)"),
                (
                    "store_ts",
                    "store timeseries without a store (any " "dataset)",
                ),
                ("load_ts", "load timeseries without a load (any dataset)"),
            )
            if int(df[key])
        ]
        return self._problems_result(problems, "no leftovers")
