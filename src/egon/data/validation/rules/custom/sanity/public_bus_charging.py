"""
Sanity check validation rules for public bus charging (vehicle class M3).

Validates what :class:`~egon.data.datasets.emobility.public_bus_charging.
PublicBusCharging` writes:

* ``demand.egon_ev_bus_charging_depot`` -- one row per depot and scenario
  with location, voltage level, bus and the hourly charging series,
* ``grid.egon_etrago_load`` / ``grid.egon_etrago_load_timeseries`` -- one
  load per eTraGo bus and scenario, carrier ``land_transport_bus``, with the
  depots' series summed.

Most rules check one scenario. A scenario that is not part of
``--scenarios`` is skipped (success, severity INFO); a configured scenario
without rows fails.

The rules take their parameters -- scenarios, input files, voltage level
sets, carrier, number of time steps -- from the dataset code, but compute
the checked property on their own (in SQL or from the input files), so
they do not just repeat the code they check. The voltage level check is the
exception: it calls the dataset's ``assign_voltage_level``, so an error in
that function itself is not detected.
"""

from pathlib import Path

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import numpy as np
import pandas as pd

import egon.data.config

#: Relative tolerance for sums compared between the Python write path and
#: SQL or a fresh read of the input. Measured deviations are below 2e-13;
#: anything above this bound is a real difference, not float rounding.
RTOL_ROUNDING = 1e-9

#: Absolute tolerance in MW for single values (peaks, hourly values). The
#: values are copied, not computed, so measured deviations are exactly 0.
ATOL_MW = 1e-9

#: Largest accepted distance in m between a stored depot location and the
#: input location. The location is only reprojected to EPSG:3035 on the
#: way; measured shifts are 0 m.
MAX_GEOM_SHIFT_M = 1.0

ETRAGO_BUS_TABLE = "grid.egon_etrago_bus"


def configured_scenarios():
    """Scenarios of the current run (``--scenarios``)."""
    return list(egon.data.config.settings()["egon-data"]["--scenarios"])


def _dataset():
    """The dataset class, imported lazily to avoid a circular import."""
    from egon.data.datasets.emobility.public_bus_charging import (
        PublicBusCharging,
    )

    return PublicBusCharging


def _examples(values, n=5):
    """Up to ``n`` values as a comma-separated string."""
    values = [str(v) for v in list(values)[:n]]
    return ", ".join(values) if values else "-"


class _PublicBusRule(DataFrameRule):
    """
    Common base for the public bus charging rules.

    Subclasses return exactly one row per query (or evaluate the whole
    result in pandas), because the framework reports an empty result as a
    failure.
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
        # The framework counts the whole table before every rule. The eTraGo
        # tables are shared and large, and empty scenarios are detected per
        # scenario by the rules instead.
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
        return super().evaluate(engine, ctx)

    def get_params(self, ctx):
        return {"scenario": self.scenario}

    def _depot_table(self):
        return _dataset().targets.tables["charging_depot"]

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

    def _no_rows(self):
        """Failure for a configured scenario without rows."""
        return self._result(
            success=False,
            observed=0,
            message=(
                f"Scenario '{self.scenario}' is configured, but "
                f"{self._depot_table()} has no rows for it"
            ),
        )


class PublicBusMinimumDepots(_PublicBusRule):
    """
    Check that a scenario has at least a minimum number of depots.

    Depots that fall in no grid district are dropped by the dataset, so a
    broken spatial join or a CRS error shows up as missing depots. The
    expected minimum is measured per dataset boundary and passed via
    :func:`~egon.data.validation.resolve_boundary_dependence`.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str, min_depots, **kwargs
    ):
        """
        Parameters
        ----------
        min_depots : int or BoundaryDependent
            Minimum number of depots of the scenario.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            min_depots=min_depots,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT count(*) AS n_depots
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        n_depots = int(df["n_depots"].values[0])
        min_depots = int(self.params["min_depots"])
        if n_depots == 0:
            return self._no_rows()
        success = n_depots >= min_depots
        return self._result(
            success=success,
            observed=n_depots,
            expected=min_depots,
            message=(
                f"Scenario '{self.scenario}': {n_depots:,} depots "
                f"({'at least' if success else 'fewer than'} the minimum "
                f"of {min_depots:,})"
            ),
        )


class PublicBusSeriesConsistency(_PublicBusRule):
    """
    Check the charging series of the depots against their scalar columns.

    Every ``p_set`` must have one value per time step, contain no NULL,
    NaN or negative value, and its maximum and sum must equal
    ``peak_load_mw`` and ``annual_demand_mwh`` (1 h time steps, so the sum
    in MW is the energy in MWh). Together with
    :class:`PublicBusInputConsistency`, which ties the scalar columns to
    the input files, this anchors ``p_set`` to the input.
    """

    def get_params(self, ctx):
        from egon.data.datasets.emobility.public_bus_charging.fill_tables import (  # noqa: E501
            N_TIMESTEPS,
        )

        return {
            "scenario": self.scenario,
            "n_timesteps": int(N_TIMESTEPS),
            "atol": ATOL_MW,
            "rtol": RTOL_ROUNDING,
        }

    def get_query(self, ctx):
        return f"""
        WITH checked AS (
            SELECT
                d.depot_id,
                cardinality(d.p_set) IS DISTINCT FROM :n_timesteps
                    AS bad_length,
                NOT coalesce(abs(s.peak - d.peak_load_mw) <= :atol, false)
                    AS bad_peak,
                NOT coalesce(
                    abs(s.total - d.annual_demand_mwh)
                        <= :rtol * abs(d.annual_demand_mwh),
                    false
                ) AS bad_annual,
                s.n_invalid
            FROM {self.table} d
            CROSS JOIN LATERAL (
                SELECT
                    max(v) AS peak,
                    sum(v) AS total,
                    -- NaN = 'NaN' is TRUE in PostgreSQL, v <> v is not
                    count(*) FILTER (
                        WHERE v IS NULL OR v = 'NaN' OR v < 0
                    ) AS n_invalid
                FROM unnest(d.p_set) AS v
            ) AS s
            WHERE d.scenario = :scenario
        )
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE bad_length) AS n_bad_length,
            count(*) FILTER (WHERE bad_peak) AS n_bad_peak,
            count(*) FILTER (WHERE bad_annual) AS n_bad_annual,
            coalesce(sum(n_invalid), 0) AS n_invalid_values,
            array_agg(depot_id) FILTER (
                WHERE bad_length OR bad_peak OR bad_annual OR n_invalid > 0
            ) AS bad_depots
        FROM checked
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        counts = {
            "wrong length": int(row.n_bad_length),
            "peak_load_mw != max(p_set)": int(row.n_bad_peak),
            "annual_demand_mwh != sum(p_set)": int(row.n_bad_annual),
        }
        n_invalid = int(row.n_invalid_values)
        n_bad = sum(counts.values()) + n_invalid
        message = (
            f"Scenario '{self.scenario}', {int(row.n_rows):,} depots: "
            + ", ".join(f"{n} {label}" for label, n in counts.items())
            + f", {n_invalid} NULL/NaN/negative values in p_set"
        )
        if n_bad:
            message += f" (e.g. {_examples(row.bad_depots or [])})"
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class PublicBusVoltageLevel(_PublicBusRule):
    """
    Check that each depot's voltage level matches its peak load.

    The expected level is computed with the dataset's own
    ``assign_voltage_level`` from the stored ``peak_load_mw``, so a level
    derived from another scenario's peak or set elsewhere is detected. An
    error inside ``assign_voltage_level`` itself is not.
    """

    def get_query(self, ctx):
        return f"""
        SELECT depot_id, peak_load_mw, voltage_level
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        from egon.data.datasets.emobility.public_bus_charging.spatial_assignment import (  # noqa: E501
            assign_voltage_level,
        )

        expected = assign_voltage_level(df["peak_load_mw"]).reset_index(
            drop=True
        )
        stored = df["voltage_level"].astype("Int64").reset_index(drop=True)
        bad = (stored != expected).fillna(True)
        n_bad = int(bad.sum())
        message = (
            f"Scenario '{self.scenario}': {n_bad} of {len(df):,} depots "
            f"have a voltage level that does not match their peak load"
        )
        if n_bad:
            message += f" (e.g. {_examples(df.loc[bad.values, 'depot_id'])})"
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class PublicBusBusAssignment(_PublicBusRule):
    """
    Check the eTraGo bus and grid district assigned to each depot.

    * ``bus_id`` must be an AC bus in Germany of the same scenario.
    * Voltage levels served by the MV grid districts: ``mv_grid_id`` equals
      ``bus_id`` and the depot lies within that district.
    * Voltage levels served by the EHV substation Voronoi cells:
      ``mv_grid_id`` is NULL and the depot lies within the cell of its bus.
      No depot of the current input reaches these levels, so this branch
      has not been exercised on real data.

    The level sets are taken from the dataset; containment is tested with
    ``ST_Within`` like the dataset's ``sjoin(predicate="within")``.
    """

    def get_params(self, ctx):
        from egon.data.datasets.emobility.public_bus_charging.spatial_assignment import (  # noqa: E501
            EHV_LEVELS,
            MV_LEVELS,
        )

        return {
            "scenario": self.scenario,
            "mv_levels": [int(v) for v in MV_LEVELS],
            "ehv_levels": [int(v) for v in EHV_LEVELS],
        }

    def get_query(self, ctx):
        sources = _dataset().sources.tables
        return f"""
        SELECT
            count(*) AS n_rows,
            count(*) FILTER (WHERE d.geom IS NULL) AS n_no_geom,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS n_no_ac_bus,
            count(*) FILTER (
                WHERE d.voltage_level = ANY(:mv_levels)
                  AND (d.mv_grid_id IS DISTINCT FROM d.bus_id
                       OR NOT coalesce(ST_Within(d.geom, m.geom), false))
            ) AS n_bad_mv,
            count(*) FILTER (
                WHERE d.voltage_level = ANY(:ehv_levels)
                  AND (d.mv_grid_id IS NOT NULL
                       OR NOT coalesce(
                           ST_Within(
                               d.geom, ST_Transform(v.geom, ST_SRID(d.geom))
                           ),
                           false))
            ) AS n_bad_ehv,
            count(*) FILTER (
                WHERE NOT coalesce(
                    d.voltage_level = ANY(:mv_levels)
                    OR d.voltage_level = ANY(:ehv_levels),
                    false
                )
            ) AS n_unknown_level,
            count(*) FILTER (
                WHERE d.voltage_level = ANY(:ehv_levels)
            ) AS n_ehv,
            array_agg(d.depot_id) FILTER (
                WHERE b.bus_id IS NULL
                   OR d.geom IS NULL
                   OR (d.voltage_level = ANY(:mv_levels)
                       AND (d.mv_grid_id IS DISTINCT FROM d.bus_id
                            OR NOT coalesce(ST_Within(d.geom, m.geom),
                                            false)))
            ) AS bad_depots
        FROM {self.table} d
        LEFT JOIN (
            SELECT DISTINCT bus_id
            FROM {ETRAGO_BUS_TABLE}
            WHERE scn_name = :scenario
              AND carrier = 'AC'
              AND country = 'DE'
        ) b ON b.bus_id = d.bus_id
        LEFT JOIN {sources["mv_grid_district"]} m
            ON m.bus_id = d.mv_grid_id
        LEFT JOIN {sources["ehv_substation_voronoi"]} v
            ON v.bus_id = d.bus_id
        WHERE d.scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        counts = {
            "without geometry": int(row.n_no_geom),
            "without an AC bus in Germany of the scenario": int(
                row.n_no_ac_bus
            ),
            "MV level: wrong mv_grid_id or outside the district": int(
                row.n_bad_mv
            ),
            "EHV level: mv_grid_id set or outside the Voronoi cell": int(
                row.n_bad_ehv
            ),
            "with an unknown voltage level": int(row.n_unknown_level),
        }
        n_bad = sum(counts.values())
        message = (
            f"Scenario '{self.scenario}', {int(row.n_rows):,} depots "
            f"({int(row.n_ehv)} at EHV levels): "
            + ", ".join(f"{n} {label}" for label, n in counts.items())
        )
        if n_bad:
            message += f" (e.g. {_examples(row.bad_depots or [])})"
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class PublicBusAnnualEnergy(_PublicBusRule):
    """
    Check the total annual energy of a scenario against a measured value.

    The other rules confirm that the tables match the input files, but pass
    for any input. This rule flags a substantial change of the input itself
    (or depots with much energy being dropped). It fails with severity
    WARNING, because a new input version is a legitimate reason: re-measure
    and update the expected value then.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        expected_mwh,
        rtol: float = 0.05,
        **kwargs,
    ):
        """
        Parameters
        ----------
        expected_mwh : float or BoundaryDependent
            Measured total annual energy of the scenario in MWh.
        rtol : float
            Accepted relative deviation in both directions.
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
        SELECT count(*) AS n_rows, sum(annual_demand_mwh) AS total_mwh
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_rows) == 0:
            return self._no_rows()
        total = float(row.total_mwh)
        expected = float(self.params["expected_mwh"])
        rtol = float(self.params["rtol"])
        deviation = (total - expected) / expected
        success = abs(deviation) <= rtol
        return self._result(
            success=success,
            observed=total,
            expected=expected,
            severity=None if success else Severity.WARNING,
            message=(
                f"Scenario '{self.scenario}': {total / 1e6:.4f} TWh, "
                f"{deviation:+.1%} from the measured {expected / 1e6:.4f} TWh "
                f"({'within' if success else 'outside'} +/-{rtol:.0%}"
                f"{'' if success else '; has the input data changed?'})"
            ),
        )


class PublicBusInputConsistency(_PublicBusRule):
    """
    Check the depot table against the input files of the data bundle.

    For every depot of the scenario in the table, ``annual_demand_mwh`` and
    ``peak_load_mw`` must equal the sum and maximum of the depot's column
    in the scenario's input CSV, and the location must equal the one in
    ``depots.gpkg``. Only depots in the table are compared, so depots
    dropped by ``--dataset-boundary`` do not count. This catches a scaled
    or shifted series, a wrong column and an input file written under the
    wrong scenario, which leave the tables consistent among themselves.

    Missing values are skipped when summing, which equals the dataset's
    zero-filling.
    """

    def get_query(self, ctx):
        return f"""
        SELECT
            depot_id,
            annual_demand_mwh,
            peak_load_mw,
            ST_X(geom) AS x,
            ST_Y(geom) AS y,
            ST_SRID(geom) AS srid
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def evaluate_df(self, df, ctx):
        import geopandas as gpd

        from egon.data.datasets.emobility.public_bus_charging.scenarios import (  # noqa: E501
            SCENARIO_FILES,
        )

        input_dir = Path(_dataset().sources.files["bus_input_dir"])
        csv_path = input_dir / SCENARIO_FILES[self.scenario]
        gpkg_path = input_dir / "depots.gpkg"
        for path in (csv_path, gpkg_path):
            if not path.is_file():
                return self._result(
                    success=False,
                    message=f"Input file {path.resolve()} not found",
                )

        series = pd.read_csv(csv_path, index_col=0)
        series.columns = series.columns.astype(str)
        depots = gpd.read_file(gpkg_path)
        depots["depot_id"] = depots["depot_id"].astype(str)
        depots = depots.set_index("depot_id").to_crs(
            epsg=int(df["srid"].iloc[0])
        )

        db = df.set_index(df["depot_id"].astype(str))
        not_in_csv = db.index.difference(series.columns)
        not_in_gpkg = db.index.difference(depots.index)
        common = db.index.intersection(series.columns).intersection(
            depots.index
        )
        db = db.loc[common]
        csv = series[common]

        annual_diff = (db["annual_demand_mwh"] - csv.sum()).abs()
        bad_annual = annual_diff > RTOL_ROUNDING * csv.sum().abs()
        bad_peak = (db["peak_load_mw"] - csv.max()).abs() > ATOL_MW
        geoms = depots.loc[common].geometry
        shift = np.hypot(db["x"] - geoms.x, db["y"] - geoms.y)
        bad_geom = ~(shift <= MAX_GEOM_SHIFT_M)

        counts = {
            "not in the input CSV": len(not_in_csv),
            "not in depots.gpkg": len(not_in_gpkg),
            "annual demand differs": int(bad_annual.sum()),
            "peak load differs": int(bad_peak.sum()),
            f"moved by more than {MAX_GEOM_SHIFT_M:g} m": int(bad_geom.sum()),
        }
        n_bad = sum(counts.values())
        message = (
            f"Scenario '{self.scenario}', {len(df):,} depots compared with "
            f"{csv_path.name}: "
            + ", ".join(f"{n} {label}" for label, n in counts.items())
        )
        if n_bad:
            bad = not_in_csv.union(not_in_gpkg).union(
                common[(bad_annual | bad_peak | bad_geom).values]
            )
            message += f" (e.g. {_examples(bad)})"
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class PublicBusEtragoConsistency(_PublicBusRule):
    """
    Check the eTraGo loads of a scenario against the depot table.

    The loads of the dataset's carrier must cover exactly the buses of the
    depots, with one load per bus, one time series per load of the full
    length, ``sign = -1``, and each load's series must equal the hourly sum
    of its depots' series. Comparing every hour (not only annual sums)
    catches a time shift or a mix-up between buses in the aggregation.
    """

    def get_params(self, ctx):
        from egon.data.datasets.emobility.public_bus_charging.etrago_integration import (  # noqa: E501
            CARRIER,
            N_TIMESTEPS,
        )

        return {
            "scenario": self.scenario,
            "carrier": CARRIER,
            "n_timesteps": int(N_TIMESTEPS),
            "atol": ATOL_MW,
        }

    def get_query(self, ctx):
        targets = _dataset().targets.tables
        return f"""
        WITH loads AS (
            SELECT load_id, bus, sign
            FROM {self.table}
            WHERE scn_name = :scenario AND carrier = :carrier
        ),
        series AS (
            SELECT t.load_id, t.p_set
            FROM {targets["etrago_load_timeseries"]} t
            JOIN loads USING (load_id)
            WHERE t.scn_name = :scenario
        ),
        depots AS (
            SELECT bus_id, p_set
            FROM {targets["charging_depot"]}
            WHERE scenario = :scenario AND bus_id IS NOT NULL
        ),
        depot_hourly AS (
            SELECT d.bus_id, u.i, sum(u.v) AS v
            FROM depots d,
                 unnest(d.p_set) WITH ORDINALITY AS u(v, i)
            GROUP BY d.bus_id, u.i
        ),
        load_hourly AS (
            SELECT l.bus AS bus_id, u.i, u.v
            FROM loads l
            JOIN series s USING (load_id),
                 unnest(s.p_set) WITH ORDINALITY AS u(v, i)
        ),
        hourly AS (
            SELECT
                count(*) FILTER (
                    WHERE NOT coalesce(abs(d.v - l.v) <= :atol, false)
                ) AS n_hours_off,
                array_agg(DISTINCT coalesce(d.bus_id, l.bus_id)) FILTER (
                    WHERE NOT coalesce(abs(d.v - l.v) <= :atol, false)
                ) AS buses_off,
                max(abs(d.v - l.v)) AS max_abs_diff
            FROM depot_hourly d
            FULL JOIN load_hourly l USING (bus_id, i)
        )
        SELECT
            (SELECT count(*) FROM loads) AS n_loads,
            (SELECT count(DISTINCT bus_id) FROM depots) AS n_depot_buses,
            (SELECT count(*) - count(DISTINCT bus) FROM loads)
                AS n_extra_loads_per_bus,
            (SELECT count(*) FROM loads WHERE sign IS DISTINCT FROM -1)
                AS n_bad_sign,
            (SELECT count(*) FROM loads l
             WHERE (SELECT count(*) FROM series s
                    WHERE s.load_id = l.load_id) <> 1)
                AS n_bad_series_rows,
            (SELECT count(*) FROM series
             WHERE cardinality(p_set) IS DISTINCT FROM :n_timesteps)
                AS n_bad_length,
            (SELECT count(*) FROM (
                SELECT bus_id FROM depots EXCEPT SELECT bus FROM loads
             ) x) AS n_bus_without_load,
            (SELECT count(*) FROM (
                SELECT bus FROM loads EXCEPT SELECT bus_id FROM depots
             ) x) AS n_load_without_depot,
            hourly.n_hours_off,
            hourly.buses_off,
            hourly.max_abs_diff
        FROM hourly
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row.n_depot_buses) == 0:
            return self._no_rows()
        counts = {
            "buses with depots but no load": int(row.n_bus_without_load),
            "loads without depots": int(row.n_load_without_depot),
            "extra loads on a bus": int(row.n_extra_loads_per_bus),
            "loads without exactly one series": int(row.n_bad_series_rows),
            "series of wrong length": int(row.n_bad_length),
            "loads with sign != -1": int(row.n_bad_sign),
            "hours off the depot sum": int(row.n_hours_off),
        }
        n_bad = sum(counts.values())
        max_diff = row.max_abs_diff
        message = (
            f"Scenario '{self.scenario}', {int(row.n_loads):,} loads for "
            f"{int(row.n_depot_buses):,} depot buses: "
            + ", ".join(f"{n} {label}" for label, n in counts.items())
            + (
                f" (max hourly deviation {float(max_diff):.2e} MW)"
                if max_diff is not None and not pd.isna(max_diff)
                else ""
            )
        )
        if int(row.n_hours_off):
            message += f"; buses off: {_examples(row.buses_off or [])}"
        return self._result(
            success=n_bad == 0, observed=n_bad, expected=0, message=message
        )


class PublicBusScenarioCoverage(_PublicBusRule):
    """
    Check that no scenario without bus data carries public bus loads.

    Only the scenarios of the dataset's ``SCENARIO_FILES`` have bus data;
    ``eGon2035`` stays unchanged, ``eGon100RE`` is discarded and no lowflex
    variant is written. Lowflex scenarios overlay their base scenario, so
    bus loads there would count the demand twice. Checked for all scenarios
    in the table, whatever ``--scenarios`` is.
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        super().__init__(table=table, rule_id=rule_id, **kwargs)

    def get_params(self, ctx):
        from egon.data.datasets.emobility.public_bus_charging.etrago_integration import (  # noqa: E501
            CARRIER,
        )
        from egon.data.datasets.emobility.public_bus_charging.scenarios import (  # noqa: E501
            SCENARIO_FILES,
        )

        return {"carrier": CARRIER, "scenarios": list(SCENARIO_FILES)}

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_loads,
            array_agg(DISTINCT scn_name) AS scenarios
        FROM {self.table}
        WHERE carrier = :carrier
          AND scn_name <> ALL(:scenarios)
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_loads = int(row.n_loads)
        allowed = ", ".join(self.get_params(ctx)["scenarios"])
        if n_loads:
            return self._result(
                success=False,
                observed=n_loads,
                expected=0,
                message=(
                    f"{n_loads} public bus loads in scenarios without bus "
                    f"data: {_examples(sorted(row.scenarios), n=10)} "
                    f"(bus data exists for {allowed})"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=f"Public bus loads only in {allowed}",
        )
