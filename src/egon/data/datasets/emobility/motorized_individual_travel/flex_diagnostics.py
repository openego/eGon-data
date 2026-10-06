"""
Flexibility diagnostics of the new eMobility MIT/LGV methodology.

The eTraGo model shape does not record which reference point the
`land_transport_EV` load is measured against: it carries the *grid-side
charging energy* in dumb and lowflex scenarios but the *battery-side
driving energy* in flexible ones. The three exports of this module write
what the pipeline already computes but used to discard, so that
cross-scenario and flex/lowflex comparisons are well defined without
knowing which model shape a scenario got:

* **A** -- :class:`EgonEvMitLgvFlexTimeseries`: the grid-side dumb
  charging load, its flexible share and the battery-side driving load
  per MV grid district and hour,
* **B** -- :class:`EgonEvMitLgvEnergyBalance`: an annual energy balance
  per grid district, charging use case and vehicle type,
* **D** -- :class:`EgonEvMitLgvChargingProfileUseCase`: the grid-side
  dumb charging load per grid district, hour *and* use case.

Reference points
----------------
Over a year::

    Sum(driving_load) = eta_cp * Sum(charging_load_grid)

is an identity, not an inconsistency. It holds up to two terms:

1. `driving_load` is filled only where the state of charge actually
   decreases, so PHEV kilometres driven on fuel never enter it;
2. the annual battery state-of-charge drift of the fleet -- the store is
   not cyclic and the delivered events carry the same drift (0.11 % on
   delivery v1.4).

`charging_load_grid_flex` (export A) and `flexible` (export B) are a
**potential, not a realised flexibility**: they are populated for dumb
charging scenarios as well, which get no bus, link or store at all. Read
as "share of charging that occurs at a use case eligible for flexible
charging" the values are meaningful and comparable across scenarios;
read as "flexibility the model used" they are wrong for dumb charging
scenarios and incomplete for the others, where realised usage is the
difference against the eTraGo result.

These tables are written for the new methodology only: the legacy
use-case taxonomy (`public`/`home`/`work`/empty) would make exports B
and D incomparable to the new scenarios.
"""

import json

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY, REAL
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    CHARGING_USE_CASES,
    FLEX_USE_CASES,
    is_legacy_scenario,
)
from egon.data.datasets.mv_grid_districts import MvGridDistricts

Base = declarative_base()

#: Prefix of the per-use-case charging load columns carried through
#: `generate_load_time_series()` and the hourly resample.
USE_CASE_COLUMN_PREFIX = "charging_load_grid_uc_"


def use_case_column(use_case: str) -> str:
    """Column name carrying the charging load of one use case."""
    return f"{USE_CASE_COLUMN_PREFIX}{use_case}"


#: The eight per-use-case load columns, in a fixed order.
USE_CASE_COLUMNS = [use_case_column(_) for _ in CHARGING_USE_CASES]


class EgonEvMitLgvFlexTimeseries(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_flex_timeseries.

    Export A: one row per MV grid district and scenario.

    **Columns**

    charging_load_grid:
        Grid-side charging load of the dumb charging schedule, hourly
        for one year, in MW.
    charging_load_grid_flex:
        The share of `charging_load_grid` that occurs at a use case
        eligible for flexible charging, in MW. This is a potential, not
        a realised flexibility. The inflexible share is the difference
        of the two and is not stored.
    driving_load:
        Battery-side driving load, hourly for one year, in MWh/h.

    Notes
    -----
    The state-of-charge band and the plugged-in capacity are not stored:
    they are recoverable from `grid.egon_etrago_store_timeseries`
    (`e_min_pu`/`e_max_pu` times `e_nom`) and
    `grid.egon_etrago_link_timeseries` (`p_max_pu` times `p_nom`).

    `charging_load_grid` and `charging_load_grid_flex` are aggregates of
    :class:`EgonEvMitLgvChargingProfileUseCase` and are stored
    redundantly on purpose: the flexible share here is defined by the
    *model's* flex mask, i.e. the same mask the state-of-charge bands
    were built from. A consumer re-deriving it by picking use cases
    could diverge from what the model actually treated as flexible; the
    equality of the two is therefore a free consistency check.
    """

    __tablename__ = "egon_ev_mit_lgv_flex_timeseries"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    charging_load_grid = Column(ARRAY(REAL))
    charging_load_grid_flex = Column(ARRAY(REAL))
    driving_load = Column(ARRAY(REAL))


class EgonEvMitLgvEnergyBalance(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_energy_balance.

    Export B: annual energy balance per MV grid district, charging use
    case and vehicle type. `use_case` is the empty string for driving
    events and for parking events without charging.

    Aggregable to municipality via
    `demand.egon_ev_mit_lgv_mv_grid_district.ags`, to federal state or
    to Germany.

    **Columns**

    flexible:
        Whether the use case is eligible for flexible charging. This is
        eligibility, not realised usage.
    charging_demand_battery_mwh:
        Charging energy at the battery side.
    charging_demand_grid_mwh:
        Charging energy at the grid side, i.e. the battery side divided
        by `eta_cp` of the scenario.
    driving_consumption_mwh:
        Raw energy consumption of the driving events. Not
        interchangeable with the modelled driving load of
        :class:`EgonEvMitLgvFlexTimeseries`, which excludes PHEV
        kilometres driven on fuel.
    """

    __tablename__ = "egon_ev_mit_lgv_energy_balance"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    use_case = Column(String(20), primary_key=True)
    type = Column(String(24), primary_key=True)
    flexible = Column(Boolean)
    charging_demand_battery_mwh = Column(Float)
    charging_demand_grid_mwh = Column(Float)
    driving_consumption_mwh = Column(Float)
    n_events = Column(BigInteger)
    n_charging_events = Column(BigInteger)


class EgonEvMitLgvChargingProfileUseCase(Base):
    """
    Class definition of table
    demand.egon_ev_mit_lgv_charging_profile_use_case.

    Export D: the grid-side charging load of the dumb charging schedule
    per MV grid district, charging use case and hour, in MW.

    Only use cases that actually occur in a grid district get a row --
    `highway_fast` exists in a minority of grid districts -- so the
    theoretical worst case of eight rows per grid district is not
    reached in practice.
    """

    __tablename__ = "egon_ev_mit_lgv_charging_profile_use_case"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    use_case = Column(String(20), primary_key=True)
    charging_load_grid = Column(ARRAY(REAL))


def create_tables() -> None:
    """(Re-)create the three flexibility diagnostics tables."""
    engine = db.engine()
    for table in (
        EgonEvMitLgvFlexTimeseries,
        EgonEvMitLgvEnergyBalance,
        EgonEvMitLgvChargingProfileUseCase,
    ):
        table.__table__.drop(bind=engine, checkfirst=True)
        table.__table__.create(bind=engine, checkfirst=True)


def eta_cp(scenario_name: str) -> float:
    """Charging point efficiency of a scenario, read from the database.

    Read from `demand.egon_ev_mit_lgv_metadata` rather than assumed:
    delivery v1.2 shipped `eta_cp = 1`, v1.4 is back to 0.9, and the
    grid-side/battery-side distinction of the exports hinges on it.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    float
        `eta_cp` of the scenario's simBEV run
    """
    value = db.select_dataframe(
        f"""
        SELECT (simbev_config -> 'config' -> 'basic' ->> 'eta_cp')::float
                   AS eta_cp
        FROM demand.egon_ev_mit_lgv_metadata
        WHERE scenario = '{scenario_name}'
        """
    )
    if value.empty or value.eta_cp.isna().all():
        raise ValueError(
            f"No eta_cp recorded for scenario '{scenario_name}' in "
            f"demand.egon_ev_mit_lgv_metadata. Run `write-metadata-to-db` "
            f"first."
        )
    return float(value.eta_cp.iloc[0])


def write_flex_timeseries(
    bus_id: int, scenario_name: str, hourly_load_time_series_df
) -> None:
    """Write export A for one MV grid district.

    Must be called from `write_model_data_to_db()` itself, never from
    its nested `write_to_db()`: the latter runs twice for flexible
    scenarios (once for the flex model, once for lowflex), which would
    duplicate every row.

    Parameters
    ----------
    bus_id : int
        ID of the MV grid district
    scenario_name : str
        Scenario name
    hourly_load_time_series_df : pd.DataFrame
        Hourly model timeseries of the grid district
    """
    with db.session_scope() as session:
        # Delete first so a task retry does not hit the primary key.
        session.query(EgonEvMitLgvFlexTimeseries).filter(
            EgonEvMitLgvFlexTimeseries.scenario == scenario_name,
            EgonEvMitLgvFlexTimeseries.bus_id == int(bus_id),
        ).delete(synchronize_session=False)
        session.add(
            EgonEvMitLgvFlexTimeseries(
                scenario=scenario_name,
                bus_id=int(bus_id),
                charging_load_grid=(
                    hourly_load_time_series_df.load_time_series.astype(
                        float
                    ).to_list()
                ),
                charging_load_grid_flex=(
                    hourly_load_time_series_df.flex_time_series.astype(
                        float
                    ).to_list()
                ),
                driving_load=(
                    hourly_load_time_series_df.driving_load_time_series.astype(
                        float
                    ).to_list()
                ),
            )
        )


def write_charging_profile_use_case(
    bus_id: int, scenario_name: str, hourly_load_time_series_df
) -> None:
    """Write export D for one MV grid district.

    Use cases that do not occur in the grid district are skipped, cf.
    :class:`EgonEvMitLgvChargingProfileUseCase`.

    Parameters
    ----------
    bus_id : int
        ID of the MV grid district
    scenario_name : str
        Scenario name
    hourly_load_time_series_df : pd.DataFrame
        Hourly model timeseries of the grid district
    """
    rows = []
    for use_case in CHARGING_USE_CASES:
        column = use_case_column(use_case)
        if column not in hourly_load_time_series_df.columns:
            # Missing rather than empty means the column was dropped
            # somewhere between the event loop and the hourly resample,
            # which would silently empty this export.
            raise KeyError(
                f"Column '{column}' missing from the hourly model "
                f"timeseries of grid district {bus_id}. It has to be "
                f"part of the `.agg()` dict of the hourly resample."
            )
        profile = hourly_load_time_series_df[column].astype(float)
        if not (profile > 0).any():
            continue
        rows.append(
            EgonEvMitLgvChargingProfileUseCase(
                scenario=scenario_name,
                bus_id=int(bus_id),
                use_case=use_case,
                charging_load_grid=profile.to_list(),
            )
        )

    with db.session_scope() as session:
        # Delete first so a task retry does not hit the primary key.
        session.query(EgonEvMitLgvChargingProfileUseCase).filter(
            EgonEvMitLgvChargingProfileUseCase.scenario == scenario_name,
            EgonEvMitLgvChargingProfileUseCase.bus_id == int(bus_id),
        ).delete(synchronize_session=False)
        if rows:
            session.add_all(rows)


def write_energy_balance() -> None:
    """Write export B for all configured new-methodology scenarios.

    Runs once per scenario, not once per grid district, and depends only
    on the imported tables plus the allocation -- not on the timeseries
    generation.

    Notes
    -----
    The event table (~72 million rows) must not be joined against the
    vehicle instance table (up to ~40 million rows) directly; the
    product would be ~1e11 rows. Both sides are aggregated first, cf.
    the CTEs below.
    """
    scenarios = [
        _
        for _ in config.settings()["egon-data"]["--scenarios"]
        if not is_legacy_scenario(_)
    ]
    if not scenarios:
        print(
            "No scenario on the new methodology configured, skipping "
            "the energy balance."
        )
        return

    flex_array = ", ".join(f"'{_}'" for _ in FLEX_USE_CASES)

    for scenario_name in scenarios:
        print(f"SCENARIO: {scenario_name}")
        scenario_eta_cp = eta_cp(scenario_name)
        print(f"  Writing energy balance (eta_cp = {scenario_eta_cp})...")

        db.execute_sql(
            f"""
            DELETE FROM demand.egon_ev_mit_lgv_energy_balance
            WHERE scenario = '{scenario_name}';

            INSERT INTO demand.egon_ev_mit_lgv_energy_balance (
                scenario, bus_id, use_case, type, flexible,
                charging_demand_battery_mwh, charging_demand_grid_mwh,
                driving_consumption_mwh, n_events, n_charging_events)
            WITH ev_sums AS (
                SELECT
                    ev_id,
                    COALESCE(use_case, '') AS use_case,
                    COALESCE(
                        SUM(charging_demand::double precision), 0
                    ) AS charging_demand,
                    COALESCE(
                        SUM(consumption::double precision), 0
                    ) AS consumption,
                    COUNT(*) AS n_events,
                    COUNT(*) FILTER (
                        WHERE charging_demand > 0
                    ) AS n_charging_events
                FROM demand.egon_ev_mit_lgv_trip
                WHERE scenario = '{scenario_name}'
                GROUP BY ev_id, COALESCE(use_case, '')
            ),
            instances AS (
                SELECT bus_id, ev_id, COUNT(*) AS n
                FROM demand.egon_ev_mit_lgv_mv_grid_district
                WHERE scenario = '{scenario_name}'
                GROUP BY bus_id, ev_id
            )
            SELECT
                '{scenario_name}',
                i.bus_id,
                s.use_case,
                p.type,
                s.use_case IN ({flex_array}) AS flexible,
                SUM(s.charging_demand * i.n) / 1000.0,
                SUM(s.charging_demand * i.n) / 1000.0
                    / {scenario_eta_cp},
                SUM(s.consumption * i.n) / 1000.0,
                SUM(s.n_events * i.n),
                SUM(s.n_charging_events * i.n)
            FROM ev_sums s
            JOIN instances i ON i.ev_id = s.ev_id
            JOIN demand.egon_ev_mit_lgv_pool p
                ON p.ev_id = s.ev_id AND p.scenario = '{scenario_name}'
            GROUP BY i.bus_id, s.use_case, p.type;
            """
        )

        summary = db.select_dataframe(
            f"""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT bus_id) AS grid_districts,
                SUM(charging_demand_grid_mwh) AS charging_grid_mwh,
                SUM(charging_demand_grid_mwh) FILTER (
                    WHERE flexible
                ) AS charging_grid_flex_mwh,
                SUM(driving_consumption_mwh) AS driving_mwh
            FROM demand.egon_ev_mit_lgv_energy_balance
            WHERE scenario = '{scenario_name}'
            """
        ).iloc[0]

        flex_share = (
            summary.charging_grid_flex_mwh / summary.charging_grid_mwh
            if summary.charging_grid_mwh
            else float("nan")
        )
        print(
            f"  {int(summary.rows)} rows in "
            f"{int(summary.grid_districts)} grid districts; "
            f"grid-side charging "
            f"{summary.charging_grid_mwh / 1e6:.3f} TWh "
            f"(flexible share {flex_share:.1%}), "
            f"driving consumption "
            f"{summary.driving_mwh / 1e6:.3f} TWh."
        )


def log_reference_point_check(tolerance: float = 0.01) -> None:
    """Log the reference point identity of export A per grid district.

    Compares `Sum(driving_load)` against
    `eta_cp * Sum(charging_load_grid)`. The identity is exact only up to
    the PHEV fuel share and the annual battery state-of-charge drift, so
    the residual is logged together with both terms rather than as a
    bare pass/fail -- with either term omitted from the tolerance the
    check reports a deviation for essentially every grid district.

    Parameters
    ----------
    tolerance : float
        Relative residual above which a grid district is listed
    """
    scenarios = [
        _
        for _ in config.settings()["egon-data"]["--scenarios"]
        if not is_legacy_scenario(_)
    ]

    for scenario_name in scenarios:
        try:
            scenario_eta_cp = eta_cp(scenario_name)
        except ValueError as e:
            print(f"  {e}")
            continue

        residuals = db.select_dataframe(
            f"""
            SELECT
                bus_id,
                driving_sum,
                {scenario_eta_cp} * charging_sum AS reference,
                CASE WHEN charging_sum > 0
                    THEN driving_sum
                         / ({scenario_eta_cp} * charging_sum) - 1
                    ELSE NULL
                END AS residual
            FROM (
                SELECT
                    bus_id,
                    (SELECT SUM(v) FROM UNNEST(driving_load) v)
                        AS driving_sum,
                    (SELECT SUM(v) FROM UNNEST(charging_load_grid) v)
                        AS charging_sum
                FROM demand.egon_ev_mit_lgv_flex_timeseries
                WHERE scenario = '{scenario_name}'
            ) sums
            """
        )
        if residuals.empty:
            print(
                f"SCENARIO {scenario_name}: no flex timeseries written, "
                f"skipping the reference point check."
            )
            continue

        off = residuals[residuals.residual.abs() > tolerance]
        print(
            f"SCENARIO {scenario_name}: reference point check over "
            f"{len(residuals)} grid districts. "
            f"Sum(driving_load) = {residuals.driving_sum.sum():.1f} MWh, "
            f"eta_cp * Sum(charging_load_grid) = "
            f"{residuals.reference.sum():.1f} MWh, "
            f"median residual {residuals.residual.median():.4%}, "
            f"{len(off)} grid districts above {tolerance:.1%}."
        )
        if len(off) > 0:
            print(f"  Grid districts above tolerance: {off.bus_id.to_list()}")


def log_use_case_consistency_check(tolerance: float = 1e-3) -> None:
    """Log the A/D consistency check.

    The sum of export D over all use cases must equal
    `charging_load_grid` of export A, and the sum over the flexible use
    cases must equal `charging_load_grid_flex`.

    Parameters
    ----------
    tolerance : float
        Relative deviation above which a grid district is listed
    """
    scenarios = [
        _
        for _ in config.settings()["egon-data"]["--scenarios"]
        if not is_legacy_scenario(_)
    ]
    flex_array = ", ".join(f"'{_}'" for _ in FLEX_USE_CASES)

    for scenario_name in scenarios:
        deviations = db.select_dataframe(
            f"""
            WITH per_use_case AS (
                SELECT
                    bus_id,
                    SUM(
                        (SELECT SUM(v) FROM UNNEST(charging_load_grid) v)
                    ) AS total,
                    SUM(
                        (SELECT SUM(v) FROM UNNEST(charging_load_grid) v)
                    ) FILTER (
                        WHERE use_case IN ({flex_array})
                    ) AS flex
                FROM demand.egon_ev_mit_lgv_charging_profile_use_case
                WHERE scenario = '{scenario_name}'
                GROUP BY bus_id
            ),
            aggregated AS (
                SELECT
                    bus_id,
                    (SELECT SUM(v) FROM UNNEST(charging_load_grid) v)
                        AS total,
                    (SELECT SUM(v)
                     FROM UNNEST(charging_load_grid_flex) v) AS flex
                FROM demand.egon_ev_mit_lgv_flex_timeseries
                WHERE scenario = '{scenario_name}'
            )
            SELECT
                a.bus_id,
                a.total AS total_a,
                COALESCE(d.total, 0) AS total_d,
                a.flex AS flex_a,
                COALESCE(d.flex, 0) AS flex_d
            FROM aggregated a
            LEFT JOIN per_use_case d ON d.bus_id = a.bus_id
            """
        )
        if deviations.empty:
            print(
                f"SCENARIO {scenario_name}: no diagnostics written, "
                f"skipping the A/D consistency check."
            )
            continue

        def _relative(left, right):
            return (left - right).abs() / right.where(right != 0, 1.0)

        deviations["total_dev"] = _relative(
            deviations.total_d, deviations.total_a
        )
        deviations["flex_dev"] = _relative(
            deviations.flex_d, deviations.flex_a
        )
        off = deviations[
            (deviations.total_dev > tolerance)
            | (deviations.flex_dev > tolerance)
        ]
        print(
            f"SCENARIO {scenario_name}: A/D consistency over "
            f"{len(deviations)} grid districts, "
            f"{len(off)} above {tolerance:.2%}."
        )
        if len(off) > 0:
            print(
                "  "
                + json.dumps(
                    off[["bus_id", "total_dev", "flex_dev"]]
                    .head(20)
                    .round(6)
                    .to_dict("records")
                )
            )


def log_diagnostics_checks() -> None:
    """Run the logged consistency checks of the diagnostic exports."""
    log_reference_point_check()
    log_use_case_consistency_check()
