"""
Flexibility diagnostics of the new eMobility MIT/LGV methodology.

The `land_transport_EV` load of the eTraGo model carries the *grid-side
charging energy* in dumb and lowflex scenarios but the *battery-side
driving energy* in flexible ones. The three exports of this module write
both sides, per model and hour, so that cross-scenario and flex/lowflex
comparisons are well defined without knowing which model shape a scenario
got:

* **A** -- :class:`EgonEvMitLgvFlexTimeseries`: the grid-side dumb
  charging load, its flexible share and the battery-side driving load,
* **B** -- :class:`EgonEvMitLgvEnergyBalance`: the annual energy per
  charging use case and vehicle type,
* **D** -- :class:`EgonEvMitLgvChargingProfileUseCase`: the grid-side dumb
  charging load per charging use case.

All three are keyed by the AC bus of the model and the model type
(`district` or `transmission`); the transmission models on one bus are
aggregated. They are written by
:func:`.placement_model.generate_placement_models` from the same placements
the eTraGo model is built from.

Reference points
----------------
Over a year::

    Sum(driving_load) = eta_cp * Sum(charging_load_grid)

holds exactly, up to the energy that dumb charging past the year end
loses to the crop: the driving load *is* the deficit of the placements,
i.e. their battery-side charging demand (ADR-0001).

`charging_load_grid_flex` (export A) and `flexible` (export B) are a
**potential, not a realised flexibility**: they are populated for dumb
charging scenarios as well, which get no bus, link or store at all.

These tables are written for the new methodology only.
"""

import json

from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY, REAL
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    CHARGING_USE_CASES,
    FLEX_USE_CASES,
    is_legacy_scenario,
)

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

    Export A: one row per scenario, AC bus and model type.

    **Columns**

    bus_id:
        AC bus of the model: an MV grid district's bus, or an EHV
        substation's bus for transmission models at voltage levels 1-2.
    model_type:
        `district` or `transmission`; the transmission models on one bus
        are aggregated.

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
    bus_id = Column(Integer, primary_key=True)
    model_type = Column(String(12), primary_key=True)
    charging_load_grid = Column(ARRAY(REAL))
    charging_load_grid_flex = Column(ARRAY(REAL))
    driving_load = Column(ARRAY(REAL))


class EgonEvMitLgvEnergyBalance(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_energy_balance.

    Export B: annual energy balance per AC bus, model type, charging use
    case and vehicle type, from the placements of the models.

    **Columns**

    flexible:
        Whether the use case is eligible for flexible charging. This is
        eligibility, not realised usage.
    charging_demand_battery_mwh:
        Charging energy at the battery side. This is also the deficit
        energy, i.e. the driving load of the model.
    charging_demand_grid_mwh:
        Charging energy at the grid side, i.e. the battery side divided
        by `eta_cp` of the scenario.
    n_placements:
        Number of placements.
    """

    __tablename__ = "egon_ev_mit_lgv_energy_balance"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, primary_key=True)
    model_type = Column(String(12), primary_key=True)
    use_case = Column(String(20), primary_key=True)
    type = Column(String(24), primary_key=True)
    flexible = Column(Boolean)
    charging_demand_battery_mwh = Column(Float)
    charging_demand_grid_mwh = Column(Float)
    n_placements = Column(BigInteger)


class EgonEvMitLgvChargingProfileUseCase(Base):
    """
    Class definition of table
    demand.egon_ev_mit_lgv_charging_profile_use_case.

    Export D: the grid-side charging load of the dumb charging schedule
    per AC bus, model type, charging use case and hour, in MW.

    Only use cases that actually occur in a model get a row.
    """

    __tablename__ = "egon_ev_mit_lgv_charging_profile_use_case"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, primary_key=True)
    model_type = Column(String(12), primary_key=True)
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


def _delete(orm_class, scenario_name, bus_id, model_type, session) -> None:
    """Delete one model's rows first, so a task retry does not hit the
    primary key."""
    session.query(orm_class).filter(
        orm_class.scenario == scenario_name,
        orm_class.bus_id == int(bus_id),
        orm_class.model_type == model_type,
    ).delete(synchronize_session=False)


def write_flex_timeseries(
    scenario_name: str, bus_id: int, model_type: str, hourly: dict
) -> None:
    """Write export A for the models of one type on one AC bus.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    bus_id : int
        AC bus of the models
    model_type : str
        `district` or `transmission`
    hourly : dict
        Hourly series `dumb`, `dumb_flex` and `driving`, cf.
        :func:`.placement_model.hourly_model`
    """
    with db.session_scope() as session:
        _delete(
            EgonEvMitLgvFlexTimeseries,
            scenario_name,
            bus_id,
            model_type,
            session,
        )
        session.add(
            EgonEvMitLgvFlexTimeseries(
                scenario=scenario_name,
                bus_id=int(bus_id),
                model_type=model_type,
                charging_load_grid=[float(_) for _ in hourly["dumb"]],
                charging_load_grid_flex=[
                    float(_) for _ in hourly["dumb_flex"]
                ],
                driving_load=[float(_) for _ in hourly["driving"]],
            )
        )


def write_charging_profile_use_case(
    scenario_name: str, bus_id: int, model_type: str, hourly: dict
) -> None:
    """Write export D for the models of one type on one AC bus.

    Use cases that do not occur are skipped.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    bus_id : int
        AC bus of the models
    model_type : str
        `district` or `transmission`
    hourly : dict
        Hourly series `dumb_<use case>`, cf.
        :func:`.placement_model.hourly_model`
    """
    rows = [
        EgonEvMitLgvChargingProfileUseCase(
            scenario=scenario_name,
            bus_id=int(bus_id),
            model_type=model_type,
            use_case=use_case,
            charging_load_grid=[float(_) for _ in hourly[f"dumb_{use_case}"]],
        )
        for use_case in CHARGING_USE_CASES
        if (hourly[f"dumb_{use_case}"] > 0).any()
    ]
    with db.session_scope() as session:
        _delete(
            EgonEvMitLgvChargingProfileUseCase,
            scenario_name,
            bus_id,
            model_type,
            session,
        )
        session.add_all(rows)


def write_energy_balance_rows(
    scenario_name: str, bus_id: int, model_type: str, balance
) -> None:
    """Write export B for the models of one type on one AC bus.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    bus_id : int
        AC bus of the models
    model_type : str
        `district` or `transmission`
    balance : pandas.DataFrame
        Columns `use_case`, `type`, `flexible`, `battery` and `grid`
        (MWh) and `n_placements`
    """
    rows = [
        EgonEvMitLgvEnergyBalance(
            scenario=scenario_name,
            bus_id=int(bus_id),
            model_type=model_type,
            use_case=row.use_case,
            type=row.type,
            flexible=bool(row.flexible),
            charging_demand_battery_mwh=float(row.battery),
            charging_demand_grid_mwh=float(row.grid),
            n_placements=int(row.n_placements),
        )
        for row in balance.itertuples()
    ]
    with db.session_scope() as session:
        _delete(
            EgonEvMitLgvEnergyBalance,
            scenario_name,
            bus_id,
            model_type,
            session,
        )
        session.add_all(rows)


def log_reference_point_check(tolerance: float = 1e-4) -> None:
    """Log the reference point identity of export A per model.

    Compares `Sum(driving_load)` against `eta_cp * Sum(charging_load_grid)`.
    It holds exactly up to dumb charging cropped at the year end, which
    the tolerance covers.

    Parameters
    ----------
    tolerance : float
        Relative residual above which a model is listed
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
                model_type,
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
                    model_type,
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
            f"{len(residuals)} models. "
            f"Sum(driving_load) = {residuals.driving_sum.sum():.1f} MWh, "
            f"eta_cp * Sum(charging_load_grid) = "
            f"{residuals.reference.sum():.1f} MWh, "
            f"median residual {residuals.residual.median():.4%}, "
            f"{len(off)} models above {tolerance:.2%}."
        )
        if len(off) > 0:
            print(
                "  Models above tolerance: "
                f"{off[['bus_id', 'model_type']].to_dict('records')}"
            )


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
                    model_type,
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
                GROUP BY bus_id, model_type
            ),
            aggregated AS (
                SELECT
                    bus_id,
                    model_type,
                    (SELECT SUM(v) FROM UNNEST(charging_load_grid) v)
                        AS total,
                    (SELECT SUM(v)
                     FROM UNNEST(charging_load_grid_flex) v) AS flex
                FROM demand.egon_ev_mit_lgv_flex_timeseries
                WHERE scenario = '{scenario_name}'
            )
            SELECT
                a.bus_id,
                a.model_type,
                a.total AS total_a,
                COALESCE(d.total, 0) AS total_d,
                a.flex AS flex_a,
                COALESCE(d.flex, 0) AS flex_d
            FROM aggregated a
            LEFT JOIN per_use_case d
                ON d.bus_id = a.bus_id AND d.model_type = a.model_type
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
            f"{len(deviations)} models, "
            f"{len(off)} above {tolerance:.2%}."
        )
        if len(off) > 0:
            print(
                "  "
                + json.dumps(
                    off[["bus_id", "model_type", "total_dev", "flex_dev"]]
                    .head(20)
                    .round(6)
                    .to_dict("records")
                )
            )


def log_diagnostics_checks() -> None:
    """Run the logged consistency checks of the diagnostic exports."""
    log_reference_point_check()
    log_use_case_consistency_check()
