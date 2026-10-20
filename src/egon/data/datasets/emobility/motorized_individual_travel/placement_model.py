"""
eTraGo model of the new methodology, built from placements.

The unit of the model is the **placement**: one vehicle's realisation of one
charging event at one charging location (ADR-0001). A placement arrives with
its **deficit** -- its battery-side charging demand -- which is taken from
the store as driving load at `park_start`, and is back to full by
`park_end`. Nothing of the vehicle is carried between placements, so a
model only needs the placements of its own charging locations.

Models
------
* one **district model** per MV grid district: all its charging locations
  at voltage levels 4-7;
* one **transmission model** per grid connection at voltage levels 1-3,
  with carriers of its own (ADR-0003).

Per placement with deficit `d` (kWh), battery capacity `b`, half-open
parking window `[s, e)` and grid-side nominal power `P`:

=================================  ==========================================
driving load                       `d` in timestep `s`
outstanding deficit, upper bound   flexible: `d` on `[s, e)`
outstanding deficit, lower bound   flexible: `0` on `[s, e)`
both bounds, inflexible            `d * (1 - linspace(0, 1, e - s))`: a fixed
                                   linear ramp over the parking window, as in
                                   the previous model (ADR-0002)
plugged-in battery                 `b` on `[s, e)`
plugged-in power                   `P` on `[s, e)`
dumb charging load (grid side)     `P` from `s` until `d / eta_cp` is drawn
=================================  ==========================================

and `e_nom` = peak plugged-in battery capacity, `e_min_pu` / `e_max_pu` =
`(e_nom - deficit bound) / e_nom`, `e_initial = e_nom` (everything is full
before the year), `p_nom` = peak plugged-in power. Flexible use cases are
those of :data:`.helpers.FLEX_USE_CASES` (D10).

Scenarios without flexible charging (status2024) get the dumb charging load
on the AC bus only; flexible ones additionally a lowflex counterpart with
the dumb charging load, cf.
:func:`.model_timeseries.is_flexible`.

The legacy methodology (eGon2035) is built by :mod:`.model_timeseries` and
is not touched by this module.
"""

import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvMitLgvChargingLocation,
    EgonEvMitLgvLocationEvents,
    EgonEvMitLgvPool,
    EgonEvMitLgvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics import (  # noqa: E501
    eta_cp,
    write_charging_profile_use_case,
    write_energy_balance_rows,
    write_flex_timeseries,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    BUS_CARRIER,
    CHARGING_USE_CASES,
    FLEX_USE_CASES,
    LINK_CARRIERS,
    LOAD_CARRIERS,
    MODEL_DISTRICT,
    MODEL_TRANSMISSION,
    STORE_CARRIER,
    read_simbev_metadata_file,
)
from egon.data.datasets.emobility.motorized_individual_travel.model_timeseries import (  # noqa: E501
    LAST_TIMESTEP,
    is_flexible,
)
from egon.data.datasets.etrago_setup import (
    EgonPfHvBus,
    EgonPfHvLink,
    EgonPfHvLinkTimeseries,
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
    EgonPfHvStore,
    EgonPfHvStoreTimeseries,
)

#: Quarter-hours of the modelled year plus the closing endpoint, as in
#: the legacy model. Events reaching past the year are cropped to it.
N_STEPS = LAST_TIMESTEP + 1
#: Quarter-hours per hour and hours of the year written to eTraGo.
STEPS_PER_HOUR = 4
N_HOURS = 8760


def _qualified(orm_class) -> str:
    """Qualified table name of an ORM class."""
    return f"{orm_class.__table__.schema}.{orm_class.__table__.name}"


# ---------------------------------------------------------------------------
# Models and their placements
# ---------------------------------------------------------------------------


def model_list(scenario_name: str) -> pd.DataFrame:
    """All models of a scenario.

    Returns
    -------
    pandas.DataFrame
        Columns `model_type`, `model_key` (MV grid district `bus_id` for
        district models, `grid_connection_id` for transmission models) and
        `bus_id` (the AC bus of the model), sorted by `bus_id`
    """
    return db.select_dataframe(
        f"""
        SELECT DISTINCT
            model_type,
            CASE WHEN model_type = '{MODEL_DISTRICT}'
                THEN bus_id ELSE grid_connection_id
            END AS model_key,
            bus_id
        FROM {_qualified(EgonEvMitLgvChargingLocation)}
        WHERE scenario = '{scenario_name}'
        ORDER BY bus_id, model_type, model_key
        """
    )


def load_placements(
    scenario_name: str, model_type: str, model_key: int
) -> pd.DataFrame:
    """Charging events of one model, weighted by their placements.

    Returns
    -------
    pandas.DataFrame
        One row per charging event placed in the model, with `n` the
        number of its placements there, and the event columns the model
        needs plus the vehicle `type`
    """
    key_column = (
        "bus_id" if model_type == MODEL_DISTRICT else "grid_connection_id"
    )
    return db.select_dataframe(
        f"""
        WITH weights AS (
            SELECT e.event_id, COUNT(*) AS n
            FROM {_qualified(EgonEvMitLgvChargingLocation)} cl
            JOIN {_qualified(EgonEvMitLgvLocationEvents)} le
                USING (scenario, location_id)
            CROSS JOIN LATERAL UNNEST(le.event_ids) AS e(event_id)
            WHERE cl.scenario = '{scenario_name}'
                AND cl.model_type = '{model_type}'
                AND cl.{key_column} = {int(model_key)}
            GROUP BY e.event_id
        )
        SELECT
            w.event_id,
            w.n,
            t.use_case,
            t.park_start,
            t.park_end,
            t.charging_demand,
            t.charging_capacity_grid,
            p.type
        FROM weights w
        JOIN {_qualified(EgonEvMitLgvTrip)} t
            ON t.scenario = '{scenario_name}' AND t.event_id = w.event_id
        JOIN {_qualified(EgonEvMitLgvPool)} p
            ON p.scenario = '{scenario_name}' AND p.ev_id = t.ev_id
        """
    )


# ---------------------------------------------------------------------------
# Timeseries
# ---------------------------------------------------------------------------


def _add_constant(diff: np.ndarray, start, stop, value) -> None:
    """Add `value` on `[start, stop)` to the series behind a first
    difference array."""
    np.add.at(diff, start, value)
    np.add.at(diff, stop, -value)


def _from_diff(diff: np.ndarray) -> np.ndarray:
    """Series of length :data:`N_STEPS` from a first difference array."""
    return np.cumsum(diff)[:N_STEPS]


def build_timeseries(
    placements: pd.DataFrame, battery_capacity: dict, eta: float
) -> dict:
    """Quarter-hourly series of one model.

    Parameters
    ----------
    placements : pandas.DataFrame
        Output of :func:`load_placements`
    battery_capacity : dict
        Battery capacity in kWh per vehicle type
    eta : float
        Charging point efficiency `eta_cp` of the scenario

    Returns
    -------
    dict of numpy.ndarray
        `driving` (MWh per timestep), `deficit_max`, `deficit_min` and
        `battery` (MWh), `power` (plugged-in, MW), `dumb` (MW),
        `dumb_flex` (MW) and `dumb_<use case>` (MW), each of length
        :data:`N_STEPS`
    """
    weight = placements.n.to_numpy(dtype="float64")
    start = np.clip(
        placements.park_start.to_numpy(dtype="int64"), 0, LAST_TIMESTEP
    )
    stop = np.clip(
        placements.park_end.to_numpy(dtype="int64"), 0, LAST_TIMESTEP
    )
    stop = np.maximum(stop, start)
    deficit = placements.charging_demand.to_numpy(dtype="float64") * weight
    power = placements.charging_capacity_grid.to_numpy(dtype="float64") / 1e3
    battery = placements.type.map(battery_capacity).to_numpy(dtype="float64")
    if np.isnan(battery).any():
        missing = sorted(set(placements.type[np.isnan(battery)]))
        raise KeyError(f"No battery capacity for vehicle types {missing}.")
    use_case = placements.use_case.to_numpy()
    flexible = np.isin(use_case, FLEX_USE_CASES)
    length = stop - start

    size = N_STEPS + 2
    series = {}

    # Driving load: the deficit in the arrival timestep, in MWh.
    driving = np.zeros(N_STEPS)
    np.add.at(driving, start, deficit / 1e3)
    series["driving"] = driving

    # Plugged-in battery and power.
    diff = np.zeros(size)
    _add_constant(diff, start, stop, battery * weight / 1e3)
    series["battery"] = _from_diff(diff)
    diff = np.zeros(size)
    _add_constant(diff, start, stop, power * weight)
    series["power"] = _from_diff(diff)

    # Outstanding deficit bounds, in MWh.
    upper = np.zeros(size)
    lower = np.zeros(size)
    _add_constant(
        upper, start[flexible], stop[flexible], deficit[flexible] / 1e3
    )

    # Inflexible: the fixed ramp d * (1 - linspace(0, 1, n)) on [s, s + n).
    # One step: d on [s, s + 1). Longer: a linear segment a + c (t - s)
    # with a = d and c = -d / (n - 1), written as a second difference;
    # it ends at 0 in t = s + n - 1, so only three entries are needed.
    one = ~flexible & (length == 1)
    for diff in (upper, lower):
        _add_constant(diff, start[one], stop[one], deficit[one] / 1e3)
    ramp = ~flexible & (length >= 2)
    a = deficit[ramp] / 1e3
    c = -a / (length[ramp] - 1)
    second = np.zeros(size + 1)
    np.add.at(second, start[ramp], a)
    np.add.at(second, start[ramp] + 1, c - a)
    np.add.at(second, stop[ramp], -c)
    ramp_series = np.cumsum(np.cumsum(second))[:N_STEPS]
    series["deficit_max"] = np.clip(_from_diff(upper) + ramp_series, 0, None)
    series["deficit_min"] = np.clip(_from_diff(lower) + ramp_series, 0, None)

    # Dumb charging: nominal power from arrival until d / eta is drawn.
    # The nameplate is grid-side, the deficit battery-side, so the charging
    # time is d / (P * eta) (D29). Cropped at the year end like the legacy
    # model, the remainder of the last timestep included.
    steps = (
        np.divide(
            placements.charging_demand.to_numpy(dtype="float64"),
            power * 1e3 * eta,
            out=np.zeros(len(placements)),
            where=power > 0,
        )
        * STEPS_PER_HOUR
    )
    full = np.floor(steps).astype("int64")
    part = steps - full
    full_stop = np.minimum(start + full, LAST_TIMESTEP)
    last = np.minimum(start + full, LAST_TIMESTEP)
    load = power * weight

    def dumb(mask) -> np.ndarray:
        diff = np.zeros(size)
        _add_constant(diff, start[mask], full_stop[mask], load[mask])
        result = _from_diff(diff)
        np.add.at(result, last[mask], part[mask] * load[mask])
        return result

    series["dumb"] = dumb(np.ones(len(placements), dtype=bool))
    series["dumb_flex"] = dumb(flexible)
    for case in CHARGING_USE_CASES:
        series[f"dumb_{case}"] = dumb(use_case == case)

    # Energy closure, the equivalent of the legacy load validation (D30):
    # the driving load carries every deficit, the dumb load its grid-side
    # energy except what charging past the year end loses to the crop.
    np.testing.assert_allclose(driving.sum(), deficit.sum() / 1e3, rtol=1e-9)
    drawn = (np.minimum(full, LAST_TIMESTEP - start) + part) * load
    np.testing.assert_allclose(
        series["dumb"].sum(), drawn.sum(), rtol=1e-9, atol=1e-9
    )

    return series


def _hourly(values: np.ndarray, how: str) -> np.ndarray:
    """Aggregate a quarter-hourly series to the 8760 hours of the year."""
    blocks = values[: N_HOURS * STEPS_PER_HOUR].reshape(
        N_HOURS, STEPS_PER_HOUR
    )
    return getattr(blocks, how)(axis=1)


def hourly_model(series: dict) -> dict:
    """Static parameters and hourly series of one model for eTraGo.

    Returns
    -------
    dict
        `e_nom`, `p_nom` and the hourly lists `e_min_pu`, `e_max_pu`,
        `p_max_pu`, `driving`, `dumb`, `dumb_flex` and `dumb_<use case>`
    """
    e_nom = float(series["battery"].max())
    p_nom = float(series["power"].max())

    model = {"e_nom": e_nom, "p_nom": p_nom}
    if e_nom > 0:
        # Lower energy bound: min over the hour of (e_nom - deficit_max),
        # upper bound: max of (e_nom - deficit_min) -- as the legacy model
        # takes min/max of the absolute bounds.
        model["e_min_pu"] = np.clip(
            1 - _hourly(series["deficit_max"], "max") / e_nom, 0, 1
        )
        model["e_max_pu"] = np.clip(
            1 - _hourly(series["deficit_min"], "min") / e_nom, 0, 1
        )
    if p_nom > 0:
        model["p_max_pu"] = np.clip(
            _hourly(series["power"], "mean") / p_nom, 0, 1
        )
    model["driving"] = _hourly(series["driving"], "sum")
    for key in [_ for _ in series if _.startswith("dumb")]:
        model[key] = _hourly(series[key], "mean")
    return model


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def _ac_bus(scenario_name: str, bus_id: int):
    """The AC bus a model connects to."""
    with db.session_scope() as session:
        bus = (
            session.query(
                EgonPfHvBus.bus_id,
                EgonPfHvBus.x,
                EgonPfHvBus.y,
                EgonPfHvBus.geom,
            )
            .filter(
                EgonPfHvBus.scn_name == scenario_name,
                EgonPfHvBus.bus_id == int(bus_id),
                EgonPfHvBus.carrier == "AC",
            )
            .first()
        )
    if bus is None:
        raise ValueError(
            f"No AC bus {bus_id} in grid.egon_etrago_bus for scenario "
            f"'{scenario_name}'."
        )
    return bus


@db.check_db_unique_violation
def _write_load(scenario_name, bus_id, carrier, p_set) -> None:
    load_id = db.next_etrago_id("load")
    with db.session_scope() as session:
        session.add(
            EgonPfHvLoad(
                scn_name=scenario_name,
                load_id=load_id,
                bus=int(bus_id),
                carrier=carrier,
                sign=-1,
            )
        )
        session.add(
            EgonPfHvLoadTimeseries(
                scn_name=scenario_name,
                load_id=load_id,
                temp_id=1,
                p_set=[float(_) for _ in p_set],
            )
        )


@db.check_db_unique_violation
def _write_flexible_model(
    scenario_name, model_type, ac_bus, model, eta
) -> None:
    """Write bus, link, store and driving load of one flexible model."""
    bus_id = db.next_etrago_id("bus")
    link_id = db.next_etrago_id("link")
    store_id = db.next_etrago_id("store")
    with db.session_scope() as session:
        session.add(
            EgonPfHvBus(
                scn_name=scenario_name,
                bus_id=bus_id,
                v_nom=1,
                carrier=BUS_CARRIER,
                x=ac_bus.x,
                y=ac_bus.y,
                geom=ac_bus.geom,
            )
        )
        session.add(
            EgonPfHvLink(
                scn_name=scenario_name,
                link_id=link_id,
                bus0=ac_bus.bus_id,
                bus1=bus_id,
                carrier=LINK_CARRIERS[model_type],
                efficiency=float(eta),
                p_nom=model["p_nom"],
                p_nom_extendable=False,
                p_nom_min=0,
                p_nom_max=np.inf,
                p_min_pu=0,
                p_max_pu=1,
                capital_cost=0,
                marginal_cost=0,
                length=0,
                terrain_factor=1,
            )
        )
        session.add(
            EgonPfHvLinkTimeseries(
                scn_name=scenario_name,
                link_id=link_id,
                temp_id=1,
                p_min_pu=None,
                p_max_pu=[float(_) for _ in model["p_max_pu"]],
            )
        )
        session.add(
            EgonPfHvStore(
                scn_name=scenario_name,
                store_id=store_id,
                bus=bus_id,
                carrier=STORE_CARRIER,
                e_nom=model["e_nom"],
                e_nom_extendable=False,
                e_nom_min=0,
                e_nom_max=np.inf,
                e_min_pu=0,
                e_max_pu=1,
                e_initial=model["e_nom"],
                e_cyclic=False,
                sign=1,
                standing_loss=0,
            )
        )
        session.add(
            EgonPfHvStoreTimeseries(
                scn_name=scenario_name,
                store_id=store_id,
                temp_id=1,
                e_min_pu=[float(_) for _ in model["e_min_pu"]],
                e_max_pu=[float(_) for _ in model["e_max_pu"]],
            )
        )
    _write_load(
        scenario_name, bus_id, LOAD_CARRIERS[model_type], model["driving"]
    )


def _lowflex_name(scenario_name: str):
    """Name of the lowflex counterpart of a scenario, or None."""
    sources, _ = load_sources_and_targets("MotorizedIndividualTravel")
    lowflex = sources.files["original_data"]["scenario"]["lowflex"]
    if not lowflex["create_lowflex_scenario"]:
        return None
    return lowflex["names"].get(scenario_name)


def write_model(scenario_name, model_type, ac_bus_id, model, eta) -> None:
    """Write one model's eTraGo components (cf. the module docstring)."""
    load_carrier = LOAD_CARRIERS[model_type]
    if not is_flexible(scenario_name):
        _write_load(scenario_name, ac_bus_id, load_carrier, model["dumb"])
        return

    ac_bus = _ac_bus(scenario_name, ac_bus_id)
    _write_flexible_model(scenario_name, model_type, ac_bus, model, eta)
    lowflex = _lowflex_name(scenario_name)
    if lowflex is not None:
        _write_load(lowflex, ac_bus_id, load_carrier, model["dumb"])


def _energy_balance(placements: pd.DataFrame, eta: float) -> pd.DataFrame:
    """Export B rows of one model: annual energy per use case and type."""
    balance = (
        placements.assign(
            demand=placements.charging_demand.astype("float64") * placements.n
        )
        .groupby(["use_case", "type"])
        .agg(battery=("demand", "sum"), n_placements=("n", "sum"))
        .reset_index()
    )
    balance["battery"] /= 1e3
    balance["grid"] = balance.battery / eta
    balance["flexible"] = balance.use_case.isin(FLEX_USE_CASES)
    return balance


def generate_placement_models(
    scenario_name: str, part: int, parts: int
) -> None:
    """Build and write the models of one share of a scenario's AC buses.

    Models are partitioned by AC bus (`bus_id % parts == part`), so the
    district model and all transmission models on one bus are built by
    the same task, and the diagnostic exports, which aggregate the
    transmission models per bus, are written once.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    part : int
        Share of the buses to process, `0 <= part < parts`
    parts : int
        Number of parallel tasks of the scenario
    """
    models = model_list(scenario_name)
    models = models[models.bus_id % parts == part]
    print(
        f"SCENARIO: {scenario_name}, part {part + 1}/{parts}: "
        f"{(models.model_type == MODEL_DISTRICT).sum()} district and "
        f"{(models.model_type == MODEL_TRANSMISSION).sum()} transmission "
        f"models on {models.bus_id.nunique()} buses."
    )

    eta = eta_cp(scenario_name)
    battery_capacity = (
        read_simbev_metadata_file(scenario_name, "tech_data")
        .battery_capacity.astype(float)
        .to_dict()
    )

    for (bus_id, model_type), group in models.groupby(
        ["bus_id", "model_type"], sort=True
    ):
        diagnostics = None
        balances = []
        for model_key in group.model_key:
            placements = load_placements(scenario_name, model_type, model_key)
            if placements.empty:
                continue
            model = hourly_model(
                build_timeseries(placements, battery_capacity, eta)
            )
            write_model(scenario_name, model_type, bus_id, model, eta)

            exported = {
                key: value
                for key, value in model.items()
                if key == "driving" or key.startswith("dumb")
            }
            diagnostics = (
                exported
                if diagnostics is None
                else {k: diagnostics[k] + v for k, v in exported.items()}
            )
            balances.append(_energy_balance(placements, eta))

        if diagnostics is None:
            continue
        write_flex_timeseries(scenario_name, bus_id, model_type, diagnostics)
        write_charging_profile_use_case(
            scenario_name, bus_id, model_type, diagnostics
        )
        balance = (
            pd.concat(balances)
            .groupby(["use_case", "type", "flexible"], as_index=False)
            .sum()
        )
        write_energy_balance_rows(scenario_name, bus_id, model_type, balance)
        print(
            f"  bus {bus_id}: {len(group)} {model_type} model(s), "
            f"{balance.n_placements.sum()} placements, "
            f"{balance.grid.sum():.1f} MWh grid side."
        )
