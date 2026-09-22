"""
Write HGV charging demand and flexibility model into eTraGo tables.

Fixed load (profile-based, written for all scenarios including lowflex):
  Highway day CPs — always inflexible:
    p_set[t] = profile_day[t] * el_demand_day_mwh  [MW per site]

  Lowflex scenario — all CPs use the same profile-based timeseries:
    depot:   p_set[t] = Σ_cp  profile_depot[t] * el_demand_mwh_cp
    highway night: p_set[t] = profile_night[t] * el_demand_night_mwh

Flex model (bus + link + store + driving load, written for flex scenarios):
  One model per MV grid district (MV-connected depots + highway night aggregated).
  One model per site for HV/EHV-connected sites.
  Highway day is always excluded from the flex model.
"""

from collections import namedtuple

from loguru import logger
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.hgv_charging.scenarios import active_scenario_map
from egon.data.datasets.etrago_setup import (
    EgonPfHvBus,
    EgonPfHvLink,
    EgonPfHvLinkTimeseries,
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
    EgonPfHvStore,
    EgonPfHvStoreTimeseries,
)

CARRIER_LOAD = "land_transport_HGV"   # fixed loads and driving load on HGV bus
CARRIER_BUS = "HGV_charger"           # new EV bus per flex model
CARRIER_LINK = "HGV_charger"          # AC→EV charger link
CARRIER_STORE = "battery_storage"     # EV battery store

ETA_CP = 1.0
N_TIMESTEPS = 8760
TIMESTEP_HOURS = 1.0  # hourly resolution; driving_load's energy->power conversion assumes this


def _lowflex_config() -> dict:
    """Read lowflex config from datasets.yml. Returns dict with keys:
    create_lowflex_scenario (bool), names (dict scenario→lowflex_name)."""
    cfg = config.datasets().get("mobility_hgv_charging", {})
    lowflex = cfg.get("original_data", {}).get("lowflex", {})
    return {
        "create": lowflex.get("create_lowflex_scenario", False),
        "names": lowflex.get("names", {}),
    }


def write_etrago():
    """Write eTraGo HGV load and flex model for all active scenarios."""
    lowflex = _lowflex_config()

    for egon_scn in active_scenario_map():
        logger.info(f"Writing eTraGo HGV load for scenario {egon_scn}")

        sites, cps, events, profiles = _load_data(egon_scn)
        if sites.empty:
            logger.warning(f"  No HGV sites found for scenario {egon_scn}")
            continue

        # --- Fixed load: highway day (all scenarios) ---
        _write_fixed_load(egon_scn, sites, cps, profiles, day_only=True)

        # --- Fixed load: all CPs for lowflex scenario ---
        if lowflex["create"] and egon_scn in lowflex["names"]:
            lowflex_scn = lowflex["names"][egon_scn]
            _write_fixed_load(lowflex_scn, sites, cps, profiles, day_only=False, events=events)
            logger.info(f"  Wrote lowflex fixed loads for {lowflex_scn}")

        # --- Flex model: MV-aggregated and HV/EHV per-site ---
        _write_flex_model(egon_scn, sites, cps, events)

        logger.info(f"  Done writing eTraGo HGV for {egon_scn}")


def _load_data(egon_scn: str):
    """Load sites, charging points, events, and profiles from DB."""
    with db.session_scope() as session:
        sites_rows = session.execute(
            f"""
            SELECT site_id, location_type, bus_id, mv_grid_id, voltage_level,
                   "el_demand_N2_mwh", "el_demand_N3_mwh", "el_demand_N3S_mwh",
                   el_demand_day_mwh, el_demand_night_mwh,
                   p_set_aggregated_mw
            FROM demand.egon_ev_hgv_charging_site
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        cps_rows = session.execute(
            f"""
            SELECT cp_id, site_id, vehicle_class, sector, time_of_day, p_set_mw
            FROM demand.egon_ev_hgv_charging_point
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        events_rows = session.execute(
            f"""
            SELECT event_id, cp_id, site_id, vehicle_class, bat_cap, slot_id,
                   location, use_case,
                   charging_capacity_nominal, charging_capacity_grid,
                   soc_start, soc_end, charging_demand,
                   park_start, park_end, drive_start, drive_end, consumption
            FROM demand.egon_ev_hgv_charging_event
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        prof_rows = session.execute(
            f"""
            SELECT sector, profile
            FROM demand.egon_ev_hgv_profile
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

    sites = pd.DataFrame(sites_rows, columns=[
        "site_id", "location_type", "bus_id", "mv_grid_id", "voltage_level",
        "el_demand_N2_mwh", "el_demand_N3_mwh", "el_demand_N3S_mwh",
        "el_demand_day_mwh", "el_demand_night_mwh", "p_set_aggregated_mw",
    ])
    cps = pd.DataFrame(cps_rows, columns=[
        "cp_id", "site_id", "vehicle_class", "sector", "time_of_day", "p_set_mw",
    ])
    events = pd.DataFrame(events_rows, columns=[
        "event_id", "cp_id", "site_id", "vehicle_class", "bat_cap", "slot_id",
        "location", "use_case",
        "charging_capacity_nominal", "charging_capacity_grid",
        "soc_start", "soc_end", "charging_demand",
        "park_start", "park_end", "drive_start", "drive_end", "consumption",
    ])
    profiles = {row[0]: np.array(row[1], dtype=float) for row in prof_rows}

    # Enrich cps with site attributes
    cps = cps.merge(
        sites[["site_id", "bus_id", "mv_grid_id", "location_type", "voltage_level",
               "el_demand_N2_mwh", "el_demand_N3_mwh", "el_demand_N3S_mwh"]],
        on="site_id", how="left",
    )
    vc_col = {"N2": "el_demand_N2_mwh", "N3": "el_demand_N3_mwh", "N3S": "el_demand_N3S_mwh"}
    cps["el_demand_mwh"] = cps.apply(
        lambda r: r[vc_col.get(r["vehicle_class"], "el_demand_N2_mwh")], axis=1
    )

    return sites, cps, events, profiles


# ---------------------------------------------------------------------------
# Fixed load (profile-based)
# ---------------------------------------------------------------------------

def _build_bus_timeseries_day_only(sites, profiles):
    """Build profile-based timeseries per bus_id, highway day CPs only.

    Highway daytime (MCS) demand has NO real events at all by design
    (distribute_energy_highway only builds ts_{vc} from
    el_demand_night_{vc}), so there is nothing to sum here besides the
    profile x annual_energy shape.
    """
    bus_ts = {}
    highway_sites = sites[sites["location_type"] == "highway"]
    site_info = highway_sites.set_index("site_id")[["bus_id", "el_demand_day_mwh"]]

    for site_id, site_row in site_info.iterrows():
        bus_id = site_row["bus_id"]
        prof = profiles.get("hgv_highway_day")
        if prof is None:
            logger.warning(f"  Missing profile hgv_highway_day, skipping site {site_id}")
            continue
        ts = prof * site_row["el_demand_day_mwh"]
        bus_ts[bus_id] = bus_ts.get(bus_id, np.zeros(N_TIMESTEPS)) + ts

    return bus_ts


def _build_bus_timeseries_events(sites, cps, events):
    """Build event-based ("dumb charging") timeseries per bus_id, depot +
    highway night CPs -- for the lowflex fixed load.

    Matches MIV's lowflex load exactly (model_timeseries.py: "lowflex: use
    dumb charging load") -- each vehicle charges at full nominal power from
    arrival until its own charge_end, summed per bus_id. NOT profile x
    annual_energy, which has no MIV equivalent at this granularity and was
    a HGV-only simplification that masked a real modelling issue (see next
    paragraph).

    Events clipped to the year boundary (park_start == 0) are KEPT in this
    sum. A filter here previously dropped them, justified by the spike that
    such events cause in the flex model's driving_load -- but that failure
    mode belongs to driving_load alone, which lands each event's whole
    energy on a single timestep (consumption / TIMESTEP_HOURS) and is
    therefore unbounded (1757 MW at hour 0 on the SH reGon2037 run, ~1200x
    its own mean). This sum accumulates only the rated charger power
    (`cap`) per event per hour, so it is bounded by installed charger
    capacity: 6,555 simultaneous arrivals cannot draw more than their
    chargers physically allow -- 380 MW at hour 0, against ~0.17% of annual
    charging energy that dropping them would discard. (driving_load keeps
    these events too; its spike is a separate, still-open limitation.)

    Keeping them slightly overstates hour 0 and the hours just after (those
    vehicles really arrived before the year began, so they would already be
    partway charged), but it conserves energy and keeps lowflex describing
    the same fleet as flex. The physically exact fix is a partial event with
    a reduced remaining charging_demand, which belongs in
    generate_charging_events (Stage A), not in a filter here. See
    HGV/DOCUMENTATION.md "Known limitations".
    """
    bus_ts = {}

    real_events = events[events["location"] == "charging"].copy()

    cp_to_bus = cps.merge(
        sites[["site_id", "bus_id"]], on="site_id", how="left",
    ).set_index("cp_id")["bus_id"]
    real_events["bus_id"] = real_events["cp_id"].map(cp_to_bus)

    for bus_id, group in real_events.groupby("bus_id"):
        ev_data = _data_preprocessing_hgv(group)
        ts = _generate_hgv_load_time_series(ev_data)
        bus_ts[bus_id] = (
            bus_ts.get(bus_id, np.zeros(N_TIMESTEPS)) + ts["load_time_series"].to_numpy()
        )

    return bus_ts


def _write_fixed_load(scenario: str, sites, cps, profiles, day_only: bool, events=None):
    """Write fixed loads to egon_etrago_load/_timeseries.

    day_only=True:  profile-based (highway day only, see
                    _build_bus_timeseries_day_only).
    day_only=False: event-based dumb charging (depot + highway night, see
                    _build_bus_timeseries_events) -- requires `events`.
    """
    if day_only:
        bus_ts = _build_bus_timeseries_day_only(sites, profiles)
    else:
        if events is None:
            raise ValueError("_write_fixed_load(day_only=False) requires events")
        bus_ts = _build_bus_timeseries_events(sites, cps, events)
    if not bus_ts:
        return

    bus_ids = list(bus_ts.keys())
    load_ids = db.next_etrago_id("load", len(bus_ids))
    engine = db.engine()

    pd.DataFrame({
        "scn_name": scenario,
        "load_id": load_ids,
        "bus": bus_ids,
        "carrier": CARRIER_LOAD,
        "sign": -1,
        "p_set": np.nan,
        "q_set": np.nan,
        "type": np.nan,
    }).to_sql("egon_etrago_load", engine, schema="grid", if_exists="append", index=False)

    pd.DataFrame({
        "scn_name": scenario,
        "load_id": load_ids,
        "temp_id": 1,
        "p_set": [bus_ts[bus].tolist() for bus in bus_ids],
    }).to_sql("egon_etrago_load_timeseries", engine, schema="grid",
              if_exists="append", index=False)

    label = "day-only" if day_only else "all CPs"
    logger.info(f"  Wrote {len(bus_ids)} fixed-load buses ({label}) for {scenario}")


# ---------------------------------------------------------------------------
# Flex model
# ---------------------------------------------------------------------------

def _data_preprocessing_hgv(events: pd.DataFrame) -> pd.DataFrame:
    """Compute derived columns needed by _generate_hgv_load_time_series.

    Column derivation only — no row synthesis. Matches MIV's
    data_preprocessing, which likewise only derives columns from simBEV's
    already-complete event sequence; here, generate_charging_events
    (HGV/energy_distribution_depots.py) is the equivalent upstream source
    of a complete event sequence (charging events plus vacant events
    filling every gap in each physical slot's chain — see HGV/CONTEXT.md
    and HGV/docs/adr/0002). There is no separate driving-event row:
    baseline SOC is 1.0 for every slot, and each charging event carries
    its own arrival deficit and drives its own driving_load contribution
    (added directly in _generate_hgv_load_time_series).
    """
    df = events.copy()
    df["charging_capacity_grid_MW"] = df["charging_capacity_grid"] / 1e3

    # Minimum charging time in hourly timesteps
    df["minimum_charging_time"] = (
        df["charging_demand"] / df["charging_capacity_nominal"]
    ).fillna(0)

    full_ts, last_ts_share = df["minimum_charging_time"].divmod(1)
    full_ts = full_ts.astype(int)
    df["last_timestep_charging_capacity_grid_MW"] = (
        last_ts_share * df["charging_capacity_grid_MW"]
    )
    df["charge_end"] = df["park_start"] + full_ts
    df["last_timestep"] = (df["park_start"] + full_ts).clip(upper=N_TIMESTEPS - 1)

    # All HGV events are flexible
    df["flex_charging_capacity_grid_MW"] = df["charging_capacity_grid_MW"]
    df["flex_last_timestep_charging_capacity_grid_MW"] = (
        df["last_timestep_charging_capacity_grid_MW"]
    )

    # Rename cp_id → ev_id (profile_counter key in generate_load_time_series)
    df = df.rename(columns={"cp_id": "ev_id"})

    return df


def _generate_hgv_load_time_series(ev_data_df: pd.DataFrame) -> pd.DataFrame:
    """Compute load and SoC band timeseries from preprocessed HGV event data.

    Adapted from MIV generate_load_time_series for hourly (8760-step) resolution.
    ev_count is always 1 (no profile duplication for HGV): unlike MIV, ev_id here
    is a charging point id, not a shared vehicle-profile id, so counting ev_id
    occurrences would multiply each event's power by the number of unrelated
    events that ever used the same charging point.

    There is no separate driving-event row (see HGV/CONTEXT.md's "Charging
    event"/"Vacant event" terms and HGV/docs/adr/0002). Baseline SOC is 1.0
    for every physical slot: a charging event's own soc_start already
    encodes its arrival deficit relative to that baseline, and it
    registers its own driving_load contribution at its own park_start (the
    energy it will use to drive away, replenished by this charging).
    Vacant events (location="vacant") hit the soc_start == soc_end branch
    below, holding the slot's SOC flat at 1.0 through every gap in its
    chain.
    """
    arrays = {k: np.zeros(N_TIMESTEPS) for k in [
        "load", "flex", "plugged_in", "plugged_in_flex",
        "soc_min_abs", "soc_max_abs", "driving_load",
    ]}

    columns = [
        "ev_id", "drive_start", "drive_end", "park_start", "park_end",
        "charge_end", "charging_capacity_grid_MW", "last_timestep",
        "last_timestep_charging_capacity_grid_MW",
        "flex_charging_capacity_grid_MW", "flex_last_timestep_charging_capacity_grid_MW",
        "soc_start", "soc_end", "bat_cap", "location", "consumption",
    ]

    for (
        _,
        ev_id, drive_start, drive_end, start, park_end,
        end, cap, last_ts, last_ts_cap,
        flex_cap, flex_last_ts_cap,
        soc_start, soc_end, bat_cap, location, consumption,
    ) in ev_data_df[columns].itertuples():
        ev_count = 1

        arrays["load"][start:end] += cap * ev_count
        arrays["load"][last_ts] += last_ts_cap * ev_count
        arrays["flex"][start:end] += flex_cap * ev_count
        arrays["flex"][last_ts] += flex_last_ts_cap * ev_count
        arrays["plugged_in"][start:park_end + 1] += cap * ev_count
        arrays["plugged_in_flex"][start:park_end + 1] += flex_cap * ev_count

        if soc_start == soc_end:
            # Covers vacant events (location="vacant", flat at the 1.0
            # baseline) and any real charging event that happened to arrive
            # already full.
            arrays["soc_min_abs"][start:park_end + 1] += soc_start * bat_cap * ev_count
            arrays["soc_max_abs"][start:park_end + 1] += soc_end * bat_cap * ev_count

        elif soc_start < soc_end:
            if flex_cap > 0:
                arrays["soc_min_abs"][start:park_end + 1] += soc_start * bat_cap * ev_count
                arrays["soc_max_abs"][start:park_end + 1] += soc_end * bat_cap * ev_count
            else:
                lin = np.linspace(soc_start, soc_end, park_end - start + 1)
                arrays["soc_min_abs"][start:park_end + 1] += lin * bat_cap * ev_count
                arrays["soc_max_abs"][start:park_end + 1] += lin * bat_cap * ev_count

            # This vehicle's own driving load: the energy it will use to
            # leave, registered at its own arrival (consumption == the
            # charging_demand this event delivers — see energy_distribution_
            # depots.py). One-hour draw at hourly resolution.
            arrays["driving_load"][start] += consumption * ev_count / TIMESTEP_HOURS

    return pd.DataFrame({
        # cap/plugged_in accumulate charging_capacity_grid_MW, already MW — no further conversion
        "load_time_series": arrays["load"],
        "simultaneous_plugged_in_charging_capacity": arrays["plugged_in"],
        "soc_min_absolute": arrays["soc_min_abs"] / 1e3,   # kWh → MWh (bat_cap is kWh)
        "soc_max_absolute": arrays["soc_max_abs"] / 1e3,
        "driving_load_time_series": arrays["driving_load"] / 1e3,  # consumption is kWh
    })


def _calc_e_initial(events: pd.DataFrame) -> float:
    """Capacity-weighted mean SoC of first event per physical slot (HGV
    analogue of MIV simbev_event_id==0). Grouped by (cp_id, slot_id), not
    cp_id alone, so every physical slot contributes its own starting value
    — a cp_id with multiple slots would otherwise be under-represented by
    one slot's value standing in for all of them (see HGV/docs/adr/0002).
    """
    first = events.sort_values("park_start").groupby(["cp_id", "slot_id"]).first()
    total_bat = first["bat_cap"].sum()
    if total_bat == 0:
        return 0.5
    return float((first["soc_start"] * first["bat_cap"]).sum() / total_bat)


AcBus = namedtuple("AcBus", ["bus_id", "x", "y", "geom"])


def _get_etrago_bus(bus_id: int, scenario: str):
    """Fetch geometry and coordinates of an AC eTraGo bus."""
    with db.session_scope() as session:
        result = session.query(EgonPfHvBus).filter(
            EgonPfHvBus.bus_id == bus_id,
            EgonPfHvBus.scn_name == scenario,
            EgonPfHvBus.carrier == "AC",
        ).first()
        if result is None:
            return None
        # Extract scalars while the session is still open — the ORM
        # instance itself would be detached (and its attributes expired)
        # once this session closes.
        return AcBus(bus_id=result.bus_id, x=result.x, y=result.y, geom=result.geom)


def _write_flex_components(scenario: str, ac_bus, ts_df: pd.DataFrame,
                           e_nom_mwh: float, e_initial: float):
    """Write bus + link + store + driving load for one aggregated HGV flex model."""
    # New HGV bus co-located with AC substation
    hgv_bus_id = db.next_etrago_id("bus")
    with db.session_scope() as session:
        session.add(EgonPfHvBus(
            scn_name=scenario,
            bus_id=hgv_bus_id,
            v_nom=1,
            carrier=CARRIER_BUS,
            x=ac_bus.x,
            y=ac_bus.y,
            geom=ac_bus.geom,
        ))

    # Link: AC bus → HGV bus
    p_nom = float(ts_df["simultaneous_plugged_in_charging_capacity"].max())
    if p_nom == 0:
        logger.warning("  p_nom = 0 for flex model, skipping")
        return
    link_id = db.next_etrago_id("link")
    with db.session_scope() as session:
        session.add(EgonPfHvLink(
            scn_name=scenario,
            link_id=link_id,
            bus0=ac_bus.bus_id,
            bus1=hgv_bus_id,
            carrier=CARRIER_LINK,
            efficiency=ETA_CP,
            p_nom=p_nom,
            p_nom_extendable=False,
            p_nom_min=0,
            p_nom_max=np.Inf,
            p_min_pu=0,
            p_max_pu=1,
            capital_cost=0,
            marginal_cost=0,
            length=0,
            terrain_factor=1,
        ))
    p_max_pu = (
        ts_df["simultaneous_plugged_in_charging_capacity"] / p_nom
    ).clip(0, 1).tolist()
    with db.session_scope() as session:
        session.add(EgonPfHvLinkTimeseries(
            scn_name=scenario,
            link_id=link_id,
            temp_id=1,
            p_min_pu=None,
            p_max_pu=p_max_pu,
        ))

    # Store on HGV bus
    store_id = db.next_etrago_id("store")
    with db.session_scope() as session:
        session.add(EgonPfHvStore(
            scn_name=scenario,
            store_id=store_id,
            bus=hgv_bus_id,
            carrier=CARRIER_STORE,
            e_nom=e_nom_mwh,
            e_nom_extendable=False,
            e_nom_min=0,
            e_nom_max=np.Inf,
            e_min_pu=0,
            e_max_pu=1,
            e_initial=e_initial * e_nom_mwh,
            e_cyclic=False,
            sign=1,
            standing_loss=0,
        ))
    e_min_pu = (ts_df["soc_min_absolute"] / e_nom_mwh).clip(0, 1).tolist()
    e_max_pu = (ts_df["soc_max_absolute"] / e_nom_mwh).clip(0, 1).tolist()
    with db.session_scope() as session:
        session.add(EgonPfHvStoreTimeseries(
            scn_name=scenario,
            store_id=store_id,
            temp_id=1,
            e_min_pu=e_min_pu,
            e_max_pu=e_max_pu,
        ))

    # Driving load on HGV bus (removes charged energy at departure)
    load_id = db.next_etrago_id("load")
    with db.session_scope() as session:
        session.add(EgonPfHvLoad(
            scn_name=scenario,
            load_id=load_id,
            bus=hgv_bus_id,
            carrier=CARRIER_LOAD,
            sign=-1,
        ))
    with db.session_scope() as session:
        session.add(EgonPfHvLoadTimeseries(
            scn_name=scenario,
            load_id=load_id,
            temp_id=1,
            p_set=ts_df["driving_load_time_series"].tolist(),
        ))


def _run_flex_model(scenario: str, events_flex: pd.DataFrame, ac_bus):
    """Preprocess events, run timeseries generation, write flex components."""
    if events_flex.empty:
        return

    ev_data = _data_preprocessing_hgv(events_flex)
    ts_df = _generate_hgv_load_time_series(ev_data)

    # One bat_cap contribution per PHYSICAL SLOT, not per event row — a busy
    # slot's many charging/vacant events would otherwise each add their own
    # bat_cap, wildly inflating e_nom (same fix as _calc_e_initial, see
    # docs/adr/0002).
    e_nom_mwh = float(
        events_flex.groupby(["cp_id", "slot_id"])["bat_cap"].first().sum() / 1e3
    )
    e_initial = _calc_e_initial(events_flex)

    _write_flex_components(scenario, ac_bus, ts_df, e_nom_mwh, e_initial)


def _write_flex_model(scenario: str, sites, cps, events):
    """Write flex model for all MV districts and HV/EHV sites."""
    mv_sites = sites[sites["voltage_level"].isin([4, 5, 6, 7])]
    hv_sites = sites[sites["voltage_level"].isin([1, 2, 3])]

    # --- MV-aggregated: one model per mv_grid_id ---
    # Includes depot CPs + highway night CPs; highway day is always excluded.
    for mv_grid_id, group_sites in mv_sites.groupby("mv_grid_id"):
        group_site_ids = group_sites["site_id"].tolist()
        group_cp_ids = cps[
            (cps["site_id"].isin(group_site_ids)) &
            ~((cps["location_type"] == "highway") & (cps["time_of_day"] == "day"))
        ]["cp_id"].tolist()
        group_events = events[events["cp_id"].isin(group_cp_ids)]

        ac_bus = _get_etrago_bus(int(mv_grid_id), scenario)
        if ac_bus is None:
            logger.warning(f"  No AC bus for mv_grid_id={mv_grid_id}, skipping flex model")
            continue

        _run_flex_model(scenario, group_events, ac_bus)
        logger.info(f"  Wrote MV flex model for mv_grid_id={mv_grid_id}")

    # --- HV/EHV: one model per site ---
    for _, site in hv_sites.iterrows():
        site_cp_ids = cps[
            (cps["site_id"] == site["site_id"]) &
            ~((cps["location_type"] == "highway") & (cps["time_of_day"] == "day"))
        ]["cp_id"].tolist()
        site_events = events[events["cp_id"].isin(site_cp_ids)]

        ac_bus = _get_etrago_bus(int(site["bus_id"]), scenario)
        if ac_bus is None:
            logger.warning(
                f"  No AC bus for site_id={site['site_id']}, bus_id={site['bus_id']}, skipping"
            )
            continue

        _run_flex_model(scenario, site_events, ac_bus)
        logger.info(f"  Wrote HV/EHV flex model for site_id={site['site_id']}")
