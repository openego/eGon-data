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

from collections import Counter

from loguru import logger
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.hgv_charging import SCENARIO_MAP
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
    """Write eTraGo HGV load and flex model for all scenarios."""
    lowflex = _lowflex_config()

    for egon_scn in SCENARIO_MAP:
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
            _write_fixed_load(lowflex_scn, sites, cps, profiles, day_only=False)
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
                   el_demand_N2_mwh, el_demand_N3_mwh, el_demand_N3S_mwh,
                   el_demand_day_mwh, el_demand_night_mwh,
                   p_set_aggregated_mw
            FROM demand.egon_hgv_charging_site
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        cps_rows = session.execute(
            f"""
            SELECT cp_id, site_id, vehicle_class, sector, time_of_day, p_set_mw
            FROM demand.egon_hgv_charging_point
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        events_rows = session.execute(
            f"""
            SELECT event_id, cp_id, site_id, vehicle_class, bat_cap,
                   location, use_case,
                   charging_capacity_nominal, charging_capacity_grid,
                   soc_start, soc_end, charging_demand,
                   park_start, park_end, drive_start, drive_end, consumption
            FROM demand.egon_hgv_charging_event
            WHERE scenario = '{egon_scn}'
            """
        ).fetchall()

        prof_rows = session.execute(
            f"""
            SELECT sector, profile
            FROM demand.egon_hgv_profile
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
        "event_id", "cp_id", "site_id", "vehicle_class", "bat_cap",
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

def _build_bus_timeseries(sites, cps, profiles, day_only=False):
    """Build profile-based timeseries per bus_id.

    day_only=True: only highway day CPs.
    day_only=False: all CPs (depot + highway day + highway night).
    """
    bus_ts = {}

    if not day_only:
        # Depot CPs
        depot_cps = cps[cps["location_type"] == "depot"]
        for bus_id, group in depot_cps.groupby("bus_id"):
            ts = np.zeros(N_TIMESTEPS)
            for _, cp in group.iterrows():
                prof = profiles.get(cp["sector"])
                if prof is None:
                    logger.warning(f"  Missing profile for sector {cp['sector']}, skipping")
                    continue
                ts += prof * cp["el_demand_mwh"]
            bus_ts[bus_id] = bus_ts.get(bus_id, np.zeros(N_TIMESTEPS)) + ts

    # Highway CPs
    highway_sites = sites[sites["location_type"] == "highway"]
    site_info = highway_sites.set_index("site_id")[
        ["bus_id", "el_demand_day_mwh", "el_demand_night_mwh"]
    ]
    tod_sector_map = (
        {"day": "hgv_highway_day"}
        if day_only
        else {"day": "hgv_highway_day", "night": "hgv_highway_night"}
    )

    for site_id, site_row in site_info.iterrows():
        bus_id = site_row["bus_id"]
        ts = np.zeros(N_TIMESTEPS)
        for tod, sector in tod_sector_map.items():
            prof = profiles.get(sector)
            if prof is None:
                logger.warning(f"  Missing profile {sector}, skipping site {site_id}")
                continue
            ts += prof * site_row[f"el_demand_{tod}_mwh"]
        bus_ts[bus_id] = bus_ts.get(bus_id, np.zeros(N_TIMESTEPS)) + ts

    return bus_ts


def _write_fixed_load(scenario: str, sites, cps, profiles, day_only: bool):
    """Write profile-based fixed loads to egon_etrago_load/_timeseries."""
    bus_ts = _build_bus_timeseries(sites, cps, profiles, day_only=day_only)
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

    Adds charging-event rows plus synthetic driving rows (one per charging event)
    so that the energy charged leaves the store at departure.
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

    # Synthetic driving rows: one per charging event, at departure timestep.
    # location="driving", soc_start=1.0 (full after charging), soc_end=original soc_start,
    # consumption=charging_demand removes energy from the store at drive_start.
    drive_rows = df[["ev_id", "bat_cap", "soc_start", "charging_demand",
                      "park_end", "drive_start", "drive_end"]].copy()
    drive_rows["location"] = "driving"
    drive_rows["charging_capacity_grid_MW"] = 0.0
    drive_rows["last_timestep_charging_capacity_grid_MW"] = 0.0
    drive_rows["charge_end"] = drive_rows["drive_start"]
    drive_rows["last_timestep"] = drive_rows["drive_start"]
    drive_rows["flex_charging_capacity_grid_MW"] = 0.0
    drive_rows["flex_last_timestep_charging_capacity_grid_MW"] = 0.0
    # After charging soc=1.0, after driving soc=original soc_start of charging event
    drive_rows["soc_end"] = drive_rows["soc_start"]
    drive_rows["soc_start"] = 1.0
    drive_rows["consumption"] = drive_rows["charging_demand"]
    # park_start/park_end for driving row = drive timestep (zero-duration)
    drive_rows["park_start"] = drive_rows["drive_start"]
    drive_rows["park_end"] = drive_rows["drive_start"]
    drive_rows = drive_rows.drop(columns=["charging_demand"])

    # Align columns
    keep_cols = [
        "ev_id", "drive_start", "drive_end", "park_start", "park_end",
        "charge_end", "charging_capacity_grid_MW", "last_timestep",
        "last_timestep_charging_capacity_grid_MW",
        "flex_charging_capacity_grid_MW", "flex_last_timestep_charging_capacity_grid_MW",
        "soc_start", "soc_end", "bat_cap", "location", "consumption",
    ]
    df_charging = df[keep_cols]
    df_driving = drive_rows[keep_cols]

    return pd.concat([df_charging, df_driving], ignore_index=True)


def _generate_hgv_load_time_series(ev_data_df: pd.DataFrame) -> pd.DataFrame:
    """Compute load and SoC band timeseries from preprocessed HGV event data.

    Adapted from MIV generate_load_time_series for hourly (8760-step) resolution.
    All ev_count = 1 (no profile duplication for HGV).
    """
    profile_counter = Counter(ev_data_df["ev_id"])

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
        ev_count = profile_counter[ev_id]

        arrays["load"][start:end] += cap * ev_count
        arrays["load"][last_ts] += last_ts_cap * ev_count
        arrays["flex"][start:end] += flex_cap * ev_count
        arrays["flex"][last_ts] += flex_last_ts_cap * ev_count
        arrays["plugged_in"][start:park_end + 1] += cap * ev_count
        arrays["plugged_in_flex"][start:park_end + 1] += flex_cap * ev_count

        if location == "driving":
            n = drive_end - drive_start + 1
            lin = np.linspace(soc_start, soc_end, n + 1)[1:]
            arrays["soc_min_abs"][drive_start:drive_end + 1] += lin * bat_cap * ev_count
            arrays["soc_max_abs"][drive_start:drive_end + 1] += lin * bat_cap * ev_count
            if soc_start > soc_end:
                arrays["driving_load"][drive_start:drive_end + 1] += (
                    consumption * ev_count / n
                )

        elif soc_start == soc_end:
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

    return pd.DataFrame({
        "load_time_series": arrays["load"] / 1e3,          # kW → MW
        "simultaneous_plugged_in_charging_capacity": arrays["plugged_in"] / 1e3,
        "soc_min_absolute": arrays["soc_min_abs"] / 1e3,   # kWh → MWh
        "soc_max_absolute": arrays["soc_max_abs"] / 1e3,
        "driving_load_time_series": arrays["driving_load"] / 1e3,
    })


def _calc_e_initial(events: pd.DataFrame) -> float:
    """Capacity-weighted mean SoC of first event per CP (HGV analogue of MIV simbev_event_id==0)."""
    first = events.sort_values("park_start").groupby("cp_id").first()
    total_bat = first["bat_cap"].sum()
    if total_bat == 0:
        return 0.5
    return float((first["soc_start"] * first["bat_cap"]).sum() / total_bat)


def _get_etrago_bus(bus_id: int, scenario: str):
    """Fetch geometry and coordinates of an AC eTraGo bus."""
    with db.session_scope() as session:
        result = session.query(EgonPfHvBus).filter(
            EgonPfHvBus.bus_id == bus_id,
            EgonPfHvBus.scn_name == scenario,
            EgonPfHvBus.carrier == "AC",
        ).first()
    return result


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

    e_nom_mwh = float(events_flex["bat_cap"].sum() / 1e3)
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
