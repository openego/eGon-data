"""
Read Stage A output files and populate Tables 1–4.

Stage A files live in the run folder configured via datasets.yml:
  mobility_hgv_charging.original_data.sources.stage_a_run_dir

For each scenario the Stage A string (e.g. "C 2037") is mapped to the
egon-data scenario name ("reGon2037") before writing to the DB.
"""

import json
from pathlib import Path

from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.hgv_charging import SCENARIO_MAP


def _run_dir() -> Path:
    cfg = config.datasets()["mobility_hgv_charging"]
    return Path(cfg["original_data"]["sources"]["stage_a_run_dir"])


def fill_hgv_tables():
    """Read Stage A files and write Tables 1–4 for all scenarios."""
    run_dir = _run_dir()
    engine = db.engine()

    # Invert map: Stage A string → egon-data scenario name
    stage_a_to_egon = {v: k for k, v in SCENARIO_MAP.items()}

    # Read all Stage A files once (they contain all scenarios)
    sites_all = gpd.read_file(run_dir / "sites.gpkg")
    cps_all = pd.read_csv(run_dir / "charging_points.csv")
    events_all = pd.read_csv(run_dir / "charging_events.csv")
    profiles_all = pd.read_csv(run_dir / "profiles.csv")

    for egon_scn, stage_a_scn in SCENARIO_MAP.items():
        logger.info(f"Filling HGV tables for scenario {egon_scn} ({stage_a_scn})")

        _write_sites(sites_all, egon_scn, stage_a_scn, engine)
        _write_charging_points(cps_all, egon_scn, stage_a_scn, engine)
        _write_charging_events(events_all, egon_scn, stage_a_scn, engine)
        _write_profiles(profiles_all, egon_scn, stage_a_scn, engine)


def _write_sites(sites_all, egon_scn, stage_a_scn, engine):
    sites = sites_all[sites_all["scenario"] == stage_a_scn].copy()
    sites["scenario"] = egon_scn
    sites["carrier"] = "land_transport_HGV"

    # Use centroid for depots (polygon), keep point for highway
    sites["geom"] = sites.geometry.centroid
    sites = sites.set_geometry("geom").drop(columns=["geometry"], errors="ignore")

    # Rename Stage A columns to match Table 1
    sites = sites.rename(
        columns={
            "el_demand_N2": "el_demand_N2_mwh",
            "el_demand_N3": "el_demand_N3_mwh",
            "el_demand_N3S": "el_demand_N3S_mwh",
        }
    )

    # Columns filled later by spatial_assignment
    sites["mv_grid_id"] = None
    sites["bus_id"] = None
    sites["voltage_level"] = None

    keep = [
        "site_id", "scenario", "location_type", "category_name", "geom",
        "area_m2", "N2", "N3", "N3S",
        "el_demand_N2_mwh", "el_demand_N3_mwh", "el_demand_N3S_mwh",
        "el_demand_day_mwh", "el_demand_night_mwh",
        "p_set_aggregated_mw", "mv_grid_id", "bus_id", "voltage_level", "carrier",
    ]
    sites = sites[[c for c in keep if c in sites.columns]]

    sites.to_postgis(
        "egon_hgv_charging_site",
        engine,
        schema="demand",
        if_exists="append",
        index=False,
    )
    logger.info(f"  Wrote {len(sites)} sites (Table 1)")


def _write_charging_points(cps_all, egon_scn, stage_a_scn, engine):
    cps = cps_all[cps_all["scenario"] == stage_a_scn].copy()
    cps["scenario"] = egon_scn

    keep = [
        "cp_id", "scenario", "site_id", "vehicle_class",
        "num_cp", "p_set_mw", "sector", "time_of_day",
    ]
    cps = cps[[c for c in keep if c in cps.columns]]

    cps.to_sql(
        "egon_hgv_charging_point",
        engine,
        schema="demand",
        if_exists="append",
        index=False,
    )
    logger.info(f"  Wrote {len(cps)} charging points (Table 2)")


def _write_charging_events(events_all, egon_scn, stage_a_scn, engine):
    events = events_all[events_all["scenario"] == stage_a_scn].copy()
    events["scenario"] = egon_scn

    keep = [
        "event_id", "scenario", "cp_id", "site_id", "vehicle_class", "bat_cap",
        "location", "use_case",
        "charging_capacity_nominal", "charging_capacity_grid", "charging_capacity_battery",
        "soc_start", "soc_end", "charging_demand",
        "park_start", "park_end", "drive_start", "drive_end", "consumption",
    ]
    events = events[[c for c in keep if c in events.columns]]

    events.to_sql(
        "egon_hgv_charging_event",
        engine,
        schema="demand",
        if_exists="append",
        index=False,
    )
    logger.info(f"  Wrote {len(events)} charging events (Table 3)")


def _write_profiles(profiles_all, egon_scn, stage_a_scn, engine):
    profiles = profiles_all[profiles_all["scenario"] == stage_a_scn].copy()
    profiles["scenario"] = egon_scn

    # Parse JSON array string → list of floats for PostgreSQL ARRAY column
    profiles["profile"] = profiles["profile"].apply(json.loads)

    keep = ["sector", "scenario", "profile"]
    profiles = profiles[[c for c in keep if c in profiles.columns]]

    profiles.to_sql(
        "egon_hgv_profile",
        engine,
        schema="demand",
        if_exists="append",
        index=False,
    )
    logger.info(f"  Wrote {len(profiles)} profiles (Table 4)")
