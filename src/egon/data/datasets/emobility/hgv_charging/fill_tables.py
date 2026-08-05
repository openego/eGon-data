"""
Read the precomputed HGV charging demand input files and populate Tables 1–4.

Input files live in one subfolder per scenario under the directory configured
via datasets.yml:
  mobility_hgv_charging.original_data.sources.hgv_input_dir

For each scenario the input-data string (e.g. "C 2037") is mapped to the
egon-data scenario name ("reGon2037") before writing to the DB.

The input data covers all of Germany regardless of --dataset-boundary, so
sites outside the configured boundary are dropped here (before charging
points/events/profiles are written) -- otherwise spatial_assignment.py would
force-match every out-of-region site to the nearest in-region MV grid bus.
"""

import json
from pathlib import Path

from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.hgv_charging.scenarios import active_scenario_map


def _input_dir() -> Path:
    cfg = config.datasets()["mobility_hgv_charging"]
    return Path(cfg["original_data"]["sources"]["hgv_input_dir"])


def _boundary_geom():
    """Return the configured --dataset-boundary geometry, or None for 'Everything'."""
    boundary = config.settings()["egon-data"]["--dataset-boundary"]
    if boundary == "Everything":
        return None
    sta = db.select_geodataframe(
        f"SELECT geometry FROM boundaries.vg250_sta WHERE gen = '{boundary}'",
        geom_col="geometry",
        epsg=3035,
    )
    return sta.union_all() if hasattr(sta, "union_all") else sta.unary_union


def fill_hgv_tables():
    """Read input files and write Tables 1–4 for all active scenarios."""
    input_dir = _input_dir()
    engine = db.engine()
    boundary_geom = _boundary_geom()

    for egon_scn, data_scn in active_scenario_map().items():
        scenario_dir = input_dir / egon_scn
        logger.info(f"Filling HGV tables for scenario {egon_scn} ({data_scn}) from {scenario_dir}")

        sites_all = gpd.read_file(scenario_dir / "sites.gpkg")
        cps_all = pd.read_csv(scenario_dir / "charging_points.csv")
        events_all = pd.read_csv(scenario_dir / "charging_events.csv")
        profiles_all = pd.read_csv(scenario_dir / "profiles.csv")

        if boundary_geom is not None:
            sites_all = sites_all.to_crs(3035)
            n_before = len(sites_all)
            sites_all = sites_all[sites_all.within(boundary_geom)]
            logger.info(
                f"  Restricted sites to configured boundary: "
                f"{len(sites_all)}/{n_before} kept"
            )
            keep_ids = set(sites_all["site_id"])
            cps_all = cps_all[cps_all["site_id"].isin(keep_ids)]
            events_all = events_all[events_all["site_id"].isin(keep_ids)]

        _write_sites(sites_all, egon_scn, data_scn, engine)
        _write_charging_points(cps_all, egon_scn, data_scn, engine)
        _write_charging_events(events_all, egon_scn, data_scn, engine)
        _write_profiles(profiles_all, egon_scn, data_scn, engine)


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
        "location", "use_case", "slot_id",
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
