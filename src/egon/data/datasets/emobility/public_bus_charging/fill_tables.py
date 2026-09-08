"""
Read the public bus charging input files and populate
``demand.egon_ev_bus_charging_depot``.

Input files ship in the data bundle and are resolved relative to the
egon-data working directory, like every other ``data_bundle_egon_data``
path::

    data_bundle_egon_data/bus_charging/depots.gpkg
    data_bundle_egon_data/bus_charging/<per-scenario csv.gz>

``depots.gpkg`` is shared by all scenarios and holds only scenario-neutral
attributes. Its ``peak_power_mw`` / ``annual_energy_mwh`` / ``avg_power_mw``
columns, if present, carry **reGon2045** values for every depot and are
dropped on read -- using them as scenario-neutral metadata would assign
every depot its 2045 voltage level.

Peak load and annual demand are recomputed per scenario from that
scenario's own series.
"""

from pathlib import Path

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.public_bus_charging.db_classes import (
    EgonEvBusChargingDepot,
)
from egon.data.datasets.emobility.public_bus_charging.scenarios import (
    SCENARIO_FILES,
    active_scenarios,
)

#: Length of a full year at hourly resolution.
N_TIMESTEPS = 8760

#: gpkg columns holding reGon2045 values -- never scenario-neutral.
GPKG_SCENARIO_COLUMNS = ("peak_power_mw", "annual_energy_mwh", "avg_power_mw")


def _input_dir() -> Path:
    from egon.data.datasets.emobility.public_bus_charging import (
        PublicBusCharging,
    )

    return Path(PublicBusCharging.sources.files["bus_input_dir"])


def _read_depots() -> gpd.GeoDataFrame:
    """Read the shared depot geometries and attributes, in EPSG:3035."""
    path = _input_dir() / "depots.gpkg"
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found. The public bus input data ships in the data "
            "bundle under data_bundle_egon_data/bus_charging/."
        )

    depots = gpd.read_file(path)
    dropped = [c for c in GPKG_SCENARIO_COLUMNS if c in depots.columns]
    if dropped:
        # These hold reGon2045 values for every depot; peak load and annual
        # demand are recomputed per scenario instead.
        depots = depots.drop(columns=list(dropped))
        logger.debug(f"  dropped reGon2045-valued gpkg columns: {dropped}")

    depots = depots.to_crs(epsg=3035)
    if depots.geometry.name != "geom":
        depots = depots.rename_geometry("geom")
    depots = depots.drop(columns=[c for c in ["fid"] if c in depots.columns])
    depots["depot_id"] = depots["depot_id"].astype(str)

    keep = [
        c
        for c in ["depot_id", "depot_name", "depot_type", "fleet_size", "geom"]
        if c in depots.columns
    ]
    return depots[keep]


def _read_timeseries(scenario: str) -> pd.DataFrame:
    """Read one scenario's wide hourly series, in MW.

    Missing values are replaced with 0 and reported. The reGon2037 and
    reGon2045 inputs carry 1821 NaN each across 34 depots, all at Sunday
    00:00-03:00 -- a service-day boundary artefact in the source timetable
    join rather than noise. Zero-filling understates those depots' demand,
    so the substitution is logged on every run and must not be allowed to
    become invisible; remove it once the input data is corrected.
    """
    path = _input_dir() / SCENARIO_FILES[scenario]
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found for scenario {scenario!r}. The public bus "
            "input data ships in the data bundle under "
            "data_bundle_egon_data/bus_charging/."
        )

    wide = pd.read_csv(path, index_col=0)
    wide.columns = wide.columns.astype(str)

    if len(wide) != N_TIMESTEPS:
        raise ValueError(
            f"{path} has {len(wide)} timesteps, expected {N_TIMESTEPS}."
        )

    per_depot = wide.isna().sum()
    affected = per_depot[per_depot > 0]
    if len(affected):
        logger.warning(
            f"  {scenario}: zero-filling {int(per_depot.sum())} missing "
            f"values across {len(affected)} depots. This understates their "
            f"demand -- see docs/adr/0003. Depots: {list(affected.index)}"
        )
        wide = wide.fillna(0.0)

    values = wide.to_numpy(dtype="float64")
    if not np.isfinite(values).all():
        raise ValueError(
            f"{path} contains non-finite values that are not NaN (inf); "
            "refusing to write them to p_set."
        )
    if (values < 0).any():
        n_neg = int((values < 0).sum())
        raise ValueError(
            f"{path} contains {n_neg} negative values; bus charging demand "
            "cannot be negative (there is no flexibility in this dataset)."
        )
    return wide


def fill_depot_table():
    """Populate demand.egon_ev_bus_charging_depot for all active scenarios.

    Spatial columns (``bus_id``, ``mv_grid_id``, ``voltage_level``) are left
    NULL here and filled by :func:`spatial_assignment
    <egon.data.datasets.emobility.public_bus_charging.spatial_assignment.spatial_assignment>`.
    """
    depots = _read_depots()
    logger.info(f"Read {len(depots)} depots from depots.gpkg")

    scenarios = active_scenarios()
    if not scenarios:
        logger.warning(
            "No configured scenario carries public bus data "
            f"(bus data exists for {list(SCENARIO_FILES)}). Nothing to do."
        )
        return

    for scenario in scenarios:
        wide = _read_timeseries(scenario)

        frame = pd.DataFrame(
            {
                "depot_id": wide.columns,
                "scenario": scenario,
                "peak_load_mw": wide.max(axis=0).to_numpy(),
                # 1 h timesteps, so summing MW over the year gives MWh.
                "annual_demand_mwh": wide.sum(axis=0).to_numpy(),
            }
        )
        frame["p_set"] = [
            wide[depot].to_numpy(dtype="float64").tolist()
            for depot in wide.columns
        ]

        merged = frame.merge(depots, on="depot_id", how="left")
        missing = merged["geom"].isna()
        if missing.any():
            raise ValueError(
                f"{int(missing.sum())} depots in the {scenario} time series "
                "have no entry in depots.gpkg: "
                f"{list(merged.loc[missing, 'depot_id'])}"
            )

        gdf = gpd.GeoDataFrame(merged, geometry="geom", crs="EPSG:3035")
        db.execute_sql(
            f"""
            DELETE FROM demand.egon_ev_bus_charging_depot
            WHERE scenario = '{scenario}';
            """
        )
        # Insert via the ORM rather than GeoDataFrame.to_postgis: the latter
        # writes through COPY, which renders a Python list as "[...]" and
        # PostgreSQL rejects it as a malformed array literal (arrays need
        # "{...}"). The ORM binds p_set as a real ARRAY parameter.
        gdf["geom_wkt"] = gdf["geom"].apply(lambda g: g.wkt)
        records = [
            {
                "depot_id": row.depot_id,
                "scenario": row.scenario,
                "depot_name": getattr(row, "depot_name", None),
                "depot_type": getattr(row, "depot_type", None),
                "fleet_size": (
                    None
                    if pd.isna(getattr(row, "fleet_size", np.nan))
                    else float(row.fleet_size)
                ),
                "geom": f"SRID=3035;{row.geom_wkt}",
                "peak_load_mw": float(row.peak_load_mw),
                "annual_demand_mwh": float(row.annual_demand_mwh),
                "p_set": row.p_set,
            }
            for row in gdf.itertuples()
        ]
        with db.session_scope() as session:
            session.bulk_insert_mappings(EgonEvBusChargingDepot, records)

        logger.info(
            f"  {scenario}: wrote {len(records)} depots, "
            f"{gdf['annual_demand_mwh'].sum() / 1e6:.3f} TWh, "
            f"peak sum {gdf['peak_load_mw'].sum():.1f} MW"
        )
