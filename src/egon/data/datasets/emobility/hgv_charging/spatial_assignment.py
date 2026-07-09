"""
Spatial assignment for HGV charging sites.

Assigns mv_grid_id, bus_id, and voltage_level to demand.egon_hgv_charging_site
for each scenario.

Voltage level thresholds follow industry/temporal.py:identify_voltage_level():
  p_set ≤ 0.1 MW → 7 (LV)
  p_set > 0.1 MW → 6
  p_set > 0.2 MW → 5
  p_set > 5.5 MW → 4
  p_set > 20  MW → 3 (HV)
  p_set > 120 MW → 1 (EHV)

Bus assignment: all voltage levels use grid.egon_mv_grid_district (same as
industry/temporal.py:identify_bus — no separate EHV Voronoi for demand loads).
"""

import numpy as np
from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets.emobility.hgv_charging import active_scenario_map


def _assign_voltage_level(p_set_mw: pd.Series) -> pd.Series:
    vl = pd.Series(np.nan, index=p_set_mw.index)
    vl[p_set_mw <= 0.1] = 7
    vl[p_set_mw > 0.1] = 6
    vl[p_set_mw > 0.2] = 5
    vl[p_set_mw > 5.5] = 4
    vl[p_set_mw > 20.0] = 3
    vl[p_set_mw > 120.0] = 1
    return vl.astype("Int64")


def spatial_assignment():
    """Assign mv_grid_id, bus_id, voltage_level to Table 1 for all active scenarios."""
    mv_districts = db.select_geodataframe(
        "SELECT bus_id, geom FROM grid.egon_mv_grid_district",
        geom_col="geom",
        epsg=3035,
    )

    for egon_scn in active_scenario_map():
        logger.info(f"Spatial assignment for scenario {egon_scn}")

        sites = db.select_geodataframe(
            f"""
            SELECT site_id, p_set_aggregated_mw, geom
            FROM demand.egon_hgv_charging_site
            WHERE scenario = '{egon_scn}'
            """,
            geom_col="geom",
            epsg=3035,
        )

        # Assign voltage_level from site grid-connection capacity
        sites["voltage_level"] = _assign_voltage_level(sites["p_set_aggregated_mw"])

        # All sites get bus_id from MV grid district spatial join
        joined = gpd.sjoin(
            sites[["site_id", "voltage_level", "geom"]],
            mv_districts[["bus_id", "geom"]],
            how="left",
            predicate="within",
        )

        # For sites not contained within any district, fall back to nearest
        missing = joined[joined["bus_id"].isna()].index
        if len(missing) > 0:
            logger.warning(
                f"  {len(missing)} sites not within any MV grid district — "
                "falling back to nearest"
            )
            nearest = gpd.sjoin_nearest(
                sites.loc[missing, ["site_id", "geom"]],
                mv_districts[["bus_id", "geom"]],
                how="left",
            )
            joined.loc[missing, "bus_id"] = nearest["bus_id"].values

        joined = joined.rename(columns={"bus_id": "bus_id_assigned"})

        # Log voltage level distribution
        for lvl in [1, 3, 4, 5, 6, 7]:
            count = (sites["voltage_level"] == lvl).sum()
            if count:
                logger.info(f"  voltage_level {lvl}: {count} sites")

        # Update Table 1
        with db.session_scope() as session:
            for _, row in joined.iterrows():
                session.execute(
                    """
                    UPDATE demand.egon_hgv_charging_site
                    SET mv_grid_id = :mv_grid_id,
                        bus_id = :bus_id,
                        voltage_level = :voltage_level
                    WHERE site_id = :site_id AND scenario = :scenario
                    """,
                    {
                        "mv_grid_id": int(row["bus_id_assigned"]),
                        "bus_id": int(row["bus_id_assigned"]),
                        "voltage_level": int(row["voltage_level"]),
                        "site_id": int(row["site_id"]),
                        "scenario": egon_scn,
                    },
                )

        logger.info(
            f"  Assigned mv_grid_id/bus_id/voltage_level for {len(joined)} sites"
        )
