"""
Assign ``bus_id``, ``mv_grid_id`` and ``voltage_level`` to bus depots.

Voltage level follows the canonical eGon-data peak-load thresholds, matching
:func:`egon.data.datasets.industry.temporal.identify_voltage_level`::

    peak <= 0.1 MW -> 7 (LV)
    peak >  0.1 MW -> 6
    peak >  0.2 MW -> 5 (MV)
    peak >  5.5 MW -> 4
    peak >  20  MW -> 3 (HV)
    peak > 120  MW -> 1 (EHV)

Level 2 is never derived from load -- it only ever comes from MaStR's
``Spannungsebene`` string, which this dataset does not use.

The level is computed per (depot, scenario) from that scenario's own peak,
so a depot may sit at a different level in each scenario; connection
upgrades between scenarios are not modelled (``docs/adr/0001``).

Bus assignment uses two layers, following
:mod:`egon.data.datasets.power_plants`:

* voltage level 3-7 -> ``grid.egon_mv_grid_district`` (EPSG:3035)
* voltage level 1-2 -> ``grid.egon_ehv_substation_voronoi`` (EPSG:4326)

An MV grid district is the Voronoi cell of exactly one HV/MV substation and
therefore contains exactly one eTraGo bus, so every depot in a district
necessarily shares that district's bus -- there is no finer bus to choose.

Depots that fall in no district are **dropped**, not snapped to the nearest
one. In a ``--dataset-boundary`` test run the district table covers only the
test region, so a nearest-match fallback would silently pile every
out-of-region depot onto the boundary's edge buses.
"""

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.emobility.public_bus_charging.scenarios import (
    active_scenarios,
)

#: Voltage levels served by the MV grid districts.
MV_LEVELS = (3, 4, 5, 6, 7)

#: Voltage levels served by the EHV substation Voronoi cells.
EHV_LEVELS = (1, 2)


def assign_voltage_level(peak_load_mw: pd.Series) -> pd.Series:
    """Map peak load [MW] to an eGon-data voltage level."""
    peak = pd.Series(peak_load_mw, dtype="float64")
    level = pd.Series(np.nan, index=peak.index)
    level[peak <= 0.1] = 7
    level[peak > 0.1] = 6
    level[peak > 0.2] = 5
    level[peak > 5.5] = 4
    level[peak > 20.0] = 3
    level[peak > 120.0] = 1
    return level.astype("Int64")


def _join(depots: gpd.GeoDataFrame, cells: gpd.GeoDataFrame) -> pd.DataFrame:
    """Inner point-in-polygon join of depots onto bus cells."""
    joined = gpd.sjoin(
        depots[["depot_id", "geom"]],
        cells[["bus_id", "geom"]],
        how="inner",
        predicate="within",
    )
    # A depot on a shared boundary can match more than one cell; keep one.
    return (
        joined[["depot_id", "bus_id"]]
        .drop_duplicates(subset="depot_id")
        .reset_index(drop=True)
    )


def spatial_assignment():
    """Fill bus_id, mv_grid_id and voltage_level for all active scenarios."""
    scenarios = active_scenarios()
    if not scenarios:
        logger.warning("No active scenario carries public bus data.")
        return

    mv_districts = db.select_geodataframe(
        "SELECT bus_id, geom FROM grid.egon_mv_grid_district",
        geom_col="geom",
        epsg=3035,
    )
    ehv_cells = db.select_geodataframe(
        "SELECT bus_id, geom FROM grid.egon_ehv_substation_voronoi",
        geom_col="geom",
        epsg=3035,
    )
    logger.info(
        f"{len(mv_districts)} MV grid districts, "
        f"{len(ehv_cells)} EHV Voronoi cells"
    )

    for scenario in scenarios:
        depots = db.select_geodataframe(
            f"""
            SELECT depot_id, peak_load_mw, geom
            FROM demand.egon_ev_bus_charging_depot
            WHERE scenario = '{scenario}'
            """,
            geom_col="geom",
            epsg=3035,
        )
        if depots.empty:
            logger.warning(f"  {scenario}: no depots to assign")
            continue

        depots["voltage_level"] = assign_voltage_level(depots["peak_load_mw"])

        mv_part = depots[depots["voltage_level"].isin(MV_LEVELS)]
        ehv_part = depots[depots["voltage_level"].isin(EHV_LEVELS)]

        assigned = _join(mv_part, mv_districts)
        assigned["mv_grid_id"] = assigned["bus_id"]

        if not ehv_part.empty:
            # Unexercised by the current input data (no depot exceeds
            # 120 MW), so make it loud the first time real data reaches it.
            logger.warning(
                f"  {scenario}: {len(ehv_part)} depots at voltage level "
                f"{sorted(ehv_part['voltage_level'].dropna().unique())} are "
                "assigned via the EHV substation Voronoi -- this path has no "
                "coverage in the current input data, verify the result."
            )
            ehv_assigned = _join(ehv_part, ehv_cells)
            ehv_assigned["mv_grid_id"] = pd.NA
            assigned = pd.concat([assigned, ehv_assigned], ignore_index=True)

        result = depots[["depot_id", "voltage_level"]].merge(
            assigned, on="depot_id", how="left"
        )
        dropped = result[result["bus_id"].isna()]
        if len(dropped):
            logger.info(
                f"  {scenario}: dropping {len(dropped)} of {len(result)} "
                "depots that fall in no grid district (expected when "
                "--dataset-boundary restricts the run to a test region)"
            )
        result = result[result["bus_id"].notna()]

        with db.session_scope() as session:
            session.execute(
                """
                UPDATE demand.egon_ev_bus_charging_depot AS d
                SET bus_id = v.bus_id,
                    mv_grid_id = v.mv_grid_id,
                    voltage_level = v.voltage_level
                FROM (
                    SELECT
                        unnest(:depot_ids) AS depot_id,
                        unnest(:bus_ids) AS bus_id,
                        unnest(:mv_grid_ids) AS mv_grid_id,
                        unnest(:voltage_levels) AS voltage_level
                ) AS v
                WHERE d.depot_id = v.depot_id AND d.scenario = :scenario
                """,
                {
                    "depot_ids": list(result["depot_id"]),
                    "bus_ids": [int(b) for b in result["bus_id"]],
                    "mv_grid_ids": [
                        None if pd.isna(m) else int(m)
                        for m in result["mv_grid_id"]
                    ],
                    "voltage_levels": [
                        int(v) for v in result["voltage_level"]
                    ],
                    "scenario": scenario,
                },
            )
            # Depots outside every district keep NULL bus_id and are not
            # written to eTraGo; remove them so the table only holds rows
            # that made it into the model.
            session.execute(
                """
                DELETE FROM demand.egon_ev_bus_charging_depot
                WHERE scenario = :scenario AND bus_id IS NULL
                """,
                {"scenario": scenario},
            )

        levels = (
            result["voltage_level"].value_counts().sort_index().to_dict()
        )
        logger.info(
            f"  {scenario}: assigned {len(result)} depots to "
            f"{result['bus_id'].nunique()} buses, voltage levels {levels}"
        )
