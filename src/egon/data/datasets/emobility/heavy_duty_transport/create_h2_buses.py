"""
Map demand to H2 buses and write to DB.
"""
from __future__ import annotations

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)

DATASET_CFG = config.datasets()["mobility_hgv"]
CARRIER = DATASET_CFG["constants"]["carrier"]
SCENARIOS = DATASET_CFG["constants"]["scenarios"]
ENERGY_VALUE = DATASET_CFG["constants"]["energy_value_h2"]
FAC = DATASET_CFG["constants"]["fac"]
HOURS_PER_YEAR = DATASET_CFG["constants"]["hours_per_year"]


def insert_hgv_h2_demand():
    """
    Insert list of hgv H2 demand (one per NUTS3) in database.
    """
    for scenario in SCENARIOS:
        delete_old_entries(scenario)

        hgv_gdf = assign_h2_buses(scenario=scenario)

        hgv_gdf = insert_new_entries(hgv_gdf)

        ts_df = kg_per_year_to_mega_watt(hgv_gdf)

        ts_df.to_sql(
            "egon_etrago_load_timeseries",
            schema="grid",
            con=db.engine(),
            if_exists="append",
            index=False,
        )


def kg_per_year_to_mega_watt(df: pd.DataFrame | gpd.GeoDataFrame):
    df = df.assign(
        p_set=df.hydrogen_consumption * ENERGY_VALUE * FAC / HOURS_PER_YEAR,
        q_set=np.nan,
        temp_id=1,
    )

    df.p_set = [[p_set] * HOURS_PER_YEAR for p_set in df.p_set]

    logger.debug(str(df.columns))

    df = (
        df.rename(columns={"scenario": "scn_name"})
        .drop(
            columns=[
                "hydrogen_consumption",
                "geometry",
                "bus",
                "carrier",
            ]
        )
        .reset_index(drop=True)
    )

    return pd.DataFrame(df)


def insert_new_entries(hgv_h2_demand_gdf: gpd.GeoDataFrame):
    """
    Insert loads.

    Parameters
    ----------
    hgv_h2_demand_gdf : geopandas.GeoDataFrame
        Load data to insert.

    """
    new_id = db.next_etrago_id("load")
    hgv_h2_demand_gdf["load_id"] = range(
        new_id, new_id + len(hgv_h2_demand_gdf)
    )

    # Add missing columns
    c = {"sign": -1, "type": np.nan, "p_set": np.nan, "q_set": np.nan}
    rename = {"scenario": "scn_name"}
    drop = ["hydrogen_consumption", "geometry"]

    hgv_h2_demand_df = pd.DataFrame(
        hgv_h2_demand_gdf.assign(**c)
        .rename(columns=rename)
        .drop(columns=drop)
        .reset_index(drop=True)
    )

    engine = db.engine()
    # Insert data to db
    hgv_h2_demand_df.to_sql(
        "egon_etrago_load",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )

    return hgv_h2_demand_gdf


def delete_old_entries(scenario: str):
    """
    Delete loads and load timeseries.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    """
    # Clean tables
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load_timeseries
        WHERE "load_id" IN (
            SELECT load_id FROM grid.egon_etrago_load
            WHERE carrier = '{CARRIER}'
            AND scn_name = '{scenario}'
        )
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load
        WHERE carrier = '{CARRIER}'
        AND scn_name = '{scenario}'
        """
    )


def assign_h2_buses(scenario: str = "eGon2035"):
    hgv_h2_demand_gdf = read_hgv_h2_demand(scenario=scenario)

    hgv_h2_demand_gdf = db.assign_gas_bus_id(
        hgv_h2_demand_gdf, scenario, "H2_grid"
    )

    # Add carrier
    c = {"carrier": CARRIER}
    hgv_h2_demand_gdf = hgv_h2_demand_gdf.assign(**c)

    # Remove useless columns
    hgv_h2_demand_gdf = hgv_h2_demand_gdf.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"], errors="ignore"
    )

    return hgv_h2_demand_gdf


def read_hgv_h2_demand(scenario: str = "eGon2035"):
    with db.session_scope() as session:
        query = session.query(
            EgonHeavyDutyTransportVoronoi.nuts3,
            EgonHeavyDutyTransportVoronoi.scenario,
            EgonHeavyDutyTransportVoronoi.hydrogen_consumption,
        ).filter(EgonHeavyDutyTransportVoronoi.scenario == scenario)

    df = pd.read_sql(query.statement, query.session.bind, index_col="nuts3")

    sql_vg250 = """
                SELECT nuts as nuts3, geometry as geom
                FROM boundaries.vg250_krs
                WHERE gf = 4
                """

    srid = DATASET_CFG["tables"]["srid"]

    gdf_vg250 = db.select_geodataframe(sql_vg250, index_col="nuts3", epsg=srid)

    gdf_vg250["geometry"] = gdf_vg250.geom.centroid

    srid_buses = DATASET_CFG["tables"]["srid_buses"]

    return gpd.GeoDataFrame(
        df.merge(gdf_vg250[["geometry"]], left_index=True, right_index=True),
        crs=gdf_vg250.crs,
    ).to_crs(epsg=srid_buses)
