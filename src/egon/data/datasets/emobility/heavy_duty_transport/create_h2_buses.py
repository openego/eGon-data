"""
Map demand to H2 buses and write to DB.
"""

from __future__ import annotations

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)

from egon.data.datasets import load_sources_and_targets


def insert_hgv_h2_demand():
    """
    Insert list of hgv H2 demand (one per NUTS3) in database.
    """
    
    sources, targets = load_sources_and_targets("HeavyDutyTransport")
    
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    scenarios = HeavyDutyTransport.scenarios_list
    
    for scenario in scenarios:
        delete_old_entries(scenario)

        hgv_gdf = assign_h2_buses(scenario=scenario)

        hgv_gdf = insert_new_entries(hgv_gdf)

        ts_df = kg_per_year_to_mega_watt(hgv_gdf)

        table = targets.get_table_name("etrago_load_timeseries")
        schema = targets.get_table_schema("etrago_load_timeseries")

        ts_df.to_sql(
            table,
            schema=schema,
            con=db.engine(),
            if_exists="append",
            index=False,
        )


def kg_per_year_to_mega_watt(df: pd.DataFrame | gpd.GeoDataFrame):
    
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    
    ENERGY_VALUE = HeavyDutyTransport.energy_value_h2
    FAC = HeavyDutyTransport.fac
    HOURS_PER_YEAR = HeavyDutyTransport.hours_per_year
    
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
    """
    # Local Loading
    sources, targets = load_sources_and_targets("HeavyDutyTransport")

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
    
    # Dynamic Access: Use key "etrago_load" defined in __init__.py
    table = targets.get_table_name("etrago_load")
    schema = targets.get_table_schema("etrago_load")

    # Insert data to db
    hgv_h2_demand_df.to_sql(
        table,
        engine,
        schema=schema,
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
    
    sources, targets = load_sources_and_targets("HeavyDutyTransport")
    
    # Local Import for Carrier Constant
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    carrier = HeavyDutyTransport.carrier
    # Get dynamic names using keys from __init__.py
    ts_schema = targets.get_table_schema("etrago_load_timeseries")
    ts_table = targets.get_table_name("etrago_load_timeseries")
    
    load_schema = targets.get_table_schema("etrago_load")
    load_table = targets.get_table_name("etrago_load")
    
    
    db.execute_sql(
        f"""
        DELETE FROM {ts_schema}.{ts_table}
        WHERE "load_id" IN (
            SELECT load_id FROM {load_schema}.{load_table}
            WHERE carrier = '{carrier}'
            AND scn_name = '{scenario}'
        )
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM {load_schema}.{load_table}
        WHERE carrier = '{carrier}'
        AND scn_name = '{scenario}'
        """
    )


def assign_h2_buses(scenario: str = "eGon2035"):
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    carrier = HeavyDutyTransport.carrier

    hgv_h2_demand_gdf = read_hgv_h2_demand(scenario=scenario)

    hgv_h2_demand_gdf = db.assign_gas_bus_id(hgv_h2_demand_gdf, scenario, "H2")

    c = {"carrier": carrier}
    hgv_h2_demand_gdf = hgv_h2_demand_gdf.assign(**c)

    hgv_h2_demand_gdf = hgv_h2_demand_gdf.drop(
        columns=["geom", "NUTS0", "NUTS1", "bus_id"], errors="ignore"
    )

    return hgv_h2_demand_gdf


def read_hgv_h2_demand(scenario: str = "eGon2035"):
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    
    srid = HeavyDutyTransport.srid
    srid_buses = HeavyDutyTransport.srid_buses

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

    gdf_vg250 = db.select_geodataframe(sql_vg250, index_col="nuts3", epsg=srid)

    gdf_vg250["geometry"] = gdf_vg250.geom.centroid

    return gpd.GeoDataFrame(
        df.merge(gdf_vg250[["geometry"]], left_index=True, right_index=True),
        crs=gdf_vg250.crs,
    ).to_crs(epsg=srid_buses)