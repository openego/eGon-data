from geoalchemy2.types import Geometry
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)
from egon.data.datasets.etrago_helpers import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)
from egon.data.datasets.etrago_setup import link_geom_from_buses

DATASET_CFG = config.datasets()["mobility_hgv"]


def assign_h2_buses(scenario: str = "eGon2035"):
    hgv_h2_demand_gdf = read_hgv_h2_demand(scenario=scenario)

    hgv_h2_demand_copy_gdf = hgv_h2_demand_gdf.copy()

    hgv_h2_demand_gdf = db.assign_gas_bus_id(
        hgv_h2_demand_gdf, scenario, "H2_grid"
    )

    hgv_h2_demand_saltcavern_gdf = db.assign_gas_bus_id(
        hgv_h2_demand_copy_gdf, scenario, "H2_saltcavern"
    )

    saltcavern_buses_sql = f"""
                            SELECT * FROM grid.egon_etrago_bus
                            WHERE carrier = 'H2_saltcavern'
                            AND scn_name = '{scenario}'
                            """

    srid = DATASET_CFG["tables"]["srid"]

    saltcavern_buses_gdf = db.select_geodataframe(
        saltcavern_buses_sql,
        index_col="bus_id",
        epsg=srid,
    )

    nearest_saltcavern_buses = saltcavern_buses_gdf.loc[
        hgv_h2_demand_saltcavern_gdf["bus_id"]
    ].geometry

    hgv_h2_demand_saltcavern_gdf[
        "distance"
    ] = hgv_h2_demand_saltcavern_gdf.to_crs(epsg=srid).distance(
        nearest_saltcavern_buses, align=False
    )

    new_connections = hgv_h2_demand_saltcavern_gdf.loc[
        hgv_h2_demand_saltcavern_gdf["distance"] <= 10000
    ]

    num_new_connections = len(new_connections)

    if num_new_connections > 0:

        carrier = "H2_hgv_load"
        target = {"schema": "grid", "table": "egon_etrago_bus"}
        bus_gdf = initialise_bus_insertion(carrier, target, scenario=scenario)

        bus_gdf["geom"] = new_connections["geometry"]

        bus_gdf = finalize_bus_insertion(
            bus_gdf, carrier, target, scenario=scenario
        )

        # Delete existing buses
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_link
            WHERE scn_name = '{scenario}'
            AND carrier = '{carrier}'
            """
        )

        # initalize dataframe for new buses
        grid_links = pd.DataFrame(
            columns=["scn_name", "link_id", "bus0", "bus1", "carrier"]
        )

        grid_links["bus0"] = hgv_h2_demand_gdf.loc[bus_gdf.index]["bus_id"]
        grid_links["bus1"] = bus_gdf["bus_id"]
        grid_links["p_nom"] = 1e9
        grid_links["carrier"] = carrier
        grid_links["scn_name"] = scenario

        cavern_links = grid_links.copy()

        cavern_links["bus0"] = new_connections["bus_id"]

        engine = db.engine()
        for table in [grid_links, cavern_links]:
            new_id = db.next_etrago_id("link")
            table["link_id"] = range(new_id, new_id + len(table))

            link_geom_from_buses(table, scenario).to_postgis(
                "egon_etrago_link",
                engine,
                schema="grid",
                index=False,
                if_exists="append",
                dtype={"topo": Geometry()},
            )

        hgv_h2_demand_gdf.loc[bus_gdf.index, "bus"] = bus_gdf["bus_id"]

    # Add carrier
    c = {"carrier": "H2"}
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
