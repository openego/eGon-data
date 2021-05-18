import geopandas as gpd
import pandas as pd
import egon.data.config
from egon.data import db
from egon.data.processing.demandregio.temporal import calc_load_curve


def loadarea_peak_load(scenario):
    """
    Determine CTS peak load for each census cell


    Parameters
    ----------
    scenario : str
        Scenario name.

    """

    sources = (egon.data.config.datasets()
               ['electrical_load_curves_cts']['sources'])

    # Select demands per cts branch and nuts3-region
    demands_nuts = db.select_dataframe(
            f"""SELECT nuts3, wz, demand
            FROM {sources['demandregio_cts']['schema']}.
            {sources['demandregio_cts']['table']}
            WHERE scenario = '{scenario}'
            AND demand > 0
            AND wz IN (
                SELECT wz FROM
                {sources['demandregio_wz']['schema']}.
                {sources['demandregio_wz']['table']}
                WHERE sector = 'CTS')
            """).set_index(['nuts3', 'wz'])

    # Select cts demands per zensus cell including nuts3-region and substation
    demands_zensus = db.select_geodataframe(
            f"""SELECT a.zensus_population_id, a.demand,
            b.vg250_nuts3 as nuts3,
            c.subst_id,
            d.geom
            FROM {sources['zensus_electricity']['schema']}.
            {sources['zensus_electricity']['table']} a
            INNER JOIN
            {sources['map_vg250']['schema']}.{sources['map_vg250']['table']} b
            ON (a.zensus_population_id = b.zensus_population_id)
            INNER JOIN
            {sources['map_grid_districts']['schema']}.
            {sources['map_grid_districts']['table']} c
            ON (a.zensus_population_id = c.zensus_population_id)
            INNER JOIN
            "society".
            "destatis_zensus_population_per_ha" d
            ON (a.zensus_population_id = d.id)
            WHERE a.scenario = '{scenario}'
            AND a.sector = 'service'
            """, index_col='zensus_population_id')

    # Calculate shares of cts branches per nuts3-region
    nuts3_share_wz = demands_nuts.groupby('nuts3').apply(
        lambda grp: grp/grp.sum())

    # Apply shares of cts branches for each zensus cell inside the nuts3 region
    for wz in demands_nuts.index.get_level_values('wz').unique():
        demands_zensus[wz] = 0
        share = nuts3_share_wz[
            nuts3_share_wz.index.get_level_values('wz') == wz
            ].reset_index().set_index('nuts3').demand
        idx = demands_zensus.index[demands_zensus.nuts3.isin(share.index)]
        demands_zensus.loc[idx,wz] = share[
            demands_zensus.nuts3[idx].values].values

    # Retrieve CTS sector peak load for each census cell
    wz_shares = demands_zensus.drop(
        ["demand", "nuts3", "subst_id", "geom"],
        axis=1)
    cts_peak_loads_cells = calc_load_curve(wz_shares,
                                           demands_zensus["demand"],
                                           max_only=True)
    cts_peak_loads_cells = pd.concat(
        [
            demands_zensus[["demand", "subst_id", "geom"]],
            cts_peak_loads_cells.to_frame("peak_load")
        ], axis=1)

    # Identify CTS load areas of neighboring census cells with CTS load
    # Aggregate annual demand and peak load for each of the load areas
    # respecting the MV grid district boundaries
    load_area_polygons_tmp = []
    for name, group in cts_peak_loads_cells.groupby("subst_id"):
        load_areas_polygons_in_group = (group["geom"].buffer(10).unary_union).buffer(-10)
        load_area_polygons_tmp.append(load_areas_polygons_in_group)
    load_areas_polygons = gpd.GeoDataFrame(geometry=load_area_polygons_tmp,
                                  crs=3035)

    load_areas_tmp = gpd.GeoDataFrame(gpd.sjoin(cts_peak_loads_cells, load_areas_polygons, how="right", op="within").reset_index())
    load_areas = load_areas_tmp.dissolve(by="index", aggfunc={"peak_load": "sum", "demand": "sum"})
    load_areas = load_areas.explode().reset_index()


