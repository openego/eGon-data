"""The central module containing all code dealing with processing
timeseries data using demandregio

"""

from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.electricity_demand.temporal import calc_load_curve
import egon.data.config

Base = declarative_base()


def identify_voltage_level(df):
    """Identify the voltage_level of a grid component based on its peak load
    and defined thresholds.


    Parameters
    ----------
    df : pandas.DataFrame
        Data frame containing information about peak loads


    Returns
    -------
    pandas.DataFrame
        Data frame with an additional column with voltage level

    """

    df["voltage_level"] = np.nan

    # Identify voltage_level for every demand area taking thresholds into
    # account which were defined in the eGon project
    df.loc[df["peak_load"] <= 0.1, "voltage_level"] = 7
    df.loc[df["peak_load"] > 0.1, "voltage_level"] = 6
    df.loc[df["peak_load"] > 0.2, "voltage_level"] = 5
    df.loc[df["peak_load"] > 5.5, "voltage_level"] = 4
    df.loc[df["peak_load"] > 20, "voltage_level"] = 3
    df.loc[df["peak_load"] > 120, "voltage_level"] = 1

    return df


def identify_bus(load_curves, demand_area):
    """Identify the grid connection point for a consumer by determining its
    grid level based on the time series' peak load and the spatial
    intersection to mv grid districts or ehv voronoi cells.


    Parameters
    ----------
    load_curves : pandas.DataFrame
        Demand timeseries per demand area (e.g. osm landuse area, industrial
        site)

    demand_area: pandas.DataFrame
        Dataframe with id and geometry of areas where an industrial demand
        is assigned to, such as osm landuse areas or industrial sites.

    Returns
    -------
    pandas.DataFrame
        Aggregated industrial demand timeseries per bus

    """

    sources = egon.data.config.datasets()["electrical_load_curves_industry"][
        "sources"
    ]

    # Select mv griddistrict
    griddistrict = db.select_geodataframe(
        f"""SELECT bus_id, geom FROM
                {sources['egon_mv_grid_district']['schema']}.
                {sources['egon_mv_grid_district']['table']}""",
        geom_col="geom",
        epsg=3035,
    )

    # Initialize dataframe to identify peak load per demand area (e.g. osm
    # landuse area or industrial site)
    peak = pd.DataFrame(columns=["id", "peak_load"])
    peak["id"] = load_curves.max(axis=0).index
    peak["peak_load"] = load_curves.max(axis=0).values

    peak = identify_voltage_level(peak)

    # Assign bus_id to demand area by merging landuse and peak df
    peak = pd.merge(demand_area, peak, right_on="id", left_index=True)

    # Identify all demand areas connected to HVMV buses
    peak_hv = peak[peak["voltage_level"] > 1]

    # Perform a spatial join between the centroid of the demand area and mv
    # grid districts to identify grid connection point
    peak_hv["centroid"] = peak_hv["geom"].centroid
    peak_hv = peak_hv.set_geometry("centroid")
    peak_hv_c = gpd.sjoin(peak_hv, griddistrict, how="inner", op="intersects")

    # Perform a spatial join between the polygon of the demand area  and mv
    # grid districts to ensure every area got assign to a bus
    peak_hv_p = peak_hv[~peak_hv.isin(peak_hv_c)].dropna().set_geometry("geom")
    peak_hv_p = gpd.sjoin(
        peak_hv_p, griddistrict, how="inner", op="intersects"
    ).drop_duplicates(subset=["id"])

    # Bring both dataframes together
    peak_bus = pd.concat([peak_hv_c, peak_hv_p], ignore_index=True)

    # Select ehv voronoi
    ehv_voronoi = db.select_geodataframe(
        f"""SELECT bus_id, geom FROM
                {sources['egon_mv_grid_district']['schema']}.
                {sources['egon_mv_grid_district']['table']}""",
        geom_col="geom",
        epsg=3035,
    )

    # Identify all demand areas connected to EHV buses
    peak_ehv = peak[peak["voltage_level"] == 1]

    # Perform a spatial join between the centroid of the demand area and ehv
    # voronoi to identify grid connection point
    peak_ehv["centroid"] = peak_ehv["geom"].centroid
    peak_ehv = peak_ehv.set_geometry("centroid")
    peak_ehv = gpd.sjoin(peak_ehv, ehv_voronoi, how="inner", op="intersects")

    # Bring both dataframes together
    peak_bus = pd.concat([peak_bus, peak_ehv], ignore_index=True)

    # Combine dataframes to bring loadcurves and bus id together
    curves_da = pd.merge(
        load_curves.T,
        peak_bus[["bus_id", "id", "geom"]],
        left_index=True,
        right_on="id",
    )

    return curves_da


def calc_load_curves_ind_osm(scenario):
    """Temporal disaggregate electrical demand per osm industrial landuse
    area.


    Parameters
    ----------
    scenario : str
        Scenario name.

    Returns
    -------
    pandas.DataFrame
        Demand timeseries of industry allocated to osm landuse areas and
        aggregated per substation id

    """

    sources = egon.data.config.datasets()["electrical_load_curves_industry"][
        "sources"
    ]

    # Select demands per industrial branch and osm landuse area
    demands_osm_area = db.select_dataframe(
        f"""SELECT osm_id, wz, demand
            FROM {sources['osm']['schema']}.
            {sources['osm']['table']}
            WHERE scenario = '{scenario}'
            AND demand > 0
            """
    ).set_index(["osm_id", "wz"])

    # Select industrial landuse polygons as demand area
    demand_area = db.select_geodataframe(
        f"""SELECT id, geom FROM
                {sources['osm_landuse']['schema']}.
                {sources['osm_landuse']['table']}
                WHERE sector = 3 """,
        index_col="id",
        geom_col="geom",
        epsg=3035,
    )

    # Calculate shares of industrial branches per osm area
    osm_share_wz = demands_osm_area.groupby(["osm_id"], as_index=False).apply(
        lambda grp: grp / grp.sum()
    )

    osm_share_wz.reset_index(inplace=True)

    share_wz_transpose = pd.DataFrame(
        index=osm_share_wz.osm_id.unique(), columns=osm_share_wz.wz.unique()
    )
    share_wz_transpose.index.rename("osm_id", inplace=True)

    for wz in share_wz_transpose.columns:
        share_wz_transpose[wz] = (
            osm_share_wz[osm_share_wz.wz == wz].set_index("osm_id").demand
        )

    # Rename columns to bring it in line with demandregio data
    share_wz_transpose.rename(columns={1718: 17}, inplace=True)

    # Calculate industrial annual demand per osm area
    annual_demand_osm = demands_osm_area.groupby("osm_id").demand.sum()

    # Return electrical load curves per osm industrial landuse area

    load_curves = calc_load_curve(share_wz_transpose, scenario, annual_demand_osm)

    curves_da = identify_bus(load_curves, demand_area)

    # Group all load curves per bus
    curves_bus = (
        curves_da.drop(["id", "geom"], axis=1)
        .fillna(0)
        .groupby("bus_id")
        .sum()
    )

    # Initalize pandas.DataFrame for export to database
    load_ts_df = pd.DataFrame(index=curves_bus.index, columns=["p_set"])

    # Insert time series data to df as an array
    load_ts_df.p_set = curves_bus.values.tolist()

    # Create Dataframe to store time series individually
    curves_individual_interim = (
        curves_da.drop(["bus_id", "geom"], axis=1).fillna(0)
    ).set_index("id")
    curves_individual = curves_da[["id", "bus_id"]]
    curves_individual["p_set"] = curves_individual_interim.values.tolist()
    curves_individual["scn_name"] = scenario
    curves_individual = curves_individual.rename(
        columns={"id": "osm_id"}
    ).set_index(["osm_id", "scn_name"])

    return load_ts_df, curves_individual


def insert_osm_ind_load():
    """Inserts electrical industry loads assigned to osm landuse areas to the
    database.

    Returns
    -------
    None.

    """

    targets = egon.data.config.datasets()["electrical_load_curves_industry"][
        "targets"
    ]

    for scenario in egon.data.config.settings()["egon-data"]["--scenarios"]:
        # Delete existing data from database
        db.execute_sql(
            f"""
            DELETE FROM
            {targets['osm_load']['schema']}.{targets['osm_load']['table']}
            WHERE scn_name = '{scenario}'
            """
        )

        db.execute_sql(
            f"""
            DELETE FROM
            {targets['osm_load_individual']['schema']}.
            {targets['osm_load_individual']['table']}
            WHERE scn_name = '{scenario}'
            """
        )

        # Calculate cts load curves per mv substation (hvmv bus)
        data, curves_individual = calc_load_curves_ind_osm(scenario)
        data.index = data.index.rename("bus")
        data["scn_name"] = scenario

        data.set_index(["scn_name"], inplace=True, append=True)

        # Insert into database
        data.to_sql(
            targets["osm_load"]["table"],
            schema=targets["osm_load"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        curves_individual["peak_load"] = np.array(
            curves_individual["p_set"].values.tolist()
        ).max(axis=1)
        curves_individual["demand"] = np.array(
            curves_individual["p_set"].values.tolist()
        ).sum(axis=1)
        curves_individual = identify_voltage_level(curves_individual)

        curves_individual.to_sql(
            targets["osm_load_individual"]["table"],
            schema=targets["osm_load_individual"]["schema"],
            con=db.engine(),
            if_exists="append",
        )


def calc_load_curves_ind_sites(scenario):
    """Temporal disaggregation of load curves per industrial site and
    industrial subsector.


    Parameters
    ----------
    scenario : str
        Scenario name.

    Returns
    -------
    pandas.DataFrame
        Demand timeseries of industry allocated to industrial sites and
        aggregated per substation id and industrial subsector

    """
    sources = egon.data.config.datasets()["electrical_load_curves_industry"][
        "sources"
    ]

    # Select demands per industrial site including the subsector information
    demands_ind_sites = db.select_dataframe(
        f"""SELECT industrial_sites_id, wz, demand
            FROM {sources['sites']['schema']}.
            {sources['sites']['table']}
            WHERE scenario = '{scenario}'
            AND demand > 0
            """
    ).set_index(["industrial_sites_id"])

    # Select industrial sites as demand_areas from database

    demand_area = db.select_geodataframe(
        f"""SELECT id, geom FROM
                {sources['sites_geom']['schema']}.
                {sources['sites_geom']['table']}""",
        index_col="id",
        geom_col="geom",
        epsg=3035,
    )

    # Replace entries to bring it in line with demandregio's subsector
    # definitions
    demands_ind_sites.replace(1718, 17, inplace=True)
    share_wz_sites = demands_ind_sites.copy()

    # Create additional df on wz_share per industrial site, which is always
    # set to one as the industrial demand per site is subsector specific

    share_wz_sites.demand = 1
    share_wz_sites.reset_index(inplace=True)

    share_transpose = pd.DataFrame(
        index=share_wz_sites.industrial_sites_id.unique(),
        columns=share_wz_sites.wz.unique(),
    )
    share_transpose.index.rename("industrial_sites_id", inplace=True)
    for wz in share_transpose.columns:
        share_transpose[wz] = (
            share_wz_sites[share_wz_sites.wz == wz]
            .set_index("industrial_sites_id")
            .demand
        )

    load_curves = calc_load_curve(share_transpose, scenario, demands_ind_sites["demand"])

    curves_da = identify_bus(load_curves, demand_area)

    curves_da = pd.merge(
        curves_da, demands_ind_sites.wz, left_on="id", right_index=True
    )

    # Group all load curves per bus and wz
    curves_bus = (
        curves_da.drop(["id", "geom"], axis=1)
        .fillna(0)
        .groupby(["bus_id", "wz"])
        .sum()
    )

    # Initalize pandas.DataFrame for pf table load timeseries
    load_ts_df = pd.DataFrame(index=curves_bus.index, columns=["p_set"])

    # Insert data for pf load timeseries table
    load_ts_df.p_set = curves_bus.values.tolist()

    # Create Dataframe to store time series individually
    curves_individual_interim = (
        curves_da.drop(["bus_id", "geom", "wz"], axis=1).fillna(0)
    ).set_index("id")
    curves_individual = curves_da[["id", "bus_id"]]
    curves_individual["p_set"] = curves_individual_interim.values.tolist()
    curves_individual["scn_name"] = scenario
    curves_individual = curves_individual.merge(
        curves_da[["wz", "id"]], left_on="id", right_on="id"
    )
    curves_individual = curves_individual.rename(
        columns={"id": "site_id"}
    ).set_index(["site_id", "scn_name"])

    return load_ts_df, curves_individual


def insert_sites_ind_load():
    """Inserts electrical industry loads assigned to osm landuse areas to the
    database.

    Returns
    -------
    None.

    """

    targets = egon.data.config.datasets()["electrical_load_curves_industry"][
        "targets"
    ]

    for scenario in egon.data.config.settings()["egon-data"]["--scenarios"]:
        # Delete existing data from database
        db.execute_sql(
            f"""
            DELETE FROM
            {targets['sites_load']['schema']}.{targets['sites_load']['table']}
            WHERE scn_name = '{scenario}'
            """
        )

        # Delete existing data from database
        db.execute_sql(
            f"""
            DELETE FROM
            {targets['sites_load_individual']['schema']}.
            {targets['sites_load_individual']['table']}
            WHERE scn_name = '{scenario}'
            """
        )

        # Calculate industrial load curves per bus
        data, curves_individual = calc_load_curves_ind_sites(scenario)
        data.index = data.index.rename(["bus", "wz"])
        data["scn_name"] = scenario

        data.set_index(["scn_name"], inplace=True, append=True)

        # Insert into database
        data.to_sql(
            targets["sites_load"]["table"],
            schema=targets["sites_load"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        curves_individual["peak_load"] = np.array(
            curves_individual["p_set"].values.tolist()
        ).max(axis=1)
        curves_individual["demand"] = np.array(
            curves_individual["p_set"].values.tolist()
        ).sum(axis=1)
        curves_individual = identify_voltage_level(curves_individual)

        curves_individual.to_sql(
            targets["sites_load_individual"]["table"],
            schema=targets["sites_load_individual"]["schema"],
            con=db.engine(),
            if_exists="append",
        )
