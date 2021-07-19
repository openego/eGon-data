"""The central module containing all code dealing with processing
timeseries data using demandregio

"""

import pandas as pd
import geopandas as gpd
import egon.data.config
from egon.data.processing.demandregio.temporal import calc_load_curve
from egon.data import db
from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DemandCurvesOsmIndustry(Base):
    __tablename__ = "egon_osm_ind_load_curves"
    __table_args__ = {"schema": "demand"}

    bus = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))


def create_tables():
    """Create tables for distributed industrial demand curves
    Returns
    -------
    None.
    """
    targets = (egon.data.config.datasets()
               ['electrical_load_curves_industry']['targets'])


    # Drop table
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets['osm_load']['schema']}.{targets['osm_load']['table']} CASCADE;"""
    )


    engine = db.engine()
    DemandCurvesOsmIndustry.__table__.create(
        bind=engine, checkfirst=True
    )


def calc_load_curves_ind_osm(scenario):
    """Temporal disaggregate electrical demand per osm industrial landuse area.


    Parameters
    ----------
    scenario : str
        Scenario name.

    Returns
    -------
    pandas.DataFrame
        Demand timeseries of industry allocated to osm landuse areas and aggregated
        per substation id

    """

    sources = (egon.data.config.datasets()
                ['electrical_load_curves_industry']['sources'])

    # Select demands per industrial branch and osm landuse area
    demands_osm_area = db.select_dataframe(
            f"""SELECT osm_id, wz, demand
            FROM {sources['osm']['schema']}.
            {sources['osm']['table']}
            WHERE scenario = '{scenario}'
            AND demand > 0
            """).set_index(['osm_id', 'wz'])

    # Select industrial landuse polygons
    landuse = db.select_geodataframe(
            f"""SELECT gid, geom FROM
                {sources['osm_landuse']['schema']}.
                {sources['osm_landuse']['table']}
                WHERE sector = 3 """,
            index_col="gid",
            geom_col="geom",
            epsg=3035
        )

    # Select mv griddistrict

    griddistrict = db.select_geodataframe(
        f"""SELECT subst_id, geom FROM
                {sources['mv_grid_districts']['schema']}.
                {sources['mv_grid_districts']['table']}""",
            geom_col="geom",
            epsg=3035)


    # Calculate shares of industrial branches per osm area
    osm_share_wz = demands_osm_area.groupby('osm_id').apply(
        lambda grp: grp/grp.sum())

    osm_share_wz.reset_index(inplace=True)


    share_wz_transpose = pd.DataFrame(index=osm_share_wz.osm_id.unique(), columns=osm_share_wz.wz.unique())
    share_wz_transpose.index.rename('osm_id', inplace=True)

    for wz in share_wz_transpose.columns:
        share_wz_transpose[wz]=osm_share_wz[osm_share_wz.wz==wz].set_index('osm_id').demand

    # Rename columns to bring it in line with demandregio data
    share_wz_transpose.rename(columns={1718: 17}, inplace=True)

    # Calculate industrial annual demand per osm area
    annual_demand_osm = demands_osm_area.groupby('osm_id').demand.sum()

    # Return electrical load curves per osm industrial landuse area
    load_curves = calc_load_curve(share_wz_transpose, annual_demand_osm)

    # Initialize dataframe to identify peak load per osm landuse area
    peak = pd.DataFrame(columns=["osm_id", "peak_load", "voltage_level"])
    peak["osm_id"]=load_curves.max(axis=0).index
    peak["peak_load"]=load_curves.max(axis=0).values

    # Identify voltage_level for every osm landuse area
    peak.loc[peak["peak_load"] < 0.1, "voltage_level"] = 7
    peak.loc[peak["peak_load"] > 0.1, "voltage_level"] = 6
    peak.loc[peak["peak_load"] > 0.2, "voltage_level"] = 5
    peak.loc[peak["peak_load"] > 5.5, "voltage_level"] = 4
    peak.loc[peak["peak_load"] > 20, "voltage_level"] = 3
    peak.loc[peak["peak_load"] > 120, "voltage_level"] = 1


    # Assign bus_id to osm landuse area by merging landuse and peak df
    peak= pd.merge(landuse, peak, right_on="osm_id", left_index=True)

    # Identify all osm landuse areas connected to HVMV buses
    peak_hv = peak[peak["voltage_level"] > 1]

    # Perform a spatial join between the landuse centroid and mv grid districts to identify grid connection point
    peak_hv["centroid"]=peak_hv["geom"].centroid
    peak_hv = peak_hv.set_geometry("centroid")
    peak_hv_c = gpd.sjoin(peak_hv, griddistrict, how="inner", op="intersects")

    # Perform a spatial join between the landuse polygon and mv grid districts to ensure every landuse got assign to a bus
    peak_hv_p= peak_hv[~peak_hv.isin(peak_hv_c)].dropna().set_geometry("geom")
    peak_hv_p = gpd.sjoin(peak_hv_p, griddistrict, how="inner", op="intersects").drop_duplicates(subset=['osm_id'])

    # Bring both dataframes together
    peak_osm_bus = peak_hv_c.append(peak_hv_p, ignore_index=True)

    # Combine dataframes to bring loadcurves and bus id together
    curves_osm= pd.merge(load_curves.T, peak_osm_bus[["subst_id", "osm_id"]], left_index=True, right_on="osm_id")

    # Group all load curves per bus
    curves_bus= curves_osm.groupby("subst_id").sum().drop(["osm_id"], axis=1)

     # Initalize pandas.DataFrame for pf table load timeseries
    load_ts_df = pd.DataFrame(index=curves_bus.index,
                                  columns=['p_set'])

    # Insert data for pf load timeseries table
    load_ts_df.p_set = curves_bus.values.tolist()

    return load_ts_df


def insert_osm_ind_load():
    """Inserts electrical industry loads assigned to osm landuse areas to the database

    Returns
    -------
    None.

    """

    targets = (egon.data.config.datasets()
               ['electrical_load_curves_industry']['targets'])

    for scenario in ['eGon2035', 'eGon100RE']:

        # Delete existing data from database
        db.execute_sql(
            f"""
            DELETE FROM
            {targets['osm_load']['schema']}.{targets['osm_load']['table']}
            AND scn_name = '{scenario}'
            """)

        # Calculate cts load curves per mv substation (hvmv bus)
        data = calc_load_curves_ind_osm(scenario)
        data.index = data.index.rename("bus")
        data['scn_name'] = scenario


        data.set_index(['scn_name'], inplace=True, append=True)

        # Insert into database
        data.to_sql(targets['osm_load']['table'],
                       schema=targets['osm_load']['schema'],
                       con=db.engine(),
                       if_exists='append')





