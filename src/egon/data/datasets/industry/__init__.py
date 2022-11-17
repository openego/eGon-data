"""The central module containing all code dealing with the spatial
   distribution of industrial electricity demands.
   Industrial demands from DemandRegio are distributed from nuts3 level down
   to osm landuse polygons and/or industrial sites also identified within this
   processing step bringing three different inputs together.

"""


from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.industry.temporal import (
    insert_osm_ind_load,
    insert_sites_ind_load,
)
import egon.data.config

Base = declarative_base()


class EgonDemandRegioOsmIndElectricity(Base):
    __tablename__ = "egon_demandregio_osm_ind_electricity"
    __table_args__ = {"schema": "demand"}
    id = Column(Integer, primary_key=True)
    osm_id = Column(Integer)
    scenario = Column(String(20), primary_key=True)
    wz = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioSitesIndElectricity(Base):
    __tablename__ = "egon_demandregio_sites_ind_electricity"
    __table_args__ = {"schema": "demand"}
    industrial_sites_id = Column(Integer, primary_key=True)
    scenario = Column(String(20), primary_key=True)
    wz = Column(Integer)
    demand = Column(Float)


class DemandCurvesOsmIndustry(Base):
    __tablename__ = "egon_osm_ind_load_curves"
    __table_args__ = {"schema": "demand"}

    bus = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))


class DemandCurvesOsmIndustryIndividual(Base):
    __tablename__ = "egon_osm_ind_load_curves_individual"
    __table_args__ = {"schema": "demand"}

    osm_id = Column(Integer, primary_key=True)
    bus_id = Column(Integer)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))
    peak_load = Column(Float)
    demand = Column(Float)
    voltage_level = Column(Integer)


class DemandCurvesSitesIndustry(Base):
    __tablename__ = "egon_sites_ind_load_curves"
    __table_args__ = {"schema": "demand"}

    bus = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    wz = Column(Integer, primary_key=True)
    p_set = Column(ARRAY(Float))


class DemandCurvesSitesIndustryIndividual(Base):
    __tablename__ = "egon_sites_ind_load_curves_individual"
    __table_args__ = {"schema": "demand"}

    site_id = Column(Integer, primary_key=True)
    bus_id = Column(Integer)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))
    peak_load = Column(Float)
    demand = Column(Float)
    voltage_level = Column(Integer)


def create_tables():
    """Create tables for industrial sites and distributed industrial demands
    Returns
    -------
    None.
    """

    # Get data config
    targets_spatial = egon.data.config.datasets()[
        "distributed_industrial_demand"
    ]["targets"]
    targets_temporal = egon.data.config.datasets()[
        "electrical_load_curves_industry"
    ]["targets"]

    # Create target schema
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")

    # Drop tables and sequences before recreating them

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_spatial['sites']['schema']}.
            {targets_spatial['sites']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_spatial['osm']['schema']}.
            {targets_spatial['osm']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_temporal['osm_load']['schema']}.
            {targets_temporal['osm_load']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_temporal['osm_load_individual']['schema']}.
            {targets_temporal['osm_load_individual']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_temporal['sites_load']['schema']}.
            {targets_temporal['sites_load']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {targets_temporal['sites_load_individual']['schema']}.
            {targets_temporal['sites_load_individual']['table']} CASCADE;"""
    )

    engine = db.engine()

    EgonDemandRegioSitesIndElectricity.__table__.create(
        bind=engine, checkfirst=True
    )

    EgonDemandRegioOsmIndElectricity.__table__.create(
        bind=engine, checkfirst=True
    )

    DemandCurvesOsmIndustry.__table__.create(bind=engine, checkfirst=True)

    DemandCurvesOsmIndustryIndividual.__table__.create(
        bind=engine, checkfirst=True
    )

    DemandCurvesSitesIndustry.__table__.create(bind=engine, checkfirst=True)

    DemandCurvesSitesIndustryIndividual.__table__.create(
        bind=engine, checkfirst=True
    )


def industrial_demand_distr():
    """Distribute electrical demands for industry to osm landuse polygons
    and/or industrial sites, identified earlier in the process.
    The demands per subsector on nuts3-level from demandregio are distributed
    linearly to the area of the corresponding landuse polygons or evenly to
    identified industrial sites.

    Returns
    -------
    None.
    """

    # Read information from configuration file
    sources = egon.data.config.datasets()["distributed_industrial_demand"][
        "sources"
    ]

    target_sites = egon.data.config.datasets()[
        "distributed_industrial_demand"
    ]["targets"]["sites"]
    target_osm = egon.data.config.datasets()["distributed_industrial_demand"][
        "targets"
    ]["osm"]

    # Delete data from table

    db.execute_sql(
        f"""DELETE FROM {target_sites['schema']}.{target_sites['table']}"""
    )
    db.execute_sql(
        f"""DELETE FROM {target_osm['schema']}.{target_osm['table']}"""
    )

    for scn in sources["demandregio"]["scenarios"]:

        # Select spatial information from local database
        # Select administrative districts (Landkreise) including its boundaries
        boundaries = db.select_geodataframe(
            f"""SELECT nuts, geometry FROM
                {sources['vg250_krs']['schema']}.
                {sources['vg250_krs']['table']}""",
            index_col="nuts",
            geom_col="geometry",
            epsg=3035,
        )

        # Select industrial landuse polygons
        landuse = db.select_geodataframe(
            f"""SELECT id, area_ha, geom FROM
                {sources['osm_landuse']['schema']}.
                {sources['osm_landuse']['table']}
                WHERE sector = 3
                AND NOT ST_Intersects(
                    geom,
                    (SELECT ST_UNION(ST_Transform(geom,3035)) FROM
                    {sources['industrial_sites']['schema']}.
                    {sources['industrial_sites']['table']}))
                AND name NOT LIKE '%%kraftwerk%%'
                AND name NOT LIKE '%%Stadtwerke%%'
                AND name NOT LIKE '%%Müllverbrennung%%'
                AND name NOT LIKE '%%Müllverwertung%%'
                AND name NOT LIKE '%%Abfall%%'
                AND name NOT LIKE '%%Kraftwerk%%'
                AND name NOT LIKE '%%Wertstoff%%'
                AND name NOT LIKE '%%olarpark%%'
                AND name NOT LIKE '%%Gewerbegebiet%%'
                AND name NOT LIKE '%%Gewerbepark%%'
                AND name NOT LIKE '%%heizwerk%%'
                AND name NOT LIKE '%%Heizwerk%%'
                AND name NOT LIKE '%%Kläranlage%%'
                AND name NOT LIKE '%%Klärwerk%%'
                AND name NOT LIKE '%%Biogasanlage%%'
                AND name NOT LIKE '%%Wasserwerk%%'
                AND name NOT LIKE '%%Recyclinghof%%'
                AND name NOT LIKE '%%Recyclingpark%%'""",
            geom_col="geom",
            epsg=3035,
        )

        # Spatially join vg250_krs and industrial landuse areas
        landuse = gpd.sjoin(landuse, boundaries, how="inner", op="intersects")
        # Rename column
        landuse = landuse.rename({"index_right": "nuts3"}, axis=1)

        landuse_nuts3 = landuse[["area_ha", "nuts3"]]
        landuse_nuts3 = landuse.groupby(["nuts3"]).sum().reset_index()

        # Select data on industrial sites
        sites = db.select_dataframe(
            f"""SELECT id, wz, nuts3 FROM
                {sources['industrial_sites']['schema']}.
                {sources['industrial_sites']['table']}""",
            index_col=None,
        )
        # Count number of industrial sites per subsector (wz) and nuts3
        # district
        sites_grouped = (
            sites.groupby(["nuts3", "wz"]).size().reset_index(name="counts")
        )

        # Select industrial demands on nuts3 level from local database
        demand_nuts3_import = db.select_dataframe(
            f"""SELECT nuts3, demand, wz FROM
                {sources['demandregio']['schema']}.
                {sources['demandregio']['table']}
                WHERE scenario = '{scn}'
                AND demand > 0
                AND wz IN
                    (SELECT wz FROM demand.egon_demandregio_wz
                         WHERE sector = 'industry')"""
        )

        # Replace wz=17 and wz=18 by wz=1718 as a differentiation of these two
        # subsectors can't be performed
        demand_nuts3_import["wz"] = demand_nuts3_import["wz"].replace(
            [17, 18], 1718
        )

        # Group results by nuts3 and wz to aggregate demands from subsectors
        # 17 and 18
        demand_nuts3 = (
            demand_nuts3_import.groupby(["nuts3", "wz"]).sum().reset_index()
        )

        # A differentiation between those industrial subsectors (wz) which
        # aren't represented and subsectors with a representation in the
        # dataset on industrial sites is needed

        # Select industrial demand for sectors which aren't found in
        # industrial sites as category a
        demand_nuts3_a = demand_nuts3[
            ~demand_nuts3["wz"].isin([1718, 19, 20, 23, 24])
        ]

        # Select industrial demand for sectors which are found in industrial
        # sites as category b
        demand_nuts3_b = demand_nuts3[
            demand_nuts3["wz"].isin([1718, 19, 20, 23, 24])
        ]

        # Bring demands on nuts3 level and information on industrial sites per
        # nuts3 district together
        demand_nuts3_b = demand_nuts3_b.merge(
            sites_grouped,
            how="left",
            left_on=["nuts3", "wz"],
            right_on=["nuts3", "wz"],
        )

        # Define share of industrial demand per nuts3 region and subsector
        # allocated to industrial sites
        share_to_sites = 0.5

        # Define demand per site for every nuts3 region and subsector

        demand_nuts3_b["demand_per_site"] = (
            demand_nuts3_b["demand"] * share_to_sites
        ) / demand_nuts3_b["counts"]

        # Replace NaN by 0
        demand_nuts3_b = demand_nuts3_b.fillna(0)

        # Calculate demand which needs to be distributed to osm landuse areas
        # from category b
        demand_nuts3_b["demand_b_osm"] = demand_nuts3_b["demand"] - (
            demand_nuts3_b["demand_per_site"] * demand_nuts3_b["counts"]
        )

        # Add information about demand per site to sites dataframe

        sites = sites.merge(
            demand_nuts3_b[["nuts3", "wz", "demand_per_site"]],
            how="left",
            left_on=["nuts3", "wz"],
            right_on=["nuts3", "wz"],
        )
        sites = sites.rename(columns={"demand_per_site": "demand"})

        demand_nuts3_b_osm = demand_nuts3_b[["nuts3", "wz", "demand_b_osm"]]
        demand_nuts3_b_osm = demand_nuts3_b_osm.rename(
            {"demand_b_osm": "demand"}, axis=1
        )

        # Create df containing all demand per wz which will be allocated to
        # osm areas
        demand_nuts3_osm_wz = demand_nuts3_a.append(
            demand_nuts3_b_osm, ignore_index=True
        )
        demand_nuts3_osm_wz = (
            demand_nuts3_osm_wz.groupby(["nuts3", "wz"]).sum().reset_index()
        )

        # Calculate demand per hectar for each nuts3 region
        demand_nuts3_osm_wz = demand_nuts3_osm_wz.merge(
            landuse_nuts3, how="left", left_on=["nuts3"], right_on=["nuts3"]
        )
        demand_nuts3_osm_wz["demand_per_ha"] = (
            demand_nuts3_osm_wz["demand"] / demand_nuts3_osm_wz["area_ha"]
        )

        # Add information about demand per ha to landuse df
        landuse = landuse.merge(
            demand_nuts3_osm_wz[["nuts3", "demand_per_ha", "wz"]],
            how="left",
            left_on=["nuts3"],
            right_on=["nuts3"],
        )

        landuse["demand"] = landuse["area_ha"] * landuse["demand_per_ha"]

        # Adjust dataframes for export to local database

        sites = sites.rename({"id": "industrial_sites_id"}, axis=1)
        sites["scenario"] = scn
        sites.set_index("industrial_sites_id", inplace=True)

        landuse = landuse.rename({"id": "osm_id"}, axis=1)

        # Remove duplicates and adjust index
        landuse = landuse.groupby(["osm_id", "wz"]).sum().reset_index()
        landuse.index.rename("id", inplace=True)
        landuse["scenario"] = scn

        # Write data to db

        sites[["scenario", "wz", "demand"]].to_sql(
            target_sites["table"],
            con=db.engine(),
            schema=target_sites["schema"],
            if_exists="append",
        )

        landuse[["osm_id", "scenario", "wz", "demand"]].to_sql(
            target_osm["table"],
            con=db.engine(),
            schema=target_osm["schema"],
            if_exists="append",
        )


class IndustrialDemandCurves(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Industrial_demand_curves",
            version="0.0.5",
            dependencies=dependencies,
            tasks=(
                create_tables,
                industrial_demand_distr,
                insert_osm_ind_load,
                insert_sites_ind_load,
            ),
        )
