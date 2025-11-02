"""The central module containing all code dealing with the spatial
   distribution of industrial electricity demands.
   Industrial demands from DemandRegio are distributed from nuts3 level down
   to osm landuse polygons and/or industrial sites also identified within this
   processing step bringing three different inputs together.

"""

from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.config import settings
from egon.data.datasets.industry.temporal import (
    insert_osm_ind_load,
    insert_sites_ind_load,
)

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
    wz = Column(Integer)


def create_tables():
    """Create tables for industrial sites and distributed industrial demands"""
    # The old config variables are now removed.

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_spatial']['schema']}.{IndustrialDemandCurves.targets.tables['sites_spatial']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_spatial']['schema']}.{IndustrialDemandCurves.targets.tables['osm_spatial']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_load']['schema']}.{IndustrialDemandCurves.targets.tables['osm_load']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_load_individual']['schema']}.{IndustrialDemandCurves.targets.tables['osm_load_individual']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_load']['schema']}.{IndustrialDemandCurves.targets.tables['sites_load']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_load_individual']['schema']}.{IndustrialDemandCurves.targets.tables['sites_load_individual']['table']} CASCADE;""")

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
    """

    target_sites = IndustrialDemandCurves.targets.tables["sites_spatial"]
    target_osm = IndustrialDemandCurves.targets.tables["osm_spatial"]

    db.execute_sql(
        f"""DELETE FROM {target_sites['schema']}.{target_sites['table']}"""
    )
    db.execute_sql(
        f"""DELETE FROM {target_osm['schema']}.{target_osm['table']}"""
    )

    for scn in settings()["egon-data"]["--scenarios"]:
        # Select administrative districts (Landkreise) including its boundaries
        boundaries = db.select_geodataframe(
            f"""SELECT nuts, geometry FROM
                {IndustrialDemandCurves.sources.tables['vg250_krs']}""",
            index_col="nuts",
            geom_col="geometry",
            epsg=3035,
        )

        landuse = db.select_geodataframe(
            f"""SELECT id, area_ha, geom FROM
                {IndustrialDemandCurves.sources.tables['osm_landuse']}
                WHERE sector = 3
                AND NOT ST_Intersects(
                    geom,
                    (SELECT ST_UNION(ST_Transform(geom,3035)) FROM
                    {IndustrialDemandCurves.sources.tables['industrial_sites']}))
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

        landuse = gpd.sjoin(landuse, boundaries, how="inner", op="intersects")
        landuse = landuse.rename({"index_right": "nuts3"}, axis=1)
        landuse_nuts3 = landuse[["area_ha", "nuts3"]]
        landuse_nuts3 = landuse_nuts3.groupby(["nuts3"]).sum().reset_index()

        sites = db.select_dataframe(
            f"""SELECT id, wz, nuts3 FROM
                {IndustrialDemandCurves.sources.tables['industrial_sites']}""",
            index_col=None,
        )
        sites_grouped = (
            sites.groupby(["nuts3", "wz"]).size().reset_index(name="counts")
        )

        demand_nuts3_import = db.select_dataframe(
            f"""SELECT nuts3, demand, wz FROM
                {IndustrialDemandCurves.sources.tables['demandregio']}
                WHERE scenario = '{scn}'
                AND demand > 0
                AND wz IN
                    (SELECT wz FROM {IndustrialDemandCurves.sources.tables['demandregio_wz']}
                         WHERE sector = 'industry')"""
        )
        

        demand_nuts3_import["wz"] = demand_nuts3_import["wz"].replace(
            [17, 18], 1718
        )


        demand_nuts3 = (
            demand_nuts3_import.groupby(["nuts3", "wz"]).sum().reset_index()
        )

        demand_nuts3_a = demand_nuts3[
            ~demand_nuts3["wz"].isin([1718, 19, 20, 23, 24])
        ]


        demand_nuts3_b = demand_nuts3[
            demand_nuts3["wz"].isin([1718, 19, 20, 23, 24])
        ]


        demand_nuts3_b = demand_nuts3_b.merge(
            sites_grouped,
            how="left",
            left_on=["nuts3", "wz"],
            right_on=["nuts3", "wz"],
        )


        share_to_sites = 0.5


        demand_nuts3_b["demand_per_site"] = (
            demand_nuts3_b["demand"] * share_to_sites
        ) / demand_nuts3_b["counts"]

        demand_nuts3_b = demand_nuts3_b.fillna(0)


        demand_nuts3_b["demand_b_osm"] = demand_nuts3_b["demand"] - (
            demand_nuts3_b["demand_per_site"] * demand_nuts3_b["counts"]
        )


        sites = sites.merge(
            demand_nuts3_b[["nuts3", "wz", "demand_per_site"]],
            how="left",
            left_on=["nuts3", "wz"],
            right_on=["nuts3", "wz"],
        )
        sites = sites.rename(columns={"demand_per_site": "demand"}) # <-- CREATES THE 'DEMAND' COLUMN

        demand_nuts3_b_osm = demand_nuts3_b[["nuts3", "wz", "demand_b_osm"]]
        demand_nuts3_b_osm = demand_nuts3_b_osm.rename(
            {"demand_b_osm": "demand"}, axis=1
        )


        demand_nuts3_osm_wz = pd.concat(
            [demand_nuts3_a, demand_nuts3_b_osm], ignore_index=True
        )
        demand_nuts3_osm_wz = (
            demand_nuts3_osm_wz.groupby(["nuts3", "wz"]).sum().reset_index()
        )

        demand_nuts3_osm_wz = demand_nuts3_osm_wz.merge(
            landuse_nuts3, how="left", left_on=["nuts3"], right_on=["nuts3"]
        )
        demand_nuts3_osm_wz["demand_per_ha"] = (
            demand_nuts3_osm_wz["demand"] / demand_nuts3_osm_wz["area_ha"]
        )

        landuse = landuse.merge(
            demand_nuts3_osm_wz[["nuts3", "demand_per_ha", "wz"]],
            how="left",
            left_on=["nuts3"],
            right_on=["nuts3"],
        )

        landuse["demand"] = landuse["area_ha"] * landuse["demand_per_ha"]


        sites = sites.rename(columns={"id": "industrial_sites_id"}, axis=1) 
        sites["scenario"] = scn 
        sites.set_index("industrial_sites_id", inplace=True)

        landuse = landuse.rename({"id": "osm_id"}, axis=1)

        landuse = (
            landuse.drop("geom", axis="columns")
            .groupby(["osm_id", "wz"])
            .sum()
            .reset_index()
        )
        landuse.index.rename("id", inplace=True)
        landuse["scenario"] = scn
        
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
    
    sources = DatasetSources(
        tables={
            "vg250_krs": "boundaries.vg250_krs",
            "osm_landuse": "openstreetmap.osm_landuse",
            "industrial_sites": "demand.egon_industrial_sites",
            "demandregio": "demand.egon_demandregio_cts_ind",
            "demandregio_wz": "demand.egon_demandregio_wz",
        }
    )
    targets = DatasetTargets(
        tables={
            "osm_spatial": "demand.egon_demandregio_osm_ind_electricity",
            "sites_spatial": "demand.egon_demandregio_sites_ind_electricity",
            "osm_load": "demand.egon_osm_ind_load_curves",
            "osm_load_individual": "demand.egon_osm_ind_load_curves_individual",
            "sites_load": "demand.egon_sites_ind_load_curves",
            "sites_load_individual": "demand.egon_sites_ind_load_curves_individual",
        }
    )
    
    """
    Distribute industrial electricity demands to industrial sites and OSM
    landuse areas

    Creates different tables to store industrial electricity demand curves on
    different aggregation levels. In a first step industrial demands taken from
    DemandRegio are distributed to industrial sites and OSM polygons which are
    tagged as industrial areas. This method takes information on the different
    industrial sectors into account and allocates the annual demand as well as
    load curves accordingly.

    *Dependencies*
      * :py:class:`DemandRegio <egon.data.datasets.demandregio.DemandRegio>`
      * :py:class:`MergeIndustrialSites <egon.data.datasets.industrial_sites.MergeIndustrialSites>`
      * :py:class:`OsmLanduse <egon.data.datasets.loadarea.OsmLanduse>`
      * :py:func:`define_mv_grid_districts <egon.data.datasets.mv_grid_districts.define_mv_grid_districts>`
      * :py:class:`OpenStreetMap <egon.data.datasets.osm.OpenStreetMap>`

    *Resulting tables*
      * :py:class:`demand.egon_demandregio_osm_ind_electricity <egon.data.datasets.industry.EgonDemandRegioOsmIndElectricity>` is created and filled
      * :py:class:`demand.egon_demandregio_sites_ind_electricity <egon.data.datasets.industry.EgonDemandRegioSitesIndElectricity>` is created and filled
      * :py:class:`demand.egon_osm_ind_load_curves <egon.data.datasets.industry.DemandCurvesOsmIndustry>` is created and filled
      * :py:class:`demand.egon_osm_ind_load_curves_individual <egon.data.datasets.industry.DemandCurvesOsmIndustryIndividual>` is created and filled
      * :py:class:`demand.egon_sites_ind_load_curves <egon.data.datasets.industry.DemandCurvesSitesIndustry>` is created and filled
      * :py:class:`demand.egon_sites_ind_load_curves_individual <egon.data.datasets.industry.DemandCurvesSitesIndustryIndividual>` is created and filled

    """

    #:
    name: str = "Industrial_demand_curves"
    #:
    version: str = "0.0.6"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                industrial_demand_distr,
                insert_osm_ind_load,
                insert_sites_ind_load,
            ),
        )
