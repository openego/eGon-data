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
    wz = Column(Integer)


def create_tables():
    """Create tables for industrial sites and distributed industrial demands"""
    # The old config variables are now removed.

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")

    # Drop tables using the new class attributes
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_spatial']} CASCADE;""")
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_spatial']} CASCADE;""")
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_load']} CASCADE;""")
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['osm_load_individual']} CASCADE;""")
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_load']} CASCADE;""")
    db.execute_sql(f"""DROP TABLE IF EXISTS {IndustrialDemandCurves.targets.tables['sites_load_individual']} CASCADE;""")

    # ... (the rest of the function for creating tables is unchanged)
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

    # The old config variables are now removed.

    # DELETE statements are updated
    db.execute_sql(
        f"""DELETE FROM {IndustrialDemandCurves.targets.tables['sites_spatial']}"""
    )
    db.execute_sql(
        f"""DELETE FROM {IndustrialDemandCurves.targets.tables['osm_spatial']}"""
    )

    for scn in egon.data.config.settings()["egon-data"]["--scenarios"]:
        # All SQL queries are updated to use the new class attributes
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

        # ... (the rest of the data processing logic is unchanged) ...

        # The final .to_sql() calls are updated
        sites[["scenario", "wz", "demand"]].to_sql(
            IndustrialDemandCurves.targets.get_table_name("sites_spatial"),
            con=db.engine(),
            schema=IndustrialDemandCurves.targets.get_table_schema("sites_spatial"),
            if_exists="append",
        )

        landuse[["osm_id", "scenario", "wz", "demand"]].to_sql(
            IndustrialDemandCurves.targets.get_table_name("osm_spatial"),
            con=db.engine(),
            schema=IndustrialDemandCurves.targets.get_table_schema("osm_spatial"),
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
