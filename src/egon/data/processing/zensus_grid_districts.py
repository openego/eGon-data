"""
Implements mapping between mv grid districts and zensus cells
"""

import geopandas as gpd
import egon.data.config
from egon.data import db
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from egon.data.processing.zensus_vg250.zensus_population_inside_germany import DestatisZensusPopulationPerHa
from egon.data.processing.mv_grid_districts import MvGridDistricts


# will be later imported from another file ###
Base = declarative_base()

class MapZensusGridDistricts(Base):
    __tablename__ = "egon_map_zensus_grid_districts"
    __table_args__ = {"schema": "boundaries"}

    zensus_population_id = Column(Integer,
                                  ForeignKey(DestatisZensusPopulationPerHa.id),
                                  primary_key=True, index=True)
    subst_id = Column(Integer, ForeignKey(MvGridDistricts.subst_id))


def map_zensus_mv_grid_districts():
    """Perform mapping between mv grid districts and zensus grid"""

    MapZensusGridDistricts.__table__.drop(bind=db.engine(), checkfirst=True)
    MapZensusGridDistricts.__table__.create(bind=db.engine(), checkfirst=True)

    # Get information from data configuration file
    cfg = egon.data.config.datasets()["map_zensus_grid_districts"]

    # Delete existsing data
    db.execute_sql(
        f"""DELETE FROM
        {cfg['targets']['map']['schema']}.{cfg['targets']['map']['table']}"""
    )

    # Select zensus cells
    zensus = db.select_geodataframe(
        f"""SELECT id as zensus_population_id, geom_point FROM
        {cfg['sources']['zensus_population']['schema']}.
        {cfg['sources']['zensus_population']['table']}""",
        geom_col="geom_point",
    )

    grid_districts = db.select_geodataframe(
        f"""SELECT subst_id, geom
        FROM {cfg['sources']['mv_grid_districts']['schema']}.
        {cfg['sources']['mv_grid_districts']['table']}""",
        geom_col="geom",
        epsg=3035,
    )

    # Join mv grid districts with zensus cells
    join = gpd.sjoin(zensus, grid_districts, how="inner", op="intersects")

    # Insert results to database
    join[["zensus_population_id", "subst_id"]].to_sql(
        cfg["targets"]["map"]["table"],
        schema=cfg["targets"]["map"]["schema"],
        con=db.engine(),
        if_exists="replace",
    )
