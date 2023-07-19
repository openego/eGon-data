"""
Implements mapping between mv grid districts and zensus cells
"""

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa
import egon.data.config


class ZensusMvGridDistricts(Dataset):
    """
    Maps zensus cells to MV grid districts and writes it to database.

    *Dependencies*
      * :py:class:`ZensusPopulation <egon.data.datasets.zensus.ZensusPopulation>`
      * :py:mod:`mv_grid_districts_setup <egon.data.datasets.mv_grid_districts>`

    *Resulting tables*
      * :py:class:`boundaries.egon_map_zensus_grid_districts <MapZensusGridDistricts>`
        is created and filled

    """
    #:
    name: str = "ZensusMvGridDistricts"
    #:
    version: str = "0.0.1"
    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(mapping),
        )


# will be later imported from another file ###
Base = declarative_base()


class MapZensusGridDistricts(Base):
    """
    Class definition of table boundaries.egon_map_zensus_grid_districts.
    """
    __tablename__ = "egon_map_zensus_grid_districts"
    __table_args__ = {"schema": "boundaries"}

    zensus_population_id = Column(
        Integer,
        ForeignKey(DestatisZensusPopulationPerHa.id),
        primary_key=True,
        index=True,
    )
    bus_id = Column(Integer, ForeignKey(MvGridDistricts.bus_id))


def mapping():
    """
    Map zensus cells and MV grid districts and write to database.

    Newly creates and fills table boundaries.egon_map_zensus_grid_districts.

    """

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
        f"""SELECT bus_id, geom
        FROM {cfg['sources']['egon_mv_grid_district']['schema']}.
        {cfg['sources']['egon_mv_grid_district']['table']}""",
        geom_col="geom",
        epsg=3035,
    )

    # Join mv grid districts with zensus cells
    join = gpd.sjoin(zensus, grid_districts, how="inner", op="intersects")

    # Insert results to database
    join[["zensus_population_id", "bus_id"]].to_sql(
        cfg["targets"]["map"]["table"],
        schema=cfg["targets"]["map"]["schema"],
        con=db.engine(),
        if_exists="replace",
    )
