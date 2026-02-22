"""
Implements mapping between mv grid districts and zensus cells
"""

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa



class ZensusMvGridDistricts(Dataset):
    """
    Maps zensus cells to MV grid districts and writes it to database.

    *Dependencies*
      * :py:class:`ZensusPopulation <egon.data.datasets.zensus.ZensusPopulation>`
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`

    *Resulting tables*
      * :py:class:`boundaries.egon_map_zensus_grid_districts <MapZensusGridDistricts>`
        is created and filled

    """

    #:
    name: str = "ZensusMvGridDistricts"
    #:
    version: str = "0.0.3"
    
    sources = DatasetSources(
        tables={
            "zensus_population": "society.destatis_zensus_population_per_ha",
            "egon_mv_grid_district": "grid.egon_mv_grid_district",
        }
    )

    targets = DatasetTargets(
        tables={
            "map": "boundaries.egon_map_zensus_grid_districts",
        }
    )

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

    sources = ZensusMvGridDistricts.sources
    targets = ZensusMvGridDistricts.targets
    
    # Delete existsing data
    db.execute_sql(f"DELETE FROM {targets.tables['map']}")

    # Select zensus cells
    zensus = db.select_geodataframe(
        f"SELECT id as zensus_population_id, geom_point FROM {sources.tables['zensus_population']}",
        geom_col="geom_point",
    )

    grid_districts = db.select_geodataframe(
        f"SELECT bus_id, geom FROM {sources.tables['egon_mv_grid_district']}",
        geom_col="geom",
        epsg=3035,
    )

    # Join mv grid districts with zensus cells
    join = gpd.sjoin(zensus, grid_districts, how="inner", op="intersects")

    # Insert results to database
    join[["zensus_population_id", "bus_id"]].to_sql(
        targets.get_table_name("map"),
        schema=targets.get_table_schema("map"),
        con=db.engine(),
        if_exists="replace",
    )
