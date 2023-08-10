"""The module containing all code to map MV grid districts to federal states.
"""
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import config, db

Base = declarative_base()
from egon.data.datasets import Dataset


class Vg250MvGridDistricts(Dataset):
    """
    Maps MV grid districts to federal states and writes it to database.

    *Dependencies*
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`

    *Resulting tables*
      * :py:class:`boundaries.egon_map_mvgriddistrict_vg250 <MapMvgriddistrictsVg250>`
        is created and filled

    """
    #:
    name: str = "Vg250MvGridDistricts"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(mapping),
        )


class MapMvgriddistrictsVg250(Base):
    """
    Class definition of table boundaries.egon_map_mvgriddistrict_vg250.
    """
    __tablename__ = "egon_map_mvgriddistrict_vg250"
    __table_args__ = {"schema": "boundaries"}
    bus_id = Column(Integer, primary_key=True)
    vg250_lan = Column(String)


def create_tables():
    """
    Create table for mapping grid districts to federal states.

    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS boundaries;")
    engine = db.engine()
    MapMvgriddistrictsVg250.__table__.drop(bind=engine, checkfirst=True)
    MapMvgriddistrictsVg250.__table__.create(bind=engine, checkfirst=True)


def mapping():
    """
    Map MV grid districts to federal states and write to database.

    Newly creates and fills table boundaries.egon_map_mvgriddistrict_vg250.

    """
    # Create table
    create_tables()

    # Select sources and targets from dataset configuration
    sources = config.datasets()["map_mvgrid_vg250"]["sources"]
    target = config.datasets()["map_mvgrid_vg250"]["targets"]["map"]

    # Delete existing data
    db.execute_sql(f"DELETE FROM {target['schema']}.{target['table']}")

    # Select sources from database
    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT bus_id as bus_id, ST_Centroid(geom) as geom
        FROM {sources['egon_mv_grid_district']['schema']}.
        {sources['egon_mv_grid_district']['table']}
        """,
        index_col="bus_id",
    )

    federal_states = db.select_geodataframe(
        f"""
        SELECT gen,geometry
        FROM {sources['federal_states']['schema']}.
        {sources['federal_states']['table']}
        """,
        geom_col="geometry",
        index_col="gen",
    )

    # Join mv grid districts and federal states
    df = pd.DataFrame(
        gpd.sjoin(mv_grid_districts, federal_states)["index_right"]
    )

    # Rename columns
    df.rename({"index_right": "vg250_lan"}, axis=1, inplace=True)

    # Insert to database
    df.to_sql(
        target["table"],
        schema=target["schema"],
        if_exists="append",
        con=db.engine(),
    )
