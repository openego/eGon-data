"""The module containing all code dealing with pv rooftop distribution.
"""
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import config, db

Base = declarative_base()
from egon.data.datasets import Dataset


class Vg250MvGridDistricts(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Vg250MvGridDistricts",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(mapping),
        )


class MapMvgriddistrictsVg250(Base):
    __tablename__ = "egon_map_mvgriddistrict_vg250"
    __table_args__ = {"schema": "boundaries"}
    bus_id = Column(Integer, primary_key=True)
    vg250_lan = Column(String)


def create_tables():
    """Create tables for mapping grid districts to federal state
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS boundaries;")
    engine = db.engine()
    MapMvgriddistrictsVg250.__table__.drop(bind=engine, checkfirst=True)
    MapMvgriddistrictsVg250.__table__.create(bind=engine, checkfirst=True)


def mapping():
    """Map mv grid distrcits to federal states

    Returns
    -------
    None.

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
