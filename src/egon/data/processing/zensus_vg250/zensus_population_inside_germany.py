from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    SmallInteger,
    String,
    Float,
    func,
)
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db

Base = declarative_base()


class Vg250Sta(Base):
    __tablename__ = "vg250_sta"
    __table_args__ = {"schema": "boundaries"}

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(String)
    ags = Column(String)
    sdv_ars = Column(String)
    gen = Column(String)
    bez = Column(String)
    ibz = Column(BigInteger)
    bem = Column(String)
    nbd = Column(String)
    sn_l = Column(String)
    sn_r = Column(String)
    sn_k = Column(String)
    sn_v1 = Column(String)
    sn_v2 = Column(String)
    sn_g = Column(String)
    fk_s3 = Column(String)
    nuts = Column(String)
    ars_0 = Column(String)
    ags_0 = Column(String)
    wsk = Column(String)
    debkg_id = Column(String)
    rs = Column(String)
    sdv_rs = Column(String)
    rs_0 = Column(String)
    geometry = Column(Geometry(srid=4326), index=True)


class DestatisZensusPopulationPerHa(Base):
    __tablename__ = "destatis_zensus_population_per_ha"
    __table_args__ = {"schema": "society"}

    gid = Column(Integer, primary_key=True)
    grid_id = Column(String(254), nullable=False)
    x_mp = Column(Integer)
    y_mp = Column(Integer)
    population = Column(SmallInteger)
    geom_point = Column(Geometry("POINT", 3035), index=True)
    geom = Column(Geometry("POLYGON", 3035), index=True)


class DestatisZensusPopulationPerHaInsideGermany(Base):
    __tablename__ = "destatis_zensus_population_per_ha_inside_germany"
    __table_args__ = {"schema": "society"}

    gid = Column(Integer, primary_key=True)
    grid_id = Column(String(254), nullable=False)
    population = Column(SmallInteger)
    geom_point = Column(Geometry("POINT", 3035), index=True)
    geom = Column(Geometry("POLYGON", 3035), index=True)


class BkgVg250GemPopulation(Base):
    __tablename__ = "bkg_vg250_6_gem_mview"
    __table_args__ = {"schema": "boundaries"}

    gid = Column(Integer, primary_key=True)
    reference_date = Column(String)
    gen = Column(String)
    bez = Column(String)
    bem = Column(String)
    nuts = Column(String)
    ags_0 = Column(String)
    rs_0 = Column(String)
    area_ha = Column(Float)
    area_km2 = Column(Float)
    census_sum = Column(Integer)
    census_count = Column(Integer)
    census_density = Column(Integer)
    pd = (Column(Float),)
    geom = Column(Geometry("MULTIPOLYGON", 3035), index=True)


def filter_data():
    """
    Filter zensus data by data inside Germany and population > 0
    """

    # Get database engine
    engine_local_db = db.engine()

    # Create new table
    DestatisZensusPopulationPerHaInsideGermany.__table__.create(
        bind=engine_local_db, checkfirst=True
    )

    with db.session_scope() as s:
        # Query relevant data from zensus population table
        q = (
            s.query(
                DestatisZensusPopulationPerHa.gid,
                DestatisZensusPopulationPerHa.grid_id,
                DestatisZensusPopulationPerHa.population,
                DestatisZensusPopulationPerHa.geom_point,
                DestatisZensusPopulationPerHa.geom,
            )
            .filter(DestatisZensusPopulationPerHa.population > 0)
            .filter(
                func.ST_Contains(
                    func.ST_Transform(Vg250Sta.geometry, 3035),
                    DestatisZensusPopulationPerHa.geom_point,
                )
            )
        )

        # Insert above queried data into new table
        insert = DestatisZensusPopulationPerHaInsideGermany.__table__.insert(
        ).from_select(
            (
                DestatisZensusPopulationPerHaInsideGermany.gid,
                DestatisZensusPopulationPerHaInsideGermany.grid_id,
                DestatisZensusPopulationPerHaInsideGermany.population,
                DestatisZensusPopulationPerHaInsideGermany.geom_point,
                DestatisZensusPopulationPerHaInsideGermany.geom,
            ),
            q,
        )

        # Execute and commit (trigger transactions in database)
        s.execute(insert)
        s.commit()


