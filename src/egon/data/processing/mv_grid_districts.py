"""
Implements the methods for creating medium-voltage grid district areas from
HV-MV substation locations and municipality borders from Hülk et al. (2017)
(section 2.3)
https://somaesthetics.aau.dk/index.php/sepm/article/view/1833/1531
"""


from geoalchemy2.types import Geometry
from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Float,
    Integer,
    Numeric,
    Sequence,
    String,
    func,
)
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.db import session_scope
from egon.data.processing.substation import EgonHvmvSubstationVoronoi

Base = declarative_base()
metadata = Base.metadata


class Vg250GemClean(Base):
    __tablename__ = "vg250_gem_clean"
    __table_args__ = {"schema": "boundaries"}

    id = Column(Integer, primary_key=True)
    old_id = Column(Integer)
    gen = Column(String)
    bez = Column(String)
    bem = Column(String)
    nuts = Column(String)
    rs_0 = Column(String)
    ags_0 = Column(String)
    area_ha = Column(Numeric)
    count_hole = Column(Integer)
    path = Column(ARRAY(Integer()))
    is_hole = Column(Boolean)
    geometry = Column(Geometry("POLYGON", 3035), index=True)


class EgonHvmvSubstation(Base):
    __tablename__ = "egon_hvmv_substation"
    __table_args__ = {"schema": "grid"}

    subst_id = Column(Integer, primary_key=True)
    lon = Column(Float)
    lat = Column(Float)
    point = Column(Geometry("POINT", 4326), index=True)
    polygon = Column(Geometry, index=True)
    voltage = Column(String)
    power_type = Column(String)
    substation = Column(String)
    osm_id = Column(String)
    osm_www = Column(String)
    frequency = Column(String)
    subst_name = Column(String)
    ref = Column(String)
    operator = Column(String)
    dbahn = Column(String)
    status = Column(Integer)


class HvmvSubstPerMunicipality(Base):
    __tablename__ = "hvmv_subst_per_municipality"
    __table_args__ = {"schema": "grid"}

    id = Column(Integer, primary_key=True)
    old_id = Column(Integer)
    gen = Column(String)
    bez = Column(String)
    bem = Column(String)
    nuts = Column(String)
    rs_0 = Column(String)
    ags_0 = Column(String)
    area_ha = Column(Numeric)
    count_hole = Column(Integer)
    path = Column(ARRAY(Integer()))
    is_hole = Column(Boolean)
    geometry = Column(Geometry("POLYGON", 3035))
    subst_count = Column(Integer)



def substations_in_municipalities():
    """
    Create a table that counts number of HV-MV substations in each MV grid

    Counting is performed in two steps

    1. HV-MV substations are spatially joined on municipalities, grouped by
       municipality and number of substations counted
    2. Because (1) works only for number of substations >0, all municipalities
       not containing a substation, are added
    """
    engine = db.engine()
    HvmvSubstPerMunicipality.__table__.drop(bind=engine, checkfirst=True)
    HvmvSubstPerMunicipality.__table__.create(bind=engine)

    with session_scope() as session:
        # Insert municipalities with number of substations > 0
        q = (
            session.query(
                Vg250GemClean,
                func.count(EgonHvmvSubstation.point).label("subst_count"),
            )
            .filter(
                func.ST_Contains(
                    Vg250GemClean.geometry,
                    func.ST_Transform(EgonHvmvSubstation.point, 3035),
                )
            )
            .group_by(Vg250GemClean.id)
        )

        muns_with_subst = (
            HvmvSubstPerMunicipality.__table__.insert().from_select(
                HvmvSubstPerMunicipality.__table__.columns, q
            )
        )
        session.execute(muns_with_subst)
        session.commit()

        # Insert remaining municipalities
        already_inserted_muns = session.query(
            HvmvSubstPerMunicipality.id
        ).subquery()
        muns_without_subst = (
            HvmvSubstPerMunicipality.__table__.insert().from_select(
                [
                    _
                    for _ in HvmvSubstPerMunicipality.__table__.columns
                    if _.name != "subst_count"
                ],
                session.query(Vg250GemClean).filter(
                    Vg250GemClean.id.notin_(already_inserted_muns)
                ),
            )
        )
        session.execute(muns_without_subst)
        session.commit()

        # Set subst_count for municipalities with zero substations to 0
        session.query(HvmvSubstPerMunicipality).filter(
            HvmvSubstPerMunicipality.subst_count == None
        ).update(
            {HvmvSubstPerMunicipality.subst_count: 0},
            synchronize_session="fetch",
        )
        session.commit()

