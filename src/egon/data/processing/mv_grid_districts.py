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


class EgonHvmvSubstationVoronoiMunicipalityCutsBase(object):

    subst_id = Column(Integer)
    municipality_id = Column(Integer)
    voronoi_id = Column(Integer)
    ags_0 = Column(String)
    subst_count = Column(Integer)
    geom = Column(Geometry("Polygon", 3035))
    geom_sub = Column(Geometry("Point", 3035))


class EgonHvmvSubstationVoronoiMunicipalityCuts(
    EgonHvmvSubstationVoronoiMunicipalityCutsBase, Base
):
    __tablename__ = "egon_hvmv_substation_voronoi_municipality_cuts"
    __table_args__ = {"schema": "grid"}

    id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )


class EgonHvmvSubstationVoronoiMunicipalityCuts1Subst(
    EgonHvmvSubstationVoronoiMunicipalityCutsBase, Base
):
    __tablename__ = "egon_hvmv_substation_voronoi_municipality_cuts_1subst"
    __table_args__ = {"schema": "grid"}

    id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )


class EgonHvmvSubstationVoronoiMunicipalityCuts0Subst(
    EgonHvmvSubstationVoronoiMunicipalityCutsBase, Base
):
    __tablename__ = "egon_hvmv_substation_voronoi_municipality_cuts_0subst"
    __table_args__ = {"schema": "grid"}

    id = Column(Integer)
    temp_id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )


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


def split_multi_substation_municipalities():
    engine = db.engine()
    EgonHvmvSubstationVoronoiMunicipalityCuts.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonHvmvSubstationVoronoiMunicipalityCuts.__table__.create(bind=engine)
    EgonHvmvSubstationVoronoiMunicipalityCuts1Subst.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonHvmvSubstationVoronoiMunicipalityCuts1Subst.__table__.create(
        bind=engine
    )
    EgonHvmvSubstationVoronoiMunicipalityCuts0Subst.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonHvmvSubstationVoronoiMunicipalityCuts0Subst.__table__.create(
        bind=engine
    )

    with session_scope() as session:
        # Cut municipalities with voronoi polygons
        q = (
            session.query(
                HvmvSubstPerMunicipality.id.label("municipality_id"),
                HvmvSubstPerMunicipality.ags_0,
                func.ST_Dump(
                    func.ST_Intersection(
                        HvmvSubstPerMunicipality.geometry,
                        func.ST_Transform(
                            EgonHvmvSubstationVoronoi.geom, 3035
                        ),
                    )
                ).geom.label("geom"),
                EgonHvmvSubstationVoronoi.subst_id,
                EgonHvmvSubstationVoronoi.id.label("voronoi_id"),
            )
            .filter(HvmvSubstPerMunicipality.subst_count > 1)
            .filter(
                HvmvSubstPerMunicipality.geometry.intersects(
                    func.ST_Transform(EgonHvmvSubstationVoronoi.geom, 3035)
                )
            )
            .subquery()
        )

        voronoi_cuts = EgonHvmvSubstationVoronoiMunicipalityCuts.__table__.insert().from_select(
            [
                EgonHvmvSubstationVoronoiMunicipalityCuts.municipality_id,
                EgonHvmvSubstationVoronoiMunicipalityCuts.ags_0,
                EgonHvmvSubstationVoronoiMunicipalityCuts.geom,
                EgonHvmvSubstationVoronoiMunicipalityCuts.subst_id,
                EgonHvmvSubstationVoronoiMunicipalityCuts.voronoi_id,
            ],
            q,
        )
        session.execute(voronoi_cuts)
        session.commit()

        # Determine number of substations inside cut polygons
        cuts_substation_subquery = (
            session.query(
                EgonHvmvSubstationVoronoiMunicipalityCuts.id,
                EgonHvmvSubstation.subst_id,
                func.ST_Transform(EgonHvmvSubstation.point, 3035).label(
                    "geom_sub"
                ),
                func.count(EgonHvmvSubstation.point).label("subst_count"),
            )
            .filter(
                func.ST_Contains(
                    EgonHvmvSubstationVoronoiMunicipalityCuts.geom,
                    func.ST_Transform(EgonHvmvSubstation.point, 3035),
                )
            )
            .group_by(
                EgonHvmvSubstationVoronoiMunicipalityCuts.id,
                EgonHvmvSubstation.subst_id,
                EgonHvmvSubstation.point,
            )
            .subquery()
        )
        session.query(EgonHvmvSubstationVoronoiMunicipalityCuts).filter(
            EgonHvmvSubstationVoronoiMunicipalityCuts.id
            == cuts_substation_subquery.c.id
        ).update(
            {
                "subst_count": cuts_substation_subquery.c.subst_count,
                "subst_id": cuts_substation_subquery.c.subst_id,
                "geom_sub": cuts_substation_subquery.c.geom_sub,
            },
            synchronize_session="fetch",
        )
        session.commit()

        # Persist separate tables for polygons with and without substation
        # inside
        # First, insert all polygons with 1 substation
        cut_1subst = (
            session.query(EgonHvmvSubstationVoronoiMunicipalityCuts)
            .filter(EgonHvmvSubstationVoronoiMunicipalityCuts.subst_count == 1)
            .subquery()
        )

        cut_1subst_insert = EgonHvmvSubstationVoronoiMunicipalityCuts1Subst.__table__.insert().from_select(
            EgonHvmvSubstationVoronoiMunicipalityCuts1Subst.__table__.columns,
            cut_1subst,
        )
        session.execute(cut_1subst_insert)
        session.commit()

        # Second, polygons without a substation
        cut_0subst = (
            session.query(EgonHvmvSubstationVoronoiMunicipalityCuts)
            .filter(
                EgonHvmvSubstationVoronoiMunicipalityCuts.subst_count == None
            )
            .subquery()
        )

        # Determine nearest neighboring polygon that has a substation
        columns_from_cut1_subst = ["subst_id", "subst_count", "geom_sub"]
        cut_0subst_nearest_neighbor_sub = (
            session.query(
                *[
                    c
                    for c in cut_1subst.columns
                    if c.name in columns_from_cut1_subst
                ],
                *[
                    c
                    for c in cut_0subst.columns
                    if c.name not in columns_from_cut1_subst
                ]
            )
                .filter(
                cut_0subst.c.ags_0 == cut_1subst.c.ags_0,
                func.ST_DWithin(func.ST_ExteriorRing(cut_0subst.c.geom), func.ST_ExteriorRing(cut_1subst.c.geom), 100000)
            )
            .order_by(
                func.ST_Distance(func.ST_ExteriorRing(cut_0subst.c.geom), func.ST_ExteriorRing(cut_1subst.c.geom)),
            )
            .subquery()

        )

        # Group by id of cut polygons which is unique. The reason that multiple
        # rows for each id exist is that assignment to multiple polygon with
        # a substations would be possible. The are ordered by distance
        cut_0subst_nearest_neighbor_grouped = (
            session.query(cut_0subst_nearest_neighbor_sub.c.id)
            .group_by(cut_0subst_nearest_neighbor_sub.c.id)
            .subquery()
        )

        # Select one single assignment polygon (with substation) for each of
        # the polygons without a substation
        cut_0subst_nearest_neighbor = session.query(
            cut_0subst_nearest_neighbor_sub).filter(
            cut_0subst_nearest_neighbor_sub.c.id == cut_0subst_nearest_neighbor_grouped.c.id
        ).distinct(cut_0subst_nearest_neighbor_sub.c.id).subquery()

        cut_0subst_insert = EgonHvmvSubstationVoronoiMunicipalityCuts0Subst.__table__.insert().from_select(
            [
                c
                for c in cut_0subst_nearest_neighbor.columns
                if c.name not in ["temp_id"]
            ],
            cut_0subst_nearest_neighbor,
        )
        session.execute(cut_0subst_insert)
        session.commit()

        # TODO 3: join cut_1subst and cut_0subst and union/collect geom (polygon)
        # TODO 4: write the joined data to persistent table
# TODO: in the original script municipality geometries (i.e. model_draft.ego_grid_mv_griddistrict_type1) are casted into MultiPolyon. Maybe this is required later
# TODO: later, when grid districts are collected from different processing parts, ST_Multi(ST_Union(geometry)) is called. If geometry is of mixed type (Poylgon and MultiPolygon) results might be unexpected
