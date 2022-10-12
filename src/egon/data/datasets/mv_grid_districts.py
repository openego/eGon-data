"""
Medium-voltage grid districts describe the area supplied by one MV grid

Medium-voltage grid districts are defined by one polygon that represents the
supply area. Each MV grid district is connected to the HV grid via a single
substation.

The methods used for identifying the MV grid districts are heavily inspired
by `Hülk et al. (2017)
<https://somaesthetics.aau.dk/index.php/sepm/article/view/1833/1531>`_
(section 2.3), but the implementation differs in detail.
The main difference is that direct adjacency is preferred over proximity.
For polygons of municipalities
without a substation inside, it is iteratively checked for direct adjacent
other polygons that have a substation inside. Speaking visually, a MV grid
district grows around a polygon with a substation inside.

The grid districts are identified using three data sources

1. Polygons of municipalities (:class:`Vg250GemClean`)
2. HV-MV substations (:class:`EgonHvmvSubstation`)
3. HV-MV substation voronoi polygons (:class:`EgonHvmvSubstationVoronoi`)

Fundamentally, it is assumed that grid districts (supply areas) often go
along borders of administrative units, in particular along the borders of
municipalities due to the concession levy.
Furthermore, it is assumed that one grid district is supplied via a single
substation and that locations of substations and grid districts are designed
for aiming least lengths of grid line and cables.

With these assumptions, the three data sources from above are processed as
follows:

* Find the number of substations inside each municipality
* Split municipalities with more than one substation inside
  * Cut polygons of municipalities with voronoi polygons of respective
    substations
  * Assign resulting municipality polygon fragments to nearest substation
* Assign municipalities without a single substation to nearest substation in
  the neighborhood
* Merge all municipality polygons and parts of municipality polygons to a
  single polygon grouped by the assigned substation

For finding the nearest substation, as already said, direct adjacency is
preferred over closest distance. This means, the nearest substation does not
necessarily have to be the closest substation in the sense of beeline distance.
But it is the substation definitely located in a neighboring polygon. This
prevents the algorithm to find solutions where a MV grid districts consists of
multi-polygons with some space in between.
Nevertheless, beeline distance still plays an important role, as the algorithm
acts in two steps

1. Iteratively look for neighboring polygons until there are no further
   polygons
2. Find a polygon to assign to by minimum beeline distance

The second step is required in order to cover edge cases, such as islands.

For understanding how this is implemented into separate functions, please
see :func:`define_mv_grid_districts`.
"""

from functools import partial

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
from egon.data.datasets import Dataset
from egon.data.datasets.osmtgmod.substation import EgonHvmvSubstation
from egon.data.datasets.substation_voronoi import EgonHvmvSubstationVoronoi
from egon.data.db import session_scope

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


class VoronoiMunicipalityCutsBase(object):
    bus_id = Column(Integer)
    municipality_id = Column(Integer)
    voronoi_id = Column(Integer)
    ags_0 = Column(String)
    subst_count = Column(Integer)
    geom = Column(Geometry("Polygon", 3035))
    geom_sub = Column(Geometry("Point", 3035))


class VoronoiMunicipalityCuts(VoronoiMunicipalityCutsBase, Base):
    __tablename__ = "voronoi_municipality_cuts"
    __table_args__ = {"schema": "grid"}

    id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )


class VoronoiMunicipalityCutsAssigned(VoronoiMunicipalityCutsBase, Base):
    __tablename__ = "voronoi_municipality_cuts_assigned"
    __table_args__ = {"schema": "grid"}

    id = Column(Integer)
    temp_id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )


class MvGridDistrictsDissolved(Base):
    __tablename__ = "egon_mv_grid_district_dissolved"
    __table_args__ = {"schema": "grid"}

    id = Column(
        Integer,
        Sequence(f"{__tablename__}_id_seq", schema="grid"),
        primary_key=True,
    )
    bus_id = Column(Integer)
    geom = Column(Geometry("MultiPolygon", 3035))
    area = Column(Float)


class MvGridDistricts(Base):
    __tablename__ = "egon_mv_grid_district"
    __table_args__ = {"schema": "grid"}

    bus_id = Column(Integer, primary_key=True)
    geom = Column(Geometry("MultiPolygon", 3035))
    area = Column(Float)


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
    """
    Split municipalities that have more than one substation

    Municipalities that contain more than one HV-MV substation in their
    polygon are cut by HV-MV voronoi polygons. Resulting fragments are then
    assigned to the next neighboring polygon that has a substation.

    In detail, the following steps are performed:

    * Step 1: cut municipalities with voronoi polygons
    * Step 2: Determine number of substations inside cut polygons
    * Step 3: separate cut polygons with exactly one substation inside
    * Step 4: Assign polygon without a substation to next neighboring
      polygon with a substation
    * Step 5: Assign remaining polygons that are non-touching


    """
    engine = db.engine()
    VoronoiMunicipalityCuts.__table__.drop(bind=engine, checkfirst=True)
    VoronoiMunicipalityCuts.__table__.create(bind=engine)
    VoronoiMunicipalityCutsAssigned.__table__.drop(
        bind=engine, checkfirst=True
    )
    VoronoiMunicipalityCutsAssigned.__table__.create(bind=engine)

    with session_scope() as session:
        # Step 1: cut municipalities with voronoi polygons
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
                EgonHvmvSubstationVoronoi.bus_id,
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

        voronoi_cuts = VoronoiMunicipalityCuts.__table__.insert().from_select(
            [
                VoronoiMunicipalityCuts.municipality_id,
                VoronoiMunicipalityCuts.ags_0,
                VoronoiMunicipalityCuts.geom,
                VoronoiMunicipalityCuts.bus_id,
                VoronoiMunicipalityCuts.voronoi_id,
            ],
            q,
        )
        session.execute(voronoi_cuts)
        session.commit()

        # Step 2: Determine number of substations inside cut polygons
        cuts_substation_subquery = (
            session.query(
                VoronoiMunicipalityCuts.id,
                EgonHvmvSubstation.bus_id,
                func.ST_Transform(EgonHvmvSubstation.point, 3035).label(
                    "geom_sub"
                ),
                func.count(EgonHvmvSubstation.point).label("subst_count"),
            )
            .filter(
                func.ST_Contains(
                    VoronoiMunicipalityCuts.geom,
                    func.ST_Transform(EgonHvmvSubstation.point, 3035),
                )
            )
            .group_by(
                VoronoiMunicipalityCuts.id,
                EgonHvmvSubstation.bus_id,
                EgonHvmvSubstation.point,
            )
            .subquery()
        )
        session.query(VoronoiMunicipalityCuts).filter(
            VoronoiMunicipalityCuts.id == cuts_substation_subquery.c.id
        ).update(
            {
                "subst_count": cuts_substation_subquery.c.subst_count,
                "bus_id": cuts_substation_subquery.c.bus_id,
                "geom_sub": cuts_substation_subquery.c.geom_sub,
            },
            synchronize_session="fetch",
        )
        session.commit()

        # Step 3: separate cut polygons with exactly one substation inside
        # These polygons are taken as reference to assign other parts of cut
        # polygons subsequently
        cut_1subst = (
            session.query(VoronoiMunicipalityCuts)
            .filter(VoronoiMunicipalityCuts.subst_count == 1)
            .subquery()
        )

        originally_1subst = (
            VoronoiMunicipalityCutsAssigned.__table__.insert().from_select(
                [
                    _
                    for _ in VoronoiMunicipalityCutsAssigned.__table__.columns
                    if _.name != "temp_id"
                ],
                cut_1subst,
            )
        )
        session.execute(originally_1subst)
        session.commit()

        # Step 4: Assign polygon without a substation to next neighboring
        # polygon with a substation.
        # This considers only polygons that are directly neighboring poylgons
        # without any space in between (aka. polygons touch each other)

        # Initialize with very large number
        remaining_polygons = [10 ** 10]

        while True:
            # This loop runs until all polygon that inital haven't had a
            # substation inside to a next neighboring polygon.
            # The assignment process is performed iteratively. In each
            # iteration, touching polygons are used for assignment
            already_assigned_polygons_query = session.query(
                VoronoiMunicipalityCutsAssigned.id
            ).all()
            already_assigned_polygons = [
                p for p, in already_assigned_polygons_query
            ]
            cut_0subst = (
                session.query(VoronoiMunicipalityCuts)
                .filter(VoronoiMunicipalityCuts.subst_count == None)
                .filter(
                    VoronoiMunicipalityCuts.id.notin_(
                        already_assigned_polygons
                    )
                )
            )
            remaining_polygons.append(len(cut_0subst.all()))

            # Select polygons for assignment
            # This has to be done iteratively, because already assigned
            # polygons that don't have a substation assigned initially, are
            # considered as assignment target subsequently
            relevant_columns = [
                col
                for col in VoronoiMunicipalityCutsAssigned.__table__.columns
                if col.name != "temp_id"
            ]
            polygons_for_assignment = session.query(
                *relevant_columns
            ).subquery()

            # Check if in the last iteration polygons were assigned. If not,
            # there are no further polygons without a substation that touch
            # another polygon that has a substation or that was already
            # assigned
            if (remaining_polygons[-1]) < remaining_polygons[-2]:
                assign_substation_municipality_fragments(
                    polygons_for_assignment,
                    cut_0subst.subquery(),
                    "touches",
                    session,
                )
            else:
                break

        # Step 5: Assign remaining polygons that are non-touching
        assign_substation_municipality_fragments(
            polygons_for_assignment,
            cut_0subst.subquery(),
            "min_distance",
            session,
        )


def assign_substation_municipality_fragments(
    with_substation, without_substation, strategy, session
):
    """
    Assign bus_id from next neighboring polygon to municipality fragment

    For parts municipalities without a substation inside their polygon the
    next municipality polygon part is found and assigned.

    Resulting data including information about the assigned substation is
    saved to :class:`VoronoiMunicipalityCutsAssigned`.

    Parameters
    ----------
    with_substation: SQLAlchemy subquery
        Polygons that have a substation inside or are assigned to a substation
    without_substation: SQLAlchemy subquery
        Subquery that includes polygons without a substation
    strategy: str
        Either

        * "touches": Only polygons that touch another polygon from
          `with_substation` are considered
        * "within": Only polygons within a radius of 100 km of polygons
          without substation are considered for assignment
    session: SQLAlchemy session
        SQLAlchemy session obejct

    See Also
    --------
    The function :func:`nearest_polygon_with_substation` is very similar, but
    different in detail.
    """
    # Determine nearest neighboring polygon that has a substation
    columns_from_cut1_subst = ["bus_id", "subst_count", "geom_sub"]

    if strategy == "touches":
        neighboring_criterion = func.ST_Touches(
            without_substation.c.geom, with_substation.c.geom
        )
    elif strategy == "min_distance":
        neighboring_criterion = func.ST_DWithin(
            without_substation.c.geom, with_substation.c.geom, 100000
        )
    else:
        raise ValueError(f"Invalid input for 'strategy': {strategy}")
    cut_0subst_nearest_neighbor_sub = (
        (
            session.query(
                *[
                    c
                    for c in with_substation.columns
                    if c.name in columns_from_cut1_subst
                ],
                *[
                    c
                    for c in without_substation.columns
                    if c.name not in columns_from_cut1_subst
                ],
            )
        )
        .filter(without_substation.c.ags_0 == with_substation.c.ags_0)
        .filter(neighboring_criterion)
        .order_by(
            without_substation.c.id,
            func.ST_Distance(
                without_substation.c.geom, with_substation.c.geom
            ),
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
    cut_0subst_nearest_neighbor = (
        session.query(cut_0subst_nearest_neighbor_sub)
        .filter(
            cut_0subst_nearest_neighbor_sub.c.id
            == cut_0subst_nearest_neighbor_grouped.c.id
        )
        .distinct(cut_0subst_nearest_neighbor_sub.c.id)
        .subquery()
    )

    cut_0subst_insert = (
        VoronoiMunicipalityCutsAssigned.__table__.insert().from_select(
            [
                c
                for c in cut_0subst_nearest_neighbor.columns
                if c.name not in ["temp_id"]
            ],
            cut_0subst_nearest_neighbor,
        )
    )
    session.execute(cut_0subst_insert)
    session.commit()


def merge_polygons_to_grid_district():
    """
    Merge municipality polygon (parts) to MV grid districts

    Polygons of municipalities and cut parts of such polygons are merged to
    a single grid district per one HV-MV substation. Prior determined
    assignment of cut polygons parts is used as well as proximity of entire
    municipality polygons to polygons with a substation inside.

    * Step 1: Merge municipality parts that are assigned to the same substation
    * Step 2: Insert municipality polygons with exactly one substation
    * Step 3: Assign municipality polygons without a substation and insert
      to table
    * Step 4: Merge MV grid district parts
    """

    engine = db.engine()
    MvGridDistrictsDissolved.__table__.drop(bind=engine, checkfirst=True)
    MvGridDistrictsDissolved.__table__.create(bind=engine)
    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {MvGridDistricts.__table__.schema}.{MvGridDistricts.__table__.name} CASCADE; 
        """
    )

    MvGridDistricts.__table__.create(bind=engine)

    with session_scope() as session:
        # Step 1: Merge municipality parts cut by voronoi polygons according
        # to prior determined associated substation
        joined_municipality_parts = session.query(
            VoronoiMunicipalityCutsAssigned.bus_id,
            func.ST_Multi(
                func.ST_Union(VoronoiMunicipalityCutsAssigned.geom)
            ).label("geom"),
            func.sum(func.ST_Area(VoronoiMunicipalityCutsAssigned.geom)).label(
                "area"
            ),
        ).group_by(VoronoiMunicipalityCutsAssigned.bus_id)

        joined_municipality_parts_insert = (
            MvGridDistrictsDissolved.__table__.insert().from_select(
                [
                    c
                    for c in MvGridDistrictsDissolved.__table__.columns
                    if c.name != "id"
                ],
                joined_municipality_parts.subquery(),
            )
        )
        session.execute(joined_municipality_parts_insert)
        session.commit()

        # Step 2: Insert municipality polygons with exactly one substation
        one_substation = (
            session.query(
                EgonHvmvSubstation.bus_id,
                func.ST_Multi(HvmvSubstPerMunicipality.geometry).label("geom"),
                func.ST_Area(
                    func.ST_Multi(HvmvSubstPerMunicipality.geometry)
                ).label("area"),
            )
            .filter(HvmvSubstPerMunicipality.subst_count == 1)
            .filter(
                func.ST_Contains(
                    HvmvSubstPerMunicipality.geometry,
                    func.ST_Transform(EgonHvmvSubstation.point, 3035),
                )
            )
        )

        one_substation_insert = (
            MvGridDistrictsDissolved.__table__.insert().from_select(
                [
                    c
                    for c in MvGridDistrictsDissolved.__table__.columns
                    if c.name != "id"
                ],
                one_substation.subquery(),
            )
        )
        session.execute(one_substation_insert)
        session.commit()

        # Step 3: Assign municipality polygons without a substation and insert
        # to table
        already_assigned = []
        while True:
            previous_ids_length = len(already_assigned)
            with_substation = session.query(
                MvGridDistrictsDissolved.bus_id,
                MvGridDistrictsDissolved.geom,
                MvGridDistrictsDissolved.id,
            ).subquery()
            without_substation = (
                session.query(
                    HvmvSubstPerMunicipality.geometry.label("geom"),
                    HvmvSubstPerMunicipality.id,
                )
                .filter(HvmvSubstPerMunicipality.subst_count == 0)
                .filter(HvmvSubstPerMunicipality.id.notin_(already_assigned))
                .subquery()
            )

            # Find nearest neighboring polygon from with_substation for each
            # polygon from without_substation
            newly_assigned_ids = nearest_polygon_with_substation(
                with_substation, without_substation, "touches", session
            )
            already_assigned.extend(newly_assigned_ids)

            if not len(already_assigned) > previous_ids_length:
                nearest_polygon_with_substation(
                    with_substation, without_substation, "within", session
                )
                break

        # Step 4: Merge MV grid district parts
        # Forms one (multi-)polygon for each substation
        joined_mv_grid_district_parts = session.query(
            MvGridDistrictsDissolved.bus_id,
            func.ST_Multi(
                func.ST_Buffer(
                    func.ST_Buffer(
                        func.ST_Union(MvGridDistrictsDissolved.geom), 0.1
                    ),
                    -0.1,
                )
            ).label("geom"),
            func.sum(MvGridDistrictsDissolved.area).label("area"),
        ).group_by(MvGridDistrictsDissolved.bus_id)

        joined_mv_grid_district_parts_insert = (
            MvGridDistricts.__table__.insert().from_select(
                MvGridDistricts.__table__.columns,
                joined_mv_grid_district_parts.subquery(),
            )
        )
        session.execute(joined_mv_grid_district_parts_insert)
        session.commit()


def nearest_polygon_with_substation(
    with_substation, without_substation, strategy, session
):
    """
    Assign next neighboring polygon

    For municipalities without a substation inside their polygon the next MV
    grid district (part) polygon is found and assigned.

    Resulting data including information about the assigned substation is
    saved to :class:`MvGridDistrictsDissolved`.

    Parameters
    ----------
    with_substation: SQLAlchemy subquery
        Polygons that have a substation inside or are assigned to a substation
    without_substation: SQLAlchemy subquery
        Subquery that includes polygons without a substation
    strategy: str
        Either

        * "touches": Only polygons that touch another polygon from
          `with_substation` are considered
        * "within": Only polygons within a radius of 100 km of polygons
          without substation are considered for assignment
    session: SQLAlchemy session
        SQLAlchemy session obejct

    Returns
    -------
    list
        IDs of polygons that were already assigned to a polygon with a
        substation
    """
    if strategy == "touches":
        neighboring_criterion = func.ST_Touches(
            without_substation.c.geom, with_substation.c.geom
        )
    elif strategy == "within":
        neighboring_criterion = func.ST_DWithin(
            without_substation.c.geom, with_substation.c.geom, 100000
        )
    else:
        raise ValueError(f"Invalid input for 'strategy': {strategy}")

    # Find nearest neighboring polygon from with_substation for each
    # polygon from without_substation
    all_nearest_neighbors = (
        session.query(
            without_substation.c.id,
            func.ST_Multi(without_substation.c.geom).label("geom"),
            with_substation.c.bus_id,
            func.ST_Area(func.ST_Multi(without_substation.c.geom)).label(
                "area"
            ),
        )
        .filter(neighboring_criterion)
        .order_by(
            without_substation.c.id,
            func.ST_Distance(
                without_substation.c.geom, with_substation.c.geom
            ),
            # with_substation.c.id
            func.ST_Distance(
                func.ST_Centroid(without_substation.c.geom),
                func.ST_Centroid(with_substation.c.geom),
            ),
        )
        .subquery()
    )

    # Save list of newly assigned polygons
    newly_assigned = (
        session.query(all_nearest_neighbors.c.id)
        .distinct(all_nearest_neighbors.c.id)
        .all()
    )
    newly_assigned_ids = [i for i, in newly_assigned]

    # Take only one candidate polygon for assgning it
    nearest_neighbors = session.query(
        all_nearest_neighbors.c.bus_id,
        all_nearest_neighbors.c.geom,
        all_nearest_neighbors.c.area,
    ).distinct(all_nearest_neighbors.c.id)

    # Insert polygons with newly assigned substation
    assigned_polygons_insert = (
        MvGridDistrictsDissolved.__table__.insert().from_select(
            [
                c
                for c in MvGridDistrictsDissolved.__table__.columns
                if c.name not in ["id"]
            ],
            nearest_neighbors,
        )
    )
    session.execute(assigned_polygons_insert)
    session.commit()

    return newly_assigned_ids


def define_mv_grid_districts():
    """
    Define spatial extent of MV grid districts

    The process of identifying the boundary of medium-voltage grid districts
    is organized in three steps

    1. :func:`substations_in_municipalities`: The number of substations
      located inside each municipality is calculated
    2. :func:`split_multi_substation_municipalities`: The municipalities with
      >1 substation inside are split by Voronoi polygons around substations
    3. :func:`merge_polygons_to_grid_district`: All polygons are merged such
      that one polygon has exactly one single substation inside

    Finally, intermediate tables used for storing data temporarily are deleted.
    """
    substations_in_municipalities()
    split_multi_substation_municipalities()
    merge_polygons_to_grid_district()

    engine = db.engine()
    HvmvSubstPerMunicipality.__table__.drop(bind=engine, checkfirst=True)
    VoronoiMunicipalityCuts.__table__.drop(bind=engine, checkfirst=True)
    VoronoiMunicipalityCutsAssigned.__table__.drop(
        bind=engine, checkfirst=True
    )
    MvGridDistrictsDissolved.__table__.drop(bind=engine, checkfirst=True)


mv_grid_districts_setup = partial(
    Dataset,
    name="MvGridDistricts",
    version="0.0.3",
    dependencies=[],
    tasks=(define_mv_grid_districts),
)
