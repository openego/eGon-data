"""The central module containing code to create substation tables"""

import os

from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
import egon.data.config

Base = declarative_base()


class EgonEhvTransferBuses(Base):
    __tablename__ = "egon_ehv_transfer_buses"
    __table_args__ = {"schema": "grid"}
    bus_id = Column(
        Integer,
        Sequence("egon_ehv_transfer_buses_bus_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_ehv_transfer_buses_bus_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry("POINT", 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)


class EgonHvmvTransferBuses(Base):
    __tablename__ = "egon_hvmv_transfer_buses"
    __table_args__ = {"schema": "grid"}
    bus_id = Column(
        Integer,
        Sequence("egon_hvmv_transfer_buses_bus_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_hvmv_transfer_buses_bus_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry("POINT", 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)


class SubstationExtraction(Dataset):

    sources = DatasetSources(
        tables={
            "osm_ways": "openstreetmap.osm_ways",
            "osm_nodes": "openstreetmap.osm_nodes",
            "osm_points": "openstreetmap.osm_point",
            "osm_lines": "openstreetmap.osm_line",
        }
    )

    targets = DatasetTargets(
        tables={
            "hvmv_substation": "grid.egon_hvmv_transfer_buses",
            "ehv_substation": "grid.egon_ehv_transfer_buses",
            "transfer_busses": "public.transfer_busses_complete",  # Assuming public schema
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name="substation_extraction",
            version="0.0.6",
            dependencies=dependencies,
            tasks=(
                create_tables,
                create_sql_functions,
                {
                    extract_hvmv,
                    extract_ehv,
                },
                transfer_busses,
            ),
        )


def create_tables():
    """Create tables for substation data
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS grid;")

    db.execute_sql(
        f"""DROP TABLE IF EXISTS {SubstationExtraction.targets.tables['ehv_substation']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS {SubstationExtraction.targets.tables['hvmv_substation']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS {SubstationExtraction.targets.tables['hvmv_substation']}_bus_id_seq CASCADE;"""
    )

    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS {SubstationExtraction.targets.tables['ehv_substation']}_bus_id_seq CASCADE;"""
    )

    engine = db.engine()
    EgonEhvTransferBuses.__table__.create(bind=engine, checkfirst=True)
    EgonHvmvTransferBuses.__table__.create(bind=engine, checkfirst=True)


def create_sql_functions():
    """Defines Postgresql functions needed to extract substation from osm

    Returns
    -------
    None.

    """

    # Create function: utmzone(geometry)
    # source: http://www.gistutor.com/postgresqlpostgis/6-advanced-postgresqlpostgis-tutorials/58-postgis-buffer-latlong-and-other-projections-using-meters-units-custom-stbuffermeters-function.html
    db.execute_sql("""
        DROP FUNCTION IF EXISTS utmzone(geometry) CASCADE;
        CREATE OR REPLACE FUNCTION utmzone(geometry)
        RETURNS integer AS
        $BODY$
        DECLARE
        geomgeog geometry;
        zone int;
        pref int;

        BEGIN
        geomgeog:= ST_Transform($1,4326);

        IF (ST_Y(geomgeog))>0 THEN
        pref:=32600;
        ELSE
        pref:=32700;
        END IF;

        zone:=floor((ST_X(geomgeog)+180)/6)+1;

        RETURN zone+pref;
        END;
        $BODY$ LANGUAGE 'plpgsql' IMMUTABLE
        COST 100;
        """)

    # Create function: relation_geometry
    # Function creates a geometry point from relation parts of type way

    db.execute_sql("""
        DROP FUNCTION IF EXISTS relation_geometry (members text[]) CASCADE;
        CREATE OR REPLACE FUNCTION relation_geometry (members text[])
        RETURNS geometry
        AS $$
        DECLARE
        way  geometry;
        BEGIN
            way = (SELECT ST_SetSRID
                   (ST_MakePoint((max(lon) + min(lon))/200.0,(max(lat) + min(lat))/200.0),900913)
                   FROM openstreetmap.osm_nodes
                   WHERE id in (SELECT unnest(nodes)
                     FROM openstreetmap.osm_ways
                     WHERE id in (SELECT trim(leading 'w' from member)::bigint
			                     FROM (SELECT unnest(members) as member) t
	                               WHERE member~E'[w,1,2,3,4,5,6,7,8,9,0]')));
        RETURN way;
        END;
        $$ LANGUAGE plpgsql;
        """)

    # Create function: ST_Buffer_Meters(geometry, double precision)

    db.execute_sql("""
        DROP FUNCTION IF EXISTS ST_Buffer_Meters(geometry, double precision) CASCADE;
        CREATE OR REPLACE FUNCTION ST_Buffer_Meters(geometry, double precision)
        RETURNS geometry AS
        $BODY$
        DECLARE
        orig_srid int;
        utm_srid int;

        BEGIN
        orig_srid:= ST_SRID($1);
        utm_srid:= utmzone(ST_Centroid($1));

        RETURN ST_transform(ST_Buffer(ST_transform($1, utm_srid), $2), orig_srid);
        END;
        $BODY$ LANGUAGE 'plpgsql' IMMUTABLE
        COST 100;
        """)


def transfer_busses():
    """Combine EHV and HV/MV transfer buses into one table for osmTGmod.

    The EHV and HV/MV transfer buses are unioned and reduced to a single
    row per ``osm_id``.

    ``DISTINCT ON`` and ``ORDER BY`` have to live in the *same* query:
    PostgreSQL only defines which row of a group survives a
    ``DISTINCT ON`` if that very query is ordered. Ordering the subquery
    instead, as this used to do, leaves the choice to the query planner,
    cf. `#769 <https://github.com/openego/eGon-data/issues/769>`_.

    Duplicate ``osm_id``\\ s are the rule rather than the exception here:
    a substation can show up in both source tables, and
    ``hvmv_substation.sql`` unions the ``osm_polygon`` and ``osm_line``
    geometries of the same way, which yields two rows whose centroid -
    the one value osmTGmod actually reads - differs.

    The tie is broken on ``status`` first, which is 1 where the voltage
    was tagged explicitly, and then on the content of the row, so that
    the choice is stable across runs. ``bus_id`` is only the key of last
    resort, and only usable as one because ``hvmv_substation.sql`` and
    ``ehv_substation.sql`` now fill their sequences in a defined order -
    do not drop the ``ORDER BY`` from those two ``INSERT``\\ s without
    also revisiting this.

    Careful when touching the column list: osmTGmod reads the CSV export
    of this table by hardcoded column index (see
    :py:func:`egon.data.datasets.osmtgmod.osmtgmod`), so neither the
    order nor the number of columns may change.
    """

    db.execute_sql(f"""
        DROP TABLE IF EXISTS {SubstationExtraction.targets.tables['transfer_busses']};
        CREATE TABLE {SubstationExtraction.targets.tables['transfer_busses']} AS
        SELECT DISTINCT ON (osm_id) * FROM
        (SELECT * FROM {SubstationExtraction.targets.tables['ehv_substation']}
        UNION SELECT bus_id, lon, lat, point, polygon, voltage,
        power_type, substation, osm_id, osm_www, frequency, subst_name,
        ref, operator, dbahn, status
        FROM {SubstationExtraction.targets.tables['hvmv_substation']}) as foo
        ORDER BY osm_id, status, ST_AsBinary(point), voltage,
                 ST_AsBinary(polygon), bus_id;
        """)


def extract_ehv():
    db.execute_sql_script(os.path.dirname(__file__) + "/ehv_substation.sql")


def extract_hvmv():
    db.execute_sql_script(os.path.dirname(__file__) + "/hvmv_substation.sql")
