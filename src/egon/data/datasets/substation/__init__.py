"""The central module containing code to create substation tables

"""
from airflow.operators.postgres_operator import PostgresOperator
from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
import importlib_resources as resources

from egon.data import db
from egon.data.datasets import Dataset
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
    def __init__(self, dependencies):
        super().__init__(
            name="substation_extraction",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(
                create_tables,
                create_sql_functions,
                {
                    PostgresOperator(
                        task_id="hvmv_substation",
                        sql=resources.read_text(
                            __name__, "hvmv_substation.sql"
                        ),
                        postgres_conn_id="egon_data",
                        autocommit=True,
                    ),
                    PostgresOperator(
                        task_id="ehv_substation",
                        sql=resources.read_text(
                            __name__, "ehv_substation.sql"
                        ),
                        postgres_conn_id="egon_data",
                        autocommit=True,
                    ),
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
    cfg_targets = egon.data.config.datasets()["substation_extraction"][
        "targets"
    ]

    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg_targets['hvmv_substation']['schema']};"
    )

    # Drop tables
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {cfg_targets['ehv_substation']['schema']}.
            {cfg_targets['ehv_substation']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {cfg_targets['hvmv_substation']['schema']}.
            {cfg_targets['hvmv_substation']['table']} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS
            {cfg_targets['hvmv_substation']['schema']}.
            {cfg_targets['hvmv_substation']['table']}_bus_id_seq CASCADE;"""
    )

    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS
            {cfg_targets['ehv_substation']['schema']}.
            {cfg_targets['ehv_substation']['table']}_bus_id_seq CASCADE;"""
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
    db.execute_sql(
        """
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
        """
    )

    # Create function: relation_geometry
    # Function creates a geometry point from relation parts of type way

    db.execute_sql(
        """
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
        """
    )

    # Create function: ST_Buffer_Meters(geometry, double precision)

    db.execute_sql(
        """
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
        """
    )


def transfer_busses():

    targets = egon.data.config.datasets()["substation_extraction"][
        "targets"
    ]

    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {targets['transfer_busses']['table']};
        CREATE TABLE {targets['transfer_busses']['table']} AS
        SELECT DISTINCT ON (osm_id) * FROM
        (SELECT * FROM {targets['ehv_substation']['schema']}.
         {targets['ehv_substation']['table']}
        UNION SELECT bus_id, lon, lat, point, polygon, voltage,
        power_type, substation, osm_id, osm_www, frequency, subst_name,
        ref, operator, dbahn, status
        FROM {targets['hvmv_substation']['schema']}.
         {targets['hvmv_substation']['table']} ORDER BY osm_id) as foo;
        """)
