"""The central module containing code to create CH4 and H2 voronoi polygones

"""
from egon.data import db
from egon.data.datasets import Dataset
from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base

class GasAreas(Dataset):
     def __init__(self, dependencies):
         super().__init__(
             name="GasAreas",
             version="0.0.0",
             dependencies=dependencies,
             tasks=(create_voronoi),
         )


Base = declarative_base()

class EgonCH4VoronoiTmp(Base):
    __tablename__ = "egon_ch4_voronoi_tmp"
    __table_args__ = {"schema": "grid"}
    id = Column(
        Integer,
        Sequence("egon_ch4_voronoi_tmp_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_ch4_voronoi_tmp_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    bus_id = Column(Integer)
    geom = Column(Geometry("Polygon", 4326))


class EgonH2VoronoiTmp(Base):
    __tablename__ = "egon_h2_voronoi_tmp"
    __table_args__ = {"schema": "grid"}
    id = Column(
        Integer,
        Sequence("egon_h2_voronoi_tmp_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_h2_voronoi_tmp_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    bus_id = Column(Integer)
    geom = Column(Geometry("Polygon", 4326))


def create_CH4_voronoi(scn_name='eGon2035'):
    """
    Creates voronoi polygons for CH4 buses

    Returns
    -------
    None.

    """

    db.execute_sql(
        """
    DROP TABLE IF EXISTS grid.egon_ch4_voronoi_tmp CASCADE;
    DROP SEQUENCE IF EXISTS grid.egon_ch4_voronoi_tmp_id_seq CASCADE;
    """
    )

    engine = db.engine()
    EgonCH4VoronoiTmp.__table__.create(bind=engine, checkfirst=True)

    db.execute_sql(
        """
    DROP TABLE IF EXISTS grid.egon_voronoi_ch4 CASCADE;
    CREATE TABLE grid.egon_voronoi_ch4 (
        id Integer,
        bus_id Integer,
        geom Geometry('Multipolygon', 4326)
        );
    """
    )

    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS grid.egon_ch4_bus CASCADE;

        SELECT bus_id, bus_id as id, geom as point
        INTO grid.egon_ch4_bus
        FROM grid.egon_etrago_bus
        WHERE carrier = 'CH4' AND scn_name = '{scn_name}';

        """
    )

    schema = "grid"
    substation_table = "egon_ch4_bus"
    voronoi_table = "egon_ch4_voronoi_tmp"
    voronoi_table_f = "egon_voronoi_ch4"
    view = "grid.egon_voronoi_no_borders"
    boundary = "boundaries.vg250_sta_union"

    # Create view for Voronoi polygons without taking borders into account
    db.execute_sql(
        f"DROP VIEW IF EXISTS {schema}.egon_voronoi_no_borders CASCADE;"
    )

    db.execute_sql(
        f"""
        CREATE VIEW {view} AS
           SELECT (ST_Dump(ST_VoronoiPolygons(
               ST_collect(a.point),
               0.0,
               (SELECT ST_Transform(geometry, 4326) FROM {boundary})
               ))).geom
           FROM {schema}.{substation_table} a;
        """
    )

    # Fill table with voronoi polygons
    db.execute_sql(
        f"""
        INSERT INTO {schema}.{voronoi_table} (geom)
        SELECT geom FROM {view};
        """
    )

    # Assign substation id as foreign key
    db.execute_sql(
        f"""
        UPDATE {schema}.{voronoi_table} AS t1
            SET  	bus_id = t2.bus_id
	            FROM	(SELECT	voi.id AS id,
			                sub.bus_id ::integer AS bus_id
		            FROM	{schema}.{voronoi_table} AS voi,
			                {schema}.{substation_table} AS sub
		            WHERE  	voi.geom && sub.point AND
			                ST_CONTAINS(voi.geom,sub.point)
		           GROUP BY voi.id,sub.bus_id
		           )AS t2
	            WHERE  	t1.id = t2.id;
        """
    )

    db.execute_sql(
        f"""
        CREATE INDEX  	{voronoi_table}_idx
            ON          {schema}.{voronoi_table} USING gist (geom);
        """
    )

    # Clip Voronoi with boundaries
    db.execute_sql(
        f"""
        INSERT INTO {schema}.{voronoi_table_f} (id, bus_id, geom)
            SELECT b.id, bus_id, ST_Multi(ST_Intersection(
            ST_Transform(a.geometry, 4326), b.geom)) AS geom
            FROM {boundary} a
            CROSS JOIN {schema}.{voronoi_table} b;
        """
    )

    db.execute_sql(
        f"""
    DROP TABLE IF EXISTS {schema}.{voronoi_table} CASCADE;
    DROP VIEW IF EXISTS {view} CASCADE;
    DROP TABLE IF EXISTS {schema}.{substation_table} CASCADE;
    """
    )


def create_H2_voronoi(scn_name='eGon2035'):
    """
    Creates voronoi polygons for H2 buses

    Returns
    -------
    None.

    """

    db.execute_sql(
        """
    DROP TABLE IF EXISTS grid.egon_h2_voronoi_tmp CASCADE;
    DROP SEQUENCE IF EXISTS grid.egon_h2_voronoi_tmp_id_seq CASCADE;
    """
    )

    engine = db.engine()
    EgonH2VoronoiTmp.__table__.create(bind=engine, checkfirst=True)

    db.execute_sql(
        """
    DROP TABLE IF EXISTS grid.egon_voronoi_h2 CASCADE;
    CREATE TABLE grid.egon_voronoi_h2 (
        id Integer,
        bus_id Integer,
        geom Geometry('Multipolygon', 4326)
        );
    """
    )

    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS grid.egon_h2_bus CASCADE;

        SELECT bus_id, bus_id as id, geom as point
        INTO grid.egon_h2_bus
        FROM grid.egon_etrago_bus
        WHERE carrier LIKE 'H2%%' AND scn_name = '{scn_name}';

        """
    )

    schema = "grid"
    substation_table = "egon_h2_bus"
    voronoi_table = "egon_h2_voronoi_tmp"
    voronoi_table_f = "egon_voronoi_h2"
    view = "grid.egon_voronoi_no_borders"
    boundary = "boundaries.vg250_sta_union"

    # Create view for Voronoi polygons without taking borders into account
    db.execute_sql(
        f"DROP VIEW IF EXISTS {schema}.egon_voronoi_no_borders CASCADE;"
    )

    db.execute_sql(
        f"""
        CREATE VIEW {view} AS
           SELECT (ST_Dump(ST_VoronoiPolygons(
               ST_collect(a.point),
               0.0,
               (SELECT ST_Transform(geometry, 4326) FROM {boundary})
               ))).geom
           FROM {schema}.{substation_table} a;
        """
    )

    # Fill table with voronoi polygons
    db.execute_sql(
        f"""
        INSERT INTO {schema}.{voronoi_table} (geom)
        SELECT geom FROM {view};
        """
    )

    # Assign substation id as foreign key
    db.execute_sql(
        f"""
        UPDATE {schema}.{voronoi_table} AS t1
            SET  	bus_id = t2.bus_id
	            FROM	(SELECT	voi.id AS id,
			                sub.bus_id ::integer AS bus_id
		            FROM	{schema}.{voronoi_table} AS voi,
			                {schema}.{substation_table} AS sub
		            WHERE  	voi.geom && sub.point AND
			                ST_CONTAINS(voi.geom,sub.point)
		           GROUP BY voi.id,sub.bus_id
		           )AS t2
	            WHERE  	t1.id = t2.id;
        """
    )

    db.execute_sql(
        f"""
        CREATE INDEX  	{voronoi_table}_idx
            ON          {schema}.{voronoi_table} USING gist (geom);
        """
    )

    # Clip Voronoi with boundaries
    db.execute_sql(
        f"""
        INSERT INTO {schema}.{voronoi_table_f} (id, bus_id, geom)
            SELECT b.id, bus_id, ST_Multi(ST_Intersection(
            ST_Transform(a.geometry, 4326), b.geom)) AS geom
            FROM {boundary} a
            CROSS JOIN {schema}.{voronoi_table} b;
        """
    )

    db.execute_sql(
        f"""
    DROP TABLE IF EXISTS {schema}.{voronoi_table} CASCADE;
    DROP VIEW IF EXISTS {view} CASCADE;
    DROP TABLE IF EXISTS {schema}.{substation_table} CASCADE;
    """
    )


def create_voronoi():

    create_CH4_voronoi()

    create_H2_voronoi()
