"""The central module containing code to create substation voronois

"""

import egon.data.config
from egon.data import db
from egon.data.datasets import Dataset,  DatasetSources, DatasetTargets
from sqlalchemy import Column, Integer, Sequence
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry

Base = declarative_base()


class SubstationVoronoi(Dataset):
    name: str = "substation_voronoi"
    version: str = "0.0.0"

    # Defined sources and targets for the file
    sources = DatasetSources(
        tables={
            "boundaries": {"schema": "boundaries", "table": "vg250_sta_union"},  
            "hvmv_substation": {"schema": "grid", "table": "egon_hvmv_substation"},
            "ehv_substation": {"schema": "grid", "table": "egon_ehv_substation"},
        }
    )

    targets = DatasetTargets(
        tables={
            "ehv_substation_voronoi": {
                "schema": "grid",
                "table": "egon_ehv_substation_voronoi",
            },
            "hvmv_substation_voronoi": {
                "schema": "grid",
                "table": "egon_hvmv_substation_voronoi",
            },
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                substation_voronoi,
            ),
        )

class EgonHvmvSubstationVoronoi(Base):
    __tablename__ = "egon_hvmv_substation_voronoi"
    __table_args__ = {"schema": "grid"}
    id = Column(
        Integer,
        Sequence("egon_hvmv_substation_voronoi_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_hvmv_substation_voronoi_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    bus_id = Column(Integer)
    geom = Column(Geometry("Multipolygon", 4326))


class EgonEhvSubstationVoronoi(Base):
    __tablename__ = "egon_ehv_substation_voronoi"
    __table_args__ = {"schema": "grid"}
    id = Column(
        Integer,
        Sequence("egon_ehv_substation_voronoi_id_seq", schema="grid"),
        server_default=Sequence(
            "egon_ehv_substation_voronoi_id_seq", schema="grid"
        ).next_value(),
        primary_key=True,
    )
    bus_id = Column(Integer)
    geom = Column(Geometry("Multipolygon", 4326))


def create_tables():
    """Create tables for voronoi polygons
    Returns
    -------
    None.
    """

    #cfg_voronoi = egon.data.config.datasets()["substation_voronoi"]["targets"]


    db.execute_sql(
        f"""DROP TABLE IF EXISTS
             {SubstationVoronoi.targets.tables['ehv_substation_voronoi']['schema']}.
             {SubstationVoronoi.targets.tables['ehv_substation_voronoi']['table']} CASCADE;"""
    )


    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {SubstationVoronoi.targets.tables['hvmv_substation_voronoi']['schema']}.
            {SubstationVoronoi.targets.tables['hvmv_substation_voronoi']['table']} CASCADE;"""
    )

    # Drop sequences
    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS
            {SubstationVoronoi.targets.tables['ehv_substation_voronoi']['schema']}.
            {SubstationVoronoi.targets.tables['ehv_substation_voronoi']['table']}_id_seq CASCADE;"""
    )


    db.execute_sql(
        f"""DROP SEQUENCE IF EXISTS
           {SubstationVoronoi.targets.tables['hvmv_substation_voronoi']['schema']}.
           {SubstationVoronoi.targets.tables['hvmv_substation_voronoi']['table']}_id_seq CASCADE;"""
    )


    engine = db.engine()
    EgonEhvSubstationVoronoi.__table__.create(bind=engine, checkfirst=True)
    EgonHvmvSubstationVoronoi.__table__.create(bind=engine, checkfirst=True)

def substation_voronoi():
    """
    Creates voronoi polygons for hvmv and ehv substations

    Returns
    -------
    None.

    """

    substation_list = ["hvmv_substation", "ehv_substation"]

    for substation in substation_list:
        
        cfg_boundaries = SubstationVoronoi.sources.tables["boundaries"]
        cfg_substation = SubstationVoronoi.sources.tables[substation]
        cfg_voronoi = SubstationVoronoi.targets.tables[substation + "_voronoi"]

        view = "grid.egon_voronoi_no_borders"

        # Create view for Voronoi polygons without taking borders into account
        db.execute_sql(
            f"DROP VIEW IF EXISTS {view} CASCADE;"
        )

        db.execute_sql(
            f"""
            CREATE VIEW {view} AS
               SELECT (ST_Dump(ST_VoronoiPolygons(ST_collect(a.point)))).geom
               FROM {cfg_substation['schema']}.
                    {cfg_substation['table']} a;
            """
        )

        # Clip Voronoi with boundaries
        db.execute_sql(
            f"""
            INSERT INTO {cfg_voronoi['schema']}.{cfg_voronoi['table']} (geom)
            (SELECT ST_Multi(ST_Intersection(
                ST_Transform(a.geometry, 4326), b.geom)) AS geom
             FROM {cfg_boundaries['schema']}.
                  {cfg_boundaries['table']} a
             CROSS JOIN {view} b);
            """
        )

        # Assign substation id as foreign key
        db.execute_sql(
            f"""
            UPDATE {cfg_voronoi['schema']}.{cfg_voronoi['table']} AS t1
                SET  	bus_id = t2.bus_id
	            FROM	(SELECT	voi.id AS id,
			                sub.bus_id ::integer AS bus_id
		            FROM	{cfg_voronoi['schema']}.{cfg_voronoi['table']} AS voi,
			                {cfg_substation['schema']}.{cfg_substation['table']} AS sub
		            WHERE  	voi.geom && sub.point AND
			                ST_CONTAINS(voi.geom,sub.point)
		           GROUP BY voi.id,sub.bus_id
		           )AS t2
	            WHERE  	t1.id = t2.id;
            """
        )

        db.execute_sql(
            f"""
            CREATE INDEX  	{cfg_voronoi['table']}_idx
                ON          {cfg_voronoi['schema']}.{cfg_voronoi['table']} USING gist (geom);
            """
        )

        db.execute_sql(f"DROP VIEW IF EXISTS {view} CASCADE;")
