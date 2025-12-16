"""The central module containing code to create substation tables

"""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets import load_sources_and_targets

Base = declarative_base()


class EgonEhvSubstation(Base):
    __tablename__ = "egon_ehv_substation"
    __table_args__ = {"schema": "grid"}
    bus_id = Column(
        Integer,
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


class EgonHvmvSubstation(Base):
    __tablename__ = "egon_hvmv_substation"
    __table_args__ = {"schema": "grid"}
    bus_id = Column(
        Integer,
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


def create_tables():
    """Create tables for substation data
    Returns
    -------
    None.
    """

    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {EgonHvmvSubstation.__table__.schema};"
    )

    # Drop tables
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {EgonEhvSubstation.__table__.schema}.
            {EgonEhvSubstation.__table__.name} CASCADE;"""
    )

    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {EgonHvmvSubstation.__table__.schema}.
            {EgonHvmvSubstation.__table__.name} CASCADE;"""
    )

    engine = db.engine()
    EgonEhvSubstation.__table__.create(bind=engine, checkfirst=True)
    EgonHvmvSubstation.__table__.create(bind=engine, checkfirst=True)


def extract():
    """
    Extract ehv and hvmv substation from transfer buses and results from osmtgmod

    Returns
    -------
    None.

    """
    sources, targets = load_sources_and_targets("Osmtgmod")
    # Create tables for substations
    create_tables()

    # Extract eHV substations
    db.execute_sql(
        f"""
        INSERT INTO {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name}
        
        SELECT * FROM {sources.tables['ehv_transfer_buses']['schema']}.{sources.tables['ehv_transfer_buses']['table']};
        
        
        -- update ehv_substation table with new column of respective osmtgmod bus_i
        ALTER TABLE {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name}
        	ADD COLUMN otg_id bigint;

        -- fill table with bus_i from osmtgmod
        UPDATE {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name}
        	SET otg_id = {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.bus_i
        FROM {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}
        	WHERE {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.base_kv > 110 AND (SELECT TRIM(leading 'n' FROM TRIM(leading 'w' FROM TRIM(leading 'r' FROM {targets.tables['ehv_substation']['schema']}.{targets.tables['ehv_substation']['table']}.osm_id)))::BIGINT) = {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.osm_substation_id; 

        DELETE FROM {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name} WHERE otg_id IS NULL;

        UPDATE {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name}
        	SET 	bus_id = otg_id;

        ALTER TABLE {EgonEhvSubstation.__table__.schema}.{EgonEhvSubstation.__table__.name}
        	DROP COLUMN otg_id;
        """
    )

    # Extract HVMV substations
    db.execute_sql(
        f"""
        INSERT INTO {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name}
        
        SELECT * FROM {sources.tables['hvmv_transfer_buses']['schema']}.{sources.tables['hvmv_transfer_buses']['table']};
        
        
        ALTER TABLE {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name}
        	ADD COLUMN otg_id bigint;
        
        -- fill table with bus_i from osmtgmod
        UPDATE {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name}
        	SET 	otg_id = {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.bus_i
        	FROM 	{sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}
        	WHERE 	{sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.base_kv <= 110 AND (SELECT TRIM(leading 'n' FROM TRIM(leading 'w' FROM {targets.tables['hvmv_substation']['schema']}.{targets.tables['hvmv_substation']['table']}.osm_id))::BIGINT) = {sources.tables['osmtgmod_bus']['schema']}.{sources.tables['osmtgmod_bus']['table']}.osm_substation_id;
        
        DELETE FROM {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name} WHERE otg_id IS NULL;
        
        UPDATE {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name}
        	SET 	bus_id = otg_id;
        
        ALTER TABLE {EgonHvmvSubstation.__table__.schema}.{EgonHvmvSubstation.__table__.name}
        	DROP COLUMN otg_id;
        """
    )
