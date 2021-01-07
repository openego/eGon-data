from geoalchemy2.types import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Column,
    Float,
    Integer,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class OsmLine(Base):
    __tablename__ = "osm_line"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    osm_id = Column(BigInteger)
    access = Column(Text)
    addr_housename = Column("addr:housename", Text)
    addr_housenumber = Column("addr:housenumber", Text)
    addr_interpolation = Column("addr:interpolation", Text)
    admin_level = Column(Text)
    aerialway = Column(Text)
    aeroway = Column(Text)
    amenity = Column(Text)
    area = Column(Text)
    barrier = Column(Text)
    bicycle = Column(Text)
    brand = Column(Text)
    bridge = Column(Text)
    boundary = Column(Text)
    building = Column(Text)
    construction = Column(Text)
    covered = Column(Text)
    culvert = Column(Text)
    cutting = Column(Text)
    denomination = Column(Text)
    disused = Column(Text)
    embankment = Column(Text)
    foot = Column(Text)
    generator_source = Column("generator:source", Text)
    harbour = Column(Text)
    highway = Column(Text)
    historic = Column(Text)
    horse = Column(Text)
    intermittent = Column(Text)
    junction = Column(Text)
    landuse = Column(Text)
    layer = Column(Text)
    leisure = Column(Text)
    line = Column(Text)
    lock = Column(Text)
    man_made = Column(Text)
    military = Column(Text)
    motorcar = Column(Text)
    name = Column(Text)
    natural = Column(Text)
    office = Column(Text)
    oneway = Column(Text)
    operator = Column(Text)
    place = Column(Text)
    population = Column(Text)
    power = Column(Text)
    power_source = Column(Text)
    public_transport = Column(Text)
    railway = Column(Text)
    ref = Column(Text)
    religion = Column(Text)
    route = Column(Text)
    service = Column(Text)
    sidewalk = Column(Text)
    shop = Column(Text)
    sport = Column(Text)
    surface = Column(Text)
    toll = Column(Text)
    tourism = Column(Text)
    tower_type = Column("tower:type", Text)
    tracktype = Column(Text)
    tunnel = Column(Text)
    water = Column(Text)
    waterway = Column(Text)
    wetland = Column(Text)
    width = Column(Text)
    wood = Column(Text)
    z_order = Column(Integer)
    way_area = Column(Float)
    abandoned_aeroway = Column("abandoned:aeroway", Text)
    abandoned_amenity = Column("abandoned:amenity", Text)
    abandoned_building = Column("abandoned:building", Text)
    abandoned_landuse = Column("abandoned:landuse", Text)
    abandoned_power = Column("abandoned:power", Text)
    area_highway = Column("area:highway", Text)
    tags = Column(HSTORE(Text()), index=True)
    geom = Column(Geometry("LINESTRING", 3857), index=True)
    gid = Column(
        Integer,
        primary_key=True,
        server_default=text(
            "nextval('openstreetmap.osm_line_gid_seq'::regclass)"
        ),
    )


class OsmNode(Base):
    __tablename__ = "osm_nodes"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    id = Column(BigInteger, primary_key=True)
    lat = Column(Integer, nullable=False)
    lon = Column(Integer, nullable=False)


class OsmPoint(Base):
    __tablename__ = "osm_point"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    osm_id = Column(BigInteger)
    access = Column(Text)
    addr_housename = Column("addr:housename", Text)
    addr_housenumber = Column("addr:housenumber", Text)
    addr_interpolation = Column("addr:interpolation", Text)
    admin_level = Column(Text)
    aerialway = Column(Text)
    aeroway = Column(Text)
    amenity = Column(Text)
    area = Column(Text)
    barrier = Column(Text)
    bicycle = Column(Text)
    brand = Column(Text)
    bridge = Column(Text)
    boundary = Column(Text)
    building = Column(Text)
    capital = Column(Text)
    construction = Column(Text)
    covered = Column(Text)
    culvert = Column(Text)
    cutting = Column(Text)
    denomination = Column(Text)
    disused = Column(Text)
    ele = Column(Text)
    embankment = Column(Text)
    foot = Column(Text)
    generator_source = Column("generator:source", Text)
    harbour = Column(Text)
    highway = Column(Text)
    historic = Column(Text)
    horse = Column(Text)
    intermittent = Column(Text)
    junction = Column(Text)
    landuse = Column(Text)
    layer = Column(Text)
    leisure = Column(Text)
    line = Column(Text)
    lock = Column(Text)
    man_made = Column(Text)
    military = Column(Text)
    motorcar = Column(Text)
    name = Column(Text)
    natural = Column(Text)
    office = Column(Text)
    oneway = Column(Text)
    operator = Column(Text)
    place = Column(Text)
    population = Column(Text)
    power = Column(Text)
    power_source = Column(Text)
    public_transport = Column(Text)
    railway = Column(Text)
    ref = Column(Text)
    religion = Column(Text)
    route = Column(Text)
    service = Column(Text)
    sidewalk = Column(Text)
    shop = Column(Text)
    sport = Column(Text)
    surface = Column(Text)
    toll = Column(Text)
    tourism = Column(Text)
    tower_type = Column("tower:type", Text)
    tunnel = Column(Text)
    water = Column(Text)
    waterway = Column(Text)
    wetland = Column(Text)
    width = Column(Text)
    wood = Column(Text)
    z_order = Column(Integer)
    tags = Column(HSTORE(Text()), index=True)
    geom = Column(Geometry("POINT", 3857), index=True)
    gid = Column(
        Integer,
        primary_key=True,
        server_default=text(
            "nextval('openstreetmap.osm_point_gid_seq'::regclass)"
        ),
    )


class OsmPolygon(Base):
    __tablename__ = "osm_polygon"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    osm_id = Column(BigInteger)
    access = Column(Text)
    addr_housename = Column("addr:housename", Text)
    addr_housenumber = Column("addr:housenumber", Text)
    addr_interpolation = Column("addr:interpolation", Text)
    admin_level = Column(Text)
    aerialway = Column(Text)
    aeroway = Column(Text)
    amenity = Column(Text)
    area = Column(Text)
    barrier = Column(Text)
    bicycle = Column(Text)
    brand = Column(Text)
    bridge = Column(Text)
    boundary = Column(Text)
    building = Column(Text)
    construction = Column(Text)
    covered = Column(Text)
    culvert = Column(Text)
    cutting = Column(Text)
    denomination = Column(Text)
    disused = Column(Text)
    embankment = Column(Text)
    foot = Column(Text)
    generator_source = Column("generator:source", Text)
    harbour = Column(Text)
    highway = Column(Text)
    historic = Column(Text)
    horse = Column(Text)
    intermittent = Column(Text)
    junction = Column(Text)
    landuse = Column(Text)
    layer = Column(Text)
    leisure = Column(Text)
    line = Column(Text)
    lock = Column(Text)
    man_made = Column(Text)
    military = Column(Text)
    motorcar = Column(Text)
    name = Column(Text)
    natural = Column(Text)
    office = Column(Text)
    oneway = Column(Text)
    operator = Column(Text)
    place = Column(Text)
    population = Column(Text)
    power = Column(Text)
    power_source = Column(Text)
    public_transport = Column(Text)
    railway = Column(Text)
    ref = Column(Text)
    religion = Column(Text)
    route = Column(Text)
    service = Column(Text)
    sidewalk = Column(Text)
    shop = Column(Text)
    sport = Column(Text)
    surface = Column(Text)
    toll = Column(Text)
    tourism = Column(Text)
    tower_type = Column("tower:type", Text)
    tracktype = Column(Text)
    tunnel = Column(Text)
    water = Column(Text)
    waterway = Column(Text)
    wetland = Column(Text)
    width = Column(Text)
    wood = Column(Text)
    z_order = Column(Integer)
    way_area = Column(Float)
    abandoned_aeroway = Column("abandoned:aeroway", Text)
    abandoned_amenity = Column("abandoned:amenity", Text)
    abandoned_building = Column("abandoned:building", Text)
    abandoned_landuse = Column("abandoned:landuse", Text)
    abandoned_power = Column("abandoned:power", Text)
    area_highway = Column("area:highway", Text)
    tags = Column(HSTORE(Text()), index=True)
    geom = Column(Geometry(srid=3857), index=True)
    gid = Column(
        Integer,
        primary_key=True,
        server_default=text(
            "nextval('openstreetmap.osm_polygon_gid_seq'::regclass)"
        ),
    )


class OsmRel(Base):
    __tablename__ = "osm_rels"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    id = Column(BigInteger, primary_key=True)
    way_off = Column(SmallInteger)
    rel_off = Column(SmallInteger)
    parts = Column(ARRAY(BigInteger()), index=True)
    members = Column(ARRAY(Text()))
    tags = Column(ARRAY(Text()))


class OsmRoad(Base):
    __tablename__ = "osm_roads"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    osm_id = Column(BigInteger)
    access = Column(Text)
    addr_housename = Column("addr:housename", Text)
    addr_housenumber = Column("addr:housenumber", Text)
    addr_interpolation = Column("addr:interpolation", Text)
    admin_level = Column(Text)
    aerialway = Column(Text)
    aeroway = Column(Text)
    amenity = Column(Text)
    area = Column(Text)
    barrier = Column(Text)
    bicycle = Column(Text)
    brand = Column(Text)
    bridge = Column(Text)
    boundary = Column(Text)
    building = Column(Text)
    construction = Column(Text)
    covered = Column(Text)
    culvert = Column(Text)
    cutting = Column(Text)
    denomination = Column(Text)
    disused = Column(Text)
    embankment = Column(Text)
    foot = Column(Text)
    generator_source = Column("generator:source", Text)
    harbour = Column(Text)
    highway = Column(Text)
    historic = Column(Text)
    horse = Column(Text)
    intermittent = Column(Text)
    junction = Column(Text)
    landuse = Column(Text)
    layer = Column(Text)
    leisure = Column(Text)
    line = Column(Text)
    lock = Column(Text)
    man_made = Column(Text)
    military = Column(Text)
    motorcar = Column(Text)
    name = Column(Text)
    natural = Column(Text)
    office = Column(Text)
    oneway = Column(Text)
    operator = Column(Text)
    place = Column(Text)
    population = Column(Text)
    power = Column(Text)
    power_source = Column(Text)
    public_transport = Column(Text)
    railway = Column(Text)
    ref = Column(Text)
    religion = Column(Text)
    route = Column(Text)
    service = Column(Text)
    sidewalk = Column(Text)
    shop = Column(Text)
    sport = Column(Text)
    surface = Column(Text)
    toll = Column(Text)
    tourism = Column(Text)
    tower_type = Column("tower:type", Text)
    tracktype = Column(Text)
    tunnel = Column(Text)
    water = Column(Text)
    waterway = Column(Text)
    wetland = Column(Text)
    width = Column(Text)
    wood = Column(Text)
    z_order = Column(Integer)
    way_area = Column(Float)
    abandoned_aeroway = Column("abandoned:aeroway", Text)
    abandoned_amenity = Column("abandoned:amenity", Text)
    abandoned_building = Column("abandoned:building", Text)
    abandoned_landuse = Column("abandoned:landuse", Text)
    abandoned_power = Column("abandoned:power", Text)
    area_highway = Column("area:highway", Text)
    tags = Column(HSTORE(Text()), index=True)
    geom = Column(Geometry("LINESTRING", 3857), index=True)
    gid = Column(
        Integer,
        primary_key=True,
        server_default=text(
            "nextval('openstreetmap.osm_roads_gid_seq'::regclass)"
        ),
    )


class OsmWay(Base):
    __tablename__ = "osm_ways"
    __table_args__ = {
        "schema": "openstreetmap",
    }

    id = Column(BigInteger, primary_key=True)
    nodes = Column(ARRAY(BigInteger()), nullable=False, index=True)
    tags = Column(ARRAY(Text()))
