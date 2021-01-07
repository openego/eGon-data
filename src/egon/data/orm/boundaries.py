from geoalchemy2.types import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    Float,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


t_bkg_vg250_statistics_view = Table(
    "bkg_vg250_statistics_view",
    metadata,
    Column("id", Text),
    Column("area_sum_ha", Integer),
    schema="boundaries",
)


class Vg250Gem(Base):
    __tablename__ = "vg250_gem"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)


class Vg250GemClean(Base):
    __tablename__ = "vg250_gem_clean"
    __table_args__ = {"schema": "boundaries"}

    id = Column(
        Integer,
        primary_key=True,
        server_default=text(
            "nextval('boundaries.vg250_gem_clean_id_seq'::regclass)"
        ),
    )
    old_id = Column(Integer)
    gen = Column(Text)
    bez = Column(Text)
    bem = Column(Text)
    nuts = Column(String(5))
    rs_0 = Column(String(12))
    ags_0 = Column(String(12))
    area_ha = Column(Numeric)
    count_hole = Column(Integer)
    path = Column(ARRAY(Integer()))
    is_hole = Column(Boolean)
    geometry = Column(Geometry("POLYGON", 3035), index=True)


t_vg250_gem_hole = Table(
    "vg250_gem_hole",
    metadata,
    Column("id", Integer, unique=True),
    Column("old_id", Integer),
    Column("gen", Text),
    Column("bez", Text),
    Column("bem", Text),
    Column("nuts", String(5)),
    Column("rs_0", String(12)),
    Column("ags_0", String(12)),
    Column("area_ha", Numeric),
    Column("count_hole", Integer),
    Column("path", ARRAY(Integer())),
    Column("is_hole", Boolean),
    Column("geometry", Geometry("POLYGON", 3035), index=True),
    schema="boundaries",
)


t_vg250_gem_valid = Table(
    "vg250_gem_valid",
    metadata,
    Column("id", BigInteger, unique=True),
    Column("old_id", Integer),
    Column("gen", Text),
    Column("bez", Text),
    Column("bem", Text),
    Column("nuts", String(5)),
    Column("rs_0", String(12)),
    Column("ags_0", String(12)),
    Column("area_ha", Float(53)),
    Column("geometry", Geometry("POLYGON", 3035), index=True),
    schema="boundaries",
)


class Vg250Kr(Base):
    __tablename__ = "vg250_krs"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)


t_vg250_krs_area = Table(
    "vg250_krs_area",
    metadata,
    Column("id", Integer, unique=True),
    Column("gen", Text),
    Column("bez", Text),
    Column("nuts", String(5)),
    Column("rs_0", String(12)),
    Column("ags_0", String(12)),
    Column("area_ha", Float(53)),
    Column("geometry", Geometry, index=True),
    schema="boundaries",
)


class Vg250Lan(Base):
    __tablename__ = "vg250_lan"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)


t_vg250_lan_nuts_id = Table(
    "vg250_lan_nuts_id",
    metadata,
    Column("ags_0", Text),
    Column("gen", Text),
    Column("nuts", Text),
    Column("geometry", Geometry),
    schema="boundaries",
)


t_vg250_lan_union = Table(
    "vg250_lan_union",
    metadata,
    Column("ags_0", String(12), unique=True),
    Column("gen", Text),
    Column("geometry", Geometry("MULTIPOLYGON", 3035), index=True),
    schema="boundaries",
)


class Vg250Rbz(Base):
    __tablename__ = "vg250_rbz"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)


class Vg250Sta(Base):
    __tablename__ = "vg250_sta"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)


t_vg250_sta_bbox = Table(
    "vg250_sta_bbox",
    metadata,
    Column("gid", Integer, unique=True),
    Column("bez", Text),
    Column("area_ha", Float(53)),
    Column("geometry", Geometry("POLYGON", 3035), index=True),
    schema="boundaries",
)


t_vg250_sta_invalid_geometry = Table(
    "vg250_sta_invalid_geometry",
    metadata,
    Column("gid", BigInteger, unique=True),
    Column("error", Boolean),
    Column("error_reason", String),
    Column("geometry", Geometry("POINT", 3035), index=True),
    schema="boundaries",
)


t_vg250_sta_tiny_buffer = Table(
    "vg250_sta_tiny_buffer",
    metadata,
    Column("gid", Integer, unique=True),
    Column("bez", Text),
    Column("gf", Float(53)),
    Column("area_ha", Float(53)),
    Column("geometry", Geometry("MULTIPOLYGON", 3035), index=True),
    schema="boundaries",
)


t_vg250_sta_union = Table(
    "vg250_sta_union",
    metadata,
    Column("gid", Integer, unique=True),
    Column("bez", Text),
    Column("area_ha", Float(53)),
    Column("geometry", Geometry("MULTIPOLYGON", 3035), index=True),
    schema="boundaries",
)


class Vg250Vwg(Base):
    __tablename__ = "vg250_vwg"
    __table_args__ = {
        "schema": "boundaries",
    }

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(Text)
    ags = Column(Text)
    sdv_ars = Column(Text)
    gen = Column(Text)
    bez = Column(Text)
    ibz = Column(BigInteger)
    bem = Column(Text)
    nbd = Column(Text)
    sn_l = Column(Text)
    sn_r = Column(Text)
    sn_k = Column(Text)
    sn_v1 = Column(Text)
    sn_v2 = Column(Text)
    sn_g = Column(Text)
    fk_s3 = Column(Text)
    nuts = Column(Text)
    ars_0 = Column(Text)
    ags_0 = Column(Text)
    wsk = Column(Text)
    debkg_id = Column(Text)
    rs = Column(Text)
    sdv_rs = Column(Text)
    rs_0 = Column(Text)
    geometry = Column(Geometry(srid=4326), index=True)
