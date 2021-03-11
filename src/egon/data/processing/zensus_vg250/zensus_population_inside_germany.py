import json

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Integer,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
import egon.data.config

Base = declarative_base()


class Vg250Sta(Base):
    __tablename__ = "vg250_sta"
    __table_args__ = {"schema": "boundaries"}

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(String)
    ags = Column(String)
    sdv_ars = Column(String)
    gen = Column(String)
    bez = Column(String)
    ibz = Column(BigInteger)
    bem = Column(String)
    nbd = Column(String)
    sn_l = Column(String)
    sn_r = Column(String)
    sn_k = Column(String)
    sn_v1 = Column(String)
    sn_v2 = Column(String)
    sn_g = Column(String)
    fk_s3 = Column(String)
    nuts = Column(String)
    ars_0 = Column(String)
    ags_0 = Column(String)
    wsk = Column(String)
    debkg_id = Column(String)
    rs = Column(String)
    sdv_rs = Column(String)
    rs_0 = Column(String)
    geometry = Column(Geometry(srid=4326), index=True)


class Vg250Gem(Base):
    __tablename__ = "vg250_gem"
    __table_args__ = {"schema": "boundaries"}

    gid = Column(BigInteger, primary_key=True, index=True)
    ade = Column(BigInteger)
    gf = Column(BigInteger)
    bsg = Column(BigInteger)
    ars = Column(String)
    ags = Column(String)
    sdv_ars = Column(String)
    gen = Column(String)
    bez = Column(String)
    ibz = Column(BigInteger)
    bem = Column(String)
    nbd = Column(String)
    sn_l = Column(String)
    sn_r = Column(String)
    sn_k = Column(String)
    sn_v1 = Column(String)
    sn_v2 = Column(String)
    sn_g = Column(String)
    fk_s3 = Column(String)
    nuts = Column(String)
    ars_0 = Column(String)
    ags_0 = Column(String)
    wsk = Column(String)
    debkg_id = Column(String)
    rs = Column(String)
    sdv_rs = Column(String)
    rs_0 = Column(String)
    geometry = Column(Geometry(srid=4326), index=True)


class DestatisZensusPopulationPerHa(Base):
    __tablename__ = "destatis_zensus_population_per_ha"
    __table_args__ = {"schema": "society"}

    id = Column(Integer, primary_key=True, index=True)
    grid_id = Column(String(254), nullable=False)
    x_mp = Column(Integer)
    y_mp = Column(Integer)
    population = Column(SmallInteger)
    geom_point = Column(Geometry("POINT", 3035), index=True)
    geom = Column(Geometry("POLYGON", 3035), index=True)


class DestatisZensusPopulationPerHaInsideGermany(Base):
    __tablename__ = "destatis_zensus_population_per_ha_inside_germany"
    __table_args__ = {"schema": "society"}

    gid = Column(Integer, primary_key=True, index=True)
    grid_id = Column(String(254), nullable=False)
    population = Column(SmallInteger)
    geom_point = Column(Geometry("POINT", 3035), index=True)
    geom = Column(Geometry("POLYGON", 3035), index=True)


class Vg250GemPopulation(Base):
    __tablename__ = "vg250_gem_population"
    __table_args__ = {"schema": "boundaries"}

    gid = Column(Integer, primary_key=True, index=True)
    gen = Column(String)
    bez = Column(String)
    bem = Column(String)
    nuts = Column(String)
    ags_0 = Column(String)
    rs_0 = Column(String)
    area_ha = Column(Float)
    area_km2 = Column(Float)
    population_total = Column(Integer)
    cell_count = Column(Integer)
    population_density = Column(Integer)
    geom = Column(Geometry(srid=3035))


def filter_data():
    """
    Filter zensus data by data inside Germany and population > 0
    """

    # Get database engine
    engine_local_db = db.engine()

    # Create new table
    DestatisZensusPopulationPerHaInsideGermany.__table__.create(
        bind=engine_local_db, checkfirst=True
    )

    with db.session_scope() as s:
        # Query relevant data from zensus population table
        q = (
            s.query(
                DestatisZensusPopulationPerHa.id,
                DestatisZensusPopulationPerHa.grid_id,
                DestatisZensusPopulationPerHa.population,
                DestatisZensusPopulationPerHa.geom_point,
                DestatisZensusPopulationPerHa.geom,
            )
            .filter(DestatisZensusPopulationPerHa.population > 0)
            .filter(
                func.ST_Contains(
                    func.ST_Transform(Vg250Sta.geometry, 3035),
                    DestatisZensusPopulationPerHa.geom_point,
                )
            )
        )

        # Insert above queried data into new table
        insert = DestatisZensusPopulationPerHaInsideGermany.__table__.insert().from_select(
            (
                DestatisZensusPopulationPerHaInsideGermany.gid,
                DestatisZensusPopulationPerHaInsideGermany.grid_id,
                DestatisZensusPopulationPerHaInsideGermany.population,
                DestatisZensusPopulationPerHaInsideGermany.geom_point,
                DestatisZensusPopulationPerHaInsideGermany.geom,
            ),
            q,
        )

        # Execute and commit (trigger transactions in database)
        s.execute(insert)
        s.commit()


def population_in_municipalities():
    """
    Create table of municipalities with information about population
    """

    engine_local_db = db.engine()
    Vg250GemPopulation.__table__.create(bind=engine_local_db, checkfirst=True)

    srid = 3035

    # Prepare query from vg250 and zensus data
    with db.session_scope() as session:
        q = (
            session.query(
                Vg250Gem.gid.label("gid"),
                Vg250Gem.gen.label("gen"),
                Vg250Gem.bez,
                Vg250Gem.bem,
                Vg250Gem.nuts,
                Vg250Gem.rs_0,
                Vg250Gem.ags_0,
                (
                    func.ST_Area(func.ST_Transform(Vg250Gem.geometry, srid))
                    / 10000
                ).label(
                    "area_ha"
                ),  # ha
                (
                    func.ST_Area(func.ST_Transform(Vg250Gem.geometry, srid))
                    / 1000000
                ).label(
                    "area_km2"
                ),  # km
                func.sum(
                    func.coalesce(
                        DestatisZensusPopulationPerHaInsideGermany.population,
                        0,
                    )
                ).label("population_total"),
                func.count(
                    DestatisZensusPopulationPerHaInsideGermany.geom
                ).label("cell_count"),
                func.coalesce(
                    func.sum(
                        func.coalesce(
                            DestatisZensusPopulationPerHaInsideGermany.population,
                            0,
                        )
                    )
                    / (
                        func.ST_Area(
                            func.ST_Transform(Vg250Gem.geometry, srid)
                        )
                        / 1000000
                    ),
                    0,
                ).label("population_density"),
                func.ST_Transform(Vg250Gem.geometry, srid).label("geom"),
            )
            .filter(
                func.ST_Contains(
                    func.ST_Transform(Vg250Gem.geometry, srid),
                    DestatisZensusPopulationPerHaInsideGermany.geom_point,
                )
            )
            .group_by(Vg250Gem.gid)
        )

        # Insert spatially joined data
        insert = Vg250GemPopulation.__table__.insert().from_select(
            [
                Vg250GemPopulation.gid,
                Vg250GemPopulation.gen,
                Vg250GemPopulation.bez,
                Vg250GemPopulation.bem,
                Vg250GemPopulation.nuts,
                Vg250GemPopulation.ags_0,
                Vg250GemPopulation.rs_0,
                Vg250GemPopulation.area_ha,
                Vg250GemPopulation.area_km2,
                Vg250GemPopulation.population_total,
                Vg250GemPopulation.cell_count,
                Vg250GemPopulation.population_density,
                Vg250GemPopulation.geom,
            ],
            q,
        )

        # Execute and commit (trigger transactions in database)
        session.execute(insert)
        session.commit()


def add_metadata():
def add_metadata_vg250_gem_pop():
    """
    Create metadata JSON for Vg250GemPopulation

    Creates a metdadata JSON string and writes it to the database table comment
    """
    vg250_config = egon.data.config.datasets()["vg250"]

    licenses = [
        {
            "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
            "path": "www.govdata.de/dl-de/by-2-0",
            "instruction": (
                "Jede Nutzung ist unter den Bedingungen dieser „Datenlizenz "
                "Deutschland - Namensnennung - Version 2.0 zulässig.\nDie "
                "bereitgestellten Daten und Metadaten dürfen für die "
                "kommerzielle und nicht kommerzielle Nutzung insbesondere:"
                "(1) vervielfältigt, ausgedruckt, präsentiert, verändert, "
                "bearbeitet sowie an Dritte übermittelt werden;\n "
                "(2) mit eigenen Daten und Daten Anderer zusammengeführt und "
                "zu selbständigen neuen Datensätzen verbunden werden;\n "
                "(3) in interne und externe Geschäftsprozesse, Produkte und "
                "Anwendungen in öffentlichen und nicht öffentlichen "
                "elektronischen Netzwerken eingebunden werden."
            ),
            "attribution": "© Bundesamt für Kartographie und Geodäsie",
        }
    ]

    metadata = {
        "title": "Municipalities (BKG Verwaltungsgebiete 250) and population (Destatis Zensus)",
        "description": "Municipality data enriched by population data",
        "language": ["DE"],
        "spatial": {
            "location": "",
            "extent": "Germany",
            "resolution": "vector",
        },
        "temporal": {
            "referenceDate": "2020-01-01",
            "timeseries": {
                "start": "",
                "end": "",
                "resolution": "",
                "alignment": "",
                "aggregationType": "",
            },
        },
        "sources": [
            {
                "title": "Dienstleistungszentrum des Bundes für "
                "Geoinformation und Geodäsie - Open Data",
                "description": "Dieser Datenbestand steht über "
                "Geodatendienste gemäß "
                "Geodatenzugangsgesetz (GeoZG) "
                "(http://www.geodatenzentrum.de/auftrag/pdf"
                "/geodatenzugangsgesetz.pdf) für die "
                "kommerzielle und nicht kommerzielle "
                "Nutzung geldleistungsfrei zum Download "
                "und zur Online-Nutzung zur Verfügung. Die "
                "Nutzung der Geodaten und Geodatendienste "
                "wird durch die Verordnung zur Festlegung "
                "der Nutzungsbestimmungen für die "
                "Bereitstellung von Geodaten des Bundes "
                "(GeoNutzV) (http://www.geodatenzentrum.de"
                "/auftrag/pdf/geonutz.pdf) geregelt. "
                "Insbesondere hat jeder Nutzer den "
                "Quellenvermerk zu allen Geodaten, "
                "Metadaten und Geodatendiensten erkennbar "
                "und in optischem Zusammenhang zu "
                "platzieren. Veränderungen, Bearbeitungen, "
                "neue Gestaltungen oder sonstige "
                "Abwandlungen sind mit einem "
                "Veränderungshinweis im Quellenvermerk zu "
                "versehen. Quellenvermerk und "
                "Veränderungshinweis sind wie folgt zu "
                "gestalten. Bei der Darstellung auf einer "
                "Webseite ist der Quellenvermerk mit der "
                "URL http://www.bkg.bund.de zu verlinken. "
                "© GeoBasis-DE / BKG <Jahr des letzten "
                "Datenbezugs> © GeoBasis-DE / BKG "
                "<Jahr des letzten Datenbezugs> "
                "(Daten verändert) Beispiel: "
                "© GeoBasis-DE / BKG 2013",
                "path": vg250_config["original_data"]["source"]["url"],
                "licenses": licenses,
                "copyright": "© GeoBasis-DE / BKG 2016 (Daten verändert)",
            },
        ],
        "licenses": licenses,
        "contributors": [
            {
                "title": "Guido Pleßmann",
                "email": "http://github.com/gplssm",
                "date": "2021-03-11",
                "object": "",
                "comment": "Imported data",
            }
        ],
        "metaMetadata": {
            "metadataVersion": "OEP-1.4.0",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": ("https://creativecommons.org/publicdomain/zero/1.0/"),
            },
        },
    }

    meta_json = "'" + json.dumps(metadata) + "'"

    db.submit_comment(
        meta_json,
        Vg250GemPopulation.__table__.schema,
        Vg250GemPopulation.__table__.name,
    )
