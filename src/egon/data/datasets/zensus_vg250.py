import datetime
import json
import time

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Integer,
    SmallInteger,
    String,
    func,
    select,
)
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.vg250 import vg250_metadata_resources_fields
from egon.data.metadata import (
    context,
    licenses_datenlizenz_deutschland,
    meta_metadata,
)
import egon.data.config

Base = declarative_base()


class ZensusVg250(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ZensusVg250",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(
                map_zensus_vg250,
                inside_germany,
                add_metadata_zensus_inside_ger,
                population_in_municipalities,
                add_metadata_vg250_gem_pop,
            ),
        )


class Vg250Sta(Base):
    __tablename__ = "vg250_sta"
    __table_args__ = {"schema": "boundaries"}

    id = Column(BigInteger, primary_key=True, index=True)
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

    id = Column(BigInteger, primary_key=True, index=True)
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

    id = Column(Integer, primary_key=True, index=True)
    grid_id = Column(String(254), nullable=False)
    population = Column(SmallInteger)
    geom_point = Column(Geometry("POINT", 3035), index=True)
    geom = Column(Geometry("POLYGON", 3035), index=True)


class Vg250GemPopulation(Base):
    __tablename__ = "vg250_gem_population"
    __table_args__ = {"schema": "boundaries"}

    id = Column(Integer, primary_key=True, index=True)
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


class MapZensusVg250(Base):
    __tablename__ = "egon_map_zensus_vg250"
    __table_args__ = {"schema": "boundaries"}

    zensus_population_id = Column(Integer, primary_key=True, index=True)
    zensus_geom = Column(Geometry("POINT", 3035))
    vg250_municipality_id = Column(Integer)
    vg250_nuts3 = Column(String)


def map_zensus_vg250():
    """Perform mapping between municipalities and zensus grid"""

    MapZensusVg250.__table__.drop(bind=db.engine(), checkfirst=True)
    MapZensusVg250.__table__.create(bind=db.engine(), checkfirst=True)

    # Get information from data configuration file
    cfg = egon.data.config.datasets()["map_zensus_vg250"]

    local_engine = db.engine()

    db.execute_sql(
        f"""DELETE FROM
        {cfg['targets']['map']['schema']}.{cfg['targets']['map']['table']}"""
    )

    gdf = db.select_geodataframe(
        f"""SELECT * FROM
        {cfg['sources']['zensus_population']['schema']}.
        {cfg['sources']['zensus_population']['table']}""",
        geom_col="geom_point",
    )

    gdf_boundaries = db.select_geodataframe(
        f"""SELECT * FROM  {cfg['sources']['vg250_municipalities']['schema']}.
        {cfg['sources']['vg250_municipalities']['table']}""",
        geom_col="geometry",
        epsg=3035,
    )

    # Join vg250 with zensus cells
    join = gpd.sjoin(gdf, gdf_boundaries, how="inner", op="intersects")

    # Deal with cells that don't interect with boundaries (e.g. at borders)
    missing_cells = gdf[(~gdf.id.isin(join.id_left)) & (gdf.population > 0)]

    # start with buffer
    buffer = 0

    # increase buffer until every zensus cell is matched to a nuts3 region
    while len(missing_cells) > 0:
        buffer += 100
        boundaries_buffer = gdf_boundaries.copy()
        boundaries_buffer.geometry = boundaries_buffer.geometry.buffer(buffer)
        join_missing = gpd.sjoin(
            missing_cells, boundaries_buffer, how="inner", op="intersects"
        )
        join = pd.concat([join, join_missing])
        missing_cells = gdf[
            (~gdf.id.isin(join.id_left)) & (gdf.population > 0)
        ]
    print(f"Maximal buffer to match zensus points to vg250: {buffer}m")

    # drop duplicates
    join = join.drop_duplicates(subset=["id_left"])

    # Insert results to database
    join.rename(
        {
            "id_left": "zensus_population_id",
            "geom_point": "zensus_geom",
            "nuts": "vg250_nuts3",
            "id_right": "vg250_municipality_id",
        },
        axis=1,
    )[
        [
            "zensus_population_id",
            "zensus_geom",
            "vg250_municipality_id",
            "vg250_nuts3",
        ]
    ].set_geometry(
        "zensus_geom"
    ).to_postgis(
        cfg["targets"]["map"]["table"],
        schema=cfg["targets"]["map"]["schema"],
        con=local_engine,
        if_exists="replace",
    )


def inside_germany():
    """
    Filter zensus data by data inside Germany and population > 0
    """

    # Get database engine
    engine_local_db = db.engine()

    # Create new table
    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {DestatisZensusPopulationPerHaInsideGermany.__table__.schema}.{DestatisZensusPopulationPerHaInsideGermany.__table__.name} CASCADE;
        """
    )
    DestatisZensusPopulationPerHaInsideGermany.__table__.create(
        bind=engine_local_db, checkfirst=True
    )

    with db.session_scope() as s:

        # Query zensus cells in German boundaries from vg250
        cells_in_germany = s.query(MapZensusVg250.zensus_population_id)

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
            .filter(DestatisZensusPopulationPerHa.id.in_(cells_in_germany))
        )

        # Insert above queried data into new table
        insert = DestatisZensusPopulationPerHaInsideGermany.__table__.insert().from_select(
            (
                DestatisZensusPopulationPerHaInsideGermany.id,
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
    Vg250GemPopulation.__table__.drop(bind=engine_local_db, checkfirst=True)
    Vg250GemPopulation.__table__.create(bind=engine_local_db, checkfirst=True)

    srid = 3035

    gem = db.select_geodataframe(
        "SELECT * FROM boundaries.vg250_gem",
        geom_col="geometry",
        epsg=srid,
        index_col="id",
    )

    gem["area_ha"] = gem.area / 10000

    gem["area_km2"] = gem.area / 1000000

    population = db.select_dataframe(
        """SELECT id, population, vg250_municipality_id
        FROM society.destatis_zensus_population_per_ha
        INNER JOIN boundaries.egon_map_zensus_vg250 ON (
             society.destatis_zensus_population_per_ha.id =
             boundaries.egon_map_zensus_vg250.zensus_population_id)
        WHERE population > 0"""
    )

    gem["population_total"] = (
        population.groupby("vg250_municipality_id").population.sum().fillna(0)
    )

    gem["cell_count"] = population.groupby(
        "vg250_municipality_id"
    ).population.count()

    gem["population_density"] = gem["population_total"] / gem["area_km2"]

    gem.reset_index().to_postgis(
        "vg250_gem_population",
        schema="boundaries",
        con=db.engine(),
        if_exists="replace",
    )


def add_metadata_zensus_inside_ger():
    """
    Create metadata JSON for DestatisZensusPopulationPerHaInsideGermany

    Creates a metdadata JSON string and writes it to the database table comment
    """
    schema_table = ".".join(
        [
            DestatisZensusPopulationPerHaInsideGermany.__table__.schema,
            DestatisZensusPopulationPerHaInsideGermany.__table__.name,
        ]
    )

    metadata = {
        "name": schema_table,
        "title": "DESTATIS - Zensus 2011 - Population per hectar",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "National census in Germany in 2011 with the bounds on Germanys "
            "borders."
        ),
        "language": ["en-EN", "de-DE"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": "1 ha",
        },
        "temporal": {
            "reference_date": "2011-12-31",
            "timeseries": {
                "start": None,
                "end": None,
                "resolution": None,
                "alignment": None,
                "aggregationType": None,
            },
        },
        "sources": [
            {
                "title": "Statistisches Bundesamt (Destatis) - Ergebnisse des "
                "Zensus 2011 zum Download",
                "description": (
                    "Als Download bieten wir Ihnen auf dieser Seite "
                    "zusätzlich zur Zensusdatenbank CSV- und "
                    "teilweise Excel-Tabellen mit umfassenden "
                    "Personen-, Haushalts- und Familien- sowie "
                    "Gebäude- und Wohnungsmerkmalen. Die "
                    "Ergebnisse liegen auf Bundes-, Länder-, Kreis- "
                    "und Gemeindeebene vor. Außerdem sind einzelne "
                    "Ergebnisse für Gitterzellen verfügbar."
                ),
                "path": "https://www.zensus2011.de/DE/Home/Aktuelles/"
                "DemografischeGrunddaten.html",
                "licenses": [
                    licenses_datenlizenz_deutschland(
                        attribution="© Statistische Ämter des Bundes und der "
                        "Länder 2014"
                    )
                ],
            },
            {
                "title": "Dokumentation - Zensus 2011 - Methoden und Verfahren",
                "description": (
                    "Diese Publikation beschreibt ausführlich die "
                    "Methoden und Verfahren des registergestützten "
                    "Zensus 2011; von der Datengewinnung und "
                    "-aufbereitung bis hin zur Ergebniserstellung"
                    " und Geheimhaltung. Der vorliegende Band wurde "
                    "von den Statistischen Ämtern des Bundes und "
                    "der Länder im Juni 2015 veröffentlicht."
                ),
                "path": "https://www.destatis.de/DE/Publikationen/Thematisch/Be"
                "voelkerung/Zensus/ZensusBuLaMethodenVerfahren51211051"
                "19004.pdf?__blob=publicationFile",
                "licenses": [
                    licenses_datenlizenz_deutschland(
                        attribution="© Statistisches Bundesamt, Wiesbaden "
                        "2015 (im Auftrag der "
                        "Herausgebergemeinschaft)"
                    )
                ],
            },
        ],
        "licenses": [
            licenses_datenlizenz_deutschland(
                attribution="© Statistische Ämter des Bundes und der Länder "
                "2014; © Statistisches Bundesamt, Wiesbaden 2015 "
                "(Daten verändert)"
            )
        ],
        "contributors": [
            {
                "title": "Guido Pleßmann",
                "email": "http://github.com/gplssm",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
            {
                "title": "Jonathan Amme",
                "email": "http://github.com/nesnoj",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Metadata extended",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": schema_table,
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "description": "Unique identifier",
                            "type": "none",
                            "unit": "integer",
                        },
                        {
                            "name": "grid_id",
                            "description": "Grid number of source",
                            "type": "string",
                            "unit": "none",
                        },
                        {
                            "name": "population",
                            "description": "Number of registred residents",
                            "type": "integer",
                            "unit": "resident",
                        },
                        {
                            "name": "geom_point",
                            "description": "Geometry centroid",
                            "type": "Geometry",
                            "unit": "none",
                        },
                        {
                            "name": "geom",
                            "description": "Geometry",
                            "type": "Geometry",
                            "unit": "",
                        },
                    ],
                    "primaryKey": ["id"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    meta_json = "'" + json.dumps(metadata) + "'"

    db.submit_comment(
        meta_json,
        DestatisZensusPopulationPerHaInsideGermany.__table__.schema,
        DestatisZensusPopulationPerHaInsideGermany.__table__.name,
    )


def add_metadata_vg250_gem_pop():
    """
    Create metadata JSON for Vg250GemPopulation

    Creates a metdadata JSON string and writes it to the database table comment
    """
    vg250_config = egon.data.config.datasets()["vg250"]
    schema_table = ".".join(
        [
            Vg250GemPopulation.__table__.schema,
            Vg250GemPopulation.__table__.name,
        ]
    )

    licenses = [
        licenses_datenlizenz_deutschland(
            attribution="© Bundesamt für Kartographie und Geodäsie "
            "2020 (Daten verändert)"
        )
    ]

    vg250_source = {
        "title": "Verwaltungsgebiete 1:250 000 (Ebenen)",
        "description": "Der Datenbestand umfasst sämtliche Verwaltungseinheiten der "
        "hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden "
        "mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der "
        "Verwaltungseinheit sowie die spezifische Bezeichnung der "
        "Verwaltungsebene des jeweiligen Landes.",
        "path": vg250_config["original_data"]["source"]["url"],
        "licenses": licenses,
    }

    resources_fields = vg250_metadata_resources_fields()
    resources_fields.extend(
        [
            {
                "name": "area_ha",
                "description": "Area in ha",
                "type": "float",
                "unit": "ha",
            },
            {
                "name": "area_km2",
                "description": "Area in km2",
                "type": "float",
                "unit": "km2",
            },
            {
                "name": "population_total",
                "description": "Number of inhabitants",
                "type": "integer",
                "unit": "none",
            },
            {
                "name": "cell_count",
                "description": "Number of Zensus cells",
                "type": "integer",
                "unit": "none",
            },
            {
                "name": "population_density",
                "description": "Number of inhabitants per km2",
                "type": "float",
                "unit": "inhabitants/km²",
            },
        ]
    )

    metadata = {
        "name": schema_table,
        "title": (
            "Municipalities (BKG Verwaltungsgebiete 250) and population "
            "(Destatis Zensus)"
        ),
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Municipality data enriched by population data",
        "language": ["de-DE"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": "1:250000",
        },
        "temporal": {
            "referenceDate": "2020-01-01",
            "timeseries": {
                "start": None,
                "end": None,
                "resolution": None,
                "alignment": None,
                "aggregationType": None,
            },
        },
        "sources": [vg250_source],
        "licenses": licenses,
        "contributors": [
            {
                "title": "Guido Pleßmann",
                "email": "http://github.com/gplssm",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
            {
                "title": "Jonathan Amme",
                "email": "http://github.com/nesnoj",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Metadata extended",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": schema_table,
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": resources_fields,
                    "primaryKey": ["id"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    meta_json = "'" + json.dumps(metadata) + "'"

    db.submit_comment(
        meta_json,
        Vg250GemPopulation.__table__.schema,
        Vg250GemPopulation.__table__.name,
    )
