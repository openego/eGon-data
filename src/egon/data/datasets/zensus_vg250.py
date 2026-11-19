from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Column, Float, Integer, SmallInteger, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets

Base = declarative_base()


class ZensusVg250(Dataset):

    name: str = "ZensusVg250"
    version: str = "0.0.5"

    sources = DatasetSources(
        tables={
            "zensus_population": "society.destatis_zensus_population_per_ha",
            "vg250_municipalities": "boundaries.vg250_gem",
            "map_zensus_vg250": "boundaries.egon_map_zensus_vg250",
        },
        urls={
            "vg250_original_data": "https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/2020/vg250_01-01.geo84.shape.ebenen.zip"  # noqa: E501
        },
    )
    targets = DatasetTargets(
        tables={
            "map": "boundaries.egon_map_zensus_vg250",
            "zensus_inside_germany": "society.destatis_zensus_population_per_ha_inside_germany",  # noqa: E501
            "vg250_gem_population": "boundaries.vg250_gem_population",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name="ZensusVg250",
            version="0.0.4",
            dependencies=dependencies,
            tasks=(
                map_zensus_vg250,
                inside_germany,
                population_in_municipalities,
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

    sources = ZensusVg250.sources
    targets = ZensusVg250.targets

    local_engine = db.engine()

    db.execute_sql(f"DELETE FROM {targets.tables['map']}")

    gdf = db.select_geodataframe(
        f"SELECT * FROM {sources.tables['zensus_population']}",
        geom_col="geom_point",
    )

    gdf_boundaries = db.select_geodataframe(
        f"SELECT * FROM {sources.tables['vg250_municipalities']}",
        geom_col="geometry",
        epsg=3035,
    )

    # Join vg250 with zensus cells
    join = gpd.sjoin(gdf, gdf_boundaries, how="inner", predicate="intersects")

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
            missing_cells,
            boundaries_buffer,
            how="inner",
            predicate="intersects",
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
        targets.get_table_name("map"),
        schema=targets.get_table_schema("map"),
        con=local_engine,
        if_exists="append",
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
        DROP TABLE IF EXISTS
        {DestatisZensusPopulationPerHaInsideGermany.__table__.schema}.
        {DestatisZensusPopulationPerHaInsideGermany.__table__.name}
        CASCADE;
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
        insert = DestatisZensusPopulationPerHaInsideGermany.__table__.insert().from_select(  # noqa: E501
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
    sources = ZensusVg250.sources
    targets = ZensusVg250.targets

    gem = db.select_geodataframe(
        f"SELECT * FROM {sources.tables['vg250_municipalities']}",
        geom_col="geometry",
        epsg=srid,
        index_col="id",
    )

    gem["area_ha"] = gem.area / 10000
    gem["area_km2"] = gem.area / 1000000

    population = db.select_dataframe(
        f"""SELECT id, population, vg250_municipality_id
        FROM {sources.tables['zensus_population']}
        INNER JOIN {sources.tables['map_zensus_vg250']} ON (
         {sources.tables['zensus_population']}.id =
         {sources.tables['map_zensus_vg250']}.zensus_population_id)
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
        targets.get_table_name("vg250_gem_population"),
        schema=targets.get_table_schema("vg250_gem_population"),
        con=db.engine(),
        if_exists="replace",
    )
