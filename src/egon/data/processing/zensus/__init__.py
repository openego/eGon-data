"""The central module containing all code dealing with processing and
forecast Zensus data.
"""

from egon.data import db
import egon.data.config
import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
# will be later imported from another file ###
Base = declarative_base()

class MapZensusNuts3(Base):
    __tablename__ = 'egon_map_zensus_nuts3'
    __table_args__ = {'schema': 'boundaries'}
    zensus_population_id = Column(Integer, primary_key=True)
    zensus_geom = Column(Geometry('POINT', 3035))
    nuts3 = Column(String(5))

class EgonPopulationPrognosis(Base):
    __tablename__ = 'egon_population_prognosis'
    __table_args__ = {'schema': 'society'}
    zensus_population_id = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)

class EgonHouseholdPrognosis(Base):
    __tablename__ = 'egon_household_prognosis'
    __table_args__ = {'schema': 'society'}
    zensus_population_id = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Float)

def create_tables():
    """Create table to map zensus grid and administrative districts (nuts3)"""
    engine = db.engine()
    MapZensusNuts3.__table__.create(bind=engine, checkfirst=True)
    EgonPopulationPrognosis.__table__.create(bind=engine, checkfirst=True)
    EgonHouseholdPrognosis.__table__.create(bind=engine, checkfirst=True)

def map_zensus_nuts3():
    """Perform mapping between nuts3 regions and zensus grid"""
    # Get information from data configuration file
    cfg = egon.data.config.datasets()

    # Define in- and output tables
    source_zensus = (
        f"{cfg['zensus_population']['processed']['schema']}."
        f"{cfg['zensus_population']['processed']['table']}")
    source_boundaries = (
        f"{cfg['vg250']['processed']['schema']}."
        f"{cfg['vg250']['processed']['file_table_map']['VG250_KRS.shp']}")

    target_table = cfg['society_prognosis']['target']['map_nuts3']
    target_schema =  'boundaries'

    local_engine = db.engine()

    db.execute_sql(f"DELETE FROM {target_schema}.{target_table}")
    # Assign nuts3 code to zensus grid cells

    gdf = db.select_geodataframe(
        f"""SELECT * FROM {source_zensus}""",
        geom_col='geom_point')

    gdf_boundaries = db.select_geodataframe(
        f"SELECT * FROM {source_boundaries}", geom_col='geometry', epsg=3035)

    # Join nuts3 with zensus cells
    join = gpd.sjoin(gdf, gdf_boundaries, how="inner", op='intersects')

    # Deal with cells that don't interect with boundaries (e.g. at borders)
    missing_cells = gdf[~gdf.id.isin(join.id)]

    # start with buffer of 100m
    buffer = 100

    # increase buffer until every zensus cell is matched to a nuts3 region
    while len(missing_cells) > 0:
        boundaries_buffer = gdf_boundaries.copy()
        boundaries_buffer.geometry = boundaries_buffer.geometry.buffer(buffer)
        join_missing = gpd.sjoin(
            missing_cells,boundaries_buffer, how="inner", op='intersects')
        buffer += 100
        join = join.append(join_missing)
        missing_cells = gdf[~gdf.id.isin(join.id)]
    print(f"Maximal buffer to match zensus points to nuts3: {buffer}m")

    # drop duplicates
    join = join.drop_duplicates(subset=['id'])

    # Insert results to database
    join.rename({'id': 'zensus_population_id',
                 'geom_point': 'zensus_geom',
                 'nuts': 'nuts3'}, axis = 1
                )[['zensus_population_id',
                   'zensus_geom', 'nuts3']].set_geometry(
                    'zensus_geom').to_postgis(
                         target_table, schema=target_schema,
                         con=local_engine, if_exists = 'replace')


def population_prognosis_to_zensus():
    """Bring population prognosis from DemandRegio to Zensus grid"""

    cfg = egon.data.config.datasets()
    # Define in- and output tables
    source_dr = cfg['demandregio_society']['targets']['population']['table']
    source_zensus =  cfg['zensus_population']['processed']['table']
    source_map = cfg['society_prognosis']['target']['map_nuts3']
    source_schema = (cfg['demandregio_society']['targets']
                     ['population']['schema'])

    target_table = cfg['society_prognosis']['target']['population_prognosis']
    target_schema = cfg['society_prognosis']['target']['schema']

    local_engine = db.engine()

    # Input: Zensus2011 population data including the NUTS3-Code
    zensus_district = db.select_dataframe(
        f"""SELECT zensus_population_id, nuts3
        FROM boundaries.{source_map}""",
        index_col='zensus_population_id')

    zensus = db.select_dataframe(
        f"""SELECT id, population
        FROM {source_schema}.{source_zensus}
        WHERE population > 0""",
        index_col='id')

    zensus['nuts3'] = zensus_district.nuts3

    # Rename index
    zensus.index = zensus.index.rename('zensus_population_id')

    # Calculate share of population per cell in nuts3-region
    zensus['share'] = zensus.groupby(zensus.nuts3).population.apply(
        lambda grp: grp/grp.sum()).fillna(0)

    db.execute_sql(f"DELETE FROM {target_schema}.{target_table}")
    # Scale to pogosis values from demandregio
    for year in [2035, 2050]:
        # Input: dataset on population prognosis on district-level (NUTS3)
        prognosis = db.select_dataframe(
            f"""SELECT nuts3, population
            FROM {source_schema}.{source_dr} WHERE year={year}""",
            index_col = 'nuts3')

        df = pd.DataFrame(zensus['share'].mul(
            prognosis.population[zensus['nuts3']].values
            )).rename({'share': 'population'}, axis = 1)
        df['year'] = year

        # Insert to database
        df.to_sql(
                target_table, schema=target_schema, con=local_engine,
                if_exists='append')


def household_prognosis_per_year(prognosis_nuts3, zensus, year):
    """Calculate household prognosis for a specitic year"""

    prognosis_total = prognosis_nuts3.groupby(
        prognosis_nuts3.index).households.sum()

    prognosis = pd.DataFrame(index = zensus.index)
    prognosis['nuts3'] = zensus.nuts3
    prognosis['quantity'] = zensus['share'].mul(
        prognosis_total[zensus['nuts3']].values)
    prognosis['rounded'] = prognosis['quantity'].astype(int)
    prognosis['rest'] = prognosis['quantity']-prognosis['rounded']

    # Rounding process to meet exact values from demandregio on nuts3-level
    for name, group in prognosis.groupby(prognosis.nuts3):
        print(f"start progosis nuts3 {name}")
        while prognosis_total[name] > group['rounded'].sum():
            index=np.random.choice(
                    group['rest'].index.values[
                        group['rest']==max(group['rest'])])
            group.at[index, 'rounded'] += 1
            group.at[index, 'rest'] = 0
        print(f"finished progosis nuts3 {name}")
        prognosis[prognosis.index.isin(group.index)] = group

    prognosis = prognosis.drop(
        ['nuts3', 'quantity', 'rest'], axis=1).rename(
            {'rounded': 'households'}, axis=1)
    prognosis['year'] = year

    return prognosis


def household_prognosis_to_zensus():
    """Bring household prognosis from DemandRegio to Zensus grid"""
    cfg = egon.data.config.datasets()
    # Define in- and output tables
    source_dr = cfg['demandregio_society']['targets']['household']['table']
    source_zensus = cfg['zensus_misc']['processed'][
        'path_table_map']['csv_Haushalte_100m_Gitter.zip']
    source_map = cfg['society_prognosis']['target']['map_nuts3']
    source_schema = cfg['demandregio_society']['targets']['household']['schema']

    target_table = cfg['society_prognosis']['target']['household_prognosis']
    target_schema = cfg['society_prognosis']['target']['schema']

    local_engine = db.engine()

    # Input: Zensus2011 household data including the NUTS3-Code
    district = db.select_dataframe(
        f"""SELECT zensus_population_id, nuts3
        FROM boundaries.{source_map}""",
        index_col='zensus_population_id')

    zensus = db.select_dataframe(
        f"""SELECT zensus_population_id, quantity
        FROM {source_schema}.{source_zensus}""",
        index_col='zensus_population_id')

    # Group all household types
    zensus = zensus.groupby(zensus.index).sum()

    zensus['nuts3'] = district.nuts3

    # Calculate share of households per nuts3 region in each zensus cell
    zensus['share'] = zensus.groupby(zensus.nuts3).quantity.apply(
        lambda grp: grp/grp.sum()).fillna(0).values

    db.execute_sql(f"DELETE FROM {target_schema}.{target_table}")
    # Apply prognosis function
    for year in [2035, 2050]:
        print(f"start prognosis for year {year}")
        # Input: dataset on household prognosis on district-level (NUTS3)
        prognosis_nuts3 = db.select_dataframe(
            f"""SELECT nuts3, hh_size, households
            FROM {source_schema}.{source_dr} WHERE year={year}""",
            index_col='nuts3')

        # Insert into database
        household_prognosis_per_year(prognosis_nuts3, zensus, year).to_sql(
            target_table, schema=target_schema,
            con = local_engine, if_exists = 'append')
        print(f"finished prognosis for year {year}")
