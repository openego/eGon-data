"""The central module containing all code dealing with processing and
forecast Zensus data.
"""


from egon.data import db
import egon.data.config
import pandas as pd
import geopandas as gpd


def create_zensus_nuts_table():
    """Create table to map zensus grid and administrative districts (nuts3)"""
    table = "map_zensus_nuts3"
    schema = "society"

    db.execute_sql(f"DROP TABLE IF EXISTS {schema}.{table}")

    db.execute_sql(
        f"""CREATE TABLE {schema}.{table}
              (gid          integer NOT NULL,
              zensus_geom   geometry(Point,3035),
              nuts3         character varying(12),
              CONSTRAINT {table}_pkey PRIMARY KEY (gid)
              );"""
    )


def map_zensus_nuts3():
    """Perform mapping between nuts3 regions and zensus grid"""
    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus = data_config["zensus_population"]["processed"]
    local_engine = db.engine()
    population_table = (
        f"{zensus['schema']}"
        f".{zensus['table']}"
    )
    nuts3_table = "boundaries.vg250_krs"
    zn_table = "map_zensus_nuts3"
    zn_schema = "society"

    # Assign nuts3 code to zensus grid cells
    # TODO: remove LIMIT
    gdf = gpd.read_postgis(
        f"SELECT * FROM {population_table} LIMIT 10000",
        local_engine, geom_col='geom_point')
    gdf_boundaries = gpd.read_postgis(f"SELECT * FROM {nuts3_table}",
                 local_engine, geom_col='geometry').to_crs(epsg=3035)

    # Join nuts3 with zensus cells
    join = gpd.sjoin(gdf, gdf_boundaries, how="inner", op='intersects')

    # Deal with cells that don't interect with boundaries (e.g. at borders)
    missing_cells = gdf[~gdf.gid.isin(join.gid_left)]

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
        missing_cells = gdf[~gdf.gid.isin(join.gid_left)]
    print(f"Maximal buffer to match zensus points to nuts3: {buffer}m")

    # drop duplicates
    join = join.drop_duplicates(subset=['gid_left'])

    # Insert results to database
    join.rename({'gid_left': 'gid',
                 'geom_point': 'zensus_geom',
                 'nuts': 'nuts3'}, axis = 1
                )[['gid','zensus_geom', 'nuts3']].set_geometry(
                    'zensus_geom').to_postgis(
                         f'{zn_schema}.{zn_table}',
                         local_engine, if_exists = 'replace')


def population_prognosis_to_zensus():
    """Bring population prognosis from DemandRegio to Zensus grid"""

    population_DR = "egon_demandregio_population"
    schema = "society"

    local_engine = db.engine()

    # 1. Input: dataset on population prognosis on district-level (NUTS3)
    pop_prognosis_2050 = pd.read_sql(
        f"""SELECT nuts3, population
                FROM {schema}.{population_DR} WHERE year=2050""",
        local_engine)

