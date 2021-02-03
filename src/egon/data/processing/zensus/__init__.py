"""The central module containing all code dealing with processing and
forecast Zensus data.
"""

import os

from egon.data import db, subprocess
import egon.data.config
import pandas as pd


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

    population_table = (
        f"{zensus['schema']}"
        f".{zensus['table']}"
    )
    nuts3_table = "boundaries.vg250_krs"
    zn_table = "map_zensus_nuts3"
    zn_schema = "society"

    # Assign nuts3 code to zensus grid cells
    db.execute_sql(
        f"""INSERT INTO {zn_schema}.{zn_table} (gid, zensus_geom, nuts3)
            SELECT DISTINCT ON (zs.gid) zs.gid,
                    zs.geom_point,
                    krs.nuts
                FROM    {population_table} as zs,
                        {nuts3_table} as krs
                WHERE   ST_Transform(krs.geom,3035) && zs.geom AND
                ST_INTERSECTS(ST_TRANSFORM(krs.geom,3035),zs.geom)
            ORDER BY zs.gid, krs.nuts;"""
    )

    # Create index
    db.execute_sql(
        f"""CREATE INDEX {zn_table}_geom_idx
                ON {zn_schema}.{zn_table}
                USING gist
                (zensus_geom);"""
    )


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

