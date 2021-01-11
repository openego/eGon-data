"""The central module containing all code dealing with importing Zensus data."""

from urllib.request import urlretrieve
import os
import zipfile

from egon.data import db
import egon.data.config

import subprocess


def download_zensus_pop(): 
    """Download Zensus csv file on population per hectar grid cell."""
    data_config = egon.data.config.datasets()
    zensus_population_config = data_config["zensus_population"]["original_data"]

    target_file = os.path.join(
        os.path.dirname(__file__), zensus_population_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(zensus_population_config["source"]["url"], target_file)
        

def zspop_to_postgres(): 
    
        # Get information from data configuration file
    data_config = egon.data.config.datasets()
    zensus_population_orig = data_config["zensus_population"]["original_data"]
    zensus_population_processed = data_config["zensus_population"]["processed"]
    input_file = os.path.join(
        os.path.dirname(__file__), zensus_population_orig["target"]["path"]
        )
    
    # Read database configuration from docker-compose.yml   
    docker_db_config = db.credentials()
        

    # Create target schema  
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {zensus_population_processed['schema']};")
    
    # Drop and create target table
    db.execute_sql(f"DROP TABLE IF EXISTS {zensus_population_processed['schema']}.{zensus_population_processed['table']} CASCADE;")

    db.execute_sql(f"""CREATE TABLE {zensus_population_processed['schema']}.{zensus_population_processed['table']}
                       (
                        gid        SERIAL NOT NULL,
                        grid_id    character varying(254) NOT NULL,
                        x_mp       int,
                        y_mp       int,
                        population smallint,
                        geom_point geometry(Point,3035),
                        geom geometry (Polygon, 3035),
                        CONSTRAINT zensus_population_per_ha_pkey PRIMARY KEY (gid));""")
    
    with zipfile.ZipFile(input_file) as zf:
        for filename in zf.namelist():
            zf.extract(filename)
            
            subprocess.run(
                ["psql", 
                  "-h",f"{docker_db_config['HOST']}",
                  "-p",f"{docker_db_config['PORT']}",
                  "-d",f"{docker_db_config['POSTGRES_DB']}",
                  "-U",f"{docker_db_config['POSTGRES_USER']}",
                  "-c", 
                  f"""\copy {zensus_population_processed['schema']}.{zensus_population_processed['table']} (grid_id, x_mp, y_mp, population)
                              FROM '{os.path.join(os.path.dirname(__file__), filename)}' 
                              DELIMITER ';'
                                CSV HEADER; """], 
            env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]}
            )

        os.remove(filename)
    
       
    db.execute_sql(f"""UPDATE {zensus_population_processed['schema']}.{zensus_population_processed['table']} zs
                   SET geom_point= ST_SetSRID(ST_MakePoint(zs.x_mp, zs.y_mp),3035);""")
    
    db.execute_sql(f"""UPDATE {zensus_population_processed['schema']}.{zensus_population_processed['table']} zs
                   SET geom = ST_SetSRID((ST_MakeEnvelope(zs.x_mp-50,zs.y_mp-50,zs.x_mp+50,zs.y_mp+50)),3035);""")
    
    db.execute_sql(f"""CREATE INDEX destatis_zensus_population_per_ha_geom_idx
                   ON {zensus_population_processed['schema']}.{zensus_population_processed['table']}
                   USING gist
                   (geom);""")
    
    db.execute_sql(f"""CREATE INDEX destatis_zensus_population_per_ha_geom_point_idx
                   ON {zensus_population_processed['schema']}.{zensus_population_processed['table']}
                   USING gist
                   (geom_point);""")
                         






