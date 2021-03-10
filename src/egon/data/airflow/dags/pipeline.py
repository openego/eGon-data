import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow
import importlib_resources as resources

from egon.data.airflow.tasks import initdb
from egon.data.db import airflow_db_connection
import egon.data.importing.openstreetmap as import_osm
import egon.data.importing.vg250 as import_vg250
import egon.data.importing.demandregio as import_dr
import egon.data.processing.openstreetmap as process_osm
import egon.data.importing.zensus as import_zs
import egon.data.processing.power_plants as power_plants
import egon.data.importing.nep_input_data as nep_input
import egon.data.importing.etrago as etrago
import egon.data.importing.mastr as mastr
import egon.data.processing.substation as substation
import egon.data.importing.re_potential_areas as re_potential_areas
import egon.data.processing.osmtgmod as osmtgmod

# Prepare connection to db for operators
airflow_db_connection()

# Temporary set dataset variable here
dataset = 'Schleswig-Holstein'

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
    template_searchpath=[
        os.path.abspath(os.path.join(os.path.dirname(
            __file__), '..', '..', 'processing', 'vg250'))
    ],
    is_paused_upon_creation=False,
    schedule_interval=None,
) as pipeline:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)

    # Openstreetmap data import
    osm_download = PythonOperator(
        task_id="download-osm",
        python_callable=import_osm.download_pbf_file,
        op_args={dataset},
    )
    osm_import = PythonOperator(
        task_id="import-osm",
        python_callable=import_osm.to_postgres,
        op_args={dataset},
    )
    osm_migrate = PythonOperator(
        task_id="migrate-osm",
        python_callable=process_osm.modify_tables,
    )
    osm_add_metadata = PythonOperator(
        task_id="add-osm-metadata",
        python_callable=import_osm.add_metadata,
        op_args={dataset},
    )
    setup >> osm_download >> osm_import >> osm_migrate >> osm_add_metadata

    # VG250 (Verwaltungsgebiete 250) data import
    vg250_download = PythonOperator(
        task_id="download-vg250",
        python_callable=import_vg250.download_vg250_files,
    )
    vg250_import = PythonOperator(
        task_id="import-vg250", python_callable=import_vg250.to_postgres,
        op_args={dataset}
    )

    vg250_nuts_mview = PostgresOperator(
        task_id="vg250_nuts_mview",
        sql="vg250_lan_nuts_id_mview.sql",
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    vg250_metadata = PythonOperator(
        task_id="add-vg250-metadata",
        python_callable=import_vg250.add_metadata,
    )
    vg250_clean_and_prepare = PostgresOperator(
        task_id="vg250_clean_and_prepare",
        sql="cleaning_and_preparation.sql",
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    setup >> vg250_download >> vg250_import >> vg250_nuts_mview
    vg250_nuts_mview >> vg250_metadata >> vg250_clean_and_prepare

    # Zensus import
    zensus_download_population = PythonOperator(
        task_id="download-zensus-population",
        python_callable=import_zs.download_zensus_pop
    )

    zensus_download_misc = PythonOperator(
        task_id="download-zensus-misc",
        python_callable=import_zs.download_zensus_misc
    )

    zensus_tables = PythonOperator(
        task_id="create-zensus-tables",
        python_callable=import_zs.create_zensus_tables
    )

    population_import = PythonOperator(
        task_id="import-zensus-population",
        python_callable=import_zs.population_to_postgres,
        op_args={dataset}
    )

    zensus_misc_import = PythonOperator(
        task_id="import-zensus-misc",
        python_callable=import_zs.zensus_misc_to_postgres,
        op_args={dataset}
    )
    setup >> zensus_download_population >> zensus_download_misc
    zensus_download_misc >> zensus_tables >> population_import
    vg250_clean_and_prepare >> population_import
    population_import >> zensus_misc_import

    # DemandRegio data import
    demandregio_import = PythonOperator(
        task_id="import-demandregio",
        python_callable=import_dr.insert_data,
    )
    vg250_clean_and_prepare >> demandregio_import

    # Power plant setup
    power_plant_tables = PythonOperator(
        task_id="create-power-plant-tables",
        python_callable=power_plants.create_tables
    )
    setup >> power_plant_tables

    # NEP data import
    create_tables = PythonOperator(
        task_id="create-scenario-tables",
        python_callable=nep_input.create_scenario_input_tables)

    nep_insert_data = PythonOperator(
        task_id="insert-nep-data",
        python_callable=nep_input.insert_data_nep,
        op_args={dataset})

    setup >> create_tables >> nep_insert_data
    vg250_clean_and_prepare >> nep_insert_data


    # setting etrago input tables
    etrago_input_data = PythonOperator(
        task_id = "setting-etrago-input-tables",
        python_callable = etrago.create_tables
    )
    setup >> etrago_input_data

    # Retrieve MaStR data
    retrieve_mastr_data = PythonOperator(
        task_id="retrieve_mastr_data",
        python_callable=mastr.download_mastr_data
    )
    setup >> retrieve_mastr_data


    # Substation extraction
    substation_tables = PythonOperator(
        task_id="create_substation_tables",
        python_callable=substation.create_tables
    )

    substation_functions = PythonOperator(
        task_id="substation_functions",
        python_callable=substation.create_sql_functions
    )

    hvmv_substation_extraction = PostgresOperator(
        task_id="hvmv_substation_extraction",
        sql=resources.read_text(substation, "hvmv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )

    ehv_substation_extraction = PostgresOperator(
        task_id="ehv_substation_extraction",
        sql=resources.read_text(substation, "ehv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    osm_add_metadata  >> substation_tables >> substation_functions
    substation_functions >> hvmv_substation_extraction
    substation_functions >> ehv_substation_extraction


    # Import potential areas for wind onshore and ground-mounted PV
    download_re_potential_areas = PythonOperator(
        task_id="download_re_potential_area_data",
        python_callable=re_potential_areas.download_datasets,
        op_args={dataset}
    )
    create_re_potential_areas_tables = PythonOperator(
        task_id="create_re_potential_areas_tables",
        python_callable=re_potential_areas.create_tables
    )
    insert_re_potential_areas = PythonOperator(
        task_id="insert_re_potential_areas",
        python_callable=re_potential_areas.insert_data
    )
    setup >> download_re_potential_areas >> create_re_potential_areas_tables
    create_re_potential_areas_tables >> insert_re_potential_areas

    
    # osmTGmod ehv/hv grid model generation
    osmtgmod = PythonOperator(
        task_id= "osmtgmod",
        python_callable= osmtgmod.run_osmtgmod)
    
    ehv_substation_extraction >> osmtgmod
