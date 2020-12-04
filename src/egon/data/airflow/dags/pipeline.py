from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
from egon.data.db import airflow_db_connection
import egon.data.importing.openstreetmap as import_osm
import egon.data.importing.vg250 as import_vg250
import egon.data.processing.openstreetmap as process_osm


# Prepare connection to db for operators
airflow_db_connection()

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
    template_searchpath=[
        "/home/guido/git/eGon-data/src/egon/data/processing/vg250"
    ],
) as pipeline:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)

    # Openstreetmap data import
    osm_download = PythonOperator(
        task_id="download-osm", python_callable=import_osm.download_pbf_file
    )
    osm_import = PythonOperator(
        task_id="import-osm", python_callable=import_osm.to_postgres
    )
    osm_migrate = PythonOperator(
        task_id="migrate-osm",
        python_callable=process_osm.modify_tables,
    )
    osm_add_metadata = PythonOperator(
        task_id="add-osm-metadata", python_callable=import_osm.add_metadata
    )
    setup >> osm_download >> osm_import >> osm_migrate >> osm_add_metadata

    # VG250 (Verwaltungsgebiete 250) data import
    vg250_download = PythonOperator(
        task_id="download-vg250",
        python_callable=import_vg250.download_vg250_files,
    )
    vg250_import = PythonOperator(
        task_id="import-vg250", python_callable=import_vg250.to_postgres
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
