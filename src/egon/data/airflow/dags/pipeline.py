from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
import egon.data.importing.openstreetmap as import_osm
import egon.data.importing.vg250 as import_vg250
import egon.data.processing.openstreetmap as process_osm

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
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
        python_callable=import_vg250.download_vg250_files
    )
    vg250_import = PythonOperator(
        task_id="import-vg250", python_callable=import_vg250.to_postgres
    )
    setup >> vg250_download >> vg250_import
