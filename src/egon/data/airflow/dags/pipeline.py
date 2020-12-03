from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
import egon.data.importing.openstreetmap as import_osm
import egon.data.processing.openstreetmap as process_osm

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
    schedule_interval=None,
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
