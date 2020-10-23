from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
from egon.data.data_import.openstreetmap import import_osm


with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
) as example:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)

    # Openstreetmap data import
    osm_download = PythonOperator(task_id="OSM_download", python_callable=import_osm.download_osm_file)
    osm_import = PythonOperator(task_id="OSM_import", python_callable=import_osm.osm2postgres)
    osm_post_import = PythonOperator(task_id="OSM_post-import", python_callable=import_osm.post_import_modifications)
    setup >> osm_download >> osm_import >> osm_post_import
