from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
import egon.data.importing.openstreetmap as import_osm
import egon.data.processing.openstreetmap as process_osm
import egon.data.importing.zensus as import_zs

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
    
    # Zensus population import 
    zs_pop_download = PythonOperator(
        task_id="download-zensus-population", 
        python_callable=import_zs.download_zs_pop
    )
    
    zs_pop_import = PythonOperator(
        task_id="import-zensus-population",
        python_callable=import_zs.zspop_to_postgres
    )
    setup >> zs_pop_download >> zs_pop_import