from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
from egon.data.airflow.input_data_nep_scenario import setup_nep_scenario

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
) as pipeline:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)

    nep_setup = PythonOperator(task_id="nep_scenario", python_callable=setup_nep_scenario)
    setup.set_downstream(nep_setup)
    # If you a second task, e.g. `teardown`, which should come after `setup`
    # you would specify this in the following way:
    #
    # setup.set_downstream(teardown)
    #
    # or
    #
    # teardown.set_upstream(setup)
