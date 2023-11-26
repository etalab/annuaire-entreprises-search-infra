from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.task_functions.get_colors import get_colors
from dag_datalake_sirene.task_functions.snapshot_elastic_siren_index import (
    snapshot_elastic_siren_index,
)
from dag_datalake_sirene.config import (
    EMAIL_LIST,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='snapshot-sirene',
    default_args=default_args,
    schedule_interval="0 0 * * 1,3,5",
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 15),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    # on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:
    get_colors = PythonOperator(
        task_id="get_colors", provide_context=True, python_callable=get_colors
    )

    snapshot_elastic_siren_index = PythonOperator(
        task_id="snapshot_elastic_siren_index",
        provide_context=True,
        python_callable=snapshot_elastic_siren_index,
    )

    snapshot_elastic_siren_index.set_upstream(get_colors)
