from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dag_datalake_sirene.helpers.get_colors import (
    get_colors,
)

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    index import (
    check_elastic_index,
    create_elastic_index,
    fill_elastic_siren_index,
    delete_previous_elastic_indices,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    sitemap import (
    create_sitemap,
    update_sitemap,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    fetch_db import get_latest_database

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    send_notification import (
    send_notification_success_tchap,
    send_notification_failure_tchap,
)
# fmt: on
# from dag_datalake_sirene.helpers.update_color_file import update_color_file
from dag_datalake_sirene.tests.e2e_tests.run_tests import run_e2e_tests
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ELK_DAG_NAME,
    AIRFLOW_SNAPSHOT_DAG_NAME,
    AIRFLOW_DAG_FOLDER,
    AIRFLOW_ENV,
    EMAIL_LIST,
    PATH_AIO,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
)
from operators.clean_folder import CleanFolderOperator


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=AIRFLOW_ELK_DAG_NAME,
    default_args=default_args,
    schedule_interval="0 0 * * 1,3,5",
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 12),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    # on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:
    trigger_snapshot_dag = TriggerDagRunOperator(
        task_id="trigger_snapshot_dag",
        trigger_dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
        wait_for_completion=True,
        deferrable=False,
    )
