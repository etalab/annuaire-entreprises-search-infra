from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.external_data.task_functions import (
    compare_versions_file,
    preprocess_colter_data,
    preprocess_elu_data,
    publish_mattermost,
    update_es,
)
from dag_datalake_sirene.task_functions import (
    get_colors,
    get_object_minio,
    put_object_minio,
)
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "update-elk-colter"
TMP_FOLDER = "/tmp/"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60 * 8),
    tags=["colter"],
) as dag:
    get_colors = PythonOperator(
        task_id="get_colors",
        provide_context=True,
        python_callable=get_colors,
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{TMP_FOLDER}+{DAG_FOLDER}+{DAG_NAME}",
    )

    preprocess_colter_data = PythonOperator(
        task_id="preprocess_colter_data",
        python_callable=preprocess_colter_data,
        op_args=(TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",),
    )

    get_latest_colter_data = PythonOperator(
        task_id="get_latest_colter_data",
        python_callable=get_object_minio,
        op_args=(
            "colter-latest.csv",
            "ae/external_data/colter/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/colter-latest.csv",
        ),
    )

    compare_versions_file_colter = ShortCircuitOperator(
        task_id="compare_versions_file_colter",
        python_callable=compare_versions_file,
        op_args=(
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/colter-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/colter-new.csv",
        ),
    )

    update_es_colter = PythonOperator(
        task_id="update_es_colter",
        python_callable=update_es,
        op_args=(
            "colter",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/colter-new.csv",
            "colter-errors.txt",
            "current",
        ),
    )

    put_colter_file_error_to_minio = PythonOperator(
        task_id="put_colter_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "colter-errors.txt",
            "ae/external_data/colter/colter-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    put_colter_file_latest_to_minio = PythonOperator(
        task_id="put_colter_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "colter-new.csv",
            "ae/external_data/colter/colter-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    preprocess_elu_data = PythonOperator(
        task_id="preprocess_elu_data",
        python_callable=preprocess_elu_data,
        op_args=(TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",),
    )

    get_latest_elu_data = PythonOperator(
        task_id="get_latest_elu_data",
        python_callable=get_object_minio,
        op_args=(
            "elu-latest.csv",
            "ae/external_data/colter/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/elu-latest.csv",
        ),
    )

    compare_versions_file_elu = ShortCircuitOperator(
        task_id="compare_versions_file_elu",
        python_callable=compare_versions_file,
        op_args=(
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/elu-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/elu-new.csv",
        ),
    )

    update_es_elu = PythonOperator(
        task_id="update_es_elu",
        python_callable=update_es,
        op_args=(
            "elu",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/elu-new.csv",
            "elu-errors.txt",
            "current",
        ),
    )

    put_elu_file_error_to_minio = PythonOperator(
        task_id="put_elu_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "elu-errors.txt",
            "ae/external_data/colter/elu-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    put_elu_file_latest_to_minio = PythonOperator(
        task_id="put_elu_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "elu-new.csv",
            "ae/external_data/colter/elu-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des Colter (+Elus) mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_colter_data.set_upstream(clean_previous_folder)
    get_latest_colter_data.set_upstream(preprocess_colter_data)
    compare_versions_file_colter.set_upstream(get_latest_colter_data)
    update_es_colter.set_upstream(compare_versions_file_colter)
    put_colter_file_error_to_minio.set_upstream(update_es_colter)
    put_colter_file_latest_to_minio.set_upstream(update_es_colter)

    preprocess_elu_data.set_upstream(put_colter_file_error_to_minio)
    preprocess_elu_data.set_upstream(put_colter_file_latest_to_minio)
    get_latest_elu_data.set_upstream(preprocess_elu_data)
    compare_versions_file_elu.set_upstream(get_latest_elu_data)
    update_es_elu.set_upstream(compare_versions_file_elu)
    put_elu_file_error_to_minio.set_upstream(update_es_elu)
    put_elu_file_latest_to_minio.set_upstream(update_es_elu)
    publish_mattermost.set_upstream(put_elu_file_error_to_minio)
    publish_mattermost.set_upstream(put_elu_file_latest_to_minio)
