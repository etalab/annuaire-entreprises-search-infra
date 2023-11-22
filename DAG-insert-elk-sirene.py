from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.task_functions.check_elastic_index import check_elastic_index
from dag_datalake_sirene.task_functions.count_nombre_etablissements import (
    count_nombre_etablissements,
)
from dag_datalake_sirene.task_functions.count_nombre_etablissements_ouverts import (
    count_nombre_etablissements_ouverts,
)
from dag_datalake_sirene.task_functions.create_additional_data_tables import (
    create_agence_bio_table,
    create_bilan_financiers_table,
    create_colter_table,
    create_ess_table,
    create_rge_table,
    create_finess_table,
    create_egapro_table,
    create_elu_table,
    create_organisme_formation_table,
    create_spectacle_table,
    create_uai_table,
    create_convention_collective_table,
)
from dag_datalake_sirene.task_functions.get_colors import get_colors
from dag_datalake_sirene.task_functions.create_dirig_tables import (
    create_dirig_pm_table,
    create_dirig_pp_table,
)
from dag_datalake_sirene.task_functions.create_elastic_index import create_elastic_index
from dag_datalake_sirene.task_functions.create_etablissements_tables import (
    create_etablissements_table,
)
from dag_datalake_sirene.task_functions.create_etablissements_tables import (
    create_flux_etablissements_table,
)
from dag_datalake_sirene.task_functions.create_siege_only_table import (
    create_siege_only_table,
)
from dag_datalake_sirene.task_functions.create_sitemap import create_sitemap
from dag_datalake_sirene.task_functions.create_sqlite_database import (
    create_sqlite_database,
)
from dag_datalake_sirene.task_functions.create_unite_legale_tables import (
    create_flux_unite_legale_table,
)
from dag_datalake_sirene.task_functions.create_unite_legale_tables import (
    create_unite_legale_table,
)
from dag_datalake_sirene.task_functions.fill_elastic_siren_index import (
    fill_elastic_siren_index,
)
from dag_datalake_sirene.task_functions.flush_cache import flush_cache
from dag_datalake_sirene.utils.minio_helpers import (
    get_latest_file_minio,
)
from dag_datalake_sirene.task_functions.replace_etablissements_table import (
    replace_etablissements_table,
)
from dag_datalake_sirene.task_functions.replace_siege_only_table import (
    replace_siege_only_table,
)
from dag_datalake_sirene.task_functions.replace_unite_legale_table import (
    replace_unite_legale_table,
)
from dag_datalake_sirene.task_functions.send_notification import (
    send_notification_success_tchap,
    send_notification_failure_tchap,
)
from dag_datalake_sirene.task_functions.update_color_file import update_color_file
from dag_datalake_sirene.task_functions.update_sitemap import update_sitemap
from dag_datalake_sirene.tests.e2e_tests.run_tests import run_e2e_tests
from operators.clean_folder import CleanFolderOperator
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_NAME,
    AIRFLOW_DAG_FOLDER,
    AIRFLOW_ENV,
    DIRIG_DATABASE_LOCATION,
    EMAIL_LIST,
    MINIO_BUCKET,
    PATH_AIO,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
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
    dag_id=AIRFLOW_DAG_NAME,
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

    create_elastic_index = PythonOperator(
        task_id="create_elastic_index",
        provide_context=True,
        python_callable=create_elastic_index,
    )

    fill_elastic_siren_index = PythonOperator(
        task_id="fill_elastic_siren_index",
        provide_context=True,
        python_callable=fill_elastic_siren_index,
    )

    fill_elastic_siren_index.set_upstream(get_colors)
