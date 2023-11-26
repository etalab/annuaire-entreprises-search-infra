from dag_datalake_sirene.elasticsearch.snapshot_unites_legales import (
    snapshot_unites_legales,
)
from elasticsearch_dsl import connections
from dag_datalake_sirene.config import (
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_SNAPSHOT_REPOSITORY,
)

from datetime import datetime

def snapshot_elastic_siren_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"

    current_date = datetime.today().strftime('%Y%m%d%H%M%S')
    snapshot_name = f'siren-{current_date}'

    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )

    elastic_connection = connections.get_connection()

    snapshot_unites_legales(
        elastic_connection=elastic_connection,
        elastic_index=elastic_index,
        repository=ELASTIC_SNAPSHOT_REPOSITORY,
        snapshot_name=snapshot_name,
    )
