from dag_datalake_sirene.config import (
    ELASTIC_DOWNSTREAM_URLS,
    ELASTIC_DOWNSTREAM_USER,
    ELASTIC_DOWNSTREAM_PASSWORD,
    ELASTIC_DOWNSTREAM_ALIAS,
)

from datetime import datetime

def wait_for_downstream_elastic_siren_import(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"

    downstream_urls = ELASTIC_DOWNSTREAM_URLS.split(',')

    if len(downstream_urls) == 0:
        return

    pending = downstream_urls
    completed = []

    while len(pending) > 0:
        for url in pending:

            response = requests.get(
                f'{ url }/{ELASTIC_DOWNSTREAM_ALIAS}',
                auth=(ELASTIC_DOWNSTREAM_USER, ELASTIC_DOWNSTREAM_PASSWORD))

            if response.status_code == 404:
                continue

            indices = list(response.json().keys())

            if elastic_index in indices:
                completed.append(url)

        pending = [ url for url in downstream_urls if url not in completed ]
