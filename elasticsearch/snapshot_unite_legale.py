def snapshot_unites_legales(elastic_connection, elastic_index, repository, snapshot_name):
    elastic_connection.snapshot.create(
        repository=repository,
        snapshot=snapshot_name,
        body={'indices': [elastic_index]},
        wait_for_completion=False)
