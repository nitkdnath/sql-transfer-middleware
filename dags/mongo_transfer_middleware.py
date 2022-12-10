import pendulum
from airflow.decorators import dag, task_group


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nosql", "mongo", "odbc"],
    owner_links={"nitesh": "mailto:nitesh.debnath@visglobal.com.au"},
)
def mongo_transfer_middleware():
    from airflow.decorators import task
    from airflow.models import Variable
    from include.mongo_tasks import config_sanitizer

    @task(pool="source_worker", pool_slots=2)
    def do_divided_document(document_info):
        from include.mongo_tasks import run_mongo_document

        run_mongo_document(document_info)

    @task(pool="source_worker", pool_slots=1)
    def do_divided_subdocument(document_info):
        from include.mongo_tasks import run_mongo_subdocument

        run_mongo_subdocument(document_info)

    @task_group()
    def insert_document_tasks(config):
        for document_task in config:
            if document_task.get("source_foreign_key") is not None:
                continue
            do_divided_document(document_task)

    @task_group()
    def insert_subdocument_tasks(config):
        for document_task in config:
            if document_task.get("source_foreign_key") is None:
                continue
            do_divided_subdocument(document_task)

    config = config_sanitizer(
        Variable.get("MONGODB_MIDDLEWARE_MAPPING", deserialize_json=True)
    )
    insert_document_tasks(config) >> insert_subdocument_tasks(config)
    # for document in config:
    #     if document.get("source_foreign_key") is not None:
    #         continue
    #     do_divided_document(document)
    # for document in config:
    #     if document.get("source_foreign_key") is None:
    #         continue
    #     do_divided_subdocument(document)


mongo_transfer_middleware()
