from typing import Any, Dict, List


def load_sql(table_info, condition=None) -> Dict[str, Any]:
    """For MySQL queries we can specify database

    Returns: Returns a dictionary with index-matched fields.
        columns: list of column names
        rows: list of rows
    """

    from airflow.providers.odbc.hooks.odbc import OdbcHook

    # dsql_hook = MySqlHook(mysql_conn_id="mysql-material_copy1")
    dsql_hook = OdbcHook(odbc_conn_id="odbc-core-source")
    connection = dsql_hook.get_conn()
    cursor = connection.cursor()
    tables_output = {}
    table_name = table_info["source"]
    where_string = f" {table_info.get('where')}" if table_info.get("where") else None
    condition_string = None
    if condition is None:
        pass
    elif where_string is None:
        condition_string = f" {condition}"
    else:
        condition_string = f" AND {condition}"
    tables_output = {}
    columns = [column["source"] for column in table_info["fields"]]
    column_string = ", ".join(columns)
    data = (
        f"SELECT {column_string} FROM {table_name}{str(where_string or '')}"
        f"{str(condition_string or '')};"
    )
    # print(data)
    cursor.execute(data)
    columns = [i[0] for i in cursor.description]
    while True:
        fetched_rows = cursor.fetchmany(30)
        if not fetched_rows:
            break
        rows = [list(row) for row in fetched_rows]
        tables_output["fields"] = columns
        tables_output["values"] = rows
        yield tables_output


def remap_fields(source_table, table_info) -> List[Dict[str, Any]]:

    field_search_map = {}
    for field in table_info["fields"]:
        # Makes a {source: target} map from {"source": source, "target": target} map
        source_column = field["source"]
        field_search_map[source_column] = field.get("target")
    target_fields = [field_search_map[field] for field in source_table["fields"]]
    target_table_output = {
        "fields": target_fields,
        "values": source_table["values"],
    }
    return target_table_output


def insert_mongo_document(document_list, document_info, primary_value=None):
    from airflow.providers.mongo.hooks.mongo import MongoHook
    from pymongo import UpdateOne

    mongo_target_hook = MongoHook(conn_id="mongo-core-target")
    target_name = document_info["target"]
    # mongo_target_client = mongo_target_hook
    mongo_target_collection = mongo_target_hook.get_collection(target_name)
    target_keys = document_list["fields"]
    target_values = document_list["values"]
    target_primary_key = document_info["target_primary_key"]
    truncated_primary_key = target_primary_key.split(".", 1)[0]
    if document_info.get("single_row_array"):
        if len(target_keys) != 1:
            return
        target_document_generator = [target_value[0] for target_value in target_values]
    else:
        target_document_generator = (
            dict(zip(target_keys, target_value)) for target_value in target_values
        )
    target_field = document_info.get("target_subdocument_field")
    target_update_generator = []
    if target_field is None:
        target_update_generator = [
            UpdateOne(
                {target_primary_key: target_document[truncated_primary_key]},
                {"$set": target_document},
                upsert=True,
            )
            for target_document in target_document_generator
        ]
        mongo_target_collection.bulk_write(target_update_generator)
    else:
        if primary_value is None:
            return
        mongo_target_collection.update_one(
            {target_primary_key: primary_value},
            {"$push": {target_field: {"$each": list(target_document_generator)}}},
            upsert=True,
        )


def odbc_fetch_subkeys(document_info, source_foreign_key):
    from airflow.providers.odbc.hooks.odbc import OdbcHook

    dsql_hook = OdbcHook(odbc_conn_id="odbc-core-source")
    connection = dsql_hook.get_conn()
    cursor = connection.cursor()
    table_name = document_info["source"]
    query_data = f"SELECT DISTINCT {source_foreign_key} FROM {table_name}"
    cursor.execute(query_data)

    while True:
        fetched_rows = cursor.fetchmany(30)
        if not fetched_rows:
            break
        row_list = [row[0] for row in fetched_rows]
    yield row_list


def run_mongo_subdocument(document_info):
    source_foreign_key = document_info["source_foreign_key"]
    primary_value_list_generator = odbc_fetch_subkeys(document_info, source_foreign_key)
    for primary_value_list in primary_value_list_generator:
        for primary_value in primary_value_list:
            subdoc_condition_string = f"WHERE {source_foreign_key} = {primary_value}"
            run_mongo_document(document_info, subdoc_condition_string, primary_value)


def run_mongo_document(document_info, condition_string=None, primary_value=None):
    # from include.mongo_tasks import load_sql, remap_fields, insert_mongo_document
    for document_part in load_sql(document_info, condition_string):
        target_document = remap_fields(document_part, document_info)
        # print(f"Reached here at line 104: {target_table}")
        insert_mongo_document(target_document, document_info, primary_value)


def config_sanitizer(config):
    sanitized_config = []
    for document in config:
        document["target"] = document.get("target", document["source"])
        document["schema"] = document.get("schema", "dbo")
        sanitized_fields = []
        for field in document["fields"]:
            field["target"] = field.get("target", field["source"])
            field["type_string"] = field.get("type_string", "varchar(255)")
            sanitized_fields.append(field)
        document["fields"] = sanitized_fields
        document["target_primary_key"] = document.get(
            "target_primary_key", document["fields"][0]["target"]
        )
        # document["where"] = document.get("where", "TRUE")
        sanitized_config.append(document)
    return sanitized_config
