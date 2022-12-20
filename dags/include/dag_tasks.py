from typing import Any, Dict, List
import json
import secrets


# @task(pool="source_worker", pool_slots=2)
def load_sql(table_info) -> Dict[str, Any]:
    """For MySQL queries we can specify database

    Yields: A dictionary of configurable size with index-matched fields.
        columns: list of column names
        rows: list of rows
    """

    from airflow.providers.odbc.hooks.odbc import OdbcHook

    # dsql_hook = MySqlHook(mysql_conn_id="mysql-material_copy1")
    # TODO: Rename odbc-core-source to odbc-source instead
    dsql_hook = OdbcHook(odbc_conn_id="odbc-core-source")
    connection = dsql_hook.get_conn()
    cursor = connection.cursor()
    tables_output = {}
    table_name = table_info["source"]
    condition = table_info["where"]
    tables_output = {}
    columns = [
        column["source"] for column in table_info["columns"]
        if column.get("where") is None
    ]
    column_string = ", ".join(columns)
    data = f"SELECT {column_string} FROM {table_name} WHERE {condition};"
    cursor.execute(data)
    columns = [i[0] for i in cursor.description]
    while True:
        fetched_rows = cursor.fetchmany(30)
        if not fetched_rows:
            connection.close()
            break
        rows = [list(row) for row in fetched_rows]
        tables_output["columns"] = columns
        tables_output["rows"] = rows
        yield tables_output


# @task(pool_slots=1)
def remap_columns(source_table, table_info) -> List[Dict[str, Any]]:

    column_search_map = {}
    for column in table_info["columns"]:
        # Makes a {source: target} map from
        # {"source": source, "target": target} map
        source_column = column["source"]
        column_search_map[source_column] = column.get("target")
    target_columns = [
        column_search_map[column] for column in source_table["columns"]
    ]
    target_table_output = {
        "columns": target_columns,
        "rows": source_table["rows"],
    }
    return target_table_output


# @task(pool="target_worker")
def insert_data_into_temp_table(table_list, table_info):
    from airflow.providers.odbc.hooks.odbc import OdbcHook

    destination_hook = OdbcHook(odbc_conn_id="odbc-core-target")

    current_table = table_list
    # file_writer(current_table, table_info["target"])
    col_str = ", ".join(current_table["columns"])
    parameter_string = "?, " * len(current_table["columns"])
    # Parmetrized queries need commas too. Made me suffer for a hour. Oops.
    entry_sql_string = (
        f"INSERT INTO {table_info['schema']}.temp_{table_info['target']} "
        f"({col_str}) VALUES ({parameter_string.rstrip(', ')})")
    connection = destination_hook.get_conn()
    cursor = connection.cursor()
    cursor.executemany(entry_sql_string, current_table["rows"])
    cursor.commit()
    connection.close()


def file_writer(object, table_name):
    with open(
            f"dags/test_data/{table_name}{secrets.randbelow(4209)}.json",
            "a",
            encoding="utf8",
    ) as f_object:
        f_object.write(json.dumps(object))
