from typing import Any, Dict, List
import json
import secrets


# @task(pool="source_worker", pool_slots=2)
def load_sql(table_info) -> Dict[str, Any]:
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
    condition = table_info["where"]
    tables_output = {}
    columns = [column["source"] for column in table_info["columns"] if column.get("where") is None]
    column_string = ", ".join(columns)
    data = f"SELECT {column_string} FROM {table_name} WHERE {condition};"
    cursor.execute(data)
    columns = [i[0] for i in cursor.description]
    while True:
        fetched_columns = cursor.fetchmany(30)
        if not fetched_columns:
            connection.close()
            break
        rows = [list(row) for row in fetched_columns]
        tables_output["columns"] = columns
        tables_output["rows"] = rows
        yield tables_output


# @task(pool_slots=1)
def remap_columns(source_table, table_info) -> List[Dict[str, Any]]:

    column_search_map = {}
    for column in table_info["columns"]:
        # Makes a {source: target} map from {"source": source, "target": target} map
        source_column = column["source"]
        column_search_map[source_column] = column.get("target")
    target_columns = [column_search_map[column] for column in source_table["columns"]]
    target_table_output = {
        "columns": target_columns,
        "rows": source_table["rows"],
    }
    return target_table_output


# @task(pool="target_worker")
def insert_data_into_temp_table(table_list, table_info):
    from airflow.providers.odbc.hooks.odbc import OdbcHook

    destination_hook = OdbcHook(
        odbc_conn_id="odbc-core-target",
    )
    # print("Reached here at line 74")

    current_table = table_list
    # file_writer(current_table, table_info["target"])
    multi_row_string = ""
    col_str = ", ".join(current_table["columns"])
    for row in current_table["rows"]:
        row_str = ", ".join(
            f"'{i}'" if isinstance(i, str) else "null" if i is None else str(i)
            for i in row
        )
        multi_row_string += f"({row_str}),\n"
    entry_sql_string = (
        f"INSERT INTO {table_info['schema']}.temp_{table_info['target']}"
        f"({col_str}) VALUES\n"
    )
    multi_row_string_result = multi_row_string.rstrip(",\n") + ";"
    final_sql_string = entry_sql_string + multi_row_string_result
    connection = destination_hook.get_conn()
    cursor = connection.cursor()
    # print("Reached here at line 80")

    # print(f"Temp filling string is: {final_sql_string}")
    cursor.execute(final_sql_string)
    cursor.commit()
    connection.close()


def file_writer(object, table_name):
    with open(
        f"dags/test_data/{table_name}{secrets.randbelow(4209)}.json",
        "a",
        encoding="utf8",
    ) as f_object:
        f_object.write(json.dumps(object))
