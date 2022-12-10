import pendulum
from airflow.decorators import dag


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mssql", "odbc"],
    owner_links={"nitesh": "mailto:nitesh.debnath@visglobal.com.au"},
)
def sql_transfer_middleware():
    from airflow.decorators import task
    from airflow.models import Variable
    from include.utils import config_sanitizer

    @task(pool="target_worker")
    def generate_temp_table(table_info):
        from airflow.providers.odbc.hooks.odbc import OdbcHook

        destination_hook = OdbcHook(
            odbc_conn_id="odbc-core-target",
        )
        column_list = []
        pk = table_info["primary_key"]
        for column in table_info["columns"]:
            column_item_string = f"{column.get('target')} {column['type_string']}"
            column_list.append(column_item_string)
        column_string = ",\n".join(column_list)
        table_specific_sql = (
            f"DROP TABLE IF EXISTS {table_info['schema']}.temp_{table_info['target']};\n"
            f"CREATE TABLE {table_info['schema']}.temp_{table_info['target']} (\n{column_string},\n"
            f"PRIMARY KEY ({pk})\n);"
        )
        connection = destination_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(table_specific_sql)
        cursor.commit()
        connection.close()

    @task(pool="target_worker")
    def merge_temp_into_target(table_info):
        from airflow.providers.odbc.hooks.odbc import OdbcHook

        destination_hook = OdbcHook(
            odbc_conn_id="odbc-core-target",
        )
        column_list = [column["target"] for column in table_info["columns"]]
        col_str = ", ".join(column["target"] for column in table_info["columns"])
        # print(col_str)
        source_col_str = ", ".join(f"Source.{column}" for column in column_list)
        # print(source_col_str)
        table_merge_query = (
            f"ALTER TABLE {table_info['schema']}.{table_info['target']} NOCHECK CONSTRAINT ALL;\n"
            f"MERGE {table_info['schema']}.{table_info['target']} AS Target\n"
            f"USING {table_info['schema']}.temp_{table_info['target']} AS Source\n"
            f"ON Source.{table_info['primary_key']} = Target.{table_info['primary_key']}\n"
            "WHEN NOT MATCHED BY Target THEN\n"
            f"\tINSERT ({col_str})\n"
            f"\tVALUES ({source_col_str})"
        )
        # print("Reached here")
        merge_string = table_merge_query + ";"
        connection = destination_hook.get_conn()
        cursor = connection.cursor()
        # print(f"Merge string is: {merge_string}")
        cursor.execute(merge_string)
        cursor.commit()
        connection.close()

    @task(pool="source_worker", pool_slots=2)
    def do_divided_table(table_info):
        from include.dag_tasks import (
            load_sql,
            remap_columns,
            insert_data_into_temp_table,
        )

        for table_part in load_sql(table_info):
            target_table = remap_columns(table_part, table_info)
            # print(f"Reached here at line 104: {target_table}")
            insert_data_into_temp_table(target_table, table_info)

    config = config_sanitizer(
        Variable.get("AACC_MIDDLEWARE_MAPPING", deserialize_json=True)
    )
    for table in config:
        # target_table = remap_columns(load_sql(table), table)
        temp_table_gen = generate_temp_table(table)
        # temp_table_fill = insert_data_into_temp_table(target_table, table)
        table_merge = merge_temp_into_target(table)
        table_inject = do_divided_table(table)
        temp_table_gen >> table_inject >> table_merge


sql_transfer_middleware()
