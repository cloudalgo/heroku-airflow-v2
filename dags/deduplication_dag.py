from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import json
import Levenshtein
import logging
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DEDUP_CONFIG_PATH = os.path.join(CUR_DIR, 'dedup/config.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 26),
}

BATCH_SIZE = 1000


def load_config():
    """
    Load configuration from JSON file.
    """
    with open(DEDUP_CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)
    return config


def connect_to_database(connection_id):
    """
    Connect to the database using the provided connection ID.
    """
    hook = PostgresHook(postgres_conn_id=connection_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


def create_destination_table(source_conn, source_cursor, destination_conn, destination_cursor,
                             source_schema_name, source_table_name, destination_schema_name, destination_table_name):
    """
    Create the destination table if it doesn't exist and add additional columns for tracking original and merged rows.
    """
    destination_cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {destination_schema_name}.{destination_table_name} "
        f"(LIKE {source_schema_name}.{source_table_name} INCLUDING CONSTRAINTS);"
    )

    destination_cursor.execute(
        f"ALTER TABLE {destination_schema_name}.{destination_table_name} "
        "ADD COLUMN IF NOT EXISTS original_table_name TEXT, "
        "ADD COLUMN IF NOT EXISTS original_row_id INTEGER, "
        "ADD COLUMN IF NOT EXISTS merged_row_ids INTEGER[];"
    )

    destination_conn.commit()


def fetch_rows(source_conn, source_cursor, source_schema_name, source_table_name, offset):
    """
    Fetch a batch of rows from the source table using pagination.
    """
    source_cursor.execute(
        f"SELECT * FROM {source_schema_name}.{source_table_name} "
        f"LIMIT {BATCH_SIZE} OFFSET {offset};"
    )
    column_names = [desc[0] for desc in source_cursor.description]
    data = []
    for row in source_cursor.fetchall():
        row_data = dict(zip(column_names, row))
        data.append(row_data)
    return data


def apply_fuzzy_logic(field_values, unique_field_values, field, similarity_threshold):
    """
    Apply fuzzy logic to determine if field values are similar based on Levenshtein distance.
    """
    if field["enable_fuzzy_logic"]:
        distance = Levenshtein.distance(field_values[field["name"]], unique_field_values[field["name"]])
        similarity = 1 - (distance / max(len(field_values[field["name"]]), len(unique_field_values[field["name"]])))

        return similarity >= similarity_threshold
    else:
        return field_values[field["name"]] == unique_field_values[field["name"]]


def merge_duplicate_rows(unique_row, row):
    """
    Merge duplicate rows by combining non-empty field values.
    """
    merged_row = unique_row.copy()
    for field, value in row.items():
        if not merged_row[field] and value:
            merged_row[field] = value
    return merged_row


def insert_row(connection_id, destination_schema_name, destination_table_name, row):
    """
    Insert a row into the destination table using the provided connection ID.
    """
    hook = PostgresHook(postgres_conn_id=connection_id)
    hook.insert_rows(
        table=f"{destination_schema_name}.{destination_table_name}",
        rows=[row]
    )


def process_table(table_config):
    """
    Process a single table for deduplication.
    """
    similarity_threshold = table_config["similarity_threshold"]
    fields = table_config["fields"]

    source_conn, source_cursor = connect_to_database(table_config["source"]["connection_id"])
    destination_conn, destination_cursor = connect_to_database(table_config["destination"]["connection_id"])

    create_destination_table(source_conn, source_cursor, destination_conn, destination_cursor,
                             table_config["source"]["schema_name"], table_config["source"]["table_name"],
                             table_config["destination"]["schema_name"], table_config["destination"]["table_name"])

    source_cursor.execute(f"SELECT COUNT(*) FROM {table_config['source']['schema_name']}.{table_config['source']['table_name']};")
    total_rows = source_cursor.fetchone()[0]
    offset = 0

    while offset < total_rows:
        rows = fetch_rows(source_conn, source_cursor, table_config["source"]["schema_name"],
                          table_config["source"]["table_name"], offset)

        unique_rows = []
        merged_rows = []

        for row in rows:
            field_values = {field["name"]: row[field["name"]] for field in fields}
            is_duplicate = False
            duplicate_row_ids = []
            merged_row_ids = []

            for i, unique_row in enumerate(unique_rows):
                unique_field_values = {field["name"]: unique_row[field["name"]] for field in fields}

                for field in fields:
                    if not apply_fuzzy_logic(field_values, unique_field_values, field, similarity_threshold):
                        break
                else:
                    is_duplicate = True
                    duplicate_row_ids.append(i)
                    merged_row_ids.extend(unique_row["merged_row_ids"])

            if is_duplicate:
                merged_row = merge_duplicate_rows(unique_rows[duplicate_row_ids[0]], row)
                merged_row["merged_row_ids"] = merged_row_ids + [row[table_config['primary_key']]]
                merged_rows.append(merged_row)
            else:
                unique_rows.append(row)

        for unique_row in unique_rows:
            unique_row["original_table_name"] = table_config["source"]["table_name"]
            unique_row["original_row_id"] = unique_row[table_config['primary_key']]
            insert_row(table_config["destination"]["connection_id"], table_config["destination"]["schema_name"],
                       table_config["destination"]["table_name"], unique_row)

        for merged_row in merged_rows:
            merged_row["original_table_name"] = table_config["source"]["table_name"]
            merged_row["original_row_id"] = merged_row[table_config['primary_key']]
            insert_row(table_config["destination"]["connection_id"], table_config["destination"]["schema_name"],
                       table_config["destination"]["table_name"], merged_row)

        destination_conn.commit()
        offset += BATCH_SIZE

    source_cursor.close()
    source_conn.close()
    destination_cursor.close()
    destination_conn.close()


def process_tables(**kwargs):
    """
    Entry point for the deduplication process.
    """
    logging.info("Starting de-duplication process...")
    config = load_config()
    logging.info(config)

    for table_config in config["tables"]:
        logging.info(f"Processing table: {table_config['source']['table_name']}")

        #try:
        process_table(table_config)
        logging.info(f"Table {table_config['source']['table_name']} processed successfully.")
        #except Exception as e:
        #logging.error(f"Error processing table {table_config['source']['table_name']}: {str(e)}")


dag = DAG(
    'deduplication_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

dedup_task = PythonOperator(
    task_id='dedup_task',
    python_callable=process_tables,
    provide_context=True,
    dag=dag
)
