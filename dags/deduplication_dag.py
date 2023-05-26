from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
import Levenshtein
import psycopg2
import logging

"""
Deduplication DAG

This DAG performs deduplication on multiple tables based on the provided configuration.
The deduplication process involves comparing fields of each row in a table to identify and merge duplicates.
The configuration is read from the 'dedup_config.json' file.

Note: Ensure that the necessary dependencies are installed and the 'dedup_config.json' file is present.

"""

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 26),
}

BATCH_SIZE = 1000

def load_config():
    with open('dedup_config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

def connect_to_database(connection_id):
    # Connect to the database using the provided connection ID
    conn = psycopg2.connect(conn_id=connection_id)
    cursor = conn.cursor()
    return conn, cursor

def create_destination_table(source_conn, source_cursor, destination_conn, destination_cursor, source_schema_name, source_table_name, destination_schema_name, destination_table_name):
    # Create the destination table if it doesn't exist
    destination_cursor.execute(f"CREATE TABLE IF NOT EXISTS {destination_schema_name}.{destination_table_name} (LIKE {source_schema_name}.{source_table_name} INCLUDING CONSTRAINTS);")

    # Add reference columns in the destination table
    destination_cursor.execute(f"ALTER TABLE {destination_schema_name}.{destination_table_name} ADD COLUMN original_table_name TEXT, ADD COLUMN original_row_id INTEGER, ADD COLUMN merged_row_ids INTEGER[];")

    # Commit the changes
    destination_conn.commit()

def fetch_rows(source_conn, source_cursor, source_schema_name, source_table_name, offset):
    # Fetch data from the source table in batches
    source_cursor.execute(f"SELECT * FROM {source_schema_name}.{source_table_name} LIMIT {BATCH_SIZE} OFFSET {offset};")
    rows = source_cursor.fetchall()
    return rows

def apply_fuzzy_logic(field_values, unique_field_values, field):
    if field["enable_fuzzy_logic"]:
        distance = Levenshtein.distance(field_values[field["name"]], unique_field_values[field["name"]])
        similarity = 1 - (distance / max(len(field_values[field["name"]]), len(unique_field_values[field["name"]])))

        return similarity >= field["similarity_threshold"]
    else:
        return field_values[field["name"]] == unique_field_values[field["name"]]

def merge_duplicate_rows(unique_row, row):
    merged_row = unique_row.copy()
    for field, value in row.items():
        # Update the merged row with non-empty values from the duplicate row
        if not merged_row[field] and value:
            merged_row[field] = value
    return merged_row

def insert_unique_row(destination_conn, destination_cursor, destination_schema_name, destination_table_name, unique_row):
    # Insert the unique row into the destination table
    columns = ', '.join(unique_row.keys())
    values = ', '.join(f"%({key})s" for key in unique_row.keys())
    insert_query = f"INSERT INTO {destination_schema_name}.{destination_table_name} ({columns}) VALUES ({values});"
    destination_cursor.execute(insert_query, unique_row)

def insert_merged_row(destination_conn, destination_cursor, destination_schema_name, destination_table_name, merged_row):
    # Insert the merged row into the destination table
    columns = ', '.join(merged_row.keys())
    values = ', '.join(f"%({key})s" for key in merged_row.keys())
    insert_query = f"INSERT INTO {destination_schema_name}.{destination_table_name} ({columns}) VALUES ({values});"
    destination_cursor.execute(insert_query, merged_row)

def process_table(table_config):
    # Extract configuration values
    source_conn_id = table_config["source"]["connection_id"]
    source_schema_name = table_config["source"]["schema_name"]
    source_table_name = table_config["source"]["table_name"]
    destination_conn_id = table_config["destination"]["connection_id"]
    destination_schema_name = table_config["destination"]["schema_name"]
    destination_table_name = table_config["destination"]["table_name"]
    fields = table_config["fields"]

    # Connect to source and destination databases
    source_conn, source_cursor = connect_to_database(source_conn_id)
    destination_conn, destination_cursor = connect_to_database(destination_conn_id)

    # Create the destination table if it doesn't exist
    create_destination_table(source_conn, source_cursor, destination_conn, destination_cursor,
                             source_schema_name, source_table_name, destination_schema_name, destination_table_name)

    # Fetch data from the source table in batches
    source_cursor.execute(f"SELECT COUNT(*) FROM {source_schema_name}.{source_table_name};")
    total_rows = source_cursor.fetchone()[0]
    offset = 0

    while offset < total_rows:
        rows = fetch_rows(source_conn, source_cursor, source_schema_name, source_table_name, offset)

        # Perform de-duplication for the batch
        unique_rows = []
        merged_rows = []

        for row in rows:
            # Get the values of configurable fields for de-duplication
            field_values = {field["name"]: row[field["name"]] for field in fields}

            # Apply fuzzy logic to determine similarity between fields
            is_duplicate = False
            duplicate_row_ids = []
            merged_row_ids = []

            for i, unique_row in enumerate(unique_rows):
                unique_field_values = {field["name"]: unique_row[field["name"]] for field in fields}

                # Compare field values based on priority order
                for field in fields:
                    if not apply_fuzzy_logic(field_values, unique_field_values, field):
                        break  # Move to the next unique row
                else:
                    # If all fields satisfy the similarity threshold, consider it a duplicate
                    is_duplicate = True
                    duplicate_row_ids.append(i)
                    merged_row_ids.extend(unique_row["merged_row_ids"])

            # Check if the row is a duplicate based on the configurable fields
            if is_duplicate:
                # Merge the duplicate rows into a single record
                merged_row = merge_duplicate_rows(unique_rows[duplicate_row_ids[0]], row)
                merged_row["merged_row_ids"] = merged_row_ids + [row["id"]]

                merged_rows.append(merged_row)
            else:
                unique_rows.append(row)

        # Insert the unique rows into the destination table
        for unique_row in unique_rows:
            unique_row["original_table_name"] = source_table_name
            unique_row["original_row_id"] = unique_row["id"]
            insert_unique_row(destination_conn, destination_cursor, destination_schema_name,
                              destination_table_name, unique_row)

        # Insert the merged rows into the destination table
        for merged_row in merged_rows:
            merged_row["original_table_name"] = source_table_name
            merged_row["original_row_id"] = merged_row["id"]
            insert_merged_row(destination_conn, destination_cursor, destination_schema_name,
                              destination_table_name, merged_row)

        # Commit the changes after processing each batch
        destination_conn.commit()

        offset += BATCH_SIZE

    # Close the cursors and connections
    source_cursor.close()
    source_conn.close()
    destination_cursor.close()
    destination_conn.close()

def process_tables(**kwargs):
    logging.info("Starting de-duplication process...")
    config = load_config()

    for table_config in config["tables"]:
        logging.info(f"Processing table: {table_config['source']['table_name']}")

        try:
            process_table(table_config)
            logging.info(f"Table {table_config['source']['table_name']} processed successfully.")
        except Exception as e:
            logging.error(f"Error processing table {table_config['source']['table_name']}: {str(e)}")

dag = DAG(
    'deduplication_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

dedup_operator = PythonOperator(
    task_id='deduplicate_data',
    python_callable=process_tables,
    provide_context=True,
    dag=dag
)

dedup_operator
