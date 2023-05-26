"""
DAG: pg_data_transfer
Description: This DAG transfers data from a source PostgreSQL database to a target PostgreSQL database.
The process includes extracting data from source tables, transforming the data based on column mappings,
and loading the transformed data into corresponding tables in the target database.

Steps:
1. Extract data from source database.
2. Transform the extracted data based on column mappings.
3. Load the transformed data into target tables.

"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import json
import logging
import os
from psycopg2.extras import execute_values
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.exceptions import AirflowException

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 0,12 * * *'  # Run the DAG at 12:00 AM and 12:00 PM (UTC)
}

# Function to extract data from the source database
def extract_data(source_conn, source_schema, source_table, created_at_col, modified_at_col, last_run):
    logging.info(f"Extracting data from {source_schema}.{source_table} table...")
    logging.info(f"Last run data {last_run}")
    hook = PostgresHook(postgres_conn_id=source_conn)
    connection = hook.get_conn()
    cursor = connection.cursor()

    if last_run:
        # Filter records created after the last run
        sql = f"SELECT * FROM {source_schema}.{source_table} WHERE {created_at_col} >= '{last_run}' OR {modified_at_col} >= '{last_run}';"
    else:
        # Extract all records if last run is not provided
        sql = f"SELECT * FROM {source_schema}.{source_table};"
    
    cursor.execute(sql)
    
    # Get the column names from the cursor description
    column_names = [desc[0] for desc in cursor.description]
    
    data = []
    for row in cursor.fetchall():
        row_data = dict(zip(column_names, row))
        data.append(row_data)
    
    cursor.close()
    connection.close()
    
    return data

# Function to transform the extracted data
def transform_data(data, column_mapping):
    logging.info("Transforming data...")
    transformed_data = []
    for row in data:
        transformed_row = []
        for column_mapping_item in column_mapping:
            source_column = column_mapping_item['source']
            destination_column = column_mapping_item['destination']
            transformed_value = row[source_column]
            transformed_row.append(transformed_value)
        transformed_data.append(transformed_row)
    return transformed_data

# Function to load the transformed data into the target database
def load_data(target_conn, target_schema, target_table, transformed_data, insert_columns, primary_key):
    logging.info(f"Loading data into {target_schema}.{target_table} table...")
    logging.info(insert_columns)
    logging.info(transformed_data)
    logging.info(primary_key)
    hook = PostgresHook(postgres_conn_id=target_conn)
    connection = hook.get_conn()
    cursor = connection.cursor()
    # cursor.execute(f'TRUNCATE TABLE {target_schema}.{target_table};')

    # Construct the insert statement with specified columns
    # insert_statement = f'INSERT INTO {target_schema}.{target_table} ({", ".join(insert_columns)}) VALUES %s'
    
    # Execute the insert statement with transformed data
    # hook.insert_rows(table=f'{target_schema}.{target_table}', rows=transformed_data, insert_statement=insert_statement)
    
    # Generate the insert SQL statement with specified columns
    insert_sql = f'INSERT INTO {target_schema}.{target_table} ({", ".join(insert_columns)}) VALUES %s'
    
    # Execute the insert statement with transformed data
    execute_values(cursor, insert_sql, transformed_data)

    connection.commit()
    cursor.close()
    connection.close()

# Function to read the table mapping JSON file
def read_table_mappings(file_path):
    with open(file_path) as f:
        table_mappings = json.load(f)
    return table_mappings

# Function to retrieve the last run timestamp from Redis
def get_last_run(redis_conn, key):
    last_run = None
    try:
        last_run = redis_conn.get(key)
        logging.info(f"last_run: {last_run}")
        if last_run:
            last_run = last_run.decode()  # Convert bytes to string
    except AirflowException as e:
        logging.error(f"Error retrieving last run from Redis: {str(e)}")
    return last_run

# Function to store the last run timestamp in Redis
def set_last_run(redis_conn, key, value):
    try:
        redis_conn.set(key, value)
    except AirflowException as e:
        logging.error(f"Error storing last run in Redis: {str(e)}")

# Function to transfer data for a specific table
def transfer_table_data(source_conn, source_schema, source_table, target_conn, target_schema, target_table, column_mapping, insert_columns, created_at_col, modified_at_col):
    logging.info(f"Transfer process started for {source_schema}.{source_table} table to {target_schema}.{target_table} table.")
    
     # Step 1: Retrieve the last run information from Redis
    try:
        redis_hook = RedisHook(redis_conn_id='redis_default')
        redis_conn = redis_hook.get_conn()
        last_run = get_last_run(redis_conn, f'last_run:{source_schema}.{source_table}')
    except AirflowException as e:
        logging.error(f"Error retrieving last run from Redis: {str(e)}")
        last_run = None

    # Step 2: Extract data from the source table
    data = extract_data(source_conn, source_schema, source_table, created_at_col, modified_at_col, last_run)

    # Step 3: Transform the extracted data
    transformed_data = transform_data(data, column_mapping)

    # Step 4: Load the transformed data into the target table
    load_data(target_conn, target_schema, target_table, transformed_data, insert_columns, primary_key)

    # Step 5: Store the last run information in Redis
    try:
        set_last_run(redis_conn, f'last_run:{source_schema}.{source_table}', datetime.utcnow().isoformat())
    except AirflowException as e:
        logging.error(f"Error storing last run in Redis: {str(e)}")

    if redis_conn:
        redis_conn.close()
    
    logging.info(f"Transfer process completed for {source_schema}.{source_table} table to {target_schema}.{target_table} table.")

# Define the path to the table mappings JSON file
table_mappings_file = f'{CUR_DIR}/mapping/table_mappings.json'

# Read the table mappings from the JSON file
table_mappings = read_table_mappings(table_mappings_file)

with DAG(
    dag_id='pg_data_transfer',
    default_args=default_args
) as dag:

    previous_task = None  # Initialize the previous_task variable

    for mapping in table_mappings:
        source_conn = mapping['source_conn']
        source_schema = mapping['source_schema']
        source_table = mapping['source_table']
        target_conn = mapping['target_conn']
        target_schema = mapping['target_schema']
        target_table = mapping['target_table']
        column_mapping = mapping['column_mapping']
        created_at_col = mapping['created_at']
        modified_at_col = mapping['modified_at']
        primary_key = mapping['primary_key']
        insert_columns = [column['destination'] for column in column_mapping]  # Get the destination columns for insert statement

        # Define a task for transferring data for each table
        transfer_task = PythonOperator(
            task_id=f'transfer_data_{source_schema}_{source_table}_to_{target_schema}_{target_table}',
            python_callable=transfer_table_data,
            op_kwargs={
                'source_conn': source_conn,
                'source_schema': source_schema,
                'source_table': source_table,
                'target_conn': target_conn,
                'target_schema': target_schema,
                'target_table': target_table,
                'column_mapping': column_mapping,
                'insert_columns': insert_columns,
                'created_at_col': created_at_col,
                'modified_at_col': modified_at_col
            }
        )

        # Set task dependencies
        # The transfer_task should run after the previous task is complete
        if previous_task:
            transfer_task.set_upstream(previous_task)
        
        previous_task = transfer_task  # Update the previous task for the next iteration

    end_task = DummyOperator(task_id='end_task')

    # Set the final task in the DAG
    if previous_task:
        end_task.set_upstream(previous_task)