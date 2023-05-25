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

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='pg_data_transfer',
    default_args=default_args,
    schedule_interval=None
)

# Function to extract data from the source database
def extract_data(source_conn, source_table):
    logging.info(f"Extracting data from {source_table} table...")
    hook = PostgresHook(postgres_conn_id=source_conn)
    sql = f'SELECT * FROM {source_table};'
    connection = hook.get_conn()
    cursor = connection.cursor()
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
    logging.info(column_mapping)
    logging.info(data)
    transformed_data = []
    for row in data:
        transformed_row = []
        for column in column_mapping:
            source_column = column['source']
            destination_column = column['destination']
            transformed_value = row[source_column]
            transformed_row.append(transformed_value)
        transformed_data.append(tuple(transformed_row))
    return transformed_data

# Function to load the transformed data into the target database
def load_data(target_conn, table_name, transformed_data):
    logging.info(f"Loading data into {table_name} table...")
    hook = PostgresHook(postgres_conn_id=target_conn)
    connection = hook.get_conn()
    cursor = connection.cursor()
    # cursor.execute(f'TRUNCATE TABLE {table_name};')
    hook.insert_rows(table_name, transformed_data)
    cursor.close()
    connection.close()

# Function to read the column mapping JSON file
def read_column_mapping(file_path):
    with open(file_path) as f:
        column_mapping = json.load(f)
    return column_mapping

# Function to read the table mapping JSON file
def read_table_mappings(file_path):
    with open(file_path) as f:
        table_mappings = json.load(f)
    return table_mappings

# Function to transfer data for a specific table
def transfer_table_data(source_conn, source_table, target_conn, target_table, column_mapping):
    logging.info(f"Transfer process started for {source_table} table to {target_table} table.")
    
    # Step 1: Extract data from source database
    data = extract_data(source_conn, source_table)
    
    # Step 2: Transform the extracted data
    transformed_data = transform_data(data, column_mapping)
    
    # Step 3: Load the transformed data into the target database
    load_data(target_conn, target_table, transformed_data)
    
    logging.info(f"Transfer process completed for {source_table} table to {target_table} table.")

# Define the path to the table mappings JSON file
table_mappings_file = f'{CUR_DIR}/mapping/table_mappings.json'

# Read the table mappings from the JSON file
table_mappings = read_table_mappings(table_mappings_file)

with DAG(
    dag_id='pg_data_transfer',
    default_args=default_args,
    schedule_interval=None
) as dag:
    source_conn = 'postgres_default'
    target_conn = 'postgres_default'

    previous_task = None  # Initialize the previous_task variable

    for source_table, target_table in table_mappings.items():
        column_mapping = read_column_mapping(f'{CUR_DIR}/mapping/{source_table}_mapping.json')

        # Define a task for transferring data for each table
        transfer_task = PythonOperator(
            task_id=f'transfer_data_{source_table}_to_{target_table}',
            python_callable=transfer_table_data,
            op_kwargs={
                'source_conn': source_conn,
                'source_table': source_table,
                'target_conn': target_conn,
                'target_table': target_table,
                'column_mapping': column_mapping
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