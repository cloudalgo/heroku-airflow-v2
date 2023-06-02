from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import csv
import boto3
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 30),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval=None)

def load_data_from_postgres(connection_id, schema, source_table):
    pg_hook = PostgresHook(postgres_conn_id=connection_id)
    select_sql = f"SELECT * FROM {schema}.{source_table}"
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

def load_data_from_csv(s3_bucket, s3_key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(lines)
    rows = [dict(row) for row in reader]
    return rows

def transform_row_to_json(rows):
    transformed_rows = []
    for row in rows:
        transformed_row = {
            'created_at': datetime.now().isoformat(),
            'modified_at': datetime.now().isoformat(),
            'data': row
        }
        transformed_rows.append(json.dumps(transformed_row))
    return transformed_rows

def insert_rows_to_destination(connection_id, schema, rows, dest_table):
    insert_sql = f"""
        INSERT INTO {schema}.{dest_table} (created_at, modified_at, data)
        VALUES (%s, %s, %s)
    """
    pg_hook = PostgresHook(postgres_conn_id=connection_id, schema=schema)
    for row in rows:
        pg_hook.run(insert_sql, parameters=(row['created_at'], row['modified_at'], row['data']))

# Load configuration from a JSON file
with open(f"{CUR_DIR}/table_sync_config.json") as config_file:
    config_data = json.load(config_file)

for config in config_data:
    source_type = config['source_type']
    source_connection = config['source_connection']
    source_schema = config['source_schema']
    source_table = config['source_table']
    dest_connection = config['dest_connection']
    dest_schema = config['dest_schema']
    dest_table = config['dest_table']

    if source_type == 'postgres':
        rows = load_data_from_postgres(source_connection, source_schema, source_table)
    elif source_type == 'csv':
        rows = load_data_from_csv(config['s3_bucket'], config['s3_key'])

    transform_task = PythonOperator(
        task_id=f'transform_{source_type}_to_json_{dest_table}',
        python_callable=transform_row_to_json,
        op_kwargs={'rows': rows},
        dag=dag
    )

    insert_task = PythonOperator(
        task_id=f'insert_{source_type}_to_{dest_table}',
        python_callable=insert_rows_to_destination,
        op_kwargs={'connection_id': dest_connection, 'schema': dest_schema, 'rows': rows, 'dest_table': dest_table},
        dag=dag
    )

    transform_task >> insert_task
