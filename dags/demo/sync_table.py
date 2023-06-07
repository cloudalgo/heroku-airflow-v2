import os
import json
import pandas as pd
import csv
import io
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import math
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import logging as logger


# DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    # Add other required arguments here
}

dag = DAG('dag_pipeline', default_args=default_args, schedule_interval=None)

def load_table_mappings(config_file):
    with open(config_file) as file:
        config = json.load(file)
    return config['tables']

def transform_data(source_data, field_mapping, target_schema, target_table, target_hook):
    pre_processed_source_data = []
    for data in source_data:
        pre_processed_source_data.append(eval(data[-1]))

    insert_sql = f"""
        INSERT INTO {target_schema}.{target_table} (code, title, did, date_prod)
        VALUES (%s, %s, %s, %s)
    """
    field_mapping = field_mapping['data']['target_field']    
    for row in pre_processed_source_data:
        row = {k:v for k,v in zip(field_mapping, row)}
        target_hook.run(insert_sql, parameters=(row['code'], row['title'], row['did'], row['date_prod']))

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
config_file = os.path.join(CUR_DIR, 'config.json')
table_mappings = load_table_mappings(config_file)

def sync_table(table_mapping):
    source_conn_id = table_mapping['source_conn_id']
    target_conn_id = table_mapping['target_conn_id']
    source_table = table_mapping['source_table']
    target_table = table_mapping['target_table']
    source_schema = table_mapping['source_schema']
    target_schema = table_mapping['target_schema']
    field_mapping = table_mapping['field_mapping']

    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    # Extract data from source table
    source_query = f'SELECT * FROM {source_schema}.{source_table}'
    source_data = source_hook.get_records(source_query)
    ## Transform the source data
    transformed_data = transform_data(source_data, field_mapping, target_schema, target_table, target_hook)    
    # Validate the transformed data
    ## wip validate and sending email

##Create PythonOperator for each table mapping
for table_mapping in table_mappings:
    task_id = f"sync_table_{table_mapping['source_table']}_{table_mapping['target_table']}"
    python_operator = PythonOperator(
        task_id=task_id,
        python_callable=sync_table,
        op_kwargs={'table_mapping': table_mapping},
        dag=dag
    )

