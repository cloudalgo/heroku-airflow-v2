from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag_owner = 'airflow'

default_args = {'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }

with DAG(dag_id='create_table',
    default_args=default_args,
    description='Creating table',
    start_date=datetime(),
    schedule_interval='',
    catchup=False,
    tags=['test']
):
      
  PostgresOperator_task = PostgresOperator(
    task_id='PostgresOperator_task',
    postgres_conn_id='postgres_default',
    database=None,
    runtime_parameters=None,
  )

