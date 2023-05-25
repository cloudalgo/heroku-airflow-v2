import os
from airflow import DAG
from datetime import datetime,timedelta
from postgres_plugin.postgres_service import create_or_alter_columns
from airflow.operators.python import PythonOperator

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_id = 'create_tables_dag'
schema_files_folder = f"{CUR_DIR}/schema"  # Specify the folder path that contains the schema JSON files
postgres_conn_id = 'postgres_default'  # Specify your PostgreSQL connection ID

def create_tables_dag(dag_id, schema_files_folder, postgres_conn_id, default_args):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=None
    )

    schema_files = [os.path.join(schema_files_folder, file) for file in os.listdir(schema_files_folder) if file.endswith('.json')]
    
    last_task = None
    for i, schema_file in enumerate(schema_files):
        task_id = f"create_or_alter_columns_task_{i}"
        create_or_alter_columns_task = PythonOperator(
            task_id=task_id,
            python_callable=create_or_alter_columns,
            op_kwargs={'schema_file': schema_file, 'postgres_conn_id': postgres_conn_id},
            dag=dag
        )
        
        if last_task:
            create_or_alter_columns_task.set_upstream(last_task)
        
        last_task = create_or_alter_columns_task

    return dag

dag = create_tables_dag(dag_id, schema_files_folder, postgres_conn_id, default_args)
