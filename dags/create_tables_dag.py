import os
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import json
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

class CustomPostgresOperator(PostgresOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        result = hook.get_first(self.sql)
        self.log.info('Result: %s', result)
        return result

def create_or_alter_columns(schema_file, postgres_conn_id):
    # Read the JSON schema from the file
    with open(schema_file, 'r') as file:
        schema_data = json.load(file)

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    for schema in schema_data['schemas']:
        for table in schema['tables']:
            # Check if the table exists
            check_table_sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema['name']}' AND table_name = '{table['name']}';"
            table_exists = postgres_hook.get_first(check_table_sql)[0] == 1

            if not table_exists:
                # Create the table if it does not exist
                columns = ', '.join([f"{column['name']} {column['type']} {' '.join(column['constraints'])}" for column in table['columns']])
                create_table_sql = f"CREATE TABLE {schema['name']}.{table['name']} ({columns});"
                postgres_hook.run(create_table_sql)
                logging.info(f"Table '{schema['name']}.{table['name']}' created.")
            else:
                for column in table['columns']:
                    # Check if the column already exists
                    check_column_sql = f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{schema['name']}' AND table_name = '{table['name']}' AND column_name = '{column['name']}';"
                    column_exists = postgres_hook.get_first(check_column_sql)[0] == 1

                    if not column_exists:
                        # Add the column if it does not exist
                        add_column_sql = f"ALTER TABLE {schema['name']}.{table['name']} ADD COLUMN {column['name']} {column['type']};"
                        postgres_hook.run(add_column_sql)
                        logging.info(f"Column '{column['name']}' added to '{schema['name']}.{table['name']}'.")
                    else:
                        logging.info(f"Column '{column['name']}' already exists in '{schema['name']}.{table['name']}'.")

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
