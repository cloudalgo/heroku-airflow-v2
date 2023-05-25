import json
import logging
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


def create_or_alter_columns(schema_file, postgres_conn_id):
    # Read the JSON schema from the file
    with open(schema_file, 'r') as file:
        schema_data = json.load(file)
    
    for schema in schema_data['schemas']:
        for table in schema['tables']:
            # Check if the table exists
            check_table_sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema['name']}' AND table_name = '{table['name']}';"
            check_table_task = PostgresOperator(
                task_id=f"check_table_{schema['name']}_{table['name']}",
                postgres_conn_id=postgres_conn_id,
                sql=check_table_sql,
                result_type='single_value'
            )
            table_exists = check_table_task.execute() == 1

            if not table_exists:
                # Create the table if it does not exist
                columns = ', '.join([f"{column['name']} {column['type']} {' '.join(column['constraints'])}" for column in table['columns']])
                create_table_sql = f"CREATE TABLE {schema['name']}.{table['name']} ({columns});"
                create_table_task = PostgresOperator(
                    task_id=f"create_table_{schema['name']}_{table['name']}",
                    postgres_conn_id=postgres_conn_id,
                    sql=create_table_sql
                )
                create_table_task.execute()
                logging.info(f"Table '{schema['name']}.{table['name']}' created.")
            else:
                for column in table['columns']:
                    # Check if the column already exists
                    check_column_sql = f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{schema['name']}' AND table_name = '{table['name']}' AND column_name = '{column['name']}';"
                    check_column_task = PostgresOperator(
                        task_id=f"check_column_{schema['name']}_{table['name']}_{column['name']}",
                        postgres_conn_id=postgres_conn_id,
                        sql=check_column_sql,
                        result_type='single_value'
                    )
                    column_exists = check_column_task.execute() == 1

                    if not column_exists:
                        # Add the column if it does not exist
                        add_column_sql = f"ALTER TABLE {schema['name']}.{table['name']} ADD COLUMN {column['name']} {column['type']};"
                        add_column_task = PostgresOperator(
                            task_id=f"add_column_{schema['name']}_{table['name']}_{column['name']}",
                            postgres_conn_id=postgres_conn_id,
                            sql=add_column_sql
                        )
                        add_column_task.execute()
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
