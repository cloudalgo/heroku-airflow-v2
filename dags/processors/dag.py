from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from accountMapper import AccountMapper

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('account_mapping_dag', default_args=default_args, schedule_interval='0 0 * * *')

def run_account_mapping():
    account_mapper = AccountMapper('config.json')
    account_mapper.process_data()

account_mapping_task = PythonOperator(
    task_id='account_mapping_task',
    python_callable=run_account_mapping,
    dag=dag,
)

account_mapping_task
