from datetime import datetime, timedelta
import json
import os
import logging
from cerberus import Validator
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Helper functions

def validate_integer(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False

def validate_float(value):
    return isinstance(value, float)

def validate_string(value):
    return isinstance(value, str)

def validate_email(value):
    validator = Validator({"value": {"type": "string", "regex": r"^\S+@\S+$"}})
    return validator.validate({"value": value})

def validate_phone_number(value):
    validator = Validator({"value": {"type": "string", "regex": r"^\d{10}$"}})
    return validator.validate({"value": value})

def validate_country_code(value):
    validator = Validator({"value": {"type": "string", "regex": r"^[A-Z]{2}$"}})
    return validator.validate({"value": value})

def validate_custom(value, regex_pattern):
    validator = Validator({"value": {"type": "string", "regex": regex_pattern}})
    return validator.validate({"value": value})

def verify_data_types(config):
    for table_process in config["table_processes"]:
        source_table = table_process["source_table"]
        destination_table = table_process["destination_table"]
        validation_fields = table_process["validation_fields"]
        
        source_conn_id = source_table["connection_id"]
        source_schema = source_table["schema"]
        source_table_name = source_table["name"]
        
        destination_conn_id = destination_table["connection_id"]
        destination_schema = destination_table["schema"]
        destination_table_name = destination_table["name"]
        
        source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)
        
        # Read data from source table
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_query = f"SELECT * FROM {source_schema}.{source_table_name}"
        source_cursor.execute(source_query)
        source_columns = [col[0] for col in source_cursor.description]
        source_data = source_cursor.fetchall()
        
        valid_data = []
        
        for row in source_data:
            row_dict = dict(zip(source_columns, row))
            is_valid = True
            
            for field in validation_fields:
                field_name = field["name"]
                field_type = field["type"]
                field_value = row_dict[field_name]
                
                # Check if field is required
                if field.get("required") and (field_value is None or field_value == ""):
                    logging.warning(f"Required field '{field_name}' is missing or empty for row: {row}")
                    is_valid = False
                    break
                
                validation_functions = {
                    "integer": validate_integer,
                    "float": validate_float,
                    "string": validate_string,
                    "email": validate_email,
                    "phone_number": validate_phone_number,
                    "country_code": validate_country_code,
                    "custom": lambda v: validate_custom(v, field["regex"])
                }
                
                validation_func = validation_functions.get(field_type)
                if validation_func:
                    is_valid = validation_func(field_value)
                
                if not is_valid:
                    logging.warning(f"Invalid value in column '{field_name}' for row: {row}")
                    # break
            
            if is_valid:
                valid_data.append(row)
        
        # Create the destination table if it doesn't exist
        destination_conn = destination_hook.get_conn()
        destination_cursor = destination_conn.cursor()
        destination_table_qualified_name = f"{destination_schema}.{destination_table_name}"
        destination_create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {destination_table_qualified_name} AS
            SELECT * FROM {source_schema}.{source_table_name} LIMIT 0
        """
        destination_cursor.execute(destination_create_table_query)
        destination_conn.commit()

        # Load valid data into destination table
        destination_hook.insert_rows(
            table=f"{destination_schema}.{destination_table_name}",
            rows=valid_data
        )

def read_validation_config(file_path):
    with open(file_path) as f:
        table_mappings = json.load(f)
    return table_mappings

config_file = os.path.join(CUR_DIR, "validation", "config.json")
validation_configs = read_validation_config(config_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_validation_dag",
    default_args=default_args,
    schedule_interval="@once"
) as dag:
    validate_data_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=verify_data_types,
        op_kwargs={"config": validation_configs}
    )
    
    validate_data_task
