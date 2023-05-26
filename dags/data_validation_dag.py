from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from cerberus import Validator
import logging


def validate_integer(value):
    return isinstance(value, int)


def validate_float(value):
    return isinstance(value, float)


def validate_string(value):
    return isinstance(value, str)


def validate_email(value):
    # Perform email validation logic using Cerberus
    validator = Validator({"value": {"type": "string", "regex": r"^\S+@\S+$"}})
    return validator.validate({"value": value})


def validate_phone_number(value):
    # Perform phone number validation logic using Cerberus
    validator = Validator({"value": {"type": "string", "regex": r"^\d{10}$"}})
    return validator.validate({"value": value})


def validate_country_code(value):
    # Perform country code validation logic using Cerberus
    validator = Validator({"value": {"type": "string", "regex": r"^[A-Z]{2}$"}})
    return validator.validate({"value": value})


def validate_custom(value, regex_pattern):
    # Perform custom regex validation logic using Cerberus
    validator = Validator({"value": {"type": "string", "regex": regex_pattern}})
    return validator.validate({"value": value})


def verify_data_types(config_path):
    """
    Verify the data types of each column value based on the provided configuration.
    Load valid data into the destination table.
    """
    with open(config_path) as config_file:
        config = json.load(config_file)
        
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
            source_query = f"SELECT * FROM {source_schema}.{source_table_name}"
            source_data = source_hook.get_records(source_query)
            
            valid_data = []
            
            for row in source_data:
                is_valid = True
                
                for field in validation_fields:
                    field_name = field["name"]
                    field_type = field["type"]
                    field_value = row[field_name]
                    
                    if field_type == "integer":
                        is_valid = validate_integer(field_value)
                    elif field_type == "float":
                        is_valid = validate_float(field_value)
                    elif field_type == "string":
                        is_valid = validate_string(field_value)
                    elif field_type == "email":
                        is_valid = validate_email(field_value)
                    elif field_type == "phone_number":
                        is_valid = validate_phone_number(field_value)
                    elif field_type == "country_code":
                        is_valid = validate_country_code(field_value)
                    elif field_type == "custom":
                        regex_pattern = field["regex"]
                        is_valid = validate_custom(field_value, regex_pattern)
                    
                    if not is_valid:
                        logging.warning(f"Invalid value in column '{field_name}' for row: {row}")
                        break
                
                if is_valid:
                    valid_data.append(row)
            
            # Load valid data into destination table
            destination_hook.insert_rows(destination_table=f"{destination_schema}.{destination_table_name}",
                                         rows=valid_data)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("data_validation_dag", default_args=default_args, schedule_interval="@once") as dag:
    validate_data_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=verify_data_types,
        op_kwargs={"config_path": "path/to/config.json"}
    )
    
    validate_data_task
