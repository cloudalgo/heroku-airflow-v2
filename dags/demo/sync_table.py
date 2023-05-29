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


def transform_data(source_data, field_mapping):
    df = pd.DataFrame(source_data)
    transformed_data = []

    for _, row in df.iterrows():
        transformed_row = {}

        for source_field, mapping in field_mapping.items():
            target_field = mapping['target_field']
            merge_columns = mapping.get('merge_columns')
            calculated_expression = mapping.get('calculated_expression')
            manipulation_rules = mapping.get('manipulation', [])

            if merge_columns:
                transformed_value = ''.join(str(row[col]) for col in merge_columns)
            elif calculated_expression:
                transformed_value = eval(calculated_expression, {'row': row, 'math': math})
            else:
                source_value = row[source_field]
                transformed_value = source_value

                for rule in manipulation_rules:
                    if rule == 'uppercase':
                        transformed_value = transformed_value.upper()
                    elif rule == 'abs':
                        transformed_value = abs(transformed_value)
                    elif rule.startswith('round'):
                        decimal_places = int(rule.split(':')[1]) if ':' in rule else 0
                        transformed_value = round(transformed_value, decimal_places)
                    elif rule.startswith('change_type'):
                        data_type = rule.split(':')[1] if ':' in rule else 'str'
                        transformed_value = eval(f'{data_type}({transformed_value})')

            transformed_row[target_field] = transformed_value

        transformed_data.append(transformed_row)

    return transformed_data


def validate_data(transformed_data, field_mapping):
    validation_errors = []

    for row in transformed_data:
        for source_field, mapping in field_mapping.items():
            target_field = mapping['target_field']
            validation_rules = mapping.get('validation', [])
            value = row[target_field]

            for rule in validation_rules:
                rule_parts = rule.split(':')
                validation_type = rule_parts[0]
                if validation_type == 'required':
                    if value is None or value == '':
                        validation_errors.append({'row': row, 'field': target_field, 'error': f'{target_field} is required'})
                elif validation_type == 'min':
                    min_value = float(rule_parts[1])
                    if value is not None and value < min_value:
                        validation_errors.append({'row': row, 'field': target_field, 'error': f'{target_field} is less than the minimum value of {min_value}'})
                elif validation_type == 'max':
                    max_value = float(rule_parts[1])
                    if value is not None and value > max_value:
                        validation_errors.append({'row': row, 'field': target_field, 'error': f'{target_field} exceeds the maximum value of {max_value}'})
                elif validation_type == 'min_length':
                    min_length = int(rule_parts[1])
                    if value is not None and len(str(value)) < min_length:
                        validation_errors.append({'row': row, 'field': target_field, 'error': f'{target_field} length is less than the minimum length of {min_length}'})
                elif validation_type == 'max_length':
                    max_length = int(rule_parts[1])
                    if value is not None and len(str(value)) > max_length:
                        validation_errors.append({'row': row, 'field': target_field, 'error': f'{target_field} length exceeds the maximum length of {max_length}'})

    return validation_errors


def send_email(recipient, subject, body, success_csv_data, failed_csv_data):
    mailgun_api_key = 'your_mailgun_api_key'
    mailgun_domain = 'your_mailgun_domain'
    sender = 'sender_email_address'
    attachment_name = 'data.csv'

    files = [
        ('attachment', (attachment_name, success_csv_data)),
        ('attachment', (attachment_name, failed_csv_data)),
    ]

    try:
        response = requests.post(
            f'https://api.mailgun.net/v3/{mailgun_domain}/messages',
            auth=('api', mailgun_api_key),
            files=files,
            data={
                'from': sender,
                'to': recipient,
                'subject': subject,
                'text': body
            }
        )
        response.raise_for_status()
        print('Email sent successfully')
    except requests.exceptions.RequestException as e:
        print(f'Failed to send email: {str(e)}')


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

    # Transform the source data
    transformed_data = transform_data(source_data, field_mapping)

    # Validate the transformed data
    validation_errors = validate_data(transformed_data, field_mapping)

    # Prepare success and failed row data for further processing
    success_rows = []
    failed_rows = []

    for idx, row in enumerate(transformed_data):
        if validation_errors and any(error['row'] == row for error in validation_errors):
            failed_rows.append({**row, 'error': '; '.join(error['error'] for error in validation_errors if error['row'] == row)})
        else:
            success_rows.append(row)

    # Prepare CSV data for email attachment
    success_csv_data = io.StringIO()
    failed_csv_data = io.StringIO()

    success_writer = csv.DictWriter(success_csv_data, fieldnames=field_mapping.keys())
    success_writer.writeheader()
    success_writer.writerows(success_rows)

    failed_fieldnames = list(field_mapping.keys()) + ['error']
    failed_writer = csv.DictWriter(failed_csv_data, fieldnames=failed_fieldnames)
    failed_writer.writeheader()
    failed_writer.writerows(failed_rows)

    # Send email with attachment
    email_recipient = 'recipient_email_address'
    email_subject = f'{source_table} Data Sync Report'
    email_body = f'Total Rows: {len(transformed_data)}\n' \
                 f'Successful Rows: {len(success_rows)}\n' \
                 f'Failed Rows: {len(failed_rows)}\n\n' \
                 f'Please find the attached CSV files for detailed information.'

    send_email(email_recipient, email_subject, email_body, success_csv_data.getvalue(), failed_csv_data.getvalue())

    # Insert successful rows into target table
    if success_rows:
        target_hook.insert_rows(table=target_table, rows=success_rows, schema=target_schema)


# Load table mappings from config file
config_file = 'config.json'
table_mappings = load_table_mappings(config_file)

# Create PythonOperator for each table mapping
for table_mapping in table_mappings:
    task_id = f"sync_table_{table_mapping['source_table']}_{table_mapping['target_table']}"
    python_operator = PythonOperator(
        task_id=task_id,
        python_callable=sync_table,
        op_kwargs={'table_mapping': table_mapping},
        dag=dag
    )
