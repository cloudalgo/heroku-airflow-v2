import os
import pandas as pd
import json
from airflow.hooks.postgres_hook import PostgresHook
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    def __init__(self, config_file):
        self.config_file = config_file

    def process_data(self):
        # Load the configuration from the JSON file
        with open(self.config_file, 'r') as f:
            config = json.load(f)

        # Extract the configuration values
        connection_id = config['connection_id']
        schema = config['schema']
        input_table = config['input_table']
        output_table = config['output_table']
        field_mapping = config['field_mapping']
        data_manipulation = config.get('data_manipulation')

        # Connect to the PostgreSQL database using the specified connection ID
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_conn()

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Fetch data from the input table in the specified schema
        cursor.execute(f"SELECT * FROM {schema}.{input_table}")
        data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        # Validate the data
        data = self._validate_data(data, field_mapping)

        if data.empty:
            print("No valid data found. Aborting data processing.")
            return

        # Perform data processing operations
        processed_data = self._perform_processing(data, field_mapping)

        # Apply custom data manipulation functions if provided
        if data_manipulation:
            for manipulation_func in data_manipulation:
                processed_data = self._apply_data_manipulation(processed_data, manipulation_func)

        # Create the output table if it doesn't exist in the specified schema
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{output_table} (id SERIAL PRIMARY KEY, {', '.join(field_mapping.values())})")

        # Truncate the output table to remove existing data
        cursor.execute(f"TRUNCATE TABLE {schema}.{output_table}")

        # Insert the processed data into the output table
        for _, row in processed_data.iterrows():
            values = [row[field] for field in field_mapping.keys()]
            cursor.execute(f"INSERT INTO {schema}.{output_table} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['%s']*len(values))})", values)

        # Commit the changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Processed data saved to table '{output_table}' in schema '{schema}'.")

    @abstractmethod
    def _perform_processing(self, data, field_mapping):
        pass

    @abstractmethod
    def _apply_data_manipulation(self, data, manipulation_func):
        pass
    
    @abstractmethod
    def _validate_data(self, data, field_mapping):
        pass