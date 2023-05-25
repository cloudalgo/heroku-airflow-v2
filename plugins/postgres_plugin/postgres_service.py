import json
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

