import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json

# Define the source table ORM model
Base = declarative_base()

class SourceTable(Base):
    __tablename__ = 'source_table'
    id = Column(Integer, primary_key=True)
    # Add more source table columns as needed

# Function to create destination table columns if they don't exist
def create_dest_table_columns():
    with open('/path/to/field_mapping.json') as f:
        field_mapping = json.load(f)

    dest_engine = create_engine('postgresql://dest_user:dest_password@dest_host:dest_port/dest_db')
    Session = sessionmaker(bind=dest_engine)
    session = Session()

    # Check if the destination table exists
    if not dest_engine.dialect.has_table(dest_engine, 'dest_table'):
        # Create the destination table
        Base.metadata.create_all(dest_engine)

        logging.info("Destination table created")

    # Check if the destination table columns exist
    dest_table_columns = dest_engine.dialect.get_columns(dest_engine, 'dest_table')
    dest_table_column_names = [column['name'] for column in dest_table_columns]

    for source_column, dest_column in field_mapping.items():
        if dest_column not in dest_table_column_names:
            # Add the column to the destination table
            if source_column == 'id':
                column_type = Integer
            else:
                column_type = Text

            column = Column(dest_column, column_type)
            column.create(dest_engine)

            logging.info("Added column '%s' to the destination table", dest_column)

    session.close()

# Function to sync data between source and destination tables
def sync_data():
    with open('/path/to/field_mapping.json') as f:
        field_mapping = json.load(f)

    source_engine = create_engine('postgresql://source_user:source_password@source_host:source_port/source_db')
    dest_engine = create_engine('postgresql://dest_user:dest_password@dest_host:dest_port/dest_db')
    SessionSource = sessionmaker(bind=source_engine)
    SessionDest = sessionmaker(bind=dest_engine)
    session_source = SessionSource()
    session_dest = SessionDest()

    # Retrieve data from the source table
    source_data = session_source.query(SourceTable).all()

    for row in source_data:
        # Check if the record exists in the destination table based on a unique identifier (e.g., primary key)
        dest_record = session_dest.query(DestTable).filter_by(id=row.id).first()

        if dest_record:
            # Record exists in the destination table, update the record
            for source_column, dest_column in field_mapping.items():
                setattr(dest_record, dest_column, getattr(row, source_column))
        else:
            # Record doesn't exist in the destination table, insert a new record
            dest_row = DestTable(**{dest_column: getattr(row, source_column) for source_column, dest_column in field_mapping.items()})
            session_dest.add(dest_row)

    session_dest.commit()
    session_source.close()
    session_dest.close()

    logging.info("Data synced between the source and destination tables")

# Define the destination table ORM model
class DestTable(Base):
    __tablename__ = 'dest_table'
    id = Column(Integer, primary_key=True)
    # Add destination table columns dynamically based on the field mapping JSON

# Define the Airflow DAG
dag = DAG(
    'postgres_data_sync',
    description='Sync data between two PostgreSQL tables',
    schedule_interval='0 0 * * *',  # Run once daily at midnight
    start_date=datetime(2023, 5, 25),
    catchup=False
)

# Define the task to create destination table columns
create_columns_task = PythonOperator(
    task_id='create_dest_table_columns',
    python_callable=create_dest_table_columns,
    dag=dag
)

# Define the task to sync data
sync_task = PythonOperator(
    task_id='sync_data',
    python_callable=sync_data,
    dag=dag
)

# Set the task dependency
create_columns_task >> sync_task
