from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Define the DAG
with DAG(
    dag_id='create_turbofan_train_table',
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,  # Manual trigger only
    catchup=False,
    description='Create turbofan table in PostgreSQL'
) as dag:
    
    # Create the table if it doesn't exist
    @task
    def train_create_table():
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        
        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS train_turbofan_data (
            id SERIAL PRIMARY KEY,
            unit_id INTEGER NOT NULL,
            time_cycles INTEGER NOT NULL,
            op_setting_1 DECIMAL(12, 6),
            op_setting_2 DECIMAL(12, 6),
            op_setting_3 DECIMAL(12, 6),
            sensor_1 DECIMAL(12, 6),
            sensor_2 DECIMAL(12, 6),
            sensor_3 DECIMAL(12, 6),
            sensor_4 DECIMAL(12, 6),
            sensor_5 DECIMAL(12, 6),
            sensor_6 DECIMAL(12, 6),
            sensor_7 DECIMAL(12, 6),
            sensor_8 DECIMAL(12, 6),
            sensor_9 DECIMAL(12, 6),
            sensor_10 DECIMAL(12, 6),
            sensor_11 DECIMAL(12, 6),
            sensor_12 DECIMAL(12, 6),
            sensor_13 DECIMAL(12, 6),
            sensor_14 DECIMAL(12, 6),
            sensor_15 DECIMAL(12, 6),
            sensor_16 DECIMAL(12, 6),
            sensor_17 DECIMAL(12, 6),
            sensor_18 DECIMAL(12, 6),
            sensor_19 DECIMAL(12, 6),
            sensor_20 DECIMAL(12, 6),
            sensor_21 DECIMAL(12, 6),
            rul INTEGER,
            data_type VARCHAR(10),
            dataset VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute the table creation query
        postgres_hook.run(create_table_query)
        print("Turbofan table created successfully")
    
    @task
    def train_transform_data():
        # Load data from local file (adjust path as needed)
        df = pd.read_csv('/usr/local/airflow/dags/train_FD001.txt', 
                 sep='\s+',  # Use regex for multiple spaces
                 header=None, 
                 skipinitialspace=True)  # Skip leading spaces
    
        # Define column names for your 26 columns
        columns = ['unit_id', 'time_cycles', 'op_setting_1', 'op_setting_2', 'op_setting_3'] + \
              [f'sensor_{i}' for i in range(1, 22)]
        df.columns = columns[:len(df.columns)]  # Match actual column count
        # Calculate RUL (max_cycles - current_cycles for each unit)
        df['rul'] = df.groupby('unit_id')['time_cycles'].transform(lambda x: x.max() - x)
    
        # Add metadata columns
        df['data_type'] = 'train'  # or 'test' based on your file
        df['dataset'] = 'FD001'    # adjust based on your dataset
    
        # Insert data into PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        
        # Clear existing data but KEEP your schema
        postgres_hook.run("TRUNCATE TABLE train_turbofan_data")  # ← ONLY THIS LINE ADDED
        
        df.to_sql('train_turbofan_data', postgres_hook.get_sqlalchemy_engine(), 
              if_exists='append', index=False, method='multi')
    
        print(f"Inserted {len(df)} records")

    # Add to your DAG tasks
    train_create_table_task = train_create_table()
    train_transform_task = train_transform_data()

    # Set dependency
    train_create_table_task >> train_transform_task