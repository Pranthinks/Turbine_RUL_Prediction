from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
import kagglehub
import shutil

# Define the DAG
with DAG(
    dag_id='TEST_DATA_ETL',
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,  # Manual trigger only
    catchup=False,
    description='Create turbofan table in PostgreSQL'
) as dag:
    
    # Create the table if it doesn't exist
    @task
    def test_create_table():
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        
        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_turbofan_data (
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
        print("Turbofan test table created successfully")

    @task
    def test_transform_data():
        # Cache directory for dataset
        cache_dir = "/tmp/kaggle_cmaps_cache"
        
        # Check if files are already cached
        test_file_path = os.path.join(cache_dir, "test_FD001.txt")
        rul_file_path = os.path.join(cache_dir, "RUL_FD001.txt")
        
        if os.path.exists(test_file_path) and os.path.exists(rul_file_path):
            print("Using cached dataset files")
            data_path = cache_dir
        else:
            print("Downloading dataset from Kaggle...")
            # Download latest version from Kaggle
            path = kagglehub.dataset_download("behrad3d/nasa-cmaps")
            print("Path to dataset files:", path)
            
            # Files are inside CMaps folder
            cmaps_path = os.path.join(path, 'CMaps')
            
            # Create cache directory and copy files
            os.makedirs(cache_dir, exist_ok=True)
            shutil.copy2(os.path.join(cmaps_path, 'test_FD001.txt'), test_file_path)
            shutil.copy2(os.path.join(cmaps_path, 'RUL_FD001.txt'), rul_file_path)
            
            print("Files cached for future use")
            data_path = cache_dir
        
        # Load test data from cached files
        df = pd.read_csv(os.path.join(data_path, 'test_FD001.txt'),
                        sep='\s+',  # Use regex for multiple spaces
                        header=None,
                        skipinitialspace=True)  # Skip leading spaces
        
        # Define column names for your 26 columns
        columns = ['unit_id', 'time_cycles', 'op_setting_1', 'op_setting_2', 'op_setting_3'] + \
                  [f'sensor_{i}' for i in range(1, 22)]
        df.columns = columns[:len(df.columns)]  # Match actual column count
        
        # Load RUL data from cached files
        rul_df = pd.read_csv(os.path.join(data_path, 'RUL_FD001.txt'),
                            header=None,
                            names=['rul'])
        
        # Add unit_id to RUL dataframe (assuming RUL file is ordered by unit_id starting from 1)
        rul_df['unit_id'] = range(1, len(rul_df) + 1)
        
        # Simply merge the RUL values with ALL rows of test data
        # Each unit gets the same RUL value for all its time cycles
        # This represents: "At any point, this engine has X cycles remaining until failure"
        df = df.merge(rul_df, on='unit_id', how='left')
        
        # Add metadata columns
        df['data_type'] = 'test'
        df['dataset'] = 'FD001'
        
        print(f"Transformed {len(df)} records")
        return df

    @task
    def test_load_data(df):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    
    # Clear existing data but KEEP your schema
        postgres_hook.run("TRUNCATE TABLE test_turbofan_data")
    # Insert new data into existing table structure
        df.to_sql('test_turbofan_data', postgres_hook.get_sqlalchemy_engine(),
             if_exists='append', index=False, method='multi')
        print(f"Loaded {len(df)} records into PostgreSQL")
    # Add to your DAG tasks
    test_create_table_task = test_create_table()
    test_transform_task = test_transform_data()
    test_load_task = test_load_data(test_transform_task)

    # Set dependencies
    test_create_table_task >> test_transform_task >> test_load_task