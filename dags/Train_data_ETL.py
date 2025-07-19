from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='create_turbofan_table',
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,  # Manual trigger only
    catchup=False,
    description='Create turbofan table in PostgreSQL'
) as dag:
    
    # Create the table if it doesn't exist
    @task
    def create_table():
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        
        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS turbofan_data (
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
    
    # Just run the table creation task
    create_table()