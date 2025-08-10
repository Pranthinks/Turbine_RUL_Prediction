from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# Drift Detection DAG - Manual trigger only
default_args = {
    'owner': 'turbine-rul',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='Drift_Detection_Manual',
    default_args=default_args,
    schedule=None,  # Manual trigger only
    catchup=False,
    description='Calculate Reference Data for Drift Detection - Run ONCE to setup baseline',
    tags=['turbine', 'rul', 'reference-setup', 'drift-baseline']
) as dag:

    # Common Docker configuration (same as your main DAG)
    common_docker_config = {
        'image': 'turbine_rul_mlpipeline-turbine-rul:latest',
        'auto_remove': 'success',
        'mount_tmp_dir': False,
        'force_pull': False,
        'docker_url': 'unix://var/run/docker.sock',
        'retrieve_output': True,
        'mounts': [Mount(source='turbine_artifacts', target='/app/artifacts', type='volume')],
        'environment': {
            'ARTIFACTS_PATH': '/app/artifacts',
            'LOGS_PATH': '/app/logs',
            'CONFIG_PATH': '/app/config'
        }
    }

    # Stage 7: Calculate Reference Data
    calculate_reference = DockerOperator(
        task_id='run_calculate_reference',
        command='python run_stage.py 7',
        retrieve_output_path='/tmp/calculate_reference_output.log',
        **common_docker_config
    )

    # Single task - no dependencies needed
    calculate_reference