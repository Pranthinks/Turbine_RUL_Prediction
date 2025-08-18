from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

# Simple Pipeline DAG - Only Stage 1 and Stage 6 with Resource Limits
default_args = {
    'owner': 'turbine-rul',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='Prediction_Pipeline',
    default_args=default_args,
    schedule=None,  # Manual trigger
    catchup=False,
    description='Simple Pipeline - Stage 1 and 6 with Resource Limits',
    tags=['turbine', 'rul', 'simple', 'docker']
) as dag:

    # Common Docker configuration with VALID RESOURCE LIMITS ONLY
    common_docker_config = {
        'image': 'turbine_rul_mlpipeline-turbine-rul:latest',
        'auto_remove': 'success',
        'mount_tmp_dir': False,
        'force_pull': False,
        'docker_url': 'unix://var/run/docker.sock',
        'retrieve_output': True,
        'network_mode': 'turbine_rul_mlpipeline_turbine-network',
        'mounts': [Mount(source='turbine_artifacts', target='/app/artifacts', type='volume')],
        
        # VALID RESOURCE LIMITS ONLY
        'cpus': 2.0,              # Limit to 2 CPU cores
        'mem_limit': '2g',        # Limit to 2GB RAM
        # REMOVED: 'memswap_limit': '2g',  # This parameter is not supported
        # REMOVED: 'shm_size': '512m',     # This parameter is not supported
        
        'environment': {
            'ARTIFACTS_PATH': '/app/artifacts',
            'LOGS_PATH': '/app/logs',
            'CONFIG_PATH': '/app/config',
            'PROMETHEUS_ENABLED': 'true',
            'PUSHGATEWAY_URL': 'pushgateway:9091',
            # THREAD LIMITS (these work via environment variables)
            'OMP_NUM_THREADS': '2',
            'OPENBLAS_NUM_THREADS': '2',
            'MKL_NUM_THREADS': '2'
        }
    }

    # Stage 1: Data Ingestion
    stage_1 = DockerOperator(
        task_id='Data_Ingestion',
        command='python run_stage.py 1',
        retrieve_output_path='/tmp/stage1_output.log',
        # REMOVED: container_name - not supported in some Airflow versions
        **common_docker_config
    )

    # Stage 6: Prediction/Inference
    stage_6 = DockerOperator(
        task_id='Model_Prediction',
        command='python run_stage.py 6',
        retrieve_output_path='/tmp/stage6_output.log',
        # REMOVED: container_name - not supported in some Airflow versions
        **common_docker_config
    )

    # Simple workflow: Stage 1 -> Stage 6
    stage_1 >> stage_6