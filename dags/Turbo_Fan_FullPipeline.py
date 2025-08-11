from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# Full Pipeline DAG - Stages 1 through 6
default_args = {
    'owner': 'turbine-rul',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='Turbine_RUL_Full_Pipeline',
    default_args=default_args,
    schedule=None,  # Manual trigger
    catchup=False,
    description='Complete Turbine RUL ML Pipeline - Stages 1-6',
    tags=['turbine', 'rul', 'full-pipeline', 'ml']
) as dag:

    # Common Docker configuration
    common_docker_config = {
        'image': 'turbine_rul_mlpipeline-turbine-rul:latest',
        'auto_remove': 'success',
        'mount_tmp_dir': False,
        'force_pull': False,
        'docker_url': 'unix://var/run/docker.sock',
        'retrieve_output': True,
        'network_mode': 'turbine_rul_mlpipeline_turbine-network',
        'mounts': [Mount(source='turbine_artifacts', target='/app/artifacts', type='volume')],
        'environment': {
            'ARTIFACTS_PATH': '/app/artifacts',
            'LOGS_PATH': '/app/logs',
            'CONFIG_PATH': '/app/config',
            'PROMETHEUS_ENABLED': 'true',
            'PUSHGATEWAY_URL': 'pushgateway:9091'
        }
    }

    # Stage 1: Data Ingestion
    stage_1 = DockerOperator(
        task_id='run_stage_1',
        command='python run_stage.py 1',
        retrieve_output_path='/tmp/stage1_output.log',
        **common_docker_config
    )

    # Stage 2: Data Preprocessing
    stage_2 = DockerOperator(
        task_id='run_stage_2',
        command='python run_stage.py 2',
        retrieve_output_path='/tmp/stage2_output.log',
        **common_docker_config
    )

    # Stage 3: Feature Engineering
    stage_3 = DockerOperator(
        task_id='run_stage_3',
        command='python run_stage.py 3',
        retrieve_output_path='/tmp/stage3_output.log',
        **common_docker_config
    )

    # Stage 4: Model Training
    stage_4 = DockerOperator(
        task_id='run_stage_4',
        command='python run_stage.py 4',
        retrieve_output_path='/tmp/stage4_output.log',
        **common_docker_config
    )

    # Stage 5: Model Validation
    stage_5 = DockerOperator(
        task_id='run_stage_5',
        command='python run_stage.py 5',
        retrieve_output_path='/tmp/stage5_output.log',
        **common_docker_config
    )

    # Stage 6: Model Deployment/Inference
    stage_6 = DockerOperator(
        task_id='run_stage_6',
        command='python run_stage.py 6',
        retrieve_output_path='/tmp/stage6_output.log',
        **common_docker_config
    )

    # Set up sequential dependencies
    stage_1 >> stage_2 >> stage_3 >> stage_4 >> stage_5 >> stage_6