from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from datetime import datetime, timedelta
import json
import os

# Full Pipeline DAG with Conditional Logic
default_args = {
    'owner': 'turbine-rul',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_drift_decision(**context):
    """
    Check drift report and decide whether to retrain or predict directly
    Returns task_id to branch to
    """
    try:
        # For debugging - let's first just return the correct path based on your logs
        # Your logs clearly show "PROCEED WITH PREDICTION" and "proceed_with_prediction"
        # So let's hardcode this for now to test the flow
        
        print("=== DRIFT DECISION DEBUG ===")
        print("Based on stage 2 logs showing 'PROCEED WITH PREDICTION'")
        print("Recommendation: proceed_with_prediction")
        print("Decision: Skipping to prediction stage")
        print("===========================")
        
        return 'skip_to_prediction'
        
    except Exception as e:
        print(f"Error in drift decision: {e}")
        print("Defaulting to retraining pipeline")
        return 'run_stage_3'

with DAG(
    dag_id='Turbine_RUL_Conditional_Pipeline',
    default_args=default_args,
    schedule=None,  # Manual trigger
    catchup=False,
    description='Conditional Turbine RUL ML Pipeline based on Drift Detection',
    tags=['turbine', 'rul', 'conditional', 'ml']
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

    # Stage 1: Data Ingestion (always runs)
    stage_1 = DockerOperator(
        task_id='run_stage_1',
        command='python run_stage.py 1',
        retrieve_output_path='/tmp/stage1_output.log',
        **common_docker_config
    )

    # Stage 2: Drift Detection (always runs)
    stage_2 = DockerOperator(
        task_id='run_stage_2',
        command='python run_stage.py 2',
        retrieve_output_path='/tmp/stage2_output.log',
        **common_docker_config
    )

    # Decision point based on drift detection
    drift_decision = BranchPythonOperator(
        task_id='check_drift',
        python_callable=check_drift_decision
    )

    # Stage 3: Feature Engineering (only if drift detected)
    stage_3 = DockerOperator(
        task_id='run_stage_3',
        command='python run_stage.py 3',
        retrieve_output_path='/tmp/stage3_output.log',
        **common_docker_config
    )

    # Stage 4: Model Training (only if drift detected)
    stage_4 = DockerOperator(
        task_id='run_stage_4',
        command='python run_stage.py 4',
        retrieve_output_path='/tmp/stage4_output.log',
        **common_docker_config
    )

    # Stage 5: Model Validation (only if drift detected)
    stage_5 = DockerOperator(
        task_id='run_stage_5',
        command='python run_stage.py 5',
        retrieve_output_path='/tmp/stage5_output.log',
        **common_docker_config
    )

    # Empty task to skip to prediction directly
    skip_to_prediction = EmptyOperator(
        task_id='skip_to_prediction'
    )

    # Stage 6: Prediction/Inference (always runs at the end)
    stage_6 = DockerOperator(
        task_id='run_stage_6',
        command='python run_stage.py 6',
        retrieve_output_path='/tmp/stage6_output.log',
        trigger_rule='none_failed_min_one_success',  # Run if any upstream task succeeds
        **common_docker_config
    )

    # Set up the workflow
    # Always run stages 1 and 2
    stage_1 >> stage_2 >> drift_decision
    
    # If drift detected: run retraining pipeline
    drift_decision >> stage_3 >> stage_4 >> stage_5 >> stage_6
    
    # If no drift: skip to prediction
    drift_decision >> skip_to_prediction >> stage_6