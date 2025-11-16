"""
TransitX Data Pipeline — Apache Airflow DAG
--------------------------------------------
Automates the full data-to-model workflow for TransitX:
1. Extract -> 2. Transform -> 3. Feature Engineering -> 4. Train Model -> 5. DVC Push
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# -------- DAG Config. ----------
default_args = {
    "owner":"transitx",
    "depends_on_past":False,
    "retries":1,
    "retry_delay":timedelta(minutes=2)
}

with DAG(
    dag_id ="transitx-pipeline",
    description="End-to-End automated data and ML pipeline for TransitX",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["transitx","mlops", "azure", "dvc"],
) as dag:
    
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="python3 /opt/airflow/src/pipelines/extract.py"
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="python3 /opt/airflow/src/pipelines/transform.py"
    )

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command="python3 /opt/airflow/src/pipelines/feature_eng.py"
    )

    train_reg_model = BashOperator(
        task_id="train_reg_model",
        bash_command="python3 /opt/airflow/src/models/train_regressor.py"
    )

    train_class_model = BashOperator(
        task_id="train_class_model",
        bash_command="python3 /opt/airflow/src/models/train_classifier.py"
    )

    dvc_push = BashOperator(
        task_id="push_to_dvc",
        bash_command="cd /opt/transitx-project && dvc push"
    )


    # ---------- DAG EXECUTION ORDER ----------
    (
        extract_data
        >> transform_data
        >> feature_engineering
        >> train_reg_model
        >> train_class_model
        >> dvc_push
    )