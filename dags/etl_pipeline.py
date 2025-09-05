from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys, os


sys.path.append(os.path.join(os.path.dirname(__file__), "../etl/bronze"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../etl/silver"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../etl/gold"))


from ingest_anp import download_weekly_reports
from clean_data import process_silver
from load_to_postgres import load_to_postgres

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="anp_fuel_pipeline",
    description="Pipeline fim a fim: Bronze -> Silver -> Gold",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 5),
    catchup=False,
    tags=["anp", "etl", "bronze", "silver", "gold"],
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=download_weekly_reports,
    )

    silver_task = PythonOperator(
        task_id="silver_export_raw",
        python_callable=process_silver,
    )
    
    gold_task = PythonOperator(
        task_id="gold_modeling",
        python_callable=load_to_postgres,
    )
    
    task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        "cd /opt/airflow/dbt && "
        "dbt seed --profiles-dir . && "
        "dbt run --profiles-dir . && "
        "dbt test --profiles-dir ."
    ),
)
    
    bronze_task >> silver_task >> gold_task >> task_dbt_run

