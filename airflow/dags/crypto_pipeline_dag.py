from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

INGESTION_PATH = "/opt/airflow/ingestion"

if INGESTION_PATH not in sys.path:
    sys.path.append(INGESTION_PATH)

from extract_and_load import load_raw_data
from quality_checks import run_quality_checks


default_args = {
    "owner": "sandro",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="crypto_api_pipeline",
    default_args=default_args,
    description="Crypto pipeline with quality checks",
    start_date=datetime(2026, 3, 31),
    #schedule="@hourly",
    schedule="*/5 * * * *",  # Läuft alle 5 Minuten
    catchup=False,
    tags=["crypto"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=load_raw_data,
    )

    quality_task = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_quality_checks,
    )

    extract_task >> quality_task