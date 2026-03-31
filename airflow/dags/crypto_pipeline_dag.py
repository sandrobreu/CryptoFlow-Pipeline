from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


INGESTION_PATH = "/opt/airflow/ingestion"

if INGESTION_PATH not in sys.path:
    sys.path.append(INGESTION_PATH)

from extract_and_load import load_raw_data


default_args = {
    "owner": "sandro",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="crypto_api_pipeline",
    default_args=default_args,
    description="Load crypto prices from CoinGecko into Postgres",
    start_date=datetime(2026, 3, 31),
    schedule="@hourly",
    catchup=False,
    tags=["crypto", "api", "postgres"],
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load_crypto_data",
        python_callable=load_raw_data,
    )

    extract_and_load_task