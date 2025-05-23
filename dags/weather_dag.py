from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append('/opt/airflow')  # for containerized scripts

from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load


def run_etl():
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

with DAG(
    dag_id="weather_etl",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "etl"]
) as dag:

    task = PythonOperator(
        task_id="run_weather_etl",
        python_callable=run_etl
    )

    task
