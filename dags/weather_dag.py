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
    clean = transform(raw)
    load(clean)

with DAG(
    dag_id="weather_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:

    etl_task = PythonOperator(
        task_id="run_weather_etl",
        python_callable=run_etl
    )
