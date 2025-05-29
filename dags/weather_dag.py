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
    cities = ["Pittsburgh", "New York", "Chicago", "Los Angeles", "San Diego", 
              "Boise", "Las Vegas", "Anchorage", "San Francisco", "Minneapolis", 
              "Denver", "Seattle", "Atlanta", "Dallas", "Santiago", "Melbourne"]

    for city in cities:
        raw = extract(city=city, start_year=2023, end_year=2024)
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
