from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Test dag!")

with DAG("weather_etl_dag", start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    task = PythonOperator(task_id="hello_task", python_callable=hello)
