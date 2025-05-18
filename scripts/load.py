import os
import csv

def load(data):
    filename = os.path.join(os.getenv("DATA_DIR", "/opt/airflow/data"), "weather_data.csv")
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["city", "temperature", "weather"])

        if not file_exists:
            writer.writeheader()

        writer.writerow(data)