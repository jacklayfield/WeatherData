# import os
# import csv
import psycopg2

# def load(data):
#     filename = os.path.join(os.getenv("DATA_DIR", "/opt/airflow/data"), "weather_data.csv")
#     file_exists = os.path.isfile(filename)

#     with open(filename, mode="a", newline="") as file:
#         writer = csv.DictWriter(file, fieldnames=["city", "temperature", "weather", "timestamp"])

#         if not file_exists:
#             writer.writeheader()

#         writer.writerow(data)

def load(data):
    conn = psycopg2.connect(
        dbname="weather",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city TEXT,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            timestamp TIMESTAMP
        );
    """)
    
    cur.execute(
        "INSERT INTO weather_data (city, temperature, humidity, wind_speed, timestamp) VALUES (%s, %s, %s, %s, %s)",
        (data["city"], data["temperature"], data["humidity"], data["wind_speed"], data["timestamp"])
    )
    conn.commit()
    cur.close()
    conn.close()