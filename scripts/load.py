import psycopg2
import os

def load(data):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "weather"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        port=os.getenv("POSTGRES_PORT", 5432)
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            date DATE,
            city TEXT,
            temperature_max FLOAT,
            temperature_min FLOAT,
            precipitation FLOAT,
            wind_speed FLOAT
        );
    """)

    for row in data:
        cur.execute("""
            INSERT INTO weather_data (date, city, temperature_max, temperature_min, precipitation, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["date"],
            row["city"],
            row["temperature_max"],
            row["temperature_min"],
            row["precipitation"],
            row["wind_speed"]
        ))

    conn.commit()
    cur.close()
    conn.close()