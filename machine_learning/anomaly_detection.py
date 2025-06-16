import pandas as pd
import psycopg2
from scipy.stats import zscore

def get_weather_data():
    conn = psycopg2.connect(
        dbname="weather", user="airflow", password="airflow", host="localhost", port="5432"
    )
    df = pd.read_sql("SELECT date, city, temperature_max FROM weather_data", conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    return df

def detect_anomalies(df, z_thresh=2.5):
    df = df.sort_values(by=["city", "date"]).copy()

    anomalies = []

    for city, group in df.groupby("city"):
        group = group.copy()
        group['z'] = zscore(group['temperature_max'].ffill())

        city_anomalies = group[abs(group['z']) > z_thresh]
        anomalies.append(city_anomalies[['date', 'city', 'temperature_max', 'z']])

    return pd.concat(anomalies)

if __name__ == "__main__":
    df = get_weather_data()
    anomaly_df = detect_anomalies(df)
    print(anomaly_df.head(100))
