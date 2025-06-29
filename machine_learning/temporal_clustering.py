import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import psycopg2


def get_weather_data():
    conn = psycopg2.connect(
        dbname="weather", user="airflow", password="airflow", host="localhost", port="5432"
    )
    df = pd.read_sql("SELECT date, city, temperature_max FROM weather_data", conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df


def prepare_daily_features(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    daily_features = df.groupby('date')['temperature_max'].agg(['mean', 'std', 'min', 'max']).dropna()
    daily_features.columns = ['temp_mean', 'temp_std', 'temp_min', 'temp_max']
    return daily_features


def cluster_days(daily_features, n_clusters=4):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(daily_features)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    daily_features['cluster'] = kmeans.fit_predict(X_scaled)
    return daily_features


def main():
    df = get_weather_data()
    daily_features = prepare_daily_features(df)
    clustered_days = cluster_days(daily_features)
    print(clustered_days.head(50))


if __name__ == "__main__":
    main()
