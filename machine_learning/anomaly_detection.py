import pandas as pd
import psycopg2
import numpy as np
from sklearn.ensemble import IsolationForest
from statsmodels.tsa.seasonal import seasonal_decompose
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

# Z-Score Anomaly Detection
def detect_anomalies(df, z_thresh=2.5):
    df = df.sort_values(by=["city", "date"]).copy()
    anomalies = []

    for city, group in df.groupby("city"):
        group = group.copy()
        group['z'] = zscore(group['temperature_max'].ffill())
        city_anomalies = group[np.abs(group['z']) > z_thresh]
        anomalies.append(city_anomalies[['date', 'city', 'temperature_max', 'z']])

    return pd.concat(anomalies) if anomalies else pd.DataFrame()

# Isolation Forest Anomaly Detection
def detect_anomalies_isolation_forest(df):
    df = df.copy()
    df['temperature_max'] = df['temperature_max'].ffill()
    results = []

    for city in df['city'].unique():
        city_df = df[df['city'] == city].sort_values('date').copy()
        city_df['dayofyear'] = city_df['date'].dt.dayofyear
        X = city_df[['temperature_max', 'dayofyear']]

        iso = IsolationForest(contamination=0.05, random_state=42)
        city_df['anomaly'] = iso.fit_predict(X)
        city_df['anomaly'] = city_df['anomaly'].map({1: 0, -1: 1})
        results.append(city_df)

    return pd.concat(results) if results else pd.DataFrame()

# Seasonal Decomposition Anomaly Detection
def detect_anomalies_decomposition(df, period=7, threshold_std=2.5):
    df = df.copy()
    df['temperature_max'] = df['temperature_max'].ffill()
    results = []

    for city in df['city'].unique():
        city_df = df[df['city'] == city].sort_values('date').copy()
        city_df = city_df.set_index('date')
        city_df = city_df.asfreq('D')
        city_df['temperature_max'] = city_df['temperature_max'].ffill()

        if len(city_df) < period * 3:
            continue

        try:
            decomposition = seasonal_decompose(city_df['temperature_max'], model='additive', period=period)
            residual = decomposition.resid.dropna()
            threshold = threshold_std * residual.std()
            anomalies = residual[np.abs(residual) > threshold]

            city_df['anomaly'] = 0
            city_df.loc[anomalies.index, 'anomaly'] = 1
            city_df['residual'] = residual
            city_df = city_df.reset_index()
            results.append(city_df)

        except Exception as e:
            print(f"[{city}] Decomposition error: {e}")

    return pd.concat(results) if results else pd.DataFrame()

if __name__ == "__main__":
    df = get_weather_data()

    print("\n--- Z-SCORE ANOMALIES ---")
    zscore_df = detect_anomalies(df)
    print(zscore_df.head(50))

    print("\n--- ISOLATION FOREST ANOMALIES ---")
    iso_df = detect_anomalies_isolation_forest(df)
    print(iso_df[iso_df['anomaly'] == 1].head(50))

    print("\n--- DECOMPOSITION ANOMALIES ---")
    decom_df = detect_anomalies_decomposition(df)
    print(decom_df[decom_df['anomaly'] == 1].head(50))
