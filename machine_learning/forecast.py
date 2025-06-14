import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import psycopg2
from datetime import timedelta

def get_weather_data():
    conn = psycopg2.connect(
        dbname="weather", user="airflow", password="airflow", host="localhost", port="5432"
    )
    df = pd.read_sql("SELECT date, city, temperature_max FROM weather_data", conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df

def create_features(df):
    df = df.sort_values('date')
    df['dayofyear'] = df['date'].dt.dayofyear
    df['lag1'] = df['temperature_max'].shift(1)
    df['lag7'] = df['temperature_max'].shift(7)
    df = df.dropna()
    return df

def train_forecast_model(city_df):
    df = create_features(city_df)
    X = df[['dayofyear', 'lag1', 'lag7']]
    y = df['temperature_max']
    X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.2)
    
    model = RandomForestRegressor()
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    print(f"RMSE: {rmse:.2f}")
    return model, df

def forecast_next_day(model, df):
    last = df.iloc[-1]
    next_day = last['date'] + timedelta(days=1)
    dayofyear = next_day.dayofyear
    lag1 = last['temperature_max']
    lag7 = df.iloc[-7]['temperature_max'] if len(df) >= 7 else lag1
    
    features = pd.DataFrame([{
        'dayofyear': dayofyear,
        'lag1': lag1,
        'lag7': lag7
    }])
    
    prediction = model.predict(features)[0]
    return next_day.date(), prediction

def main():
    df = get_weather_data()
    
    for city in df['city'].unique():
        print(f"\n--- Forecasting for {city} ---")
        city_df = df[df['city'] == city].copy()
        
        if len(city_df) < 10:
            print(f"Not enough data for {city}, skipping...")
            continue
        
        model, processed_df = train_forecast_model(city_df)
        next_day, forecast = forecast_next_day(model, processed_df)
        print(f"Forecast for {next_day}: {forecast:.2f}°C")

if __name__ == "__main__":
    main()
