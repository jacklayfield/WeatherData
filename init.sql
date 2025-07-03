-- Create table for storing weather data
CREATE TABLE IF NOT EXISTS weather_data (
    date DATE,
    city TEXT,
    temperature_max FLOAT,
    temperature_min FLOAT,
    precipitation FLOAT,
    wind_speed FLOAT
);