-- Create the weather database if it doesn't exist (optional, handled by env var)
-- CREATE DATABASE weather;

-- Connect to the weather database (commented out because scripts here run inside weather DB)
-- \c weather;

-- Create table for storing weather data
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    timestamp TIMESTAMPTZ
);
