from flask import Flask, render_template, jsonify
import psycopg2
import pandas as pd

app = Flask(__name__)

# Database connection settings
DB_CONFIG = {
    "dbname": "weather",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT date, city, temperature_max FROM weather_data"
    df = pd.read_sql(query, conn)
    conn.close()

    df['date'] = df['date'].astype(str)

    grouped = df.groupby('city')
    result = []
    for city, group in grouped:
        result.append({
            'city': city,
            'temps': group['temperature_max'].tolist(),
            'dates': group['date'].tolist()
        })

    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

