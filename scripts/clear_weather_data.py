import psycopg2

def clear_weather_data():
    conn = psycopg2.connect(
        dbname="weather",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM weather_data;")
        conn.commit()
        print("All data in weather_data table cleared.")
    except Exception as e:
        print(f"Error clearing weather_data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    clear_weather_data()
