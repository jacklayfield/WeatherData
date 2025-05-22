import pandas as pd
import psycopg2
import seaborn as sns
import matplotlib.pyplot as plt

conn = psycopg2.connect(
    dbname="weather",
    user="airflow",
    password="airflow",
    host="localhost", 
    port="5432"
)

query = """
SELECT DISTINCT ON (city) city, temperature, humidity, wind_speed
FROM weather_data
ORDER BY city, timestamp DESC;
"""

df = pd.read_sql(query, conn)
conn.close()

df = df.dropna(subset=["city", "temperature", "humidity", "wind_speed"])
df = df.set_index("city")

# Plot heatmap
plt.figure(figsize=(14, 10))
sns.heatmap(df, annot=True, cmap="coolwarm", fmt=".1f", linewidths=0.5)
plt.title("Weather Metrics Heatmap Across Cities", fontsize=16)
plt.tight_layout()
plt.savefig("data/heatmap.png")
plt.show()
