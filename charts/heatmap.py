import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

conn = psycopg2.connect(
    dbname="weather",
    user="airflow",
    password="airflow",
    host="localhost",
    port="5432"
)

query = "SELECT date, city, temperature_max FROM weather_data"
df = pd.read_sql(query, conn)
conn.close()

df['date'] = pd.to_datetime(df['date']).dt.date

pivot = df.pivot_table(index='city', columns='date', values='temperature_max')

plt.figure(figsize=(14, 6))
sns.heatmap(pivot, cmap='coolwarm', cbar_kws={'label': 'Max Temperature (°C)'})
plt.title("Daily Max Temperature by City")
plt.xlabel("Date")
plt.ylabel("City")
plt.tight_layout()

os.makedirs("data", exist_ok=True)
output_path = os.path.join("data", "temperature_heatmap.png")
plt.savefig(output_path)
print(f"Heatmap saved to {output_path}")