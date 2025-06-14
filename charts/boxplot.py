import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import random

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

unique_cities = df['city'].nunique()
base_palette = sns.light_palette("blue", n_colors=unique_cities)
random.shuffle(base_palette)

sns.set_theme(style="whitegrid")

plt.figure(figsize=(12, 6))
ax = sns.boxplot(
    x='city',
    y='temperature_max',
    data=df,
    palette=base_palette,
    showfliers=False,
    linewidth=2
)

plt.xticks(rotation=45, ha="right")
plt.title("Maximum Temperature Distribution by City", fontsize=16)
plt.xlabel("City", fontsize=12)
plt.ylabel("Max Temperature (°C)", fontsize=12)

plt.tight_layout()
plt.savefig("data/boxplot_temperature.png")
plt.show()
