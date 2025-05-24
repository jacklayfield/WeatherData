import requests
import time

def geocode(city):
    response = requests.get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": city, "count": 1}
    )
    data = response.json()
    if "results" not in data or not data["results"]:
        raise Exception(f"City '{city}' not found.")
    result = data["results"][0]
    return result["latitude"], result["longitude"], result["name"]

def extract(city="Pittsburgh", start_year=2023, end_year=2024):
    lat, lon, resolved_city = geocode(city)
    all_data = []

    for year in range(start_year, end_year + 1):
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={year}-01-01&end_date={year}-12-31"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
            f"&timezone=auto"
        )
        response = requests.get(url)
        data = response.json()

        if "daily" in data:
            for i, date in enumerate(data["daily"]["time"]):
                all_data.append({
                    "date": date,
                    "temperature_max": data["daily"]["temperature_2m_max"][i],
                    "temperature_min": data["daily"]["temperature_2m_min"][i],
                    "precipitation": data["daily"]["precipitation_sum"][i],
                    "wind_speed": data["daily"]["windspeed_10m_max"][i],
                    "city": resolved_city
                })

        time.sleep(1)

    return all_data