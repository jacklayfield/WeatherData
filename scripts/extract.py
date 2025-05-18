import os
import requests

def extract():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY not found in environment.")

    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"q": "London", "appid": api_key, "units": "metric"}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code} - {response.text}")
    
    response.raise_for_status()

    return response.json()