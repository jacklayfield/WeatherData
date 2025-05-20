from datetime import datetime, timezone

def transform(data):
    return {
        "city": data.get("name"),
        "temperature": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
        "wind_speed": data.get("wind", {}).get("speed"),
        "timestamp": datetime.now(timezone.utc)
    }