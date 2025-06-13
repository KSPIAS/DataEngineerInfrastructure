import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("WEATHERSTACK_API_KEY")
BASE_URL = "http://api.weatherstack.com/current"

def extract_weather(city: str):
    params = {
        "access_key": API_KEY,
        "query": city,
        "units": "m"
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if "error" in data:
        print(f"API Error: {data['error']['info']}")
        return pd.DataFrame()

    current = data["current"]
    weather_info = {
        "city": city,
        "observation_time": data["location"]["localtime"],
        "temperature_c": current.get("temperature"),
        "humidity": current.get("humidity"),
        "weather_descriptions": ", ".join(current.get("weather_descriptions", [])),
        "wind_speed_kmph": current.get("wind_speed"),
        "precip_mm": current.get("precip"),
    }

    return pd.DataFrame([weather_info])
