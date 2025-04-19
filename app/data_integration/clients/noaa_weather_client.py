import requests
import json
import pandas as pd
import logging
import os
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import Config
from ..polling_thread import BasePollingThread

log = logging.getLogger(__name__)

class NOAAWeatherClient:
    BASE_URL = "https://api.weather.gov"

    def __init__(self, user_agent="(energy_price_forecasting_app)"):
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/ld+json"
        }
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._shutdown = False

    def shutdown_executor(self, wait=True):
        if not self._shutdown:
            log.info("Shutting down NOAAWeatherClient thread pool executor...")
            self.executor.shutdown(wait=wait)
            self._shutdown = True

    def get_forecast(self, lat, lon):
        if self._shutdown:
            return {"lat": lat, "lon": lon, "error": "Executor has been shut down"}
        try:
            start = time.time()
            url = f"{self.BASE_URL}/points/{lat},{lon}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            point_data = response.json()
            # log.info(f"Point data: {json.dumps(point_data, indent=4)}")

            forecast_url = point_data["forecast"]
            location = point_data["relativeLocation"]
            city = location["city"]
            state = location["state"]

            forecast_resp = requests.get(forecast_url, headers=self.headers)
            forecast_resp.raise_for_status()
            forecast = forecast_resp.json()
            end = time.time()
            log.info(f"Weather forecast fetched in {end - start} seconds")
            # log.info(f"Forecast data: {json.dumps(forecast, indent=4)}")

            return {
                "lat": lat,
                "lon": lon,
                "city": city,
                "state": state,
                "forecast": forecast
            }
        except Exception as e:
            return {
                "lat": lat,
                "lon": lon,
                "error": str(e)
            }

    def get_iso_forecast(self, iso):
        iso = iso.upper()
        coords = {
            "ISO_NE": self.get_iso_ne_points()
        }[iso]

        log.info(f"Fetching weather data using {len(coords)} coordinates for iso {iso}")
        futures = [self.executor.submit(self.get_forecast, lat, lon) for lat, lon in coords]

        for future in as_completed(futures):
            log.info("New weather data ingested")
            yield future.result()

    def get_iso_ne_points(self):
        iso_ne_csv_path = os.path.join(os.getcwd(), "data", "reference", "iso_ne_nodes_april_2025.csv")
        df = pd.read_csv(iso_ne_csv_path)
        df = df.dropna(subset=["Latitude", "Longitude"])
        return list(zip(df["Latitude"], df["Longitude"]))


class WeatherPollingThread(BasePollingThread):
    def __init__(self, config: Config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.weather_client = NOAAWeatherClient()

    def _fetch_weather_data(self, iso, output_queue):
        log.info("Fetching data...")
        try:
            for weather_data in self.weather_client.get_iso_forecast(iso):
                output_queue.put(
                    {
                        "type": "weather",
                        "location_id": f"{weather_data['lat']},{weather_data['lon']}",
                        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds'),
                        "data": weather_data,
                    }
                )
        except Exception as e:
            log.error(f"Error: {e}")

    def poll_action(self):
        log.info(f"Polling weather after {self.interval_sec} seconds...")
        self._fetch_weather_data(self.config.general.iso, self.output_queue)

    def stop_gracefully(self):
        log.info("Stopping gracefully...")
        self.weather_client.shutdown_executor()

if __name__ == "__main__":
    client = NOAAWeatherClient()
    forecasts = client.get_iso_forecast("ISO-NE")
    print(json.dumps(forecasts[:3], indent=2))  # Show only first 3 results for brevity
