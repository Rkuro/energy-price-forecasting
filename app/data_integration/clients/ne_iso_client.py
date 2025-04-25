import os

import requests
from datetime import datetime, timezone
import logging
import json

from dotenv import load_dotenv

from app.logging_helper import setup_logging
from isone_client import ApiClient
from isone_client.api import (
    DayaheadhourlydemandApi,
    FiveminutelmpApi,
    HourlylmpApi
)
from isone_client.configuration import Configuration

from ..polling_thread import BasePollingThread

class ISONEClient:
    """
    Fetches real-time LMP prices for all nodes in ISO-NE.
    """

    def __init__(self, username: str, password: str):
        self.configuration = Configuration(
            username=username,
            password=password,
        )
        self.api_client = ApiClient(configuration=self.configuration)
        self.day_ahead_hourly_demand_api = DayaheadhourlydemandApi(api_client=self.api_client)
        self.five_minute_lmp_api = FiveminutelmpApi(api_client=self.api_client)
        self.hourly_lmp_api = HourlylmpApi(api_client=self.api_client)

    def fetch_prelim_prices(self):
        data = self.five_minute_lmp_api.fiveminutelmp_current_all_get()
        return data

    def fetch_final_prices(self):
        data = self.hourly_lmp_api.hourlylmp_rt_final_day_day_get(
            day="2025-04-23T00:00:00"
        )
        # self.hourly_lmp_api.hourlylmp_rt_final_day_day_location_location_id_get(
        #
        # )
        data = self.hourly_lmp_api.hourlylmp_rt_final_info_get()
        return data

    def fetch_demand(self) -> dict:
        data = self.day_ahead_hourly_demand_api.dayaheadhourlydemand_current_get()
        print(data)

class NEISOPollingThread(BasePollingThread):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poll_action(self):
        pass

    def stop_gracefully(self):
        pass

if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    log = logging.getLogger(__name__)
    client = ISONEClient(
        os.environ.get("ISO_NE_API_USERNAME"),
        os.environ.get("ISO_NE_API_PASSWORD")
    )

    data = client.fetch_final_prices()
    log.info(json.dumps(data, indent=2))