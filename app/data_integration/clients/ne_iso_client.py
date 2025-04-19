import os

import requests
from datetime import datetime, timezone
import logging
import json

from dotenv import load_dotenv

from isone_client import ApiClient
from isone_client.api import DayaheadhourlydemandApi
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
        self.api_client = ApiClient(self.configuration)
        self.day_ahead_hourly_demand_api = DayaheadhourlydemandApi(api_client=self.api_client)


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
    client = ISONEClient(os.environ.get("ISO_NE_API_USERNAME"), os.environ.get("ISO_NE_API_PASSWORD"))

    client.fetch_demand()