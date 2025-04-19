from eia_client import ApiClient, Configuration, DataParams
from eia_client.api.ng_api import NGApi
import os
import logging
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import Config
from ..polling_thread import BasePollingThread

log = logging.getLogger(__name__)

class EIAClient:

    def __init__(self, api_key: str):
        api_client_config = Configuration(api_key={"api_key":api_key})
        self.api_client = ApiClient(configuration=api_client_config)
        self.ng_api_client: NGApi = NGApi(api_client=self.api_client)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._shutdown = False

    def shutdown_executor(self, wait=True):
        if not self._shutdown:
            log.info("Shutting down EIA Client thread pool executor...")
            self.executor.shutdown(wait=wait)
            self._shutdown = True

    def get_natural_gas_prices(self):
        # Start and end date
        response = self.ng_api_client.v2_natural_gas_route1_route2_data_post(
            route1="pri",
            route2="fut",
            data_params=DataParams(
                start="2025-04-01",
                end="2025-04-15",
                frequency="daily",
                data=['value']
            )
        )
        return response


class EIAPollingThread(BasePollingThread):
    def __init__(self, config: Config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        eia_api_key = os.environ.get("EIA_API_KEY")
        self.eia_client = EIAClient(api_key=eia_api_key)


if __name__ == "__main__":
    load_dotenv()
    client = EIAClient(api_key=os.environ.get("EIA_API_KEY"))
    print(client.get_natural_gas_prices())
    # # start=2025-04-01&end=2025-04-02
    # response = ng_api_client.v2_natural_gas_route1_route2_data_post(
    #     route1="pri",
    #     route2="fut",
    #     data_params=DataParams(
    #         start="2025-04-01",
    #         end="2025-04-02",
    #         frequency="daily",
    #         data=['value']
    #     )
    # )
    # print(response)
    # pass