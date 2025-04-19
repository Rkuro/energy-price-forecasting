import traceback

import yfinance as yf
import logging
import time
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.relativedelta import relativedelta

from app.config import Config
from ..polling_thread import BasePollingThread
from app.logging_helper import setup_logging

log = logging.getLogger(__name__)

FUTURES_MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}

def generate_ng_future_tickers(months_ahead=6, include_front_month=True):
    tickers = []
    today = datetime.today()
    if include_front_month:
        tickers.append("NG=F")
    for i in range(months_ahead):
        future_date = today + relativedelta(months=i+1) # month=0 is handled by NG=F
        month_code = FUTURES_MONTH_CODES[future_date.month]
        year_code = str(future_date.year)[-2:]
        ticker = f"NG{month_code}{year_code}.NYM"
        tickers.append(ticker)
    return tickers


class NaturalGasClient:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._shutdown = False

    def shutdown_executor(self, wait=True):
        if not self._shutdown:
            log.info("Shutting down NaturalGasClient thread pool executor...")
            self.executor.shutdown(wait=wait)
            self._shutdown = True

    def get_price(self, ticker="NG=F"):
        if self._shutdown:
            return {"ticker": ticker, "error": "Executor has been shut down"}
        try:
            start = time.time()
            data = yf.Ticker(ticker).history(period="1d", interval="1m")
            end = time.time()
            log.info(f"{ticker} price fetched in {end - start:.2f} seconds")

            for _, row in data.iterrows():
                yield {
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds'),
                    "location_id": None,
                    "data": row.to_dict()
                }
        except Exception as e:
            yield {"ticker": ticker, "error": str(e), "traceback": traceback.format_exc()}

    def get_bulk_prices(self, tickers):
        log.info(f"Fetching prices for tickers {tickers}")
        futures = [self.executor.submit(self.get_price, ticker) for ticker in tickers]
        for future in as_completed(futures):
            yield from future.result()


class NaturalGasPollingThread(BasePollingThread):
    def __init__(self, config: Config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.gas_client = NaturalGasClient()

    def _fetch_gas_prices(self, output_queue):
        try:
            horizon = getattr(self.config.general, "natural_gas_future_horizon_months", 6)
            tickers = generate_ng_future_tickers(horizon)
            for gas_data in self.gas_client.get_bulk_prices(tickers):

                if "error" not in gas_data:
                    output_queue.put({
                        "type": "natural_gas",
                        "location_id": gas_data["ticker"],
                        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds'),
                        "data": gas_data,
                    })
                else:
                    log.warning(f"Error fetching natural gas prices {gas_data['error']}")
        except Exception as e:
            log.error(f"Error fetching natural gas prices: {e}")

    def poll_action(self):
        log.info(f"Polling natural gas prices after {self.interval_sec} seconds...")
        self._fetch_gas_prices(self.output_queue)

    def stop_gracefully(self):
        log.info("Stopping gracefully...")
        self.gas_client.shutdown_executor()

if __name__ == "__main__":
    setup_logging()
    client = NaturalGasClient()
    data = client.get_bulk_prices(tickers=generate_ng_future_tickers(
        months_ahead=6, include_front_month=True
    ))

    for item in data:
        log.info(json.dumps(item, indent=2))