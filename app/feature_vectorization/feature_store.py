import multiprocessing as mp
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import logging
import random  # only if simulating data
from collections import defaultdict

from .feature_adapter import FeatureAdapter
from .adapters.feature_adapter_weather import WeatherFeatureAdapter
# from .feature_adapter_load import LoadForecastFeatureAdapter
# from .feature_adapter_generation import GenerationMixFeatureAdapter
# from .feature_adapter_transmission import TransmissionFeatureAdapter
# from .feature_adapter_market import MarketConditionFeatureAdapter
# etc.

from ..config import Config
from ..logging_helper import setup_logging

log = logging.getLogger(__name__)

def default_utcnow() -> datetime:
    """A top-level function returning current UTC time.
    Required so the default value is pickle-able in multiprocessing contexts.
    """
    return datetime.now(timezone.utc)

class FeatureStoreProcess(mp.Process):
    """
    A multiprocessing process that receives raw data messages, vectorizes them,
    and places each vector into shared_feature_store[(location_id, horizon)].

    Instead of sending large vectors to downstream processes, it sends small
    "update handle" messages with (location_id, horizon) to output_queue.
    """

    def __init__(
        self,
        config: Config,
        input_queue: mp.Queue,             # raw data from ingestion
        output_queue: mp.Queue,            # lightweight update handles
        shared_feature_store: Dict[Any, Any],  # manager dict for actual feature storage
        vectorizers: Dict[str, FeatureAdapter], # A registry of adapters, keyed by message type
    ):
        super().__init__()
        self.config = config
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.shared_feature_store = shared_feature_store
        self._stop_event = mp.Event()

        # Registry of adapters, keyed by message type
        self.vectorizers: Dict[str, FeatureAdapter] = vectorizers

    def stop(self):
        """Signal this process to terminate gracefully."""
        self._stop_event.set()

    def run(self):
        setup_logging()
        log.info("[FeatureStoreProcess] Starting vectorization loop...")
        while not self._stop_event.is_set():
            self._read_input_queue()
            time.sleep(0.5)

        log.info("[FeatureStoreProcess] Shutting down.")

    def _read_input_queue(self):
        """
        Blocks on the input queue for the next message, then handles it.
        """
        try:
            log.debug("Waiting for next data ingestion...")
            next_input_message = self.input_queue.get()
            self._handle_message(next_input_message)
        except Exception as e:
            log.error(f"Error when handling message for vectorization: {e}", exc_info=True)

    def _handle_message(self, msg: Dict[str, Any]):
        msg_type = msg.get("type")
        if not msg_type:
            log.error("Message missing 'type' field. Cannot vectorize.")
            return

        adapter = self.vectorizers.get(msg_type)
        if not adapter:
            log.error(f"No vectorizer found for '{msg_type}'. Known: {list(self.vectorizers.keys())}")
            return

        location_id = msg.get("location_id")

        # If there is a location, then use that as a subkey inside of message type
        # otherwise just use message type as a global scope
        if location_id:
            past_vector_data = self.shared_feature_store.get(msg_type, {}).get(location_id)
        else:
            past_vector_data = self.shared_feature_store.get(msg_type)

        # Expect the adapter's vectorize() to return a vector of features and
        # a set of horizons to be updated due to the new data
        horizons, feature_vector = adapter.vectorize(msg, past_data=past_vector_data)

        # For each horizon in the result, store in shared_feature_store
        for horizon in horizons:

            # Then we emit a message to run inference for each horizon
            update_msg = {
                "type": "inference",
                "horizon": horizon,
                "location_id": location_id,
                "msg_type": msg_type
            }

            if location_id:
                if msg_type not in self.shared_feature_store:
                    self.shared_feature_store[msg_type] = {}
                self.shared_feature_store[msg_type][location_id] = feature_vector
                self.output_queue.put(update_msg)
            else:
                self.shared_feature_store[msg_type] = feature_vector
                self.output_queue.put(update_msg)
            log.info(f"Emitted update => {update_msg}")

        # Optionally archive
        self._archive_data(adapter, msg)

    def _archive_data(self, adapter: FeatureAdapter, msg: Dict[str, Any]):
        """Stub for archiving data if needed."""
        try:
            archived_format = adapter.archive(msg)
            log.debug(f"Archived data for {msg.get('type')} => {archived_format}")
        except NotImplementedError:
            pass
        except Exception as e:
            log.error(f"Failed archiving data: {e}", exc_info=True)
