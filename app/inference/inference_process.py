import multiprocessing as mp
import logging
from datetime import datetime, timezone
import time
import os
import pandas as pd
from typing import Dict, Any

log = logging.getLogger(__name__)

class InferenceEngineProcess(mp.Process):
    """
    - Receives update handles (location_id, horizon) from FeatureStoreProcess.
    - Uses a simple TTL approach to decide if it should re-run inference.
    - If re-run is required, fetches the vector from shared_feature_store.
    """

    def __init__(
        self,
        config,
        shared_feature_store: Dict[Any, Any],  # manager dict of features
        input_queue: mp.Queue,
        output_queue: mp.Queue = None
    ):
        super().__init__()
        self.config = config
        self.shared_feature_store = shared_feature_store
        self.input_queue = input_queue
        self.output_queue = output_queue
        self._stop_event = mp.Event()

        # Track last inference time for each horizon/location
        self.last_inference_time = {}

    def reload_model(self):
        pass

    def load_inference_coords(self):
        pass

    def stop(self):
        self._stop_event.set()

    def get_iso_ne_points(self):
        iso_ne_csv_path = os.path.join(os.getcwd(), "data", "reference", "iso_ne_nodes_april_2025.csv")
        df = pd.read_csv(iso_ne_csv_path)
        df = df.dropna(subset=["Latitude", "Longitude"])
        return list(zip(df["Latitude"], df["Longitude"]))

    def run(self):
        log.info("[InferenceEngineProcess] Starting...")
        while not self._stop_event.is_set():
            self._check_for_updates()
            time.sleep(1)
        log.info("[InferenceEngineProcess] Exiting...")

    def _check_for_updates(self):
        """
        Blocking read of the input message queue. Each message:
          {
            "type": "inference",
            "location_id": "...",
            "horizon": "..."
          }
        Decide if we want to run inference. If so, fetch vector from shared_feature_store.
        """
        msg = self.input_queue.get()

        self._perform_inference(msg)
        self._update_last_inference_time(msg)

    def _update_last_inference_time(self, msg):
        if 'location_id' in msg:
            self.last_inference_time[msg['msg_type']][msg['location_id']] = datetime.now(tz=timezone.utc)
        else:
            self.last_inference_time[msg['msg_type']] = datetime.now(tz=timezone.utc)

    def _perform_inference(self, msg):
        """
        Fetch the vector from shared_feature_store and run a mock forecast.
        """

        horizon = msg["horizon"]
        feature_vector = self.shared_feature_store.get(key)

        if feature_vector is None:
            log.warning(f"[InferenceEngineProcess] No vector found for {key}. Skipping.")
            return

        # Example placeholder logic
        log.info(f"[InferenceEngineProcess] Running inference for {key} with features: {feature_vector}")
        forecast = [round(random.uniform(50, 100), 2) for _ in range(3)]
        log.info(f"[InferenceEngineProcess] Forecast result for {key} => {forecast}")

        # Optional: send results downstream
        if self.output_queue:
            result_msg = {
                "location_id": location_id,
                "horizon": horizon,
                "forecast": forecast,
                "timestamp": datetime.utcnow().isoformat()
            }
            self.output_queue.put(result_msg)
