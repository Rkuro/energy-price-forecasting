import time
import multiprocessing as mp
import logging
import yaml

from .config import load_config
from .logging_helper import setup_logging
from .data_integration.data_integration_manager import IngestionProcess
from .feature_vectorization.feature_store import FeatureStoreProcess
from .inference_process import InferenceEngineProcess
# from utils.cleanup import CleanupManager
# from models.training import TrainingManager

log = logging.getLogger(__name__)

def main():
    # Setup logging
    setup_logging()
    log.info("Starting up...")

    # Load config
    log.info("Loading config...")
    config = load_config()
    log.info(f"Loaded config:\n {yaml.dump(config.model_dump(), sort_keys=False)}")

    max_disk_bytes = config.general.max_disk_bytes
    training_interval_seconds = config.training.training_interval_seconds

    # Create a Manager for shared data structures
    manager = mp.Manager()

    # Queues
    data_queue = manager.Queue()   # Ingestion -> Feature Store
    inference_queue = manager.Queue() # Feature Store -> Inference Engine

    # Shared dictionary to hold feature vectors
    shared_feature_store = manager.dict()

    log.info("Starting Data Integration...")
    ingestion_process = IngestionProcess(
        output_queue=data_queue,
        config=config
    )
    ingestion_process.start()

    log.info("Starting Feature Store...")
    feature_store_process = FeatureStoreProcess(
        config=config,
        input_queue=data_queue,
        output_queue=inference_queue,
        shared_feature_store=shared_feature_store
    )
    feature_store_process.start()

    log.info("Starting Inference Engine...")
    inference_process = InferenceEngineProcess(
        config=config,
        shared_feature_store=shared_feature_store,
        input_queue=inference_queue
        # output_queue=None if you don't need final results in another queue
    )
    inference_process.start()

    log.info("All processes started.")
    try:
        while True:
            # Possibly handle other logic or check optional forecast outputs
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")

    # Graceful shutdown sequence
    ingestion_process.stop()
    ingestion_process.join()

    feature_store_process.stop()
    feature_store_process.join()

    inference_process.stop()
    inference_process.join()

    log.info("All processes stopped.")

if __name__ == "__main__":
    main()
