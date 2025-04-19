# ingestion/data_ingestion_submodule.py
import multiprocessing as mp
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import List

from apscheduler.schedulers.background import BackgroundScheduler
from .noaa_weather_client import WeatherPollingThread
from ..config import Config
from .polling_thread import BasePollingThread
from .streaming_thread import BaseStreamingThread
from ..logging_helper import setup_logging

log = logging.getLogger(__name__)


class IngestionProcess(mp.Process):
    """
    A multiprocessing.Process that orchestrates multiple ingestion threads:
    - Polling tasks: run periodically at a fixed interval
    - Streaming tasks: run continuously until stopped
    """
    def __init__(self, output_queue: mp.Queue, config: Config):
        log.info(f"Constructing Data Ingestion Process Class")
        super().__init__()
        self.output_queue = output_queue
        self.config = config
        self._stop_event = mp.Event()
        # We'll keep track of threads in lists
        self.polling_threads: List[BasePollingThread] = []
        self.streaming_threads: List[BaseStreamingThread] = []

    def add_polling_task(self, polling_thread: BasePollingThread):
        """
        Register a polling thread. E.g.:
            ingestion_proc.add_polling_task(WeatherPollingThread(...))
        """
        self.polling_threads.append(polling_thread)

    def add_streaming_task(self, streaming_thread: BaseStreamingThread):
        """
        Register a streaming thread. E.g.:
            ingestion_proc.add_streaming_task(WebSocketStreamThread(...))
        """
        self.streaming_threads.append(streaming_thread)

    def configure_tasks(self):
        for iso in self.config.general.isos_enabled:
            if iso == "ISO_NE":
                self.polling_threads.append(
                    WeatherPollingThread(
                        self.config,
                        self.output_queue,
                        interval_sec=10,
                        name="WeatherPollingThread"
                    )
                )
        log.info(f"Configuring Data Ingestion Processes")
        pass


    def run(self):
        """
        Invoked in the child process after ingestion_proc.start().
        1) Start all threads.
        2) Wait until self._stop_event is set.
        3) Join all threads gracefully.
        """
        # Create a local threading.Event to control them:
        setup_logging()
        log.info("Beginning ingestion process.")
        local_stop_event = threading.Event()

        self.configure_tasks()

        # Transfer the stop event so the threads can detect shutdown
        # (We can't directly share self._stop_event because it's an mp.Event,
        #  so we replicate the signal from mp.Event to a threading.Event.)
        def mirror_stop_signals():
            while not self._stop_event.is_set():
                time.sleep(0.5)
            local_stop_event.set()

        # Start a small bridging thread that listens for when _stop_event is triggered
        # in the parent process, and sets the local_stop_event for all threads.
        bridging_thread = threading.Thread(target=mirror_stop_signals, daemon=True)
        bridging_thread.start()

        # Actually start the ingestion threads
        for t in self.polling_threads:
            t.stop_event = local_stop_event  # ensure it has the correct event reference
            t.start()
        for t in self.streaming_threads:
            t.stop_event = local_stop_event
            t.start()

        try:
            # Wait until self._stop_event is set (mirrored to local_stop_event)
            while not self._stop_event.is_set():
                time.sleep(1)
        finally:
            log.info(f"Attempting to stop sub-threads gracefully...")
            for t in self.polling_threads + self.streaming_threads:
                t.stop_gracefully()
            # Join threads to clean up
            for t in self.polling_threads + self.streaming_threads:
                t.join()

    def stop(self):
        """
        Called from the parent process to request shutdown of this child process.
        """
        self._stop_event.set()
