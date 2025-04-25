import multiprocessing as mp
from typing import Dict, Any
from datetime import datetime, timedelta, timezone
import time
from pytimeparse import parse
import logging

from app.logging_helper import setup_logging

log = logging.getLogger(__name__)

class RetrainProcess(mp.Process):

    def __init__(self,
                 config,
                 output_queue: mp.Queue):
        super().__init__()
        self.training_interval_seconds = parse(config.training_interval)
        self.last_retrain = datetime.now(tz=timezone.utc)

    def run(self):
        setup_logging()
        while True:
            if self.should_retrain():
                self.retrain()
            time.sleep(self.training_interval_seconds)

    def should_retrain(self):
        return datetime.now(tz=timezone.utc) - self.last_retrain > timedelta(seconds=self.training_interval_seconds)

    def retrain(self):
        # Probably going to want to have some kind of data archived onto a mounted disk volume
        # We will also need to have "True" target values stored for each interval as well as what we predicted
        # Not sure...
        pass