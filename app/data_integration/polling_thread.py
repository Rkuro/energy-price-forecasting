import threading
from abc import ABC, abstractmethod
import time


class BasePollingThread(threading.Thread, ABC):
    """
    Base class for polling tasks. Subclass this to implement your specific
    'fetch API every X seconds' logic.
    """

    def __init__(self, output_queue, interval_sec: float, name=None):
        super().__init__(name=name)
        self.output_queue = output_queue
        self.interval_sec = interval_sec
        self.stop_event = None

    @abstractmethod
    def poll_action(self):
        """
        Subclass must implement the actual polling logic (e.g., fetch data from an API).
        This method will be called once per polling interval.
        """
        pass

    def run(self):
        """
        Loop until stop_event is set, calling poll_action() every interval_sec seconds.
        """
        while not self.stop_event.is_set():
            self.poll_action()
            # Sleep, but check periodically if we've been signaled to stop
            for _ in range(int(self.interval_sec)):
                if self.stop_event.is_set():
                    self.stop_gracefully()
                    break
                time.sleep(1)


    @abstractmethod
    def stop_gracefully(self):
        """
        Handle any actions you want to use to stop gracefully prior to exiting
        """
        pass