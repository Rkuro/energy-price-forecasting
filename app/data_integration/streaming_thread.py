import threading
from abc import ABC, abstractmethod

class BaseStreamingThread(threading.Thread, ABC):
    """
    Base class for streaming tasks. Subclass this for indefinite streams
    (e.g., reading from WebSockets, message queues, etc.).
    """
    def __init__(self, output_queue, stop_event: threading.Event, name=None):
        super().__init__(name=name)
        self.output_queue = output_queue
        self.stop_event = stop_event

    @abstractmethod
    def stream_action(self):
        """
        Subclass must implement the streaming logic (e.g., connect to WebSocket,
        read messages in a loop, etc.). This should also periodically check
        stop_event to break out gracefully.
        """
        pass

    def run(self):
        """
        Start the streaming logic. The method should block until stop_event is set,
        or until streaming naturally ends (e.g., remote server closed connection).
        """
        self.stream_action()