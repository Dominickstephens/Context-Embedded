import requests
from typing import Optional
from collections import deque
from threading import Lock
import time
import threading
from .metrics_datamodel import DTO_Aggregator
import logging


class MetricsAPI:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
        self.fifo_queue = deque()
        self.queue_lock = threading.Lock()
        # Start background thread to flush queue
        self.flush_thread = threading.Thread(target=self.background_flush_queue, daemon=True)
        self.flush_thread.start()

    def submit_datasnapshot(self, aggregator: DTO_Aggregator) -> bool:

        queue_length = 0
        with self.queue_lock:
            self.fifo_queue.append(aggregator)
            queue_length = len(self.fifo_queue)

        self.logger.info("Submitted snapshot for %s. Queue size is now %d. Leaving it to background thread to flush.",
                         aggregator.devices[0].name, queue_length)

    def background_flush_queue(self):
        while True:
            if self.try_flush_queue():
                time.sleep(1)
            else:
                self.logger.warning("Failed to flush queue. Sleeping for 10 seconds before retrying.")
                time.sleep(10)

    def try_flush_queue(self):
        url = f"{self.base_url}/upload_data"
        with self.queue_lock:
            queue_length = len(self.fifo_queue)

        while queue_length > 0:
            self.logger.info("Flushing queue of %d items", queue_length)
            # Peek at the next item without removing it
            with self.queue_lock:
                aggregator = self.fifo_queue[0]

            connected_successfully = False
            try:
                json_data = aggregator.to_json()
                self.logger.info("Sending snapshot to server at %s with json %s", url, json_data)
                response = requests.post(
                    url,
                    data=json_data,
                    headers={'Content-Type': 'application/json'}
                )
                self.logger.info("Response code from server: %s with text: %s", response.status_code, response.text)
                connected_successfully = True
                response.raise_for_status()
                with self.queue_lock:
                    self.fifo_queue.popleft()

            except requests.exceptions.RequestException as e:
                if connected_successfully:

                    self.logger.critical("Snapshot caused failure on server. Dropping it: %s", e)
                    with self.queue_lock:
                        self.fifo_queue.popleft()
                else:
                    self.logger.warning("Failed to connect to server. Will retry. Error: %s", e)
                    return False
            with self.queue_lock:
                queue_length = len(self.fifo_queue)

        return True
