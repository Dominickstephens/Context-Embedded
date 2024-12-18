"""
Library module for the API interface for the metrics data. Provides a very simple
wrapper around the HTTP requests, JSON serialization, and error handling to handle
the complexity of interacting with the metrics API.

Contains a resilience layer to handle intermittent server unavailability.
This is done by queuing up snapshots into a thread safe queue and running a background
thread to flush the queue to the server. If the server is unavailable, the background
thread will sleep for a bit and retry until it can successfully connect and flush
the queue.
"""

import requests
from typing import Optional
from collections import deque
from threading import Lock
import time
import threading
from .metrics_datamodel import DTO_Aggregator
import logging


class MetricsAPI:
    """SDK interface for interacting with the metrics API"""

    def __init__(self, base_url: str):
        """Initialize the API client

        Args:
            base_url: Base URL of the metrics API server
        """
        self.base_url = base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
        self.fifo_queue = deque()
        self.queue_lock = threading.Lock()
        # Start background thread to flush queue
        self.flush_thread = threading.Thread(target=self.background_flush_queue, daemon=True)
        self.flush_thread.start()

    def submit_datasnapshot(self, aggregator: DTO_Aggregator) -> bool:
        """Submit an aggregator data snapshot to the API and tries to flush
        the queue of all pending snapshots.

        Args:
            aggregator: The DTO_Aggregator containing the snapshot data

        Returns:
            bool: True if submission was successful, False otherwise

        Raises:
            requests.exceptions.RequestException: If the API request fails
        """

        # Add the snapshot to the FIFO queue. To get really funky, you
        # could find the existing aggregator object with the same device
        # name and add the new snapshot data to the existing object so
        # there is only ever one upload, but at the same time doing so
        # risks accumulating a massive upload if the server remains
        # offline a long time.

        queue_length = 0
        with self.queue_lock:
            self.fifo_queue.append(aggregator)
            queue_length = len(self.fifo_queue)

        self.logger.info("Submitted snapshot for %s. Queue size is now %d. Leaving it to background thread to flush.",
                         aggregator.devices[0].name, queue_length)

    def background_flush_queue(self):
        """Background thread to flush the FIFO queue to the server"""
        while True:
            if self.try_flush_queue():
                # If we successfully flushed, check again after only a second
                time.sleep(1)
            else:
                # If we failed to flush, sleep for a bit longer and try again
                self.logger.warning("Failed to flush queue. Sleeping for 10 seconds before retrying.")
                time.sleep(10)

    def try_flush_queue(self):
        """Try to flush the FIFO queue to the server

        Returns:
            bool: True if we connected to the server and emptied the queue, False
            if we can't connect and will retry with the next call here.
        """
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
                # Only remove from queue if successful
                with self.queue_lock:
                    self.fifo_queue.popleft()

            except requests.exceptions.RequestException as e:
                if connected_successfully:
                    # If we connected successfully, then we failed because of a
                    # server or data error, so don't want to retry. Drop the queued
                    # item and log the error.
                    self.logger.critical("Snapshot caused failure on server. Dropping it: %s", e)
                    with self.queue_lock:
                        self.fifo_queue.popleft()
                else:
                    # If we failed to connect, then we want to retry, but only when
                    # the next snapshot comes along. For now, log it and return.
                    self.logger.warning("Failed to connect to server. Will retry. Error: %s", e)
                    return False
            # Refresh the queue length after we've potentially removed an item
            with self.queue_lock:
                queue_length = len(self.fifo_queue)

        return True
