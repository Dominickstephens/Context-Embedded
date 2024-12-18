import time
import logging


class TimeyTimer:

    def __init__(self, block_name: str, logger: logging.Logger):
        self.block_name = block_name
        self.logger = logger
        self.start_time = None
        self.duration_ms = None

    def __enter__(self):
        self.start_time = time.perf_counter_ns()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.perf_counter_ns()
        self.duration_ms = (end_time - self.start_time) / 1_000_000  # Convert ns to ms
        self.logger.info("%s took %.2fms to execute", self.block_name, self.duration_ms)
