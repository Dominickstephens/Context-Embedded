# import psutil and gather two device metrics which are returned as a list of dictionaries.

import psutil
import json


def gather_metrics():
    metrics = [
        {"name": "cpu_utilization", "value": psutil.cpu_percent(interval=1)},
        {"name": "memory_utilization", "value": psutil.virtual_memory().percent}
    ]
    return json.dumps(metrics)
