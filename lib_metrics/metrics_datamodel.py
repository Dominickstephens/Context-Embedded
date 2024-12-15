"""
Library module for the data model for the metrics data. This is a pure
DTO data definition. The implementation of the logic to read and store metrics
is in the metrics_client_datamodel.py module.
"""

from datetime import datetime, timezone
from typing import List
from uuid import UUID
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
import uuid

@dataclass_json
@dataclass
class DTO_Metric:
    name: str
    value: float

@dataclass_json
@dataclass
class DTO_DataSnapshot:
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    timezone_mins: int = 0
    metrics: List[DTO_Metric] = field(default_factory=list)

@dataclass_json
@dataclass
class DTO_Device:
    name: str
    data_snapshots: List[DTO_DataSnapshot] = field(default_factory=list)

@dataclass_json
@dataclass
class DTO_Aggregator:
    guid: uuid
    name: str
    devices: List[DTO_Device] = field(default_factory=list)

    def to_dict(self):
        """Convert the DTO_Aggregator to a dictionary for JSON serialization.
        Required because UUIDs are not serializable by default."""
        return {
            'guid': str(self.guid),  # Convert UUID to string
            'name': self.name,
            'devices': [device.to_dict() for device in self.devices]  # Assuming devices have a to_dict method
        }