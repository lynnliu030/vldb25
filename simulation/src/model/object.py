from datetime import datetime, timedelta
from enum import Enum
from typing import Dict
from hashlib import sha256


class Status(str, Enum):
    pending = "pending"
    pending_deletion = "pending_deletion"
    ready = "ready"


class LogicalObjectKeyWrapper:
    def __init__(self, key):
        self.key = key

    def __hash__(self):
        return int(sha256(self.key.encode()).hexdigest(), 16)

    def __eq__(self, other):
        return self.key == other.key


class LogicalObject:
    def __init__(
        self,
        key: str,
        size: int,
        last_modified: datetime,
        status: Status = Status.ready,
    ):
        self.key = key
        self.size = size
        self.last_modified = last_modified
        self.status = status

        # Track the physical objects (locations) linked to this logical object.
        self.physical_objects: Dict[str, PhysicalObject] = {}
        self.base_region = None

        self.latest_physical_objects: Dict[str, PhysicalObject] = {}
        self.latest_version_id: int = None

    def get_latest_version_id(self):
        return self.latest_version_id

    def increment_version_id(self):
        self.latest_version_id += 1

    def add_physical_object(self, region, physical_object):
        self.physical_objects[region] = physical_object

    def set_last_modified(self, last_modified: datetime):
        self.last_modified = last_modified

    def assign_base_region(self, region):
        if self.base_region is None:
            self.base_region = region

    def is_ready_in_region(self, region: str) -> bool:
        return self.physical_objects.get(region, None).status == Status.ready

    def get_last_element(self):
        return sorted(
            self.physical_objects.values(),
            key=lambda x: x.storage_start_time + timedelta(seconds=x.ttl),
        )[-1]


class PhysicalObject:
    def __init__(
        self,
        location_tag: str,
        key: str,
        size: int,
        ttl: int,
        logical_object: LogicalObject,
        status: Status = Status.ready,
        version_id: int = 0,
    ):
        self.location_tag = location_tag
        self.cloud, self.region = self.location_tag.split(":")
        self.key = key
        self.status = status
        self.size = size
        self.ttl = ttl  # seconds
        self.logical_object = (
            logical_object  # Track the logical object linked to this physical object.
        )

        self.storage_start_time: datetime = None

        # For no base region
        self.expire_immediate: bool = False

        self.version_id = version_id

    def __str__(self):
        return f"Object Location: {self.location_tag}, Object Key: {self.key}, Object Size: {self.size}, Object storage start time: {self.storage_start_time}, Object TTL: {self.ttl}"

    def __hash__(self):
        return hash((self.key, self.location_tag))

    def __eq__(self, other):
        return (
            isinstance(other, PhysicalObject)
            and self.key == other.key
            and self.location_tag == other.location_tag
        )

    def set_status(self, status: Status):
        self.status = status

    def set_storage_start_time(self, time: datetime):
        self.storage_start_time = time

    def ready(self, timestamp):
        return (
            self.status == Status.ready and timestamp >= self.storage_start_time
        )  # > vs >=

    def get_storage_start_time(self):
        return self.storage_start_time

    def is_expired(self, timestamp: datetime):
        return self.ttl != -1 and timestamp >= (
            self.storage_start_time + timedelta(seconds=self.ttl)
        )  # > vs >=

    def set_ttl(self, ttl: int):
        self.ttl = ttl

    def get_ttl(self):
        return self.ttl

    def set_size(self, size: int):
        self.size = size

    def storage_duration(self, end_time: datetime) -> timedelta:
        if self.storage_start_time:
            return end_time - self.storage_start_time
        return timedelta(0)
