import uuid
from operations.schemas.object_schemas import DBLogicalObject
from operations.utils.conf import Status
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
import UltraDict as ud
import time
from operations.utils.conf import (
    DEFAULT_INIT_REGIONS,
    DEFAULT_SKYSTORE_BUCKET_PREFIX,
)
import os
import threading


class TraceIdx:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._value = 0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with TraceIdx._lock:
                cls._instance = TraceIdx()
        return cls._instance

    def get(self) -> int:
        with TraceIdx._lock:
            return self._value

    def increment(self) -> None:
        with TraceIdx._lock:
            self._value += 1


# Initialize a ultradict to store the policy name in shared memory
# so that can be used by multiple workers started by uvicorn
policy_ultra_dict = None
try:
    policy_ultra_dict = ud.UltraDict(name="policy_ultra_dict", create=True)
except Exception:
    time.sleep(3)
    policy_ultra_dict = ud.UltraDict(name="policy_ultra_dict", create=False)

policy_ultra_dict["get_policy"] = ""
policy_ultra_dict["put_policy"] = ""

# Default values for the environment variables
init_region_tags = (
    os.getenv("INIT_REGIONS").split(",")
    if os.getenv("INIT_REGIONS")
    else DEFAULT_INIT_REGIONS
)

skystore_bucket_prefix = (
    os.getenv("SKYSTORE_BUCKET_PREFIX")
    if os.getenv("SKYSTORE_BUCKET_PREFIX")
    else DEFAULT_SKYSTORE_BUCKET_PREFIX
)


def create_object_store_interface(region):
    _, zone = region.split(":")
    if region.startswith("azure"):
        bucket_name = f"nsdiuswest2/{skystore_bucket_prefix}-{zone}"
    else:
        bucket_name = f"{skystore_bucket_prefix}-{zone}"
    return ObjectStoreInterface.create(region, bucket_name)


# Helper function to create a logical object
def create_logical_object(
    existing_object, request, version_suspended=False, delete_marker=False
):
    return DBLogicalObject(
        bucket=existing_object.bucket if existing_object else request.bucket,
        key=existing_object.key if existing_object else request.key,
        size=existing_object.size if existing_object else None,
        last_modified=(
            existing_object.last_modified if existing_object else None
        ),  # to be updated later
        etag=existing_object.etag if existing_object else None,
        status=Status.pending,
        multipart_upload_id=(
            existing_object.multipart_upload_id
            if existing_object
            else (uuid.uuid4().hex if request.is_multipart else None)
        ),
        version_suspended=version_suspended,
        delete_marker=delete_marker,
    )
