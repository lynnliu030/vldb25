from pydantic import BaseModel
from typing import Optional


class LatencySLO(BaseModel):
    read: int  # ms
    write: int  # ms


class Config(BaseModel):
    storage_region: Optional[int] = ""  # For Spanstore
    placement_policy: str
    transfer_policy: str

    fixed_base_region: Optional[bool] = False  # for Teven and Tevict
    regions: Optional[list[str]] = []  # for Tevict and Tevict Ranges

    window_size: Optional[int] = None  # only for T_evict
    cache_size: Optional[int] = None  # in bytes, for static cache eviction
    cache_ttl: Optional[int] = None  # in hours, for fixed TTL cache eviction

    # For SkyPIE/SpanStore
    oracle_directory: Optional[str] = None  # Path to the precomputed oracle files
    replication_factor_min: Optional[
        int
    ] = 1  # The least number of replicas in placements
    storage_region: Optional[str] = ""

    # For SkyPIE
    replication_factor_max: Optional[
        int
    ] = 1  # The most number of replicas in placements

    # For SpanStore
    no_strict_replication: Optional[
        bool
    ] = True  # If True: no upper bound on the number of replicas
    region_selector: Optional[
        str
    ] = ""  # Regex filter for regions considered by SpanStore
    object_store_selector: Optional[
        str
    ] = ""  # Regex filter for object stores considered by SpanStore
