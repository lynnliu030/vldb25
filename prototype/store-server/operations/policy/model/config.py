from pydantic import BaseModel
from typing import Optional


class LatencySLO(BaseModel):
    read: int  # ms
    write: int  # ms


class Config(BaseModel):
    storage_region: Optional[int] = ""  # For Spanstore
    placement_policy: str
    transfer_policy: str

    fixed_base_region: Optional[bool] = False  # for Teven and SkyStore

    window_size: Optional[int] = None  # only for T_evict
    cache_size: Optional[int] = None  # in bytes, for static cache eviction
    cache_ttl: Optional[int] = None  # in hours, for fixed TTL cache eviction
