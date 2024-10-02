from pydantic import BaseModel
from datetime import datetime
from typing import Literal


class Request(BaseModel):
    timestamp: datetime
    op: Literal["read", "write", "evict"]
    issue_region: str
    obj_key: str
    size: float
    next_access_timestamp: datetime = datetime.max
    next_access_same_reg_timestamp: datetime = datetime.max
    read_from: str = None
