from typing import List
from src.model.region_mgmt import RegionManager
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request


class AlwaysEvict(PlacementPolicy):
    """
    Write local, and do not pull on read if data is not available locally
    """

    def __init__(self, region_manager: RegionManager) -> None:
        self.region_manager = region_manager
        self.remove_immediately = {}  # obj_id -> region
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        # Write local, and do not pull on read if data is not available locally
        if req.op == "write":
            return [req.issue_region]
        else:
            return []
