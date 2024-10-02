from typing import List
from src.model.region_mgmt import RegionManager
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request


class LRU(PlacementPolicy):
    """
    Write local, pull on read, and evict LRU if exceeds the cost threshold
    """

    def __init__(self, region_manager: RegionManager, config: Config) -> None:
        self.region_manager = region_manager
        self.cache_size = config.cache_size

        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        place_region = req.issue_region

        if req.op == "read" and not self.region_manager.has_object_in_region(
            place_region, req.obj_key
        ):
            if (
                req.size + self.region_manager.get_region_object_size(place_region)
                > self.cache_size
            ):
                # Evict until the size is less than the cache size
                self.region_manager.evict_lru(
                    place_region, req.size, self.cache_size, req.timestamp
                )

        return [req.issue_region]
