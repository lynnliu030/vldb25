from collections import defaultdict
from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class AlwaysEvict(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, init_regions: List[str] = []) -> None:
        super().__init__(init_regions)
        self.storage_cost = defaultdict(int)
        self.hits = 0
        self.miss = 0

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """

        return [req.client_from_region]

    def add_to_cost(self, timedelta: int, region: str, size):
        self.storage_cost[region] += (
            timedelta
            / 3600
            / 24
            * self.stat_graph.nodes[region]["priceStorage"]
            * 3
            * size
            / (1024 * 1024 * 1024)
        )

    def name(self) -> str:
        return "always_evict"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return 0
