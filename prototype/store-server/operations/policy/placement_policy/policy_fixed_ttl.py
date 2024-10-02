from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class FixedTTL(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, init_regions: List[str] = ...) -> None:
        super().__init__(init_regions)
        self.ttl = 12 * 60 * 60

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """

        return [req.client_from_region]

    def name(self) -> str:
        return "fixed_ttl"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        if fixed_base_region is True:
            return -1
        return self.ttl
