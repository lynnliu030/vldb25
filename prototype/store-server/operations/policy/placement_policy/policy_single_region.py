from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class SingleRegionWrite(PlacementPolicy):
    """
    Write to the same region as the original storage region defined in the config
    """

    def __init__(self, init_regions: List[str]) -> None:
        super().__init__(init_regions)
        pass

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: single region to write to
        """
        single_store_region = "aws:us-west-1"

        assert single_store_region in self.init_regions
        return [single_store_region]

    def name(self) -> str:
        return "single_region"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return -1  # -1 means store forever
