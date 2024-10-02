from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.transfer_policy.base import DataTransferGraph
from operations.policy.placement_policy.base import PlacementPolicy


class ReplicateAll(PlacementPolicy):
    """
    Replicate all objects to all regions
    """

    def __init__(self, init_regions: List[str]) -> None:
        super().__init__(init_regions)
        self.stat_graph = DataTransferGraph.get_instance()
        pass

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: all available regions in the current nodes graph
        """
        return self.init_regions

    def name(self) -> str:
        return "replicate_all"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return -1  # -1 means store forever
