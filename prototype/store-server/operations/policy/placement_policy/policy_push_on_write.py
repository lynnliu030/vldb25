from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class PushonWrite(PlacementPolicy):
    """
    Write local and push asynchronously to a set of pushed regions
    """

    def __init__(self, init_regions: List[str]) -> None:
        super().__init__(init_regions)

    def place(
        self,
        req: StartUploadRequest,
    ) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the regions to push to, including the primary region and the regions we want to push to
        """
        push_regions = ["aws:us-west-1", "aws:us-east-1"]
        assert all(r in self.init_regions for r in push_regions)

        return list(set([req.client_from_region] + push_regions))

    def name(self) -> str:
        return "push"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return -1  # -1 means store forever
