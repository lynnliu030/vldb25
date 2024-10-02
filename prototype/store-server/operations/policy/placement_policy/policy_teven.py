from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class Teven(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, init_regions: List[str] = ...) -> None:
        super().__init__(init_regions)
        self.ttl = 12 * 60 * 60  

    def place(self, req: StartUploadRequest) -> List[str]:
        return [req.client_from_region]

    def name(self) -> str:
        return "t_even"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        if fixed_base_region is True:
            return -1

        if not self.stat_graph.has_edge(src, dst) or not self.stat_graph.has_node(dst):
            return self.ttl

        network_cost = self.stat_graph[src][dst]["cost"]
        storage_cost = self.stat_graph.nodes[dst]["priceStorage"]
        if network_cost == 0:
            network_cost = 0.056
        return network_cost / storage_cost * 60 * 60
