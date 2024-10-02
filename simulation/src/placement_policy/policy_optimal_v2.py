from typing import List
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from typing import Dict
from src.model.object import LogicalObject
from src.utils.helpers import get_avg_network_cost
import networkx as nx
from src.model.request import Request
from src.model.object import Status
from datetime import datetime


class OptimalV2(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.total_graph = total_graph
        self.config = config
        self.logical_objects = objects
        self.region_manager = regionManager
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)

        self.not_worth = 0
        self.worth = 0
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op
        place_regions = []
        if op == "write":
            is_base_region = issue_region == self.logical_objects[key].base_region
            no_copies = len(self.logical_objects[key].physical_objects.keys()) == 0
            has_reads = req.next_access_same_reg_timestamp != datetime.max
            if is_base_region or no_copies or has_reads:
                place_regions.append(issue_region)

        else:
            if issue_region not in self.logical_objects[key].physical_objects:
                if self.worth_storing(issue_region, key, req):
                    place_regions.append(issue_region)
        return list(set(place_regions))

    def worth_storing(self, issue_region, key, request: Request):
        region_list = [
            obj.location_tag
            for obj in self.logical_objects[key].physical_objects.values()
            if obj.location_tag != issue_region and obj.ready(request.timestamp)
        ]

        if len(region_list) == 0:
            assert (
                len(
                    [
                        obj.location_tag
                        for obj in self.logical_objects[key].physical_objects.values()
                        if obj.location_tag == issue_region
                    ]
                )
                == 1
            )
            net_cost = self.avgNetworkCost
        else:
            src = min(
                region_list,
                key=lambda x: (
                    self.total_graph[x][issue_region]["cost"],
                    self.total_graph[x][issue_region]["latency"],
                ),
            )
            net_cost = self.total_graph[src][issue_region]["cost"]

        teven = (
            net_cost
            / self.total_graph.nodes[issue_region]["priceStorage"]
            * 60
            * 60
            * 24
            / 3
        )

        if (
            request.next_access_same_reg_timestamp - request.timestamp
        ).total_seconds() <= teven:
            self.worth += 1
            return True

        self.not_worth += 1
        return False
