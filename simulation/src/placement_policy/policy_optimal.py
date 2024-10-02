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


class Optimal(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.curReq = {}
        self.requests = {}
        self.total_graph = total_graph
        self.config = config
        self.logical_objects = objects
        self.region_manager = regionManager
        self.remove_immediately = {}
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op
        place_regions = []
        if op == "write":
            is_base_region = issue_region == self.logical_objects[key].base_region
            no_copies = len(self.logical_objects[key].physical_objects.keys()) == 0
            has_reads = (
                issue_region in self.curReq
                and key in self.curReq[issue_region]
                and self.curReq[issue_region][key]
                <= len(self.requests[issue_region][key]) - 1
            )
            if is_base_region or no_copies or has_reads:
                place_regions.append(issue_region)

        else:
            if issue_region not in self.logical_objects[key].physical_objects:
                if self.worth_storing(issue_region, key):
                    place_regions.append(issue_region)

            self.curReq[issue_region][key] += 1

        return list(set(place_regions))

    def worth_storing(self, issue_region, key):
        region_list = [
            obj.location_tag
            for obj in self.logical_objects[key].physical_objects.values()
            if obj.location_tag != issue_region and obj.status == Status.ready
        ]

        if len(region_list) == 0:
            # check that this object is located in issue_region
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
        )  # in seconds
        if (
            issue_region in self.curReq
            and key in self.curReq[issue_region]
            and self.curReq[issue_region][key]
            < len(self.requests[issue_region][key]) - 1
            and (
                self.requests[issue_region][key][self.curReq[issue_region][key] + 1]
                - self.requests[issue_region][key][self.curReq[issue_region][key]]
            )
            <= teven
        ):
            return True
        return False
