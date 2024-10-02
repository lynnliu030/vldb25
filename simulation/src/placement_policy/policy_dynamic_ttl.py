import datetime
import math
from typing import List, Dict
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from src.model.request import Request
from src.model.object import LogicalObject
import networkx as nx
from src.utils.definitions import GB
from datetime import timedelta
from src.utils.helpers import get_min_network_cost, get_avg_network_cost
from src.placement_policy.policy import PlacementPolicy


"""
    Schedule evict for next put
    Read to object that should be gone
"""


class DynamicTTL(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.config = config

        self.total_graph = total_graph
        self.logical_objects = objects
        self.region_manager = regionManager

        self.minNetworkCost = get_min_network_cost(self.total_graph)
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)

        self.object_hits = {}
        self.global_ttls = {}

        self.remove_immediately = {}
        self.epsilon = 0.01

        super().__init__()

    def update_global_ttl(self, duration: float, obj_key: str, src: str, dst: str):
        physical_object = self.logical_objects[obj_key].physical_objects.get(dst, None)
        num_hits = self.object_hits.get((dst, obj_key), 0)  #
        net_cost = self.total_graph[src][dst]["cost"]
        if net_cost == 0:
            net_cost = self.avgNetworkCost
        storage_cost_per_hour = self.total_graph.nodes[dst]["priceStorage"] / 24 * 3
        duration_in_hour = duration / 3600

        if duration_in_hour != 0:
            delta_ttl = (
                self.epsilon
                * (
                    -duration_in_hour * storage_cost_per_hour
                    + num_hits / duration_in_hour * net_cost
                )
                * 3600
            )
            self.global_ttls[dst] = max(self.global_ttls.get(dst, 0) + delta_ttl, 0)

        self.object_hits.pop((src, obj_key), None)

    def object_hit(self, obj_key, region):
        self.object_hits[(region, obj_key)] = (
            self.object_hits.get((region, obj_key), 0) + 1
        )

    def get_ttl(self, region, teven):
        if region not in self.global_ttls:
            self.global_ttls[region] = teven / 3600  # teven
        return self.global_ttls[region]

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op

        place_regions = []

        if op == "write":
            place_regions.append(issue_region)
        else:
            if issue_region not in self.logical_objects[key].physical_objects:
                place_regions.append(issue_region)

        return list(set(place_regions))
