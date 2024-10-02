from typing import List, Dict, Tuple
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from typing import Dict
from src.model.object import LogicalObject
import networkx as nx
from src.model.request import Request
from collections import deque
import datetime
from src.utils.definitions import GB
from src.utils.helpers import get_avg_network_cost
from src.model.object import Status


class EWMA(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.num_requests = 0
        self.requests = {}
        self.total_graph = total_graph
        self.config = config
        self.logical_objects = objects
        self.region_manager = regionManager
        self.initial_t_prev = 0
        self.alpha = 0.5
        self.factor = 30

        self.tnext_vars = {}
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)
        self.remove_immediately = {}
        super().__init__()

    def add_request_to_queue(self, region: str, obj_key: str, timestamp: datetime):
        if (region, obj_key) not in self.tnext_vars:
            self.tnext_vars[(region, obj_key)] = [(None, self.initial_t_prev), None]
        Tcur = self.tnext_vars[(region, obj_key)][0][0]
        if Tcur is None:
            Tcur = 0
        Tprev = self.tnext_vars[(region, obj_key)][0][1]
        last_req = self.tnext_vars[(region, obj_key)][1]
        if last_req is None:
            last_req = datetime.datetime.fromtimestamp(0)
        self.tnext_vars[(region, obj_key)] = [
            (
                (timestamp - last_req).total_seconds(),
                self.alpha * Tcur + (1 - self.alpha) * Tprev,
            ),
            timestamp,
        ]

    def estimate_arrival_recency(self, region: str, obj_key: str) -> float:
        if (region, obj_key) not in self.tnext_vars:
            return self.initial_t_prev
        Tcur = self.tnext_vars[(region, obj_key)][0][0]
        Tprev = self.tnext_vars[(region, obj_key)][0][1]
        return self.alpha * Tcur + (1 - self.alpha) * Tprev

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op
        place_regions = []

        if op == "write":
            place_regions.append(issue_region)
        else:
            region_list = [
                obj.location_tag
                for obj in self.logical_objects[key].physical_objects.values()
                if obj.location_tag != issue_region and obj.status == Status.ready
            ]

            if len(region_list) == 0:
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
            self.add_request_to_queue(issue_region, key, req.timestamp)
            estimated_recency = self.estimate_arrival_recency(issue_region, key)

            # Whenever you have one operation, you need to compare
            # the estimated recency of the object in the issue region with the teven value
            # If <, then store
            # If >, then evict
            if (
                issue_region not in self.logical_objects[key].physical_objects
                and estimated_recency * self.factor <= teven
            ):
                place_regions.append(issue_region)

        return place_regions
