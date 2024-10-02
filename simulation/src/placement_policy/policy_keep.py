from typing import List, Dict, Tuple
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from typing import Dict
from src.model.object import LogicalObject
import networkx as nx
from src.model.request import Request
from collections import deque
from src.utils.helpers import get_avg_network_cost
from datetime import datetime
from src.utils.definitions import GB
from src.model.object import Status


class IndividualTTL(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.requests = {}
        self.total_graph = total_graph
        self.config = config
        self.logical_objects = objects
        self.region_manager = regionManager
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)
        self.factor = 1

        self.window_size = self.config.window_size  
        self.request_times: Dict[Tuple[str, str], deque] = {}
        super().__init__()

    def add_request_to_queue(self, region: str, obj_key: str, timestamp: datetime):
        if (region, obj_key) not in self.request_times:
            self.request_times[(region, obj_key)] = deque()

        self.request_times[(region, obj_key)].append(timestamp)
        request_queue = self.request_times[(region, obj_key)]

        # remove the oldest request if the window size is exceeded (window size is time)
        while (
            request_queue
            and (timestamp - request_queue[0]).total_seconds() > self.window_size
        ):
            request_queue.popleft()

    def estimate_arrival_recency(self, region: str, obj_key: str) -> float:
        # If the object has not been requested before, store it
        if (region, obj_key) not in self.request_times:
            return 0

        request_queue = self.request_times[(region, obj_key)]
        if not request_queue:
            return 0

        return sum(
            (request_queue[i] - request_queue[i - 1]).total_seconds()
            for i in range(1, len(request_queue))
        ) / len(request_queue)

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op
        place_regions = []

        if op == "write":
            place_regions.append(issue_region)
            self.add_request_to_queue(issue_region, key, req.timestamp)
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
                / (self.total_graph.nodes[issue_region]["priceStorage"] * 3)
                * 60
                * 60
                * 24
            )  # in seconds

            self.add_request_to_queue(issue_region, key, req.timestamp)
            estimated_recency = self.estimate_arrival_recency(issue_region, key)

            if op == "read":
                if (
                    issue_region not in self.logical_objects[key].physical_objects
                    and estimated_recency <= teven
                ):
                    place_regions.append(issue_region)

        return list(set(place_regions))
