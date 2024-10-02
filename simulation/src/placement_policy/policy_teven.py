import datetime
from typing import List, Dict
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from src.model.object import LogicalObject
from src.model.request import Request
import networkx as nx
from src.utils.definitions import GB

from src.placement_policy.policy import PlacementPolicy


class Teven(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.config = config
        self.total_graph = total_graph
        self.objects = objects
        self.region_manager = regionManager
        self.remove_immediately = {}  # obj_id -> region
        self.workload = None
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        key, issue_region, op = req.obj_key, req.issue_region, req.op

        place_regions = []

        if op == "write":
            place_regions.append(issue_region)
        else:
            # read case
            if issue_region not in self.objects[key].physical_objects:
                place_regions.append(issue_region)

        return list(set(place_regions))
