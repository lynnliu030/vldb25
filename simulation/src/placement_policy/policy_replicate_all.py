from typing import List
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
import networkx as nx
import random


class ReplicateAll(PlacementPolicy):
    """
    Replicate all objects to all regions
    """

    def __init__(self, total_graph: nx.DiGraph, config: Config) -> None:
        self.total_graph = total_graph
        self.regions = config.regions
        super().__init__()
        random.seed(0)

    def place(self, req: Request, config: Config) -> List[str]:
        if req.op == "write":
            return self.regions
        else:
            return []
