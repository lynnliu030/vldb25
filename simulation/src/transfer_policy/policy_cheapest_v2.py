from datetime import timedelta
import networkx as nx
from src.model.config import Config
from src.model.request import Request
from src.transfer_policy.policy import TransferPolicy
from src.placement_policy.policy import PlacementPolicy
import networkx as nx
from typing import Dict, Tuple

from src.model.object import LogicalObject, Status
from src.model.region_mgmt import RegionManagerV2


class CheapestTransferV2(TransferPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        object_dict: Dict[str, LogicalObject],
        region_manager: RegionManagerV2,
        placement_policy: PlacementPolicy = None,
    ):
        self.config = config
        self.total_graph = total_graph
        self.logical_objects = object_dict
        self.region_manager = region_manager
        self.placement_policy = placement_policy
        super().__init__(config, total_graph, object_dict)

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        # Select the cheapest region (network cost) of replicas to read from
        dst = req.issue_region

        if req.obj_key in self.logical_objects:
            new_phys_objs = {}
            phys_objs = self.logical_objects[req.obj_key].physical_objects
            src = None
            toRemove = None
            toRemEvictTime = None
            inactive = 0

            for region in phys_objs:
                obj = phys_objs[region]

                if req.timestamp < obj.storage_start_time:
                    new_phys_objs[region] = obj
                    inactive += 1
                else:
                    # make sure not expired and atleast one element
                    if obj.get_ttl() == float("inf") or not obj.is_expired(
                        req.timestamp
                    ):
                        new_phys_objs[region] = obj

                        if src is None:
                            src = obj.location_tag
                        else:
                            src = min(
                                src,
                                obj.location_tag,
                                key=lambda x: (
                                    self.total_graph[x][dst]["cost"],
                                    self.total_graph[x][dst]["latency"],
                                ),
                            )
                    else:
                        evict_time = obj.storage_start_time + timedelta(
                            seconds=obj.get_ttl()
                        )
                        if toRemove is None or evict_time > toRemEvictTime:
                            toRemove = obj
                            toRemEvictTime = evict_time
                        self.region_manager.remove_object_from_region(
                            obj.location_tag, obj, evict_time
                        )
                        if self.config.placement_policy == "dynamicttl":
                            self.placement_policy.update_global_ttl(
                                obj.get_ttl(), obj.key, region, dst
                            )

            if len(new_phys_objs) - inactive <= 0:
                new_phys_objs[toRemove.location_tag] = toRemove
                toRemove.set_storage_start_time(toRemEvictTime)
                self.region_manager.add_object_to_region(
                    toRemove.location_tag, toRemove
                )
                toRemove.expire_immediate = True
                toRemove.set_ttl(-1)
                src = toRemove.location_tag

            self.logical_objects[req.obj_key].physical_objects = new_phys_objs
            assert src is not None

        else:
            assert self.logical_objects[req.obj_key].base_region is not None
            src = self.logical_objects[req.obj_key].base_region

        assert req.obj_key in self.logical_objects
        assert (
            self.logical_objects[req.obj_key].physical_objects[src].status
            == Status.ready
        )

        G = nx.DiGraph()
        G.add_edge(
            src,
            dst,
            obj_key=req.obj_key,
            size=req.size,
            num_partitions=1,
            partitions=[0],
            throughput=self.total_graph[src][dst]["throughput"],
            cost=self.total_graph[src][dst]["cost"],
            latency=self.total_graph[src][dst]["latency"],
        )
        G.nodes[src]["src"] = True
        G.nodes[dst]["dst"] = True

        return src, G

    def write_transfer_path(self, req: Request, dst: str) -> nx.DiGraph:
        src = req.issue_region
        G = nx.DiGraph()
        G.add_edge(
            src,
            dst,
            obj_key=req.obj_key,
            size=req.size,
            num_partitions=1,
            partitions=[0],
            throughput=self.total_graph[src][dst]["throughput"],
            cost=self.total_graph[src][dst]["cost"],
            latency=self.total_graph[src][dst]["latency"],
        )
        G.nodes[src]["src"] = True
        G.nodes[dst]["dst"] = True
        return G
