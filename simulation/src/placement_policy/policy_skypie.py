import csv
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
from src.model.object import LogicalObject
from typing import Dict, Tuple, List
import networkx as nx

from skypie.api import create_oracle, OracleType
from collections import defaultdict
from src.utils.helpers import refine_string, convert_hyphen_to_colon
from src.model.region_mgmt import RegionManager
from src.model.object import LogicalObject, Status


class SkyPIE(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        region_mgmt: RegionManager,
        trace_path: str,
        verbose: int = -1,
    ) -> None:
        self.config = config
        self.total_graph = total_graph
        self.objects = objects
        self.object_sizes: Dict[str, float] = {}

        self.access_sets: Dict[str, Tuple] = {}  # access sets for each object
        self.access_sets_objects: Dict[
            Tuple, List[str]
        ] = {}  # for each access sets, what are the objects
        self._update_access_sets(trace_path)

        oracle_directory = config.oracle_directory
        assert oracle_directory is not None, "oracle_directory field is not set"
        self.oracle = create_oracle(
            oracle_directory=oracle_directory,
            oracle_type=OracleType.SKYPIE,
            verbose=verbose,
        )

        self.region_mgmt = region_mgmt
        self.workload = None
        self.placement_decisions: Dict[str, List[str]] = {}
        self.get_decisions: Dict[str, Dict[str, str]] = {}
        self.past_get_decisions: Dict[str, Dict[str, str]] = {}
        super().__init__()

    def _update_access_sets(self, trace_path: str):
        """
        Calculate access sets given a particular trace

        Args:
            trace_path (str): path to the trace file
        """
        region_to_objects = {}
        versions = {}

        with open(trace_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                region = row["issue_region"]
                obj_key = row["obj_key"]
                size = float(row["size"])
                op = None
                if row["op"] == "GET" or row["op"] == "REST.GET.OBJECT":
                    op = "read"
                elif row["op"] == "PUT" or row["op"] == "REST.PUT.OBJECT":
                    op = "write"
                else:
                    continue

                if op == "write":
                    if obj_key not in versions:
                        versions[obj_key] = 0
                    else:
                        versions[obj_key] += 1

                obj_key = obj_key + "-v" + str(versions[obj_key])

                self.object_sizes[obj_key] = size
                if region not in region_to_objects:
                    region_to_objects[region] = set()
                region_to_objects[region].add(obj_key)

        access_sets = {}
        for region, objects in region_to_objects.items():
            for obj in objects:
                # Update the access_sets
                if obj not in access_sets:
                    access_sets[obj] = set()
                access_sets[obj].add(region)

        # refactor access_sets so that the value is tuple as well
        self.access_sets = {k: tuple(sorted(v)) for k, v in access_sets.items()}

        # Invert keys and values of access_sets
        self.access_sets_objects = {}
        for obj_key, regions in self.access_sets.items():
            if regions not in self.access_sets_objects:
                self.access_sets_objects[regions] = []
            self.access_sets_objects[regions].append(obj_key)

    def aggregate_per_object(
        self,
        *,
        requests: List[Request],
    ) -> Tuple[int, Dict[str, int], Dict[str, int], Dict[str, float], Dict[str, float]]:
        """
        Aggregate requests to the same object and return inputs to SkyPIE (i.e. size, put, get, ingress, egress per object)

        Args:
            requests (List[Request]): list of requests to aggregate

        Returns:
            Dict[int, Tuple[int, Dict[str, int], Dict[str, int], Dict[str, float], Dict[str, float]]]: obj_key: (size, put, get, ingress, egress)
            size: size of the object
            put: number of put request for each region to the object
            get: number of get request for each region to the object
            ingress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
            egress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
        """

        # Initialize result
        result = defaultdict(
            lambda: (
                0,
                defaultdict(int),
                defaultdict(int),
                defaultdict(float),
                defaultdict(float),
            )
        )

        for request in requests:
            obj_key = request.obj_key
            issue_region = convert_hyphen_to_colon(request.issue_region)
            size, put, get, ingress, egress = result[obj_key]

            size = request.size
            if request.op == "write":
                put[issue_region] += 1
                ingress[issue_region] += request.size
            elif request.op == "read":
                get[issue_region] += 1
                egress[issue_region] += request.size

            result[obj_key] = (size, put, get, ingress, egress)

        return result

    def generate_decisions(self, requests: List[Request], batch_size=1024):
        """
        Generates placement decisions for each object using the SkyPIE oracle.
        Uses batching for faster optimization.
        """
        # Reset the decisions periodically
        self.placement_decisions = {}
        if len(self.get_decisions) > 0:
            self.past_get_decisions = self.get_decisions

        self.get_decisions = {}
        # Iterate over each access set
        # print(f"Requests passing in: {requests}")

        workloads_per_object = self.aggregate_per_object(requests=requests)

        # Compute the optimal placement for a batch of objects
        # Querying the oracle in batches is faster, but
        # doing all objects at once requires too much memory on the CPU and GPU!
        def batch(iterable, n):
            iterable = iter(iterable)
            while True:
                chunk = []
                for _i in range(n):
                    try:
                        chunk.append(next(iterable))
                    except StopIteration:
                        yield chunk
                        return
                yield chunk

        for workload_batch in batch(workloads_per_object.items(), batch_size):
            # Convert to Workload instances per object
            workloads = [
                self.oracle.create_workload_by_region_name(
                    size=size, put=put, get=get, ingress=ingress, egress=egress
                )
                for _obj_key, (size, put, get, ingress, egress) in workload_batch
            ]

            # Query all workloads at once
            decisions = self.oracle.query(w=workloads, translateOptSchemes=True)

            # Update the placement decisions of each object
            for (obj_key, _), (_cost, decision) in zip(workload_batch, decisions):
                place_regions = decision.replication_scheme.object_stores
                app_assignments = decision.replication_scheme.app_assignments

                # NOTE: why would v be a set?
                place_regions = list(set(refine_string(r) for r in place_regions))
                app_assignments = {
                    refine_string(a.app): refine_string(a.object_store)
                    for a in app_assignments
                }

                self.placement_decisions[obj_key] = place_regions
                self.get_decisions[obj_key] = app_assignments

    def place(self, key: str) -> List[str]:
        # print(f"Access set for {key}: {access_set}")
        # Get the placement decisions for this object
        place_regions = self.placement_decisions[key]
        # print(f"Placement decisions for {key}: {place_regions}")
        return place_regions

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        dst = req.issue_region
        assert req.obj_key in self.objects

        new_policy_decision = self.get_decisions[req.obj_key][req.issue_region]
        # print(f"New read region: {new_policy_decision}, object key: {req.obj_key}")
        if self.region_mgmt.has_object_in_region(new_policy_decision, req.obj_key):
            # print(f"New policy decision, object has been transferred")
            src = new_policy_decision
        else:
            # Get from cheapest region where it exists
            region_list = [
                obj.location_tag
                for obj in self.objects[req.obj_key].physical_objects.values()
                if obj.status == Status.ready
            ]
            src = min(
                region_list,
                key=lambda x: (
                    self.total_graph[x][dst]["cost"],
                    self.total_graph[x][dst]["latency"],
                ),
            )

        if self.region_mgmt.has_object_in_region(req.issue_region, req.obj_key):
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

        # set the src attribute to src, and dst attribute to dst
        G.nodes[src]["src"] = True
        G.nodes[dst]["dst"] = True

        return src, G

    def write_transfer_path(self, req: Request, dst: str) -> nx.DiGraph:
        G = nx.DiGraph()
        src = req.issue_region

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

        # set the src attribute to src, and dst attribute to dst
        G.nodes[src]["src"] = True
        G.nodes[dst]["dst"] = True

        return G
