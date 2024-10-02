import csv
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
from src.model.object import LogicalObject
from typing import Dict, Tuple, List, Set
import networkx as nx

from skypie.api import create_oracle, OracleType
from sky_pie_baselines import spanstore_aggregate as spanstore_aggregate_rust
from collections import defaultdict
from src.utils.helpers import refine_string, convert_hyphen_to_colon
from src.model.region_mgmt import RegionManager
from src.model.object import LogicalObject, Status


class SPANStore(PlacementPolicy):
    def __init__(
        self,
        policy: str,
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

        if policy == "spanstore":
            self.oracle = create_oracle(
                oracle_directory=oracle_directory,
                oracle_type=OracleType.ILP,
                oracle_impl_args={
                    "noStrictReplication": config.no_strict_replication,
                    "minReplicationFactor": config.replication_factor_min,
                    "region_selector": config.region_selector,
                    "object_store_selector": config.object_store_selector,
                },
                verbose=verbose,
                verify_experiment_args=False,
            )
        else:
            raise NotImplementedError("Policy {} not supported yet".format(policy))

        self.region_mgmt = region_mgmt
        self.policy = policy
        self.workload = None
        self.placement_decisions: Dict[str, List[str]] = {}
        self.get_decisions: Dict[str, str] = {}
        self.past_get_decisions: Dict[str, str] = {}
        self.remove_immediately = {}  # obj_id -> region
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

    def spanstore_aggregate(
        self,
        requests: List[Request],
        access_set: Set[str],
    ) -> Tuple[int, Dict[str, int], Dict[str, int], Dict[str, float], Dict[str, float]]:
        """
        Aggregate requests and return inputs to SPANStore (i.e. size, put, get, ingress, egress)

        Args:
            requests (List[Request]): list of requests to aggregate

        Returns:
            size: total amount of data of all objects in this access set
            put: number of put request for each region in access set to the objects in that access sets
            get: number of get request for each region in access set to the objects in that access sets
            ingress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
            egress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
        """

        objects_in_access_set = self.access_sets_objects[tuple(sorted(access_set))]
        try:
            (
                put_counts,
                get_counts,
                ingress_counts,
                egress_counts,
            ) = spanstore_aggregate_rust(requests, objects_in_access_set)
        except Exception as e:
            print(
                f"Got error eggregating with Rust: {e}\nFalling back to Python implementation"
            )

            put_counts = defaultdict(int)
            get_counts = defaultdict(int)
            ingress_counts = defaultdict(float)
            egress_counts = defaultdict(float)

            for i in range(len(requests)):
                request = requests[i]
                if request.obj_key in objects_in_access_set:
                    if request.op == "write":
                        put_counts[request.issue_region] += 1
                        ingress_counts[request.issue_region] += request.size
                    elif request.op == "read":
                        get_counts[request.issue_region] += 1
                        egress_counts[request.issue_region] += request.size

        size = sum([self.object_sizes[obj_key] for obj_key in objects_in_access_set])

        for region in access_set:
            if region not in put_counts:
                put_counts[region] = 0
            if region not in get_counts:
                get_counts[region] = 0
            if region not in ingress_counts:
                ingress_counts[region] = 0
            if region not in egress_counts:
                egress_counts[region] = 0

        # replace the key with first '-' with ':'
        put_counts = {convert_hyphen_to_colon(k): v for k, v in put_counts.items()}
        get_counts = {convert_hyphen_to_colon(k): v for k, v in get_counts.items()}
        ingress_counts = {
            convert_hyphen_to_colon(k): v for k, v in ingress_counts.items()
        }
        egress_counts = {
            convert_hyphen_to_colon(k): v for k, v in egress_counts.items()
        }
        return (
            size,
            dict(put_counts),
            dict(get_counts),
            dict(ingress_counts),
            dict(egress_counts),
        )

    def generate_decisions(self, requests: List[Request]):
        # Reset the decisions periodically
        self.placement_decisions = {}
        if len(self.get_decisions) > 0:
            self.past_get_decisions = self.get_decisions

        self.get_decisions = {}
        costs = 0  # total cost
        for access_set in self.access_sets_objects.keys():
            size, put, get, ingress, egress = self.spanstore_aggregate(
                requests, access_set
            )
            self.workload = self.oracle.create_workload_by_region_name(
                size=size, put=put, get=get, ingress=ingress, egress=egress
            )
            decisions = self.oracle.query(w=self.workload, translateOptSchemes=True)
            cost, decision = decisions[0]
            # print(f"Decision: {decision}")
            assert len(decision.objectStores) == 1
            assert len(decision.assignments) == 1
            # NOTE: why would v be a set?
            place_regions = list(
                set(refine_string(r) for r in decision.objectStores[0])
            )
            app_assignments = {
                refine_string(k): refine_string(list(v)[0])
                for k, v in decision.assignments[0].items()
            }

            # All objects in this access sets are placed in the same regions
            self.placement_decisions[access_set] = place_regions
            self.get_decisions = app_assignments
            costs += cost

    def place(self, key: str) -> List[str]:
        # Find the access set this object belongs to
        access_set = self.access_sets[key]
        place_regions = self.placement_decisions[access_set]
        return place_regions

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        dst = req.issue_region
        assert req.obj_key in self.objects
        new_policy_decision = self.get_decisions[req.issue_region]
        if self.region_mgmt.has_object_in_region(new_policy_decision, req.obj_key):
            src = new_policy_decision
        else:
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
        G.nodes[src]["src"] = True
        G.nodes[dst]["dst"] = True

        return G
