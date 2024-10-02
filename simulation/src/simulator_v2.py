import csv
from datetime import datetime
import os
from typing import Dict, List, Set
from src.placement_policy import (
    LocalWrite,
    SingleRegionWrite,
    AlwaysStore,
    ReplicateAll,
    Teven,
    TevictV2,
    TevictWindow,
    Tevict,
    TevictRanges,
    TevictRangesV2,
    OptimalV2,
    SPANStore,
    LRU,
    Fixed_TTL,
    AlwaysEvict,
    EWMA,
    IndividualTTL,
    DynamicTTL,
    # SkyPIE
)
from src.transfer_policy import DirectTransfer, ClosestTransfer, CheapestTransferV2
from src.utils.helpers import (
    load_config,
    get_full_path,
    make_nx_graph,
    get_min_network_cost,
    get_avg_network_cost,
    get_median_network_cost,
)
from src.utils.definitions import (
    aws_instance_throughput_limit,
    gcp_instance_throughput_limit,
    azure_instance_throughput_limit,
)
from src.model.region_mgmt import RegionManagerV2
from src.model.object import LogicalObject, PhysicalObject, Status
from src.model.tracker import Tracker
from src.model.request import Request
import networkx as nx

# import matplotlib.pyplot as plt
import logging
from prettytable import PrettyTable
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from src.utils.definitions import GB
import textwrap
from datetime import timedelta
from collections import defaultdict

logging.basicConfig(level=logging.CRITICAL, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


def versionate_trace(trace_path: str, out_path: str):
    versions = defaultdict(int)
    with open(get_full_path(trace_path), "r") as f, open(
        out_path, "w", newline=""
    ) as out_file:
        reader = csv.DictReader(f)
        writer = csv.DictWriter(out_file, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row["op"] == "PUT" or row["op"] == "REST.PUT.OBJECT":
                versions[row["obj_key"]] += 1
            new_version_id = row["obj_key"] + "-v" + str(versions[row["obj_key"]])
            row["obj_key"] = new_version_id
            writer.writerow(row)


class SimulatorV2:
    def __init__(
        self,
        config_path: str,
        trace_path: str,
        num_vms: int = 1,
        set_base_region: bool = False,
        days: int = 0,
        version_enable: bool = False,
        store_decision: bool = False,
    ):
        self.config = load_config(config_path)
        self.trace_path = trace_path
        self._print_config_details()

        self.store_decision = store_decision

        self.set_base_region = set_base_region
        self.version_enable = version_enable
        self.days_to_ignore_cost = days  # days to calculate the cost (e.g. if 4, then only calculate costs after Day 4)
        self.ignore_cost = False if days == 0 else True

        # add the fixed_base_region to the config
        if self.set_base_region:
            print("Fixed Base Region is set")
            self.config.fixed_base_region = True
        else:
            print("Fixed Base Region is not set")
            self.config.fixed_base_region = False

        self.eviction_based_policies = [
            "teven",
            "tevict",
            "tevict_ranges",
            "tevict_window",
            "optimal",
            "fixedttl",
            "to_keep",
            "ewma",
            "dynamicttl",
        ]
        self.other_eviction_policies = [
            "lru",
            "always_evict",
            "pull_on_read",
            "replicate_all",
        ]
        self.spanstore_policies = ["spanstore", "oracle"]
        self.precompute_policy = ["spanstore", "oracle", "optimal", "ewma"]
        self.total_graph = make_nx_graph()

        self.minNetworkCost = get_min_network_cost(self.total_graph)
        self.avgNetworkCost = get_avg_network_cost(self.total_graph)
        self.medianNetworkCost = get_median_network_cost(self.total_graph)

        self.num_vms = num_vms
        self.tracker = Tracker()
        self.tracker.set_day_to_ignore(self.days_to_ignore_cost)

        self.logical_objects: Dict[str, LogicalObject] = {}  # key to logical object
        self.region_manager = RegionManagerV2(
            self.total_graph, logger
        )  # region to physical object

        self.placement_policy = self._select_placement_policy(
            self.config.placement_policy
        )
        if self.config.placement_policy not in ["oracle", "spanstore"]:
            self.transfer_policy = self._select_transfer_policy(
                self.config.transfer_policy, self.placement_policy
            )
        else:
            # Oracle, SPANStore: decide both placement and transfer
            self.transfer_policy = None

        self.events = []
        self.tevens = []
        self.runtime_throughputs = []
        self.hits = 0
        self.misses = 0
        self.hits_size = 0
        self.misses_size = 0

        # SPANStore: request aggregation
        self.all_requests: List[Request] = []
        self.moving_idx = -1
        self.request_buffer: List[Request] = []
        self.last_processed_time = None
        self.placement_decision_generated_time = None
        self.update_time_interval = timedelta(hours=1)
        self.place_decisions: Dict[str, List[str]] = {}
        self.is_updating_placement = False

        self.evict = 0
        self.ttl_log = []

        self.good_choices = 0
        self.total_choices = 1

    def initiate_data_transfer(
        self,
        current_timestamp: datetime,
        request: Request,
        transfer_graphs: List[nx.DiGraph],
        place_regions: List[str],
        ttl: bool,
        op: str,
        cache_regions: Set[str],
    ):
        if (
            self.config.placement_policy == "tevict_ranges"
            or self.config.placement_policy == "tevict"
            or self.config.placement_policy == "tevict_window"
        ) and op == "read":
            self.placement_policy.update_past_requests(request, request.issue_region)

        transfer_times = []
        for i in range(len(transfer_graphs)):
            transfer_graph = transfer_graphs[i]
            src = [
                node
                for node in transfer_graph.nodes
                if transfer_graph.nodes[node].get("src", False)
            ]
            dsts = [
                node
                for node in transfer_graph.nodes
                if transfer_graph.nodes[node].get("dst", False)
            ]

            if op == "read":
                request.read_from = src

            assert (
                len(src) == 1
            ), f"Graphs look like: {transfer_graph.nodes.data()}, src: {src}, dsts: {dsts}"
            transfer_time_millis = max(
                [transfer_graph[src[0]][dst]["latency"] for dst in dsts]
            )
            logger.debug(f"transfer time (ms): {transfer_time_millis}")

            schedule_place_region = place_regions[i] if len(place_regions) > 0 else []
            src = list(transfer_graph.edges)[0][0]

            key = request.obj_key
            physical_objects = self.logical_objects[key].physical_objects
            logical_object = self.logical_objects[request.obj_key]
            physical_object_region = src

            refresh_ttl = (
                ttl
                and src in physical_objects
                and src != logical_object.base_region
                and src == request.issue_region
                and op == "read"
            )  # Only refresh on reads form the same region
            # Refresh TTL for the object in the cache
            if refresh_ttl:
                physical_object = physical_objects[src]
                physical_object.expire_immediate = False

                if self.config.placement_policy == "dynamicttl":
                    self.placement_policy.object_hit(key, request.issue_region)

                # Decide store or evict based on the policy
                if self.config.placement_policy == "optimal":
                    region_list = [
                        obj.location_tag
                        for obj in self.logical_objects[key].physical_objects.values()
                        if obj.location_tag != request.issue_region
                        and obj.ready(request.timestamp)
                    ]
                    if len(region_list) == 0:
                        # check that this object is located in issue_region
                        assert (
                            len(
                                [
                                    obj.location_tag
                                    for obj in self.logical_objects[
                                        key
                                    ].physical_objects.values()
                                    if obj.location_tag == request.issue_region
                                ]
                            )
                            == 1
                        )
                        net_cost = self.medianNetworkCost
                    else:
                        src_2 = min(
                            region_list,
                            key=lambda x: (
                                self.total_graph[x][request.issue_region]["cost"],
                                self.total_graph[x][request.issue_region]["latency"],
                            ),
                        )
                        net_cost = self.total_graph[src_2][request.issue_region]["cost"]

                    # calculate `teven`
                    teven = (
                        net_cost
                        / self.total_graph.nodes[request.issue_region]["priceStorage"]
                        * 60
                        * 60
                        * 24
                        / 3
                    )

                    # if object is the base region or
                    # (the request is not the last request and `tnext` (cur `timestamp` - prev `timestamp`) < `teven`), then keep it in cache
                    if (
                        request.next_access_same_reg_timestamp - request.timestamp
                    ).total_seconds() >= teven:
                        logger.debug("remove from cache")
                        if self.config.fixed_base_region:
                            # fixed base region case - always evict
                            self.region_manager.remove_object_from_region(
                                physical_object_region,
                                physical_object,
                                request.timestamp,
                            )
                            self.logical_objects[
                                physical_object.key
                            ].physical_objects.pop(physical_object_region, None)

                        else:
                            # no base region - only remove if there are more than one object
                            if (
                                len(self.logical_objects[key].physical_objects.keys())
                                > 1
                            ):
                                self.region_manager.remove_object_from_region(
                                    physical_object_region,
                                    physical_object,
                                    request.timestamp,
                                )
                                self.logical_objects[
                                    physical_object.key
                                ].physical_objects.pop(physical_object_region, None)
                            else:
                                physical_object.expire_immediate = True
                                physical_object.set_ttl(-1)

                else:
                    # everything else but optimal
                    if (
                        self.config.placement_policy == "to_keep"
                        or self.config.placement_policy == "ewma"
                    ):
                        estimated_recency = (
                            self.placement_policy.estimate_arrival_recency(src, key)
                        )
                        region_list = [
                            obj.location_tag
                            for obj in self.logical_objects[
                                key
                            ].physical_objects.values()
                            if obj.location_tag != request.issue_region
                            and obj.ready(request.timestamp)
                        ]
                        if len(region_list) == 0:
                            net_cost = self.medianNetworkCost
                        else:
                            src_2 = min(
                                region_list,
                                key=lambda x: (
                                    self.total_graph[x][request.issue_region]["cost"],
                                    self.total_graph[x][request.issue_region][
                                        "latency"
                                    ],
                                ),
                            )
                            net_cost = self.total_graph[src_2][request.issue_region][
                                "cost"
                            ]

                        teven = (
                            net_cost
                            / self.total_graph.nodes[request.issue_region][
                                "priceStorage"
                            ]
                            * 60
                            * 60
                            * 24
                            / 3
                        )
                        if estimated_recency * self.placement_policy.factor <= teven:
                            # refresh case
                            new_ttl = round(
                                min(
                                    estimated_recency * self.placement_policy.factor,
                                    teven,
                                )
                            )

                            physical_object.set_ttl(
                                new_ttl
                                + (
                                    request.timestamp
                                    - physical_object.get_storage_start_time()
                                ).total_seconds()
                            )

                            if (
                                new_ttl
                                >= (
                                    request.next_access_same_reg_timestamp
                                    - request.timestamp
                                ).total_seconds()
                            ):
                                self.good_choices += 1
                            self.total_choices += 1
                            self.ttl_log.append(new_ttl)
                        else:
                            if (
                                teven
                                < (
                                    request.next_access_same_reg_timestamp
                                    - request.timestamp
                                ).total_seconds()
                            ):
                                self.good_choices += 1
                            self.total_choices += 1

                    else:
                        # Simulat a cache hit: Handle refresh TTL for other eviction policies (teven, tevict, fixedttl)
                        if self.config.placement_policy in [
                            "teven",
                            "tevict",
                            "tevict_ranges",
                            "tevict_window",
                            "dynamicttl",
                        ]:
                            region_list = [
                                obj.location_tag
                                for obj in self.logical_objects[
                                    physical_object.key
                                ].physical_objects.values()
                                if obj.location_tag != request.issue_region
                                and obj.ready(request.timestamp)
                            ]
                            if len(region_list) == 0:
                                net_cost = self.medianNetworkCost
                            else:
                                src_2 = min(
                                    region_list,
                                    key=lambda x: (
                                        self.total_graph[x][request.issue_region][
                                            "cost"
                                        ],
                                        self.total_graph[x][request.issue_region][
                                            "latency"
                                        ],
                                    ),
                                )
                                net_cost = self.total_graph[src_2][
                                    request.issue_region
                                ]["cost"]

                            ttl = (
                                net_cost
                                / self.total_graph.nodes[request.issue_region][
                                    "priceStorage"
                                ]
                                * 60
                                * 60
                                * 24
                                / 3
                            )
                            if (
                                self.config.placement_policy == "tevict"
                                or self.config.placement_policy == "tevict_ranges"
                                or self.config.placement_policy == "tevict_window"
                            ):
                                if len(region_list) > 0:
                                    read_region = None
                                    set_ttl = None
                                    for read_r in region_list:
                                        temp_ttl = self.placement_policy.get_tevict(
                                            read_r,
                                            request.issue_region,
                                            request.timestamp,
                                        )
                                        phys: PhysicalObject = self.logical_objects[
                                            request.obj_key
                                        ].physical_objects[read_r]
                                        if (
                                            read_region is None
                                            or phys.ttl == -1
                                            or (
                                                temp_ttl < set_ttl
                                                and timedelta(seconds=temp_ttl)
                                                + request.timestamp
                                                <= phys.storage_start_time
                                                + timedelta(seconds=phys.get_ttl())
                                            )
                                        ):
                                            read_region = read_r
                                            set_ttl = temp_ttl
                                    ttl = set_ttl
                            elif self.config.placement_policy == "dynamicttl":
                                ttl = self.placement_policy.get_ttl(
                                    request.issue_region, ttl
                                )

                            ttl = round(ttl)
                            physical_object.set_ttl(
                                ttl
                                + (
                                    request.timestamp
                                    - physical_object.get_storage_start_time()
                                ).total_seconds()
                            )

            # Schedule the completion of the transfer after the transfer time
            if (
                len(schedule_place_region) > 0
                and schedule_place_region in cache_regions
            ):
                if (
                    schedule_place_region
                    not in self.logical_objects[request.obj_key].physical_objects
                ):
                    if (
                        not self.config.fixed_base_region
                        and len(self.logical_objects[request.obj_key].physical_objects)
                        == 1
                    ):
                        for region, obj in self.logical_objects[
                            request.obj_key
                        ].physical_objects.items():
                            # ttl is currently set to expire at the current timestamp.
                            phys_obj = self.logical_objects[
                                request.obj_key
                            ].physical_objects[region]
                            if phys_obj.expire_immediate:
                                seconds = (
                                    current_timestamp - obj.get_storage_start_time()
                                ).total_seconds()
                                phys_obj.set_ttl(seconds + transfer_time_millis / 1000)
                                phys_obj.expire_immediate = False

                    obj_ttl = self.calculate_ttl(
                        request, current_timestamp, src, schedule_place_region
                    )
                    phys_object = PhysicalObject(
                        schedule_place_region,
                        request.obj_key,
                        request.size,
                        obj_ttl,
                        self.logical_objects[request.obj_key],
                        Status.ready,
                    )
                    phys_object.set_storage_start_time(
                        current_timestamp + timedelta(milliseconds=transfer_time_millis)
                    )
                    self.logical_objects[request.obj_key].physical_objects[
                        schedule_place_region
                    ] = phys_object
                    self.region_manager.add_object_to_region(
                        schedule_place_region, phys_object
                    )

            transfer_times.append(transfer_time_millis)

        return transfer_times

    def get_placements(self, request: Request):
        obj_key = request.obj_key
        place_regions = []
        # Simulate policy decision
        if self.config.placement_policy in self.spanstore_policies:
            # SPANStore: update placement decisions
            if self._should_update_policy(request.timestamp):
                logger.debug(
                    "Update placement decisions. For teven case, purge cached elements"
                )
                place_regions = self._update_placement_decisions(request.timestamp)[
                    obj_key
                ]
            else:
                if self.is_updating_placement is False:
                    # Never updated placement policy, so use fallback
                    place_regions = [self._fallback_policy()[obj_key]]
                else:
                    # Computed placement policy, but not enough data to update
                    place_regions = self.placement_policy.place(obj_key)

            removed_regions = []
            for region in place_regions:
                if self.region_manager.has_object_in_region(region, obj_key):
                    logger.debug(
                        f"Region {region} already contains object {obj_key} at timestamp {request.timestamp}, so don't transfer"
                    )
                    removed_regions.append(region)
                else:
                    logger.debug(
                        f"Region {region} does not contain object {obj_key} at timestamp {request.timestamp}, so transfer"
                    )
            place_regions = [
                region for region in place_regions if region not in removed_regions
            ]

        else:
            place_regions = self.placement_policy.place(request, self.config)

        logger.debug(f"Place regions: {place_regions} ")
        return place_regions

    def calculate_ttl(
        self, request: Request, current_timestamp: datetime, source: str, dst: str
    ) -> int:
        obj_key = request.obj_key
        ttl = -1

        if (
            not self.config.fixed_base_region
            or self.logical_objects[obj_key].base_region != dst
        ):
            if self.config.placement_policy == "optimal":
                ttl = float("inf")

            elif self.config.placement_policy == "teven":
                net_cost = self.total_graph[source][dst]["cost"]
                # for moving `base_region` case, make sure N is not zero because N/S=0 => ttl=0
                if not self.config.fixed_base_region and net_cost == 0:
                    net_cost = self.medianNetworkCost
                ttl = (
                    net_cost
                    / self.total_graph.nodes[dst]["priceStorage"]
                    * 60
                    * 60
                    * 24
                    / 3
                )
                ttl = round(ttl)

                self.tevens.append(ttl / 3600)

            elif (
                self.config.placement_policy == "tevict"
                or self.config.placement_policy == "tevict_ranges"
                or self.config.placement_policy == "tevict_window"
            ):
                region_list = [
                    obj.location_tag
                    for obj in self.logical_objects[obj_key].physical_objects.values()
                    if obj.location_tag != request.issue_region
                    and obj.ready(request.timestamp)
                ]
                if len(region_list) == 0:
                    net_cost = self.medianNetworkCost
                else:
                    src_2 = min(
                        region_list,
                        key=lambda x: (
                            self.total_graph[x][request.issue_region]["cost"],
                            self.total_graph[x][request.issue_region]["latency"],
                        ),
                    )
                    net_cost = self.total_graph[src_2][request.issue_region]["cost"]

                ttl = (
                    net_cost
                    / self.total_graph.nodes[request.issue_region]["priceStorage"]
                    * 60
                    * 60
                    * 24
                    / 3
                )
                if len(region_list) > 0:
                    read_region = None
                    set_ttl = None
                    for read_r in region_list:
                        temp_ttl = self.placement_policy.get_tevict(
                            read_r, request.issue_region, request.timestamp
                        )
                        phys: PhysicalObject = self.logical_objects[
                            obj_key
                        ].physical_objects[read_r]
                        if (
                            read_region is None
                            or phys.ttl == -1
                            or (
                                temp_ttl < set_ttl
                                and timedelta(seconds=temp_ttl) + request.timestamp
                                <= phys.storage_start_time
                                + timedelta(seconds=phys.get_ttl())
                            )
                        ):
                            read_region = read_r
                            set_ttl = temp_ttl
                    ttl = set_ttl

                ttl = round(ttl)
                self.tevens.append(ttl / 3600)

            elif self.config.placement_policy == "fixedttl":
                ttl = (
                    self.config.cache_ttl * 60 * 60
                )  # translate cache_ttl from hours to seconds
                ttl = round(ttl)

            elif self.config.placement_policy == "dynamicttl":
                net_cost = self.total_graph[source][dst]["cost"]
                # for moving `base_region` case, make sure N is not zero because N/S=0 => ttl=0
                if not self.config.fixed_base_region and net_cost == 0:
                    net_cost = self.medianNetworkCost

                teven = (
                    net_cost
                    / self.total_graph.nodes[dst]["priceStorage"]
                    * 60
                    * 60
                    * 24
                    / 3
                )
                ttl = self.placement_policy.get_ttl(request.issue_region, teven)
                ttl = round(ttl)

            elif (
                self.config.placement_policy == "to_keep"
                or self.config.placement_policy == "ewma"
            ):
                net_cost = self.total_graph[source][dst]["cost"]
                if not self.config.fixed_base_region and net_cost == 0:
                    net_cost = self.medianNetworkCost
                ttl = min(
                    self.placement_policy.estimate_arrival_recency(dst, obj_key)
                    * self.placement_policy.factor,
                    net_cost
                    / self.total_graph.nodes[dst]["priceStorage"]
                    * 60
                    * 60
                    * 24
                    / 3,
                )
                ttl = round(ttl)
                if (
                    ttl
                    >= (
                        request.next_access_same_reg_timestamp - request.timestamp
                    ).total_seconds()
                ):
                    self.good_choices += 1
                self.total_choices += 1

            elif self.config.placement_policy == "pull_on_read":
                ttl = -1

            elif self.config.placement_policy == "always_evict":
                ttl = 0

            elif self.config.placement_policy == "replicate_all":
                ttl = -1

            else:
                raise ValueError("Invalid placement policy type")

        return ttl

    def run(self):
        logger.info("Running simulations...")

        read_graphs, write_graphs = [], []
        start_timestamp, end_timestamp = None, None
        num_lines = sum(1 for _ in open(get_full_path(self.trace_path), "r"))

        if self.version_enable:
            versionate_trace(self.trace_path, self.trace_path + "-versioned")
            self.trace_path += "-versioned"

        # Actual simulation of requests
        with open(get_full_path(self.trace_path), "r") as f, open(
            get_full_path(self.trace_path)
            + "."
            + self.config.placement_policy
            + ".prototype",
            "w",
        ) as outfile:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames + ["answer_region"]
            fieldnames = fieldnames[:5] + [fieldnames[7]]
            with Progress(
                TextColumn("{task.fields[filename]}"),
                SpinnerColumn(),
                TextColumn("{task.percentage:>3.0f}%"),
                BarColumn(),
                transient=True,
            ) as progress:
                task = progress.add_task(
                    "[cyan]Processing...",
                    total=num_lines - 1,
                    filename="Processing requests",
                )

                for row in reader:
                    write_graphs, read_graphs = [], []
                    read_transfer_graph, write_transfer_graph = None, None
                    runtime, latency, throughput, cost = 0, 0, 0, 0
                    policy = (
                        self.placement_policy
                        if self.config.placement_policy in self.spanstore_policies
                        else self.transfer_policy
                    )
                    read_region = ""

                    timestamp_str = row["timestamp"]
                    if (
                        timestamp_str.replace("-", "")
                        .replace(":", "")
                        .replace(" ", "")
                        .isdigit()
                    ):
                        request_timestamp = datetime.fromtimestamp(
                            int(timestamp_str) / 1000
                        )
                    else:
                        request_timestamp = datetime.fromisoformat(timestamp_str / 1000)

                    request_data = {k: v for k, v in row.items()}
                    request_data["timestamp"] = request_timestamp

                    if (
                        request_data["op"] == "GET"
                        or request_data["op"] == "REST.GET.OBJECT"
                    ):
                        request_data["op"] = "read"
                    elif (
                        request_data["op"] == "PUT"
                        or request_data["op"] == "REST.PUT.OBJECT"
                    ):
                        request_data["op"] = "write"
                    else:
                        continue

                    next_access_timestamp_str = row["time_to_next_access"]
                    if (
                        next_access_timestamp_str.replace("-", "")
                        .replace(":", "")
                        .replace(" ", "")
                        .isdigit()
                    ):
                        next_access_timestamp = datetime.fromtimestamp(
                            int(next_access_timestamp_str) / 1000
                        )
                    else:
                        next_access_timestamp = datetime.fromisoformat(
                            next_access_timestamp_str / 1000
                        )
                    request_data["next_access_timestamp"] = next_access_timestamp

                    next_access_same_reg_timestamp_str = row[
                        "time_to_next_access_same_reg"
                    ]
                    if next_access_same_reg_timestamp_str != "-1":
                        if (
                            next_access_same_reg_timestamp_str.replace("-", "")
                            .replace(":", "")
                            .replace(" ", "")
                            .isdigit()
                        ):
                            next_access_same_reg_timestamp = datetime.fromtimestamp(
                                int(next_access_same_reg_timestamp_str) / 1000
                            )
                        else:
                            next_access_same_reg_timestamp = datetime.fromisoformat(
                                next_access_same_reg_timestamp_str / 1000
                            )
                        request_data[
                            "next_access_same_reg_timestamp"
                        ] = next_access_same_reg_timestamp

                    request = Request(**request_data)
                    obj_key = request.obj_key

                    if start_timestamp is None:
                        start_timestamp = request.timestamp
                        self.region_manager.set_start_time_and_ignored_days(
                            start_timestamp, self.days_to_ignore_cost
                        )

                    current_day = (request.timestamp - start_timestamp).days
                    if current_day >= self.days_to_ignore_cost:
                        self.ignore_cost = False

                    # Create logical object if it doesn't exist
                    if obj_key not in self.logical_objects:
                        logical_obj = LogicalObject(
                            key=obj_key,
                            size=request.size,
                            last_modified=request.timestamp,
                        )
                        self.logical_objects[obj_key] = logical_obj
                    else:
                        # Update access time of the object if accessed again (e.g. LRU)
                        self.logical_objects[obj_key].set_last_modified(
                            request.timestamp
                        )

                    # Set base region: to local if write
                    if (
                        request.op == "write"
                        and self.set_base_region
                        and self.config.placement_policy
                        in self.eviction_based_policies + self.other_eviction_policies
                    ):
                        # print(f"Set base region to {request.issue_region} for object {obj_key}")
                        self.logical_objects[obj_key].assign_base_region(
                            request.issue_region
                        )

                    if request.op == "read":
                        read_region, read_transfer_graph = policy.read_transfer_path(
                            request
                        )
                        if read_region != request.issue_region:
                            self.misses += 1
                            self.misses_size += request.size / GB
                        else:
                            self.hits += 1
                            self.hits_size += request.size / GB
                        read_graphs.append(read_transfer_graph)

                        place_regions = self.get_placements(request)

                        # Schedule placed region at the same time
                        transfer_time = self.initiate_data_transfer(
                            request.timestamp,
                            request,
                            [read_transfer_graph],
                            [request.issue_region]
                            if request.issue_region in place_regions
                            else [],
                            True
                            if self.config.placement_policy
                            in self.eviction_based_policies
                            else False,
                            "read",
                            set(place_regions),
                        )
                        assert len(transfer_time) == 1

                        # NOTE: For SPANStore, evict objects from old regions if have updated placement decisions
                        # Eviction time is the placement generated time
                        if self.config.placement_policy in self.spanstore_policies:
                            original_regions = list(
                                self.logical_objects[obj_key].physical_objects.keys()
                            )

                            # Update placement decision generated time if read region is in the list
                            region_to_evict_objects = [
                                region
                                for region in original_regions
                                if region not in place_regions
                            ]
                            if read_region in region_to_evict_objects:
                                self.placement_decision_generated_time = (
                                    request.timestamp
                                    + timedelta(seconds=transfer_time[0])
                                )

                            region_to_evict_objects = [
                                region
                                for region in region_to_evict_objects
                                if region != read_region
                            ]
                            for region in region_to_evict_objects:
                                self.region_manager.remove_object_from_region(
                                    region,
                                    self.logical_objects[obj_key].physical_objects[
                                        region
                                    ],
                                    self.placement_decision_generated_time,
                                )

                                # Removed from logical objects too
                                self.logical_objects[obj_key].physical_objects.pop(
                                    region, None
                                )

                    elif request.op == "write":
                        place_regions = self.get_placements(request)
                        for region in place_regions:
                            write_transfer_graph = policy.write_transfer_path(
                                request, dst=region
                            )
                            write_graphs.append(write_transfer_graph)

                        self.initiate_data_transfer(
                            request.timestamp,
                            request,
                            write_graphs[-len(place_regions) :],
                            place_regions,
                            True
                            if self.config.placement_policy
                            in self.eviction_based_policies
                            else False,
                            "write",
                            set(place_regions),
                        )
                        if self.config.placement_policy in self.spanstore_policies:
                            original_regions = list(
                                self.logical_objects[obj_key].physical_objects.keys()
                            )
                            region_to_evict_objects = [
                                region
                                for region in original_regions
                                if region not in place_regions + [read_region]
                            ]
                            for region in region_to_evict_objects:
                                self.region_manager.remove_object_from_region(
                                    region,
                                    self.logical_objects[obj_key].physical_objects[
                                        region
                                    ],
                                    self.placement_decision_generated_time,
                                )
                                self.logical_objects[obj_key].physical_objects.pop(
                                    region, None
                                )

                    else:
                        raise ValueError("Invalid operation type")

                    # Update metrics
                    self.tracker.add_request_size(request.size)
                    if read_transfer_graph is not None:
                        (
                            tput,
                            tput_runtime,
                            read_latency,
                            c,
                        ) = self._update_transfer_metric(read_transfer_graph, request)
                        logger.debug(
                            f"Read latency: {read_latency}, throughput: {tput}, throughput_runtime: {tput_runtime}, cost: {c}"
                        )
                        self.tracker.add_latency("read", read_latency)
                        self.tracker.add_throughput("read", tput)
                        self.tracker.add_tput_runtime("read", tput_runtime)

                        if not self.ignore_cost:
                            self.tracker.add_transfer_cost(c)

                        runtime += tput_runtime
                        latency += read_latency
                        throughput += tput
                        cost += c

                    if write_transfer_graph is not None:
                        overall_latency, overall_runtime, overall_tput = [], [], []
                        for write_graph in write_graphs[-len(place_regions) :]:
                            (
                                tput,
                                tput_runtime,
                                w_latency,
                                c,
                            ) = self._update_transfer_metric(write_graph, request)
                            logger.debug(
                                f"Write latency: {w_latency}, throughput: {tput}, throughput_runtime: {tput_runtime}, cost: {c}"
                            )

                            if not self.ignore_cost:
                                self.tracker.add_transfer_cost(c)

                            overall_latency.append(w_latency)
                            overall_runtime.append(tput_runtime)
                            overall_tput.append(tput)
                            cost += c

                        runtime += min(overall_runtime)
                        latency += min(overall_latency)
                        throughput += min(overall_tput)

                        if request.op == "write":
                            logger.debug("Add write latency")

                            write_tput_runtime = min(overall_runtime)
                            write_latency = min(overall_latency)
                            write_tput = min(overall_tput)

                            self.tracker.add_throughput("write", write_tput)
                            self.tracker.add_tput_runtime("write", write_tput_runtime)
                            self.tracker.add_latency("write", write_latency)

                    for region in place_regions:
                        put_cost = self.total_graph.nodes[region]["pricePut"]
                        if not self.ignore_cost:
                            self.tracker.add_request_cost(put_cost)

                        cost += put_cost

                    if read_region != "":
                        get_cost = self.total_graph.nodes[region]["priceGet"]

                        if not self.ignore_cost:
                            self.tracker.add_request_cost(get_cost)
                        cost += get_cost

                    progress.update(task, advance=1)
                    end_timestamp = request.timestamp

                    # For SPANStore, regenerate decisions
                    if self.config.placement_policy in self.spanstore_policies:
                        self.request_buffer.append(request)
                        self.moving_idx += 1

                        if self.moving_idx % 10000 == 0:
                            print(
                                f"Now processed {self.moving_idx} requests out of {num_lines}: {round(self.moving_idx / num_lines * 100, 2)}%"
                            )

                    if self.store_decision:
                        temp_row = row.copy()
                        if request.op == "write":
                            temp_row["answer_region"] = request.issue_region
                        else:
                            temp_row["answer_region"] = request.read_from[0]
                        temp_row.pop("time_to_next_access", None)
                        temp_row.pop("time_to_next_access_same_reg", None)
                        # writer.writerow(temp_row)

                if end_timestamp and start_timestamp:
                    remained_process_time = timedelta(seconds=50)
                    duration = remained_process_time + end_timestamp - start_timestamp
                    self.tracker.set_duration(duration)
                    self.region_manager.calculate_remaining_storage_costs(
                        remained_process_time + end_timestamp, self.logical_objects
                    )

        self._print_region_manager()
        if self.config.placement_policy == "teven":
            print(len(self.tevens))
            print("Avg Teven: ", sum(self.tevens) / len(self.tevens))
        if (
            self.config.placement_policy == "tevict"
            or self.config.placement_policy == "tevict_ranges"
            or self.config.placement_policy == "tevict_window"
        ):
            print("Moving TTLs:", self.placement_policy.ttls)
            print(self.placement_policy.region_pairs_ttl)

        print("hits:", self.hits)
        print("misses:", self.misses)
        print("hit/miss ratio:", self.hits / (self.hits + self.misses) * 100)
        print("hits size:", self.hits_size)
        print("misses size:", self.misses_size)
        print(
            "hit/miss size ratio:",
            self.hits_size / (self.hits_size + self.misses_size) * 100,
        )
        print(
            "Avg runtime_throughput:",
            sum(self.runtime_throughputs) / len(self.runtime_throughputs),
        )

        print(self.good_choices, self.total_choices)
        print(self.ttl_log)

        print(self.trace_path)
        if os.path.exists(self.trace_path):
            os.remove(self.trace_path)

    def _print_config_details(self):
        config_str = textwrap.dedent(
            """
            ------ Config Details ------
            storage_region: {}
            
            placement_policy: {}
            
            transfer_policy: {}

            window_size: {}
            
            ttl: {}
            ---------------------------
        """
        ).format(
            self.config.storage_region,
            self.config.placement_policy,
            self.config.transfer_policy,
            self.config.window_size,
            self.config.cache_ttl,
        )
        logger.info(config_str)

    ############################ REPORT FUNCTIONS ############################
    def report_metrics(self):
        table = PrettyTable()
        table.field_names = ["Metric", "Value"]
        print(
            self.region_manager.storage_costs_without_base,
            self.region_manager.storage_costs,
        )
        self.tracker.add_storage_cost(self.region_manager.aggregate_storage_cost())
        self.tracker.add_storage_cost_without_base(
            self.region_manager.aggregate_storage_cost_without_base()
        )

        metrics = self.tracker.get_metrics()

        for key, value in metrics.items():
            table.add_row([key, value])
        print("\n" + str(table))

    def _print_region_manager(self):
        table2 = PrettyTable()
        table2.field_names = ["Region", "Objects", "Cost ($)"]

        for region, objects in self.region_manager.regions.items():
            objects_list = [str(obj) for obj in objects]
            cost = self.region_manager.storage_costs.get(region, 0.0)
            table2.add_row([region, ", ".join(objects_list), cost])

        self.region_manager.print_stat()

    def _update_transfer_metric(self, transfer_graph: nx.DiGraph, request: Request):
        num_partitions = list(transfer_graph.edges.data())[0][-1]["num_partitions"]
        throughput = min(
            [edge[-1]["throughput"] for edge in transfer_graph.edges.data()]
        )
        throughput_runtime = min(
            [
                request.size
                * len(edge[-1]["partitions"])
                / (GB * num_partitions)
                * (1 / edge[-1]["throughput"])
                for edge in transfer_graph.edges.data()
            ]
        )

        # Latency
        src = [
            node
            for node in transfer_graph.nodes
            if transfer_graph.nodes[node].get("src", False)
        ]
        dsts = [
            node
            for node in transfer_graph.nodes
            if transfer_graph.nodes[node].get("dst", False)
        ]

        assert (
            len(src) == 1
        ), f"Graphs look like: {transfer_graph.nodes.data()}, src: {src}, dsts: {dsts}"
        latency = max([transfer_graph[src[0]][dst]["latency"] for dst in dsts])

        # cannot exceeds the ingress limit
        if request.op == "read":
            ingress_limit = (
                aws_instance_throughput_limit[1]
                if request.issue_region.startswith("aws")
                else gcp_instance_throughput_limit[1]
                if request.issue_region.startswith("gcp")
                else azure_instance_throughput_limit[1]
            )
            ingress_limit = ingress_limit * self.num_vms

            throughput_runtime = max(
                throughput_runtime, request.size / (GB * ingress_limit)
            )
        elif request.op == "write":
            egress_limit = (
                aws_instance_throughput_limit[0]
                if request.issue_region.startswith("aws")
                else gcp_instance_throughput_limit[0]
                if request.issue_region.startswith("gcp")
                else azure_instance_throughput_limit[0]
            )
            egress_limit = egress_limit * self.num_vms
            throughput_runtime = max(
                throughput_runtime, request.size / (GB * egress_limit)
            )

        self.runtime_throughputs.append(throughput_runtime)

        logger.debug("Transfer graph: {}".format(transfer_graph.edges.data()))
        each_edge_cost = [
            edge[-1]["cost"]
            * request.size
            * len(edge[-1]["partitions"])
            / (GB * num_partitions)
            for edge in transfer_graph.edges.data()
        ]
        logger.debug("netcost: {}".format(each_edge_cost))
        cost = sum(each_edge_cost)
        logger.debug(
            f"Size of data: {request.size / GB}, transfer runtime: {throughput_runtime}, transfer latency: {latency}, transfer cost: {cost}, transfer graph: {transfer_graph.edges}, each edge cost = {each_edge_cost}"
        )

        return throughput, throughput_runtime, latency, cost

    def _select_placement_policy(self, policy_type: str):
        if policy_type == "local":
            return LocalWrite()

        elif policy_type == "single_region":
            return SingleRegionWrite()

        elif policy_type == "replicate_all":
            return ReplicateAll(total_graph=self.total_graph, config=self.config)

        elif policy_type == "spanstore":
            return SPANStore(
                policy="spanstore",
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                region_mgmt=self.region_manager,
                trace_path=self.trace_path,
            )

        elif policy_type == "lru":
            return LRU(
                region_manager=self.region_manager,
                config=self.config,
            )

        elif policy_type == "teven":
            return Teven(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "always_evict":
            return AlwaysEvict(region_manager=self.region_manager)

        elif policy_type == "pull_on_read":
            return AlwaysStore(region_manager=self.region_manager)

        elif policy_type == "tevict_window":
            return TevictWindow(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "tevict":
            return TevictV2(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "tevict_ranges":
            return TevictRangesV2(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "optimal":
            return OptimalV2(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "fixedttl":
            return Fixed_TTL(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "dynamicttl":
            return DynamicTTL(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "to_keep":
            return IndividualTTL(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        elif policy_type == "ewma":
            return EWMA(
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                regionManager=self.region_manager,
            )

        else:
            raise ValueError("Invalid placement policy type")

    def _select_transfer_policy(self, policy_type: str, placement_policy):
        if policy_type == "direct":
            return DirectTransfer(config=self.config, total_graph=self.total_graph)

        elif policy_type == "closest":
            return ClosestTransfer(
                config=self.config,
                total_graph=self.total_graph,
                object_dict=self.logical_objects,
            )

        elif policy_type == "cheapest":
            return CheapestTransferV2(
                config=self.config,
                total_graph=self.total_graph,
                object_dict=self.logical_objects,
                region_manager=self.region_manager,
                placement_policy=placement_policy,
            )
        else:
            raise ValueError("Invalid transfer policy type")
