import datetime
import math
from typing import List, Dict
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from src.model.request import Request
from src.model.object import LogicalObject
import networkx as nx
from collections import defaultdict
from src.utils.definitions import GB
from datetime import timedelta
from src.placement_policy.policy import PlacementPolicy
from src.utils.helpers import (
    get_avg_network_cost,
    get_min_network_cost,
    get_avg_storage_cost,
)


"""
    Schedule evict for next put
    Read to object that should be gone
"""


class TevictV2(PlacementPolicy):
    def __init__(
        self,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        regionManager: RegionManager,
    ) -> None:
        self.config = config
        self.window_size = (
            self.config.window_size
        )  # if -1, then window is entire history
        self.total_graph = total_graph
        self.objects = objects
        self.region_manager = regionManager

        self.avgNetworkCost = get_avg_network_cost(self.total_graph)
        self.avgStorageCost = get_avg_storage_cost(self.total_graph)
        self.minNetworkCost = get_min_network_cost(self.total_graph)

        self.past_requests = {}  
        self.hist = {}  
        self.last_hist = {} 
        self.num_requests = {}  

        self.next_past_requests = {}  
        self.next_hist = {}  
        self.next_last_hist = {}
        self.next_num_requests = {}  

        if self.window_size != -1:
            self.last_updated = None
        else:
            self.last_updated = datetime.datetime.fromtimestamp(0)

        self.ttls = {}
        self.time_passed = None
        self.seen_days = {} 

        self.remove_immediately = {}  

        self.regions = (
            self.config.regions
        )  
        self.region_pairs_ttl = defaultdict(list)
        for region in self.regions:
            for region2 in self.regions:
                self.region_pairs_ttl[(region, region2)] = []

        self.k = 12
        self.c = 0

        super().__init__()

    def is_next_window(self, old_datetime: datetime, new_datetime: datetime):
        # Check if `old_datetime` and `new_datetime` are in the same window
        time_difference = new_datetime - old_datetime
        return time_difference > timedelta(hours=self.window_size)

    def is_large_time_gap(self, old_datetime: datetime, new_datetime: datetime):
        # Check if the `new_datetime` is `self.window_size` past the last time the histograms were built
        time_difference = abs(new_datetime - old_datetime)
        # Check if the difference is greater than self.window_size hours
        return time_difference > timedelta(hours=self.window_size * 2)

    def round_to_next_hour(self, dt: datetime):
        # If the minute component is non-zero, round up to the next hour
        if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
            dt += timedelta(hours=1)  # Add one hour to round up
            dt = dt.replace(
                minute=0, second=0, microsecond=0
            )  # Reset minute and second components to zero
        else:
            dt += timedelta(hours=1)
        return dt

    def round_to_next_hour_seconds(self, dt: int):
        return (dt // 3600 + 1) * 3600

    def update_past_requests(self, request: Request, place_region: str):
        self.c += 1
        if self.window_size != -1:
            if self.last_updated is None or self.is_large_time_gap(
                self.last_updated, request.timestamp
            ):
                self.last_updated = request.timestamp.replace(
                    minute=0, second=0, microsecond=0
                )
                self.past_requests = {}
                self.hist = {}
                self.num_requests = {}
                self.last_hist = {}
                self.next_last_hist = {}
                self.next_past_requests = {}
                self.next_hist = {}
                self.next_num_requests = {}
            elif self.is_next_window(self.last_updated, request.timestamp):
                self.last_updated = self.last_updated + timedelta(
                    hours=self.window_size
                )
                self.past_requests = self.next_past_requests
                self.hist = self.next_hist
                self.last_hist = self.next_last_hist
                self.num_requests = self.next_num_requests
                self.next_past_requests = {}
                self.next_hist = {}
                self.next_num_requests = {}
                self.next_last_hist = {}

        # timestamp is in seconds since start
        tnext = 0
        self.next_num_requests[place_region] = (
            self.next_num_requests.get(place_region, 0) + 1
        )
        if place_region not in self.next_past_requests:
            self.next_past_requests[place_region] = {}
        if request.obj_key in self.next_past_requests[place_region]:
            req_to_sec = (
                request.timestamp - datetime.datetime.fromtimestamp(0)
            ).total_seconds()
            tnext = (
                req_to_sec - self.next_past_requests[place_region][request.obj_key][-1]
            )
            if tnext == 0:
                tnext = 1

            # get old time until end of window. subtract from last cost hist. add new request to end of last cost hist
            curentWindow = int(
                math.ceil(
                    (
                        self.round_to_next_hour_seconds(
                            self.next_past_requests[place_region][request.obj_key][-1]
                        )
                        - self.next_past_requests[place_region][request.obj_key][-1]
                    )
                    / 3600
                )
            )

            if (
                place_region in self.next_last_hist
                and curentWindow in self.next_last_hist[place_region]
            ):
                self.next_last_hist[place_region][curentWindow] -= request.size / GB

            if int(math.ceil(tnext / 3600)) not in self.next_hist[place_region]:
                self.next_hist[place_region][int(math.ceil(tnext / 3600))] = 0

            self.next_hist[place_region][int(math.ceil(tnext / 3600))] += (
                request.size / GB
            )
            self.next_past_requests[place_region][request.obj_key].append(
                (request.timestamp - datetime.datetime.fromtimestamp(0)).total_seconds()
            )

            if place_region not in self.next_last_hist:
                self.next_last_hist[place_region] = {}
            tendWindow = int(
                math.ceil(
                    (
                        self.round_to_next_hour(request.timestamp) - request.timestamp
                    ).total_seconds()
                    / 3600
                )
            )

            self.next_last_hist[place_region][tendWindow] = (
                self.next_last_hist[place_region].get(tendWindow, 0) + request.size / GB
            )

        else:
            tnext = (
                request.timestamp - datetime.datetime.fromtimestamp(0)
            ).total_seconds()

            if place_region not in self.next_hist:
                self.next_hist[place_region] = {}
            self.next_past_requests[place_region][request.obj_key] = []
            self.next_past_requests[place_region][request.obj_key].append(
                (request.timestamp - datetime.datetime.fromtimestamp(0)).total_seconds()
            )
            if place_region not in self.next_last_hist:
                self.next_last_hist[place_region] = {}
            tendWindow = int(
                math.ceil(
                    (
                        self.round_to_next_hour(request.timestamp) - request.timestamp
                    ).total_seconds()
                    / 3600
                )
            )
            self.next_last_hist[place_region][tendWindow] = (
                self.next_last_hist[place_region].get(tendWindow, 0) + request.size / GB
            )

        self.time_passed = request.timestamp

    def calc_evict_cost(self, region, teven_hours, net_cost, storage_cost_hour):
        if self.window_size == -1:
            if region not in self.next_hist or region not in self.next_last_hist:
                return -1, -1
            X = self.next_hist[region]
            last_X = self.next_last_hist[region]
        else:
            if region not in self.hist or region not in self.last_hist:
                return -1, -1
            X = self.hist[region]
            last_X = self.last_hist[region]

        # storage_cost_hour = net_cost / teven_hours

        cost_hist = {}  # [0] * (int(max(X.keys())) + 1)
        # print("X: ", X)
        if len(X.keys()) > 0:
            maxKey = int(max(X.keys()))
        else:
            maxKey = 1

        ret = 0
        # list(X.keys()) + [maxKey + 1]
        # print(min(maxKey+1, math.ceil(teven_hours)))
        for c in range(min(maxKey + 1, math.ceil(teven_hours))):
            # print(range(maxKey + 1))
            c = int(c)
            if c not in cost_hist:
                cost_hist[c] = 0
            tevict = c
            for i in X:
                if i <= tevict:
                    # print(X)
                    assert i >= 1
                    cost_hist[c] += (
                        float(X.get(i, 0)) * ((i - 1) + 0.6) * storage_cost_hour
                    )

                else:
                    cost_hist[c] += float(X.get(i, 0)) * (
                        (tevict * storage_cost_hour) + net_cost
                    )

                cost_hist[c] += float(last_X.get(i, 0)) * (tevict * storage_cost_hour)

            if cost_hist[c] <= cost_hist[ret]:
                ret = c
        return (ret, cost_hist[ret])

    def get_tevict(self, src, dst, timestamp):  # decide ttl for obj in src
        if dst in self.seen_days and timestamp < self.seen_days[dst][-1] + timedelta(
            hours=self.k
        ):
            return self.region_pairs_ttl[(src, dst)][-1]

        # if new day, calculate yesterday's ttl, otherwise use yesterday's ttl
        for region in self.regions:
            for region2 in self.regions:
                if region != region2:
                    net_cost = self.total_graph[region][region2]["cost"]
                    storage = self.total_graph.nodes[region2]["priceStorage"] * 3
                    storage_cost_per_hour = storage / 24
                    teven = net_cost / storage * 60 * 60 * 24

                    best_ttl, calculated_cost = self.calc_evict_cost(
                        region2, teven / 3600, net_cost, storage_cost_per_hour
                    )
                    self.region_pairs_ttl[(region, region2)].append(
                        self.find_min(best_ttl, teven, region2, timestamp)
                    )
        return self.region_pairs_ttl[(src, dst)][-1]

    def find_min(self, y, teven, region, timestamp):
        noPastData = False
        if self.window_size == -1:
            if region not in self.next_num_requests:
                noPastData = True
                ttl = teven / 3600 / 2
            else:
                numRequests = self.next_num_requests[region]
        else:
            if region not in self.num_requests:
                noPastData = True
                ttl = teven / 3600 / 2
            else:
                numRequests = self.num_requests[region]

        if not noPastData:
            if (
                y == -1 or self.last_updated is None or numRequests < 1000
            ):  
                ttl = teven / 3600 / 2

            else:
                ttl = y  

        if region not in self.seen_days:
            self.seen_days[region] = []
            self.ttls[region] = []
        if len(self.seen_days[region]) == 0 or timestamp >= self.seen_days[region][
            -1
        ] + timedelta(hours=self.k):
            self.seen_days[region].append(timestamp)
            self.ttls[region].append(ttl)

        # print(ttl)
        return ttl * 3600

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
