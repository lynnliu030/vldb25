import datetime
import math
import statistics
from typing import List, Dict
from src.model.config import Config
from src.model.region_mgmt import RegionManager
from src.model.request import Request
from src.model.object import LogicalObject
import networkx as nx
from src.utils.definitions import GB
from datetime import timedelta
from src.placement_policy.policy import PlacementPolicy
from src.utils.helpers import get_avg_network_cost, get_min_network_cost
from collections import OrderedDict


"""
    Schedule evict for next put
    Read to object that should be gone
"""


class TevictRangesV2(PlacementPolicy):
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
        self.region_pairs_ttl = {}
        for region in self.regions:
            for region2 in self.regions:
                self.region_pairs_ttl[(region, region2)] = []

        super().__init__()

    def is_next_window(self, old_datetime: datetime, new_datetime: datetime):
        time_difference = new_datetime - old_datetime
        return time_difference > timedelta(hours=self.window_size)

    def is_large_time_gap(self, old_datetime: datetime, new_datetime: datetime):
        time_difference = abs(new_datetime - old_datetime)
        return time_difference > timedelta(hours=self.window_size * 2)

    def round_to_next_hour(self, dt: datetime):
        if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
            dt += timedelta(hours=1)  # Add one hour to round up
            dt = dt.replace(
                minute=0, second=0, microsecond=0
            ) 
        else:
            dt += timedelta(hours=1)
        return dt

    def round_to_next_hour_seconds2(self, dt: int, ranges):
        dt_in_hours = dt / 3600
        for i in range(0, len(ranges), 1):
            if dt_in_hours <= ranges[i]:
                return ranges[i] * 3600

    def round_to_next_hour_seconds(self, dt: int, ranges):
        return (dt // 3600 + 1) * 3600

    def find_next_index_in_range(self, tnext: int, ranges):
        tnext_in_hours = tnext / 3600
        for i in range(0, len(ranges), 1):
            if tnext_in_hours <= ranges[i]:
                return i

    def update_past_requests(self, request: Request, place_region: str):
        ############################################################
        KB = 1024
        MB = 1024 * KB
        GB = 1024 * MB
        SEC = 1000
        MIN = 60 * SEC
        HOUR = 60 * MIN
        SEC_IN_HOUR = 0.00028
        MIN_IN_HOUR = 0.01666
        s = MIN_IN_HOUR * HOUR
        ranges_2per = [0] + [s * (1 + (2 / 100)) ** i for i in range(700)]
        ranges_1H = [i * HOUR for i in range(169 + 1)]
        ranges_30M = [i / 2 * HOUR for i in range(169 * 2)]
        ranges = [x / HOUR for x in ranges_2per]
        ############################################################

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
                            self.next_past_requests[place_region][request.obj_key][-1],
                            ranges,
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

            ###

            next_index_in_range = self.find_next_index_in_range(tnext, ranges)
            if next_index_in_range not in self.next_hist[place_region]:
                self.next_hist[place_region][next_index_in_range] = 0
            self.next_hist[place_region][next_index_in_range] += request.size / GB
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

    def calc_evict_cost(self, region, teven_hours, net_cost):
        if self.window_size == -1:
            if region not in self.next_hist or region not in self.next_last_hist:
                return -1
            X = self.next_hist[region]
            last_X = self.next_last_hist[region]
        else:
            if region not in self.hist or region not in self.last_hist:
                return -1
            X = self.hist[region]
            last_X = self.last_hist[region]

        if teven_hours == 0:
            storage_cost_hour = self.avgNetworkCost / teven_hours
        else:
            storage_cost_hour = net_cost / teven_hours
        ############################################################
        KB = 1024
        MB = 1024 * KB
        GB = 1024 * MB
        SEC = 1000
        MIN = 60 * SEC
        HOUR = 60 * MIN
        SEC_IN_HOUR = 0.00028
        MIN_IN_HOUR = 0.01666
        s = MIN_IN_HOUR * HOUR
        ranges_2per = [0] + [s * (1 + (2 / 100)) ** i for i in range(700)]
        ranges_1H = [i * HOUR for i in range(169 + 1)]
        ranges_30M = [i / 2 * HOUR for i in range(169 * 2)]
        ranges = [x / HOUR for x in ranges_2per]
        ############################################################
        cost_hist = {}  # [0] * (int(max(X.keys())) + 1)
        # print("X: ", X)
        if len(X.keys()) > 0:
            maxKey = int(max(X.keys()))
        else:
            maxKey = 1

        ret = 0

        # list(X.keys()) + [maxKey + 1]
        for c in range(len(ranges)):  # hours
            # print(range(maxKey + 1))
            c = int(c)
            if c not in cost_hist:
                cost_hist[c] = 0
            tevict = ranges[c]
            # for i in X:
            for i in range(len(ranges)):
                if (X.get(i, 0)) == 0:
                    continue
                if tevict == 0:
                    cost_hist[c] += float(X.get(i, 0)) * (net_cost)
                    continue
                if ranges[i] <= tevict:
                    cost_hist[c] += (
                        float(X.get(i, 0))
                        * ((ranges[i] + int(i != 0) * (ranges[i - 1])) / 2)
                        * storage_cost_hour
                    )
                else:
                    cost_hist[c] += float(X.get(i, 0)) * (
                        (tevict * storage_cost_hour) + net_cost
                    )
            for i in range(len(ranges)):
                if (last_X.get(i, 0)) == 0:
                    continue
                cost_hist[c] += float(last_X.get(i, 0)) * (tevict * storage_cost_hour)

            if cost_hist[c] <= cost_hist[ret]:
                ret = c
        ##

        Y = OrderedDict()
        for i in sorted(X.keys()):
            Y[i] = X.get(i, 0)
        # print(region)
        # print(Y)

        # print(ranges[ret])
        return ranges[ret]

    def get_tevict(self, src, dst, timestamp):
        if dst in self.seen_days and timestamp < self.seen_days[dst][-1] + timedelta(
            hours=12
        ):
            return self.region_pairs_ttl[(src, dst)][-1]
        # if new day, calculate yesterday's ttl, otherwise use yesterday's ttl
        for region in self.regions:
            for region2 in self.regions:
                if region != region2:
                    net_cost = self.total_graph[region][region2]["cost"]
                    storage = self.total_graph.nodes[region2]["priceStorage"]
                    teven = net_cost / storage * 60 * 60 * 24
                    calculated_cost = self.calc_evict_cost(
                        region2, teven / 3600, net_cost
                    )
                    self.region_pairs_ttl[(region, region2)].append(
                        self.find_min(calculated_cost, teven, region2, timestamp)
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
        ] + timedelta(hours=12):
            self.seen_days[region].append(timestamp)
            self.ttls[region].append(ttl)

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
