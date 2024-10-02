from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import (
    PlacementPolicy,
)
from datetime import timedelta, datetime, timezone
import math
from collections import defaultdict
from operations.policy.utils.definitions import GB
from operations.policy.utils.helpers import (
    get_avg_network_cost,
    get_avg_storage_cost,
    get_min_network_cost,
    get_median_network_cost,
)
from operations.schemas.object_schemas import LocateObjectResponse
from operations.utils.helper import TraceIdx
import threading
import csv


class SkyStore(PlacementPolicy):
    _instance = None
    _lock = threading.Lock()

    def read_timestamps_from_csv(self, file_path: str, timestamp_column: str) -> list:
        timestamps = []

        with open(file_path, mode="r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                timestamp_str = row[timestamp_column]
                try:
                    timestamps.append(int(timestamp_str))
                except ValueError:
                    print(f"Invalid timestamp format: {timestamp_str}")

        print(f"Total length of timestamps: {len(timestamps)}")
        return timestamps

    def __init__(self, init_regions: List[str] = []) -> None:
        super().__init__(init_regions)

        self.region_pairs_ttl = defaultdict(list)
        for region in self.init_regions:
            for region2 in self.init_regions:
                self.region_pairs_ttl[(region, region2)] = 0
        self.window_size = -1

        self.avgNetworkCost = get_avg_network_cost(self.stat_graph)
        self.avgStorageCost = get_avg_storage_cost(self.stat_graph)
        self.minNetworkCost = get_min_network_cost(self.stat_graph)
        self.medNetworkCost = get_median_network_cost(self.stat_graph)

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
            self.last_updated = datetime.fromtimestamp(0, tz=timezone.utc).replace(
                tzinfo=None
            )

        self.regions = self.init_regions
        self.region_pairs_ttl = defaultdict(list)
        for region in self.regions:
            for region2 in self.regions:
                self.region_pairs_ttl[(region, region2)] = []
        self.timestamps = self.read_timestamps_from_csv(
            "../experiment/trace/Tr065.typeE.3reg.tevict.prototype", "timestamp"
        )

        self.k = 12
        self.seen_days = None
        self.storage_cost = defaultdict(int)
        self.network_cost = []
        self.previous_hour = None
        self.hits = 0
        self.miss = 0

    @classmethod
    def get_instance(cls, init_regions: List[str]):
        if cls._instance is None:
            with cls._lock:
                cls._instance = SkyStore(init_regions=init_regions)
        return cls._instance

    def add_to_network_cost(self, src, dst, size):
        if src == dst:
            n_cost = 0
        else:
            if self.stat_graph.has_edge(src, dst):
                n_cost = self.stat_graph[src][dst]["cost"] * (size / GB)
            else:
                n_cost = "-1"
        self.network_cost.append(n_cost)

    def add_to_cost(self, timedelta: int, region: str, size):
        added_cost = (
            timedelta
            / 3600
            / 24
            * self.stat_graph.nodes[region]["priceStorage"]
            * 3
            * size
            / (1024 * 1024 * 1024)
        )
        self.storage_cost[region] += added_cost

    def place(self, req: StartUploadRequest) -> List[str]:
        return [req.client_from_region]

    def is_next_window(self, old_datetime: datetime, new_datetime: datetime):
        time_difference = new_datetime - old_datetime
        return time_difference > timedelta(hours=self.window_size)

    def is_large_time_gap(self, old_datetime: datetime, new_datetime: datetime):
        time_difference = abs(new_datetime - old_datetime)
        return time_difference > timedelta(hours=self.window_size * 2)

    def round_to_next_hour(self, dt: datetime):
        if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
            dt += timedelta(hours=1)
            dt = dt.replace(minute=0, second=0, microsecond=0)
        else:
            dt += timedelta(hours=1)
        return dt

    def round_to_next_hour_seconds(self, dt: int):
        return (dt // 3600 + 1) * 3600

    def update_past_requests(
        self, cur_timestamp_idx: int, response: LocateObjectResponse, place_region: str
    ):
        with SkyStore._lock:
            cur_timestamp = datetime.fromtimestamp(
                self.timestamps[cur_timestamp_idx] / 1000, tz=timezone.utc
            ).replace(tzinfo=None)

            if self.window_size != -1:
                if self.last_updated is None or self.is_large_time_gap(
                    self.last_updated, cur_timestamp
                ):
                    self.last_updated = cur_timestamp.replace(
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
                elif self.is_next_window(self.last_updated, cur_timestamp):
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

            tnext = 0
            self.next_num_requests[place_region] = (
                self.next_num_requests.get(place_region, 0) + 1
            )
            if place_region not in self.next_past_requests:
                self.next_past_requests[place_region] = {}
            if response.key in self.next_past_requests[place_region]:
                req_to_sec = (
                    cur_timestamp
                    - datetime.fromtimestamp(0, tz=timezone.utc).replace(tzinfo=None)
                ).total_seconds()
                tnext = (
                    req_to_sec - self.next_past_requests[place_region][response.key][-1]
                )
                if tnext == 0:
                    tnext = 1

                curentWindow = int(
                    math.ceil(
                        (
                            self.round_to_next_hour_seconds(
                                self.next_past_requests[place_region][response.key][-1]
                            )
                            - self.next_past_requests[place_region][response.key][-1]
                        )
                        / 3600
                    )
                )

                if (
                    place_region in self.next_last_hist
                    and curentWindow in self.next_last_hist[place_region]
                ):
                    self.next_last_hist[place_region][curentWindow] -= (
                        response.size / GB
                    )

                if int(math.ceil(tnext / 3600)) not in self.next_hist[place_region]:
                    self.next_hist[place_region][int(math.ceil(tnext / 3600))] = 0

                self.next_hist[place_region][int(math.ceil(tnext / 3600))] += (
                    response.size / GB
                )
                self.next_past_requests[place_region][response.key].append(
                    (
                        cur_timestamp
                        - datetime.fromtimestamp(0, tz=timezone.utc).replace(
                            tzinfo=None
                        )
                    ).total_seconds()
                )

                if place_region not in self.next_last_hist:
                    self.next_last_hist[place_region] = {}
                tendWindow = int(
                    math.ceil(
                        (
                            self.round_to_next_hour(cur_timestamp) - cur_timestamp
                        ).total_seconds()
                        / 3600
                    )
                )

                self.next_last_hist[place_region][tendWindow] = (
                    self.next_last_hist[place_region].get(tendWindow, 0)
                    + response.size / GB
                )
            else:
                tnext = (
                    cur_timestamp
                    - datetime.fromtimestamp(0, tz=timezone.utc).replace(tzinfo=None)
                ).total_seconds()
                if place_region not in self.next_hist:
                    self.next_hist[place_region] = {}
                self.next_past_requests[place_region][response.key] = []
                self.next_past_requests[place_region][response.key].append(
                    (
                        cur_timestamp
                        - datetime.fromtimestamp(0, tz=timezone.utc).replace(
                            tzinfo=None
                        )
                    ).total_seconds()
                )
                if place_region not in self.next_last_hist:
                    self.next_last_hist[place_region] = {}
                tendWindow = int(
                    math.ceil(
                        (
                            self.round_to_next_hour(cur_timestamp) - cur_timestamp
                        ).total_seconds()
                        / 3600
                    )
                )
                self.next_last_hist[place_region][tendWindow] = (
                    self.next_last_hist[place_region].get(tendWindow, 0)
                    + response.size / GB
                )

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

        cost_hist = {}
        if len(X.keys()) > 0:
            maxKey = int(max(X.keys()))
        else:
            maxKey = 1

        ret = 0
        for c in range(min(maxKey + 1, math.ceil(teven_hours))):
            c = int(c)
            if c not in cost_hist:
                cost_hist[c] = 0
            tevict = c
            for i in X:
                if i <= tevict:
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

    def get_tevict(self, src, dst):
        return self.region_pairs_ttl[(src, dst)][-1]

    def find_min(self, y, teven, region):
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
            if y == -1 or self.last_updated is None or numRequests < 1000:
                ttl = teven / 3600 / 2
            else:
                ttl = y

        return ttl * 3600

    def name(self) -> str:
        return "skystore"

    def get_ttl(
        self,
        idx: int,
        src: str = None,
        dst: str = None,
        fixed_base_region: bool = False,
    ) -> int:
        with SkyStore._lock:
            now_timestamp = datetime.fromtimestamp(
                self.timestamps[idx] / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
            if self.seen_days is None or now_timestamp >= self.seen_days + timedelta(
                hours=self.k
            ):
                for region in self.regions:
                    for region2 in self.regions:
                        if region != region2:
                            net_cost = self.stat_graph[region][region2]["cost"]
                            storage = self.stat_graph.nodes[region2]["priceStorage"] * 3
                            storage_cost_per_hour = storage / 24
                            teven = net_cost / storage * 60 * 60 * 24

                            best_ttl, calculated_cost = self.calc_evict_cost(
                                region2, teven / 3600, net_cost, storage_cost_per_hour
                            )
                            self.region_pairs_ttl[(region, region2)].append(
                                self.find_min(best_ttl, teven, region2)
                            )
                self.seen_days = now_timestamp

            if fixed_base_region is True:
                return -1
            if src is None:
                return (
                    self.med
                    / self.stat_graph.nodes[dst]["priceStorage"]
                    * 60
                    * 60
                    * 24
                    / 3
                )

            return self.get_tevict(src, dst)
