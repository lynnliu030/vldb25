from collections import defaultdict
import csv
import threading
from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class AlwaysStore(PlacementPolicy):
    _instance = None
    _lock = threading.Lock()
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, init_regions: List[str] = []) -> None:
        super().__init__(init_regions)
        self.storage_cost = defaultdict(int)
        self.hits = 0
        self.miss = 0
        self.timestamps = self.read_timestamps_from_csv(
            "../experiment/trace/test.csv", "timestamp"
        )

    @classmethod
    def get_instance(cls, init_regions: List[str]):
        if cls._instance is None:
            with cls._lock:
                cls._instance = AlwaysStore(init_regions=init_regions)
        return cls._instance

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

        return timestamps

    def add_to_cost(self, timedelta: int, region: str, size):
        self.storage_cost[region] += (
            timedelta
            / 3600
            / 24
            * self.stat_graph.nodes[region]["priceStorage"]
            * 3
            * size
            / (1024 * 1024 * 1024)
        )

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """

        return [req.client_from_region]

    def name(self) -> str:
        return "always_store"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return -1  # -1 means store forever
