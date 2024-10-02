from typing import Dict, Set
import networkx as nx
from src.model.object import PhysicalObject
from datetime import timedelta, datetime
import logging
from src.utils.definitions import GB
from src.model.object import LogicalObject
from src.model.object import Status


class RegionManagerV2:
    def __init__(self, total_graph: nx.DiGraph, logger: logging.Logger):
        self.total_graph = total_graph
        self.logger = logger

        self.regions: Dict[str, Set[int]] = {}
        self.region_objects: Dict[str, Set[PhysicalObject]] = {}
        self.region_tot_size: Dict[str, int] = {}

        self.storage_costs: Dict[str, float] = {}
        self.storage_costs_without_base: Dict[str, float] = {}

        self.trace_start_time: datetime = None
        self.days_to_ignore: int = 0

    def set_start_time_and_ignored_days(
        self, start_time: datetime, days_to_ignore: int
    ):
        self.trace_start_time = start_time
        self.days_to_ignore = days_to_ignore

    def _price_per_GB(self, region: str, duration: timedelta) -> float:
        price_per_gb_per_month = (
            self.total_graph.nodes[region].get("priceStorage", 0.0) * 3
        )
        days = duration.total_seconds() / (24 * 3600)
        return price_per_gb_per_month * days

    def _get_storage_cost_per_gb(self, region: str) -> float:
        return self.total_graph.nodes[region].get("priceStorage", None)

    def add_object_to_region(self, region: str, physical_object: PhysicalObject):
        """Add object to region."""
        if region not in self.regions:
            self.regions[region] = set()
            self.region_objects[region] = set()
            self.region_tot_size[region] = 0

        self.regions[region].add(physical_object.key)
        self.region_objects[region].add(physical_object)
        self.region_tot_size[region] += physical_object.size
        self.logger.info(f"Adding object {physical_object.key} to region {region}.")

    def get_objects_in_region(self, region):
        """Get object in region."""
        objects = self.regions.get(region, [])
        self.logger.info(f"Getting object in region {region}: {objects}.")
        return objects

    def has_object_in_region(self, region: str, physical_object_key: str):
        """Check if object is in region."""
        objects = self.regions.get(region, [])
        physical_object = objects.get(physical_object_key, None)
        if physical_object is not None and physical_object.status == Status.ready:
            physical_object = None
        self.logger.info(
            f"Object {physical_object_key} in region {region}: {physical_object is not None}."
        )
        return physical_object is not None

    def clear_all_objects(self):
        """Clear all objects in all regions."""
        self.region_objects = {}
        self.region_tot_size = {}
        self.logger.info("Clearing all objects in all regions.")

    def remove_object_from_region(
        self, region: str, physical_object: PhysicalObject, end_time: datetime
    ):
        """Remove object from the region and calculate its storage cost."""
        if region in self.regions and physical_object.key in self.regions[region]:
            self.regions[region].remove(physical_object.key)
            self.region_objects[region].remove(physical_object)
            self.region_tot_size[region] -= physical_object.size

            ignore_timestamp = self.trace_start_time + timedelta(
                days=self.days_to_ignore
            )
            if physical_object.storage_start_time > ignore_timestamp:
                duration = physical_object.storage_duration(end_time)
            else:
                if end_time < ignore_timestamp:
                    duration = timedelta(0)
                else:
                    start_time = self.trace_start_time + timedelta(
                        days=self.days_to_ignore
                    )
                    duration = end_time - start_time

            if duration < timedelta(0):
                duration = timedelta(0)

            cost = self._price_per_GB(region, duration) * physical_object.size / GB
            self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost

            if (
                physical_object.logical_object.base_region is None
                or region != physical_object.logical_object.base_region
            ):
                cost_without_base = (
                    self._price_per_GB(region, duration) * physical_object.size / GB
                )
                self.storage_costs_without_base[region] = (
                    self.storage_costs_without_base.get(region, 0.0) + cost_without_base
                )

            self.logger.info(
                f"Removing object {physical_object.key} from region {region}."
            )

    def get_region_object_size(self, region: str):
        return self.region_tot_size.get(region, 0)

    def evict_lru(
        self, region: str, obj_size: int, cache_size: int, end_time: datetime
    ):
        """Evict objects from the region using LRU."""
        while self.region_tot_size[region] + obj_size > cache_size:
            lru_object = min(
                [obj.logical_object for obj in self.region_objects[region]],
                key=lambda x: x.last_modified,
            )
            lru_physical_object = lru_object.physical_objects[region]
            assert (
                lru_physical_object.storage_start_time is not None
            ), "Storage start time is None."
            assert (
                lru_physical_object.storage_start_time <= end_time
            ), f"Storage start time {lru_physical_object.storage_start_time} is greater than end time {end_time}."
            self.remove_object_from_region(region, lru_physical_object, end_time)

    def calculate_remaining_storage_costs(
        self, end_time: datetime, logical_objects: Dict[str, LogicalObject]
    ):
        """Calculate and aggregate storage costs for objects still stored."""
        ignore_timestamp = self.trace_start_time + timedelta(days=self.days_to_ignore)
        c = 0
        for region, objects in self.region_objects.items():
            # For each of the object
            for physical_object in objects:
                duration = None
                if physical_object.storage_start_time < ignore_timestamp:
                    # Duration is the time object is stored from the ignore timestamp
                    if (
                        physical_object.ttl == -1
                        or len(
                            logical_objects[physical_object.key].physical_objects.keys()
                        )
                        == 1
                    ):
                        duration = end_time - max(
                            physical_object.storage_start_time, ignore_timestamp
                        )
                    else:
                        duration = min(
                            timedelta(seconds=physical_object.ttl),
                            end_time - ignore_timestamp,
                            end_time - physical_object.storage_start_time,
                        )
                else:
                    logical_object = logical_objects[physical_object.key]

                    # Duration is the time object is stored
                    if (
                        physical_object.ttl == -1
                        or physical_object.ttl == float("inf")
                        or (
                            logical_object.base_region is None
                            and logical_object.get_last_element() == physical_object
                        )
                    ):
                        duration = physical_object.storage_duration(end_time)
                        if physical_object.ttl == -1:
                            c += 1
                    else:
                        stop_store_time = (
                            physical_object.storage_start_time
                            + timedelta(seconds=physical_object.get_ttl())
                        )
                        duration = physical_object.storage_duration(
                            min(end_time, stop_store_time)
                        )

                # If duration is negative, set it to 0
                if duration < timedelta(0):
                    duration = timedelta(0)

                cost = self._price_per_GB(region, duration) * (
                    physical_object.size / GB
                )
                self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost

                # Calculate the storage cost without the base region
                if (
                    physical_object.logical_object.base_region is None
                    or region != physical_object.logical_object.base_region
                ):
                    cost_without_base = (
                        self._price_per_GB(region, duration) * physical_object.size / GB
                    )
                    self.storage_costs_without_base[region] = (
                        self.storage_costs_without_base.get(region, 0.0)
                        + cost_without_base
                    )
        print("c:", c)

    def aggregate_storage_cost(self):
        for region, cost in self.storage_costs.items():
            self.logger.info(f"Storage cost for region {region}: {cost}.")
            print(f"Storage cost for region {region}: {cost}.")

        return sum(self.storage_costs.values())

    def aggregate_storage_cost_without_base(self):
        for region, cost in self.storage_costs_without_base.items():
            self.logger.info(f"Storage cost without base for region {region}: {cost}.")
            print(f"Storage cost without base for region {region}: {cost}.")

        return sum(self.storage_costs_without_base.values())

    def print_stat(self):
        self.logger.info(
            f"Total number of regions storing objects: {len(self.regions)}"
        )
        print_rst = [(region, len(objects)) for region, objects in self.regions.items()]
        self.logger.info(f"Number of objects stored in each region: {print_rst}")
        size = 0
        for _, objects in self.region_objects.items():
            for physical_object in objects:
                size += physical_object.size / GB

        assert size == sum(self.region_tot_size.values()) / GB
        self.logger.info(f"Sum of all object sizes stored in all regions: {size} GB.")


class RegionManager:
    def __init__(self, total_graph: nx.DiGraph, logger: logging.Logger):
        self.total_graph = total_graph
        self.logger = logger

        self.regions: Dict[str, Set[int]] = {}
        self.region_objects: Dict[str, Set[PhysicalObject]] = {}
        self.region_tot_size: Dict[str, int] = {}

        self.storage_costs: Dict[str, float] = {}
        self.storage_costs_without_base: Dict[str, float] = {}

        self.trace_start_time: datetime = None
        self.days_to_ignore: int = 0

    def set_start_time_and_ignored_days(
        self, start_time: datetime, days_to_ignore: int
    ):
        self.trace_start_time = start_time
        self.days_to_ignore = days_to_ignore

    def _price_per_GB(self, region: str, duration: timedelta) -> float:
        price_per_gb_per_month = (
            self.total_graph.nodes[region].get("priceStorage", 0.0) * 3
        )

        days = duration.total_seconds() / (24 * 3600)
        return price_per_gb_per_month * days

    def _get_storage_cost_per_gb(self, region: str) -> float:
        return self.total_graph.nodes[region].get("priceStorage", None)

    def add_object_to_region(self, region: str, physical_object: PhysicalObject):
        """Add object to region."""
        if region not in self.regions:
            self.regions[region] = set()
            self.region_objects[region] = set()
            self.region_tot_size[region] = 0

        self.regions[region].add(physical_object.key)
        self.region_objects[region].add(physical_object)
        self.region_tot_size[region] += physical_object.size
        self.logger.info(f"Adding object {physical_object.key} to region {region}.")

    def get_object_in_region(self, region):
        """Get object in region."""
        objects = self.regions.get(region, [])
        self.logger.info(f"Getting object in region {region}: {objects}.")
        return objects

    def has_object_in_region(self, region: str, physical_object_key: str):
        """Check if object is in region."""
        exist = physical_object_key in self.regions.get(region, [])
        self.logger.info(f"Object {physical_object_key} in region {region}: {exist}.")
        return exist

    def clear_all_objects(self):
        """Clear all objects in all regions."""
        self.region_objects = {}
        self.region_tot_size = {}
        self.logger.info("Clearing all objects in all regions.")

    def remove_object_from_region(
        self, region: str, physical_object: PhysicalObject, end_time: datetime
    ):
        """Remove object from the region and calculate its storage cost."""
        if region in self.regions and physical_object.key in self.regions[region]:
            self.regions[region].remove(physical_object.key)
            self.region_objects[region].remove(physical_object)
            self.region_tot_size[region] -= physical_object.size
            ignore_timestamp = self.trace_start_time + timedelta(
                days=self.days_to_ignore
            )
            if physical_object.storage_start_time > ignore_timestamp:
                duration = physical_object.storage_duration(end_time)
            else:
                if end_time < ignore_timestamp:
                    duration = timedelta(0)
                else:
                    start_time = self.trace_start_time + timedelta(
                        days=self.days_to_ignore
                    )
                    duration = end_time - start_time

            if duration < timedelta(0):
                duration = timedelta(0)

            cost = self._price_per_GB(region, duration) * physical_object.size / GB
            self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost

            if (
                physical_object.logical_object.base_region is None
                or region != physical_object.logical_object.base_region
            ):
                cost_without_base = (
                    self._price_per_GB(region, duration) * physical_object.size / GB
                )
                self.storage_costs_without_base[region] = (
                    self.storage_costs_without_base.get(region, 0.0) + cost_without_base
                )

            self.logger.info(
                f"Removing object {physical_object.key} from region {region}."
            )

    def get_region_object_size(self, region: str):
        return self.region_tot_size.get(region, 0)

    def evict_lru(
        self, region: str, obj_size: int, cache_size: int, end_time: datetime
    ):
        """Evict objects from the region using LRU."""
        while self.region_tot_size[region] + obj_size > cache_size:
            lru_object = min(
                [obj.logical_object for obj in self.region_objects[region]],
                key=lambda x: x.last_modified,
            )
            lru_physical_object = lru_object.physical_objects[region]
            assert (
                lru_physical_object.storage_start_time is not None
            ), "Storage start time is None."
            assert (
                lru_physical_object.storage_start_time <= end_time
            ), f"Storage start time {lru_physical_object.storage_start_time} is greater than end time {end_time}."
            self.remove_object_from_region(region, lru_physical_object, end_time)

    def calculate_remaining_storage_costs(
        self, end_time: datetime, logical_objects: Dict[str, LogicalObject]
    ):
        """Calculate and aggregate storage costs for objects still stored."""
        ignore_timestamp = self.trace_start_time + timedelta(days=self.days_to_ignore)
        print(self.storage_costs)
        for region, objects in self.region_objects.items():
            # For each of the object
            print("remainig in", region)
            print(len(objects))
            for physical_object in objects:
                if physical_object.storage_start_time < ignore_timestamp:
                    if (
                        physical_object.ttl == -1
                        or len(
                            logical_objects[physical_object.key].physical_objects.keys()
                        )
                        == 1
                    ):
                        duration = end_time - max(
                            physical_object.storage_start_time, ignore_timestamp
                        )
                    else:
                        duration = min(
                            timedelta(seconds=physical_object.ttl),
                            end_time - ignore_timestamp,
                            end_time - physical_object.storage_start_time,
                        )
                else:
                    if (
                        physical_object.ttl == -1
                        or len(
                            logical_objects[physical_object.key].physical_objects.keys()
                        )
                        == 1
                    ):
                        duration = physical_object.storage_duration(end_time)
                    else:
                        if (
                            physical_object.storage_duration(end_time).total_seconds()
                            > physical_object.ttl
                        ):
                            duration = timedelta(seconds=physical_object.ttl)
                        else:
                            duration = physical_object.storage_duration(end_time)

                if duration < timedelta(0):
                    duration = timedelta(0)

                cost = self._price_per_GB(region, duration) * (
                    physical_object.size / GB
                )
                self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost

                if (
                    physical_object.logical_object.base_region is None
                    or region != physical_object.logical_object.base_region
                ):
                    cost_without_base = (
                        self._price_per_GB(region, duration) * physical_object.size / GB
                    )
                    self.storage_costs_without_base[region] = (
                        self.storage_costs_without_base.get(region, 0.0)
                        + cost_without_base
                    )

    def aggregate_storage_cost(self):
        for region, cost in self.storage_costs.items():
            self.logger.info(f"Storage cost for region {region}: {cost}.")
            print(f"Storage cost for region {region}: {cost}.")

        return sum(self.storage_costs.values())

    def aggregate_storage_cost_without_base(self):
        for region, cost in self.storage_costs_without_base.items():
            self.logger.info(f"Storage cost without base for region {region}: {cost}.")
            print(f"Storage cost without base for region {region}: {cost}.")

        return sum(self.storage_costs_without_base.values())

    def print_stat(self):
        self.logger.info(
            f"Total number of regions storing objects: {len(self.regions)}"
        )
        print_rst = [(region, len(objects)) for region, objects in self.regions.items()]
        self.logger.info(f"Number of objects stored in each region: {print_rst}")
        size = 0
        for _, objects in self.region_objects.items():
            for physical_object in objects:
                size += physical_object.size / GB

        assert size == sum(self.region_tot_size.values()) / GB
        self.logger.info(f"Sum of all object sizes stored in all regions: {size} GB.")
