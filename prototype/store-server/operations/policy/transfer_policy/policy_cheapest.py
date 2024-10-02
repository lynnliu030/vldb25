from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from typing import List
from operations.policy.transfer_policy.base import TransferPolicy
from operations.utils.conf import Status
from datetime import timedelta, datetime
from typing import Optional


class CheapestTransfer(TransferPolicy):
    def get(
        self,
        req: LocateObjectRequest,
        physical_locators: List[DBPhysicalObjectLocator],
        timestamp: Optional[datetime] = None,
    ) -> DBPhysicalObjectLocator:
        client_from_region = req.client_from_region
        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        return min(
            physical_locators,
            key=lambda loc: (
                self.stat_graph[loc.location_tag][client_from_region]["cost"],
                (
                    self.stat_graph[loc.location_tag][client_from_region]["latency"]
                    if "latency"
                    in self.stat_graph[loc.location_tag][client_from_region]
                    else 0.7
                ),
            ),
        )

    def name(self) -> str:
        return "cheapest"
