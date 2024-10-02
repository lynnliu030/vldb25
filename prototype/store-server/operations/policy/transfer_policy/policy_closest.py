from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from typing import List
from operations.policy.transfer_policy.base import TransferPolicy
from datetime import datetime
from typing import Optional


class ClosestTransfer(TransferPolicy):
    def get(
        self,
        req: LocateObjectRequest,
        physical_locators: List[DBPhysicalObjectLocator],
        timestamp: Optional[datetime] = None,
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req: LocateObjectRequest
            physical_locators: List[DBPhysicalObjectLocator]: physical locators of the object
        Returns:
            DBPhysicalObjectLocator: the closest physical locator to fetch from
        """

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # find the closest region to get from client_from_region
        return max(
            physical_locators,
            key=lambda loc: self.stat_graph[client_from_region][loc.location_tag][
                "throughput"
            ],
        )

    def name(self) -> str:
        return "closest"
