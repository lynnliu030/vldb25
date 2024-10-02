from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from typing import List
from operations.policy.transfer_policy.base import TransferPolicy
import random
from datetime import datetime
from typing import Optional


class DirectTransfer(TransferPolicy):
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
            DBPhysicalObjectLocator: the single matched region to fetch from
        """

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # just return a random sampled locator
        return random.choice(physical_locators)

    def name(self) -> str:
        return "direct"
