from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from typing import List
from operations.policy.transfer_policy.base import TransferPolicy
from datetime import datetime
from typing import Optional


class Manual(TransferPolicy):
    def __init__(self) -> None:
        super().__init__()

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
        if req.op != "GET":
            for locator in physical_locators:
                if client_from_region == locator.location_tag:
                    return locator

        # extract the next line from the csv, must follow the order

        if req.op == "GET":
            idx = self.line_idx.get()
            if idx >= len(self.data):
                raise Exception("No more data in the csv")

            line = self.data.iloc[self.line_idx.get()]

            # extract issue region and answer region, trim the white space
            csv_issue_region = line["issue_region"].strip()
            csv_answer_region = line["answer_region"].strip()

            assert (
                csv_issue_region == client_from_region
            ), f"Client: Expected {client_from_region}, got {csv_issue_region}"

            # return the locator from answer region
            for locator in physical_locators:
                if csv_answer_region == locator.location_tag:
                    self.line_idx.increment()
                    return locator

            # raise error if no match
            raise Exception(
                f"Line idx: {self.line_idx}, No matching region found for {csv_answer_region}, physical locators: {physical_locators}"
            )
        else:
            # if others, should return earlier already
            raise Exception("Invalid operation")

    def name(self) -> str:
        return "direct"
