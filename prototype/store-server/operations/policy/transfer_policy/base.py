from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from operations.policy.utils.helpers import make_nx_graph, make_csv
from typing import List, Optional
from datetime import datetime


class DataTransferGraph:
    """
    A singleton class representing the graph used for data transfer calculations.
    This ensures that only one instance of the graph is created and used throughout the application.
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls._create_graph()
        return cls._instance

    @staticmethod
    def _create_graph():
        return make_nx_graph()


class ManualPolicyCSV:
    """
    Manual policy that reads from a CSV file to determine the next region to fetch from.
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls._create_graph()
        return cls._instance

    @staticmethod
    def _create_graph():
        return make_csv()


class StatefulInteger:
    _instance = None

    def __init__(self):
        self._value = 0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = StatefulInteger()
        return cls._instance

    def get(self) -> int:
        return self._value

    def increment(self) -> None:
        self._value += 1


class TransferPolicy:
    def __init__(self) -> None:
        self.stat_graph = DataTransferGraph.get_instance()
        self.data = ManualPolicyCSV.get_instance()
        self.line_idx = StatefulInteger.get_instance()
        pass

    def get(
        self,
        req: LocateObjectRequest,
        physical_locators: List[DBPhysicalObjectLocator],
        timestamp: Optional[datetime] = None,
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req (LocateObjectRequest): request to locate an object
            physical_locators (List[DBPhysicalObjectLocator]): list of candidate physical locators (ready, and contains the object)

        Returns:
            DBPhysicalObjectLocator: return the selected physical locator
        """
        pass

    def name(self) -> str:
        """
        Returns:
            str: policy name
        """
        return ""
