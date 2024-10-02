from operations.policy.placement_policy.base import PlacementPolicy
from typing import List
from operations.policy.placement_policy.policy_replicate_all import ReplicateAll
from operations.policy.placement_policy.policy_single_region import SingleRegionWrite
from operations.policy.placement_policy.policy_push_on_write import PushonWrite
from operations.policy.placement_policy.policy_always_store import AlwaysStore
from operations.policy.placement_policy.policy_always_evict import AlwaysEvict
from operations.policy.placement_policy.policy_teven import Teven
from operations.policy.placement_policy.policy_fixed_ttl import FixedTTL
from operations.policy.placement_policy.policy_skystore import SkyStore


def eviction_policies() -> List[str]:
    return ["always_evict", "always_store", "fixed_ttl", "t_even", "skystore"]


def get_placement_policy(name: str, init_regions: List[str]) -> PlacementPolicy:
    if name == "single_region":
        return SingleRegionWrite(init_regions)
    elif name == "replicate_all":
        return ReplicateAll(init_regions)
    elif name == "push":
        return PushonWrite(init_regions)
    elif name == "always_store":
        return AlwaysStore.get_instance(init_regions)
    elif name == "always_evict":
        return AlwaysEvict(init_regions)
    elif name == "t_even":
        return Teven(init_regions)
    elif name == "fixed_ttl":
        return FixedTTL(init_regions)
    elif name == "skystore":
        return SkyStore.get_instance(init_regions)
    else:
        raise ValueError(f"Unknown policy name: {name}")
