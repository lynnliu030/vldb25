from .policy_local import LocalWrite
from .policy_single_region import SingleRegionWrite
from .policy_always_store import AlwaysStore


# from .policy_oracle import Oracle
from .policy_spanstore import SPANStore
from .policy_replicate_all import ReplicateAll
from .policy_teven import Teven
from .policy_tevict_window import TevictWindow
from .policy_tevict import Tevict
from .policy_tevict_new import TevictV2
from .policy_tevict_ranges import TevictRanges
from .policy_tevict_ranges_new import TevictRangesV2
from .policy_optimal import Optimal
from .policy_optimal_v2 import OptimalV2
from .policy_lru import LRU
from .policy_fixed_ttl import Fixed_TTL
from .policy_keep import IndividualTTL
from .policy_ewma import EWMA
from .policy_always_evict import AlwaysEvict
from .policy_dynamic_ttl import DynamicTTL

# from .policy_skypie import SkyPIE
