# SkyStore-Simulation
## Getting Started 
To setup the environment:

- Ensure that you have Python toolchain installed 
```bash
# Install dependencies 
pip install -r requirements.txt
# Install optimizer code 
cd op; pip install -e . 
```

- After making code changes, ensure your code is autoformatted
```
black -l 140 .
```

## Trace Format 
```
timestamp, op, issue_region, obj_key, size
2023-10-12 10:00:01,read,aws:us-west-1,1234,1000000
2023-10-12 10:00:02,read,aws:us-east-1,1234,1000000
```

## Config Format 
```yml
placement_policy: "always_evict"
transfer_policy: "cheapest"
```

## Usage 
```bash 
python main.py --config [CONFIG] --trace [TRACE] --vm [NUM_VMS] --setbase [SET_BASE] --simversion [SIM_VERSION]
```

## Set Base
--setbase is an option for the simulator to run the simnulation with a fixed base region or no base region set. The difference is with a fixed base region, it will be true that an object is always stored in the base region. The base region is dictated by the initial PUT of an object. Without a base region, the first PUT could potentially be evicted. However, with no base region, there will always be at least one copy of each object

## Simulator Version
There are two simulator versions. Version 0 utilizes a priority queue to schedule different events. Such events include eviction, start transfer, and complete transfer events. The downside of using such an approach is there may be way too many events that result from a lot of evictions, thus slowing down the simulator. Version 1 of the simulator aims to fix this issue and does so by not using an event based approach. Version 1 uses the current events timestamp as a way to determine what objects should exist or not exist (due to evictions). In this repository, some files include `[file name]_v2.py` which are files used by simulator version 1. Otherwise, simulator v0's files can still be used in simulator v1. Notable files include `src/placement_policy/policy_tevict_new.py`, `src/placement_policy/policy_tevict_ranges_new.py`, and `src/transfer_policy/policy_cheapest_v2.py`. The new transfer policy deals with removing stale objects from the object store now.

We are currently using simulator v1.