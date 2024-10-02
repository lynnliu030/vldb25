# IBM TRACES from SNIA

Download from SNIA: [link](http://iotta.snia.org/traces/key-value)

Paper:It's time to revisit LRU vs. FIFO [link](https://research.ibm.com/publications/its-time-to-revisit-lru-vs-fifo))<br>
Blog: Object Storage Traces: A Treasure Trove of Information for Optimizing Cloud Workloads [link](https://www.ibm.com/blog/object-storage-traces/)<br>
Info:

There are 98 traces each for a single bucket.<br>
Each trace contains the REST operations issued against a single bucket in IBM Cloud Object Storage during the same single week in 2019.

The format of each trace record is <time stamp of request>, <request type>, <object ID>, <optional: size of object>, <optional: beginning offset>, <optional: ending offset>.  The timestamp is the number of milliseconds from the point where we began collecting the traces.

```
For example: 
1219008 REST.PUT.OBJECT 8d4fcda3d675bac9 1056
1221974 REST.HEAD.OBJECT 39d177fb735ac5df 528
1232437 REST.HEAD.OBJECT 3b8255e0609a700d 1456
```

# Transfer trace to per object
The transfer is for furthere analytics and statistics.
It creates a dictionary, where the key is the key and the value is a list of all the operations for this key.

Each item in the list is a list with a fix items:
```
[0] - "timestamp"
[1] - "request type"
[2] - "size"
[3] - "range_rd_begin"
[4] - "range_rd_end"
```

### Script name: parser.py<br>
input: csv file
output: pickle file
```
python3 ../parser_data.py ../obj/pickle/IBMObjectStoreTrace036Part0.pickle  ../obj/pickle/IBMObjectStoreTrace036Part0.pickle 
```

### Script name: parse augmented trace
input: csv file (augmented trace)
output: pickle file
```
python3 ../parser_aug_data.py ../obj/pickle/IBMObjectStoreTrace036Part0.pickle  ../obj/pickle/IBMObjectStoreTrace036Part0.pickle 
```

# Generate Synthetic Traces (no region)

We can Create a synthetic traces<br>
Here is the link to the documantation [link](Documentation/SYNTHETIC_TRACE.md)<br>

```
timestamp op obj_key size range_rd_begin range_rd_end
110 REST.GET.OBJECT P1_8131 788402576
646 REST.GET.OBJECT P1_9858 748756940
761 REST.GET.OBJECT P1_4403 616310209
1204 REST.GET.OBJECT P1_2955 855639157
1714 REST.GET.OBJECT P1_1767 512734669
1921 REST.GET.OBJECT P1_2871 1038093396
```

Script name: synthetic_trace.py<br>
Input: synthetic YMAL <br>
Output: The name of the output trace <br>

We have a basic YMAL trace and we can overload its parameters using args<br>
--gets - change the number of gets<br>
--objs - change the number of objects <br>
--get_put_ratio - change the get put ratio <br>

Assume we have 7 days (168 hours) trace, so if you wish to have on average  one get per hour, the number of gets should be 168<br>

The basic YMAL has 10000 Objects
The size is: 
50% in the range of 100KB-1KB<br>
47% in the range of 1MB-10MB<br>
3% in the range of 1GB-1TB<br>
 
## Example: Create Synthetic trace

#### One Hit Wonder - OBJs 1M (no puts)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal onehitwonder.csv --gets 1 --objs 1000000 --get_put_ratio N/A 
```

#### Every 1 hour - OBJs 10000 / Gets 168 (over the week) /  PUT 24 (over the week) (get_put_ratio 1:7)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal one_hour.csv --gets 168 --objs 10000 --get_put_ratio 7 
```

# Generate a multi_region trace out of no_region:<br>

We can take a trace (IBM trace or synthetic trace) and transfer it to multi-cloud according to policy
For more details [link](Documentation/MULTI_TRACE.md) 

Script name: multi_cloud_trace.py<br>
Input: Trace_file (E.x.: IBMObjectStoreSample)<br>
       multi_cloud YAML (E.x.: mc.ymal) <br>
Output: Multi_cloud Trace (E.x.: IBMObjectStoreSample.mc)<br>

## Example synthetic data
### 2 Regions Random 

Init: PUTs: 50% region 1/ 50% region 2 <br>
Trace: Gets+PUsT - 50% region 1/ 50% region 2 <br>

```
python3 multi_cloud_trace.py one_hour.csv ymal/mc_7days_2regions_random.ymal
```


# How to run test_evict_policy.py <br>
This script runs a 168 hours trace and calculates the cost according to different policies:<br>
The script runs the same network cost and changes the Tevict to 24H, 48H, 72H <br>
[link](https://github.com/lynnliu030/SkyStore-Simulation/tree/main/SNIA_traces/plots#how-to-run-test_evict_policypy-)

For our algorithm evict policy explanation, see [here](https://github.com/lynnliu030/SkyStore-Simulation/blob/main/SNIA_traces/Documentation/EVICT_POLICY.md)



### Running new trace gen
This script generates script depending on the config that is passed in. It also takes in a path to a pickled trace file, an output file for the actual trace, and an option to augment the trace with some additional info. The augment trace is required if running simulator v2 which relies on this additional data to speed up and optimize the memory usage for the optimal policy. The order of arguments go path to pickled trace, generation config, output file, and --augment.

The geneartion config is a yaml file which will change depending on the type of trace that needs to be generated. For exmaple, typeA will look different than typeC since typeC is group based rather than object based. However, all traces will have `POSSIBLE_REGIONS` as a list of regions that can be used as a region in the trace.

Here is an example of running the script
```
python3 new_trace_gen.py ../traces/raw_traces/IBMObjectStoreTrace095Part0.pickle yaml/newtypeC.yaml ../traces/raw_traces/test/95/IBMObjectStoreTrace095Part0.typeC.mc --augment True
```