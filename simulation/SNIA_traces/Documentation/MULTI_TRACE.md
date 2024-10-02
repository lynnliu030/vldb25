# Generate a multi_region trace out of no_region:<br>

We can take a trace (IBM trace or synthetic trace) and transfer it to multi-cloud according to policy

Script name: multi_cloud_trace.py<br>
Input: Trace_file (E.x.: IBMObjectStoreSample)<br>
       multi_cloud YAML (E.x.: mc.ymal) <br>
Output: Multi_cloud Trace (E.x.: IBMObjectStoreSample.mc)<br>

## Example synthetic data
### 2 Regions Random 

Init: PUTs: 50% region 1/ 50% region 2 <br>
Trace: Gets+PUsT - 50% region 1/ 50% region 2 <br>

```
python3 multi_cloud_trace.py one_hour.csv ymal/mc_7days_2regions_random.ymal one_hour_2region_random.csv

```
Init:<br>
PUTs: region 1<br>
Trace:<br>
GETs: region 2<br>
PUTs: 50% region 1 / 50% region 2<br>

### 2 Regions Burst
```
python3 multi_cloud_trace.py one_hour.csv ymal/mc_7days_2regions_burst.ymal one_hour_2region_burst.csv
``` 

## Example Trace 36 

### 2 Regions 

```
python3 multi_cloud_trace.py IBMObjectStoreTrace036Part0   ymal/mc_7days_2regions_random.ymal IBMObjectStoreTrace036Part0_2regions.mc
```

### 3 regions 
```
python3 multi_cloud_trace.py IBMObjectStoreTrace036Part0  ymal/mc_7days_3regions_1init_25p_random_25p_each.ymal IBMObjectStoreTrace036Part0_3regions.mc
```

```
python3 multi_cloud_trace.py IBMObjectStoreTrace036Part0  ymal/mc_7days_3regions_random.ymal IBMObjectStoreTrace036Part0_3regions.mc
```

