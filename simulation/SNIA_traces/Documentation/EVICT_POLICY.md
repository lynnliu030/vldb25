# Cost logic

## Definitions:

net_cost: refers to the cost incurred for transporting an object from a distant region.<br>
storage_cost: The cost required to store an object in a specific region for a time period <br>
Teven: net_cost/storage_cost,  The time period between storing the object and retrieving it from a distant location<br>

## Definitions: 
net_cost: the network cost to bring the object from a far ragion<br>
storage_cost: the cost to store the object in this region <br>
Teven: net_cost/storage_cost, which is the even time between storing the object after a GET to bring the object again<br> 

## Logic 
The cost of each region is calculated separately.

If the client is in Region X and has a GET request for an object not in Region X ->  then the object is transferred from a far region.<br>
The cost is the network cost to bring the object and we cache the object with TTL time in Region X.<br>

If there was a GET request before the TTL time, get_diff_time is the time between the two GETs and the cost is the storage cost * get_diff_time.
The new GET gets the current TTL time.

After the TTL has expired, the object is evicted from Region X -> The cost is the storage cost * TTL time.<br>

After the eviction, if the client has another GET from region X, the object is transferred again from a far Region and the cost is the network cost.<br>

## Cacluate cost per Region X in a window 
This benchmark assumes two regions: all objects reside in the far region and all GET requests originate from Region X.<br>
We assume that each bucket has a TTL and calculate the cost of the GETs <br>
We calculate the GETS over a period of time (window) and assume that the TTL does not change during this period<br> 
We calculate the new TTL before the beginning of a new window using the Function: <br> 

calc_cost_tvict_ttl() <br>
inpu
- obj_dict - A per obj  dictionary - the key is the obj id and the values are all the REST requests for this object<br>
- storage_cost<br>
- net_cost<br> 
- TTL - the TTL for this window<br>
- start_window_time
- end_window_time<br>
- object_in_cache - All the objects that reside in this bucket at the beginning of the window <br>

We have a for loop of all the objects: in each object, we scan all the GETs in the time window<br> 

GETs are divided into three types
- The first GET - If the object is not cached, it is brought from the far region; otherwise, it may be cached in the region.<br>
- The Middle GET - Calculate the cost between the previous and new get based on the TTL (We start the calculation at the beginning of the window).<br>
- The last GET - This is the last GET of the window, so we calculate the cost until the end of the window.<br>

## Middle GET

Assuming that there are two GETs in the window, if the time difference between the two GETs is less than the TTL, then a cache hit has occurred <br>

 Otherwise a cache miss occurred, and the cost is the TTL*storage_cost + net_cost

```
 if (get_diff_time <= ttl*HOUR):
   cost += storage_cost_per_milli * get_diff_time * obj_size_GB                
else:    
   cost += storage_cost_per_milli * ttl*HOUR * obj_size_GB + net_cost * obj_size_GB
```


### First GET in the window: 

if the object is not in the cache (in the region).
Need to bring it from the far region
```
cost += net_cost * obj_size_GB
```

If the object is in the cache:
It has a previous GET before the beginning of the window.

--Previous_GET-----|[Start_window]--------------------First_GET_in_Win --------------------------|[End window]-------------------


If the new TTL is longer than the time difference between the previous GET and the beginning of the window.<br>
We evict the object at the beginning of the window, and the cost of the first GET is the network cost.<br>
```
if (time_till_win >= ttl*HOUR):                                    
   cost += net_cost * obj_size_GB
```

Otherwise, the logic is similar to the middle GET (the only difference is that the time of the storage cost is calculated from the beginning of the window).
```
get_diff_time = First_GET_in_Win - Previous_GET  
time_till_win = Start_window-Previous_GET
if (get_diff_time < ttl*HOUR): 
    cost += storage_cost_per_milli * (First_GET_in_Win-Start_window) * obj_size_GB 
else: # 
    cost += storage_cost_per_milli * (ttl*HOUR - time_till_win) * obj_size_GB + net_cost * obj_size_GB
```

Corner-cases: 
When there is an object in Cache and there is no GET in the Window.<br>
 In this case, the object is cached for the duration of its TTL <br>
 (in the case where the TTL is longer than the window, the cost is up the end of the window and the object remains in cache).<br>

```
leafover_time = ttl*HOUR - time_till_wie
 if (leafover_time < (window_size)):
    cost += storage_cost_per_milli * (leafover_time) * obj_size_GB 
else:
    cost += storage_cost_per_milli * (window_size) * obj_size_GB 
    cache_list.append(obj)
```


### last GET:

The last GET is stores for TTL time or by the end of the window in this case the object stays in cache. 

```
last_time = end_time_milli - int(obj_dict[obj][op][0])
if (last_time <= ttl*HOUR):
   cost += storage_cost_per_milli * last_time * obj_size_GB
   cache_list.append(obj)
else:    
   cost += storage_cost_per_milli * ttl*HOUR * obj_size_GB
   last_cost += storage_cost_per_milli * ttl*HOUR * obj_size_GB
```

# Caculate histogram:

As part of our policy, we need to calculate a histogram in a window. <br>
A histogram is produced per hour based on the time difference between two consecutive GETs in each object. <br>
Histogram in the first position, containing all difference times between GETs equal to or smaller than one hour. <br>
The second position contains all difference times between GETs equal to or smaller than two hours and bigger than one hour and so on. <br>


The histogram of the consecutive GETs are: <br>
get_diff_hist - Count of GETs whose time difference was smaller or equal to the time in the histogram position and larger than the position-1 hour. <br>
get_diff_size_hist - The size in Bytes of all the GETs whose time difference was smaller or equal to the time in the histogram position and larger than the position-1 hour.<br>

For example We have 3 GETs in an object of size 1000B, the first time difference diff was 2.5, and the second diff was 4.6.<br>
The get_diff_size_hist will look like that <br>
hist - 1H, 2H, 3H, 4H, 5H, ... <br>
get_diff_hist - 0, 0, 1, 0, 0, 1, ...<br>
get_diff_size_hist - 0, 0, 1000, 0, 0, 1000, .... <br>

A histogram is also calculated for the last GET. <br>
We measure the time of the last GET till the end of the window. <br>

The histogram of the last GETs <br>
last_diff_hist -  Count of last GETs whose time difference was smaller or equal to the time in the histogram position and larger than the position-1 hour. <br>
last_diff_size_hist - The size in Bytes of all the GETs whose time difference was smaller or equal to the time in the histogram position and larger than the position-1 hour. <br>

Example: In our example, let us assume that the time difference between the last GET and the end of the window is 1.5 hours <br>
hist - 1H, 2H, 3H, 4H, 5H, ... <br>
last_diff_hist - 0, 1, 0, 0, 0, 0, ... <br>
last_diff_size_hist - 0, 1000, 0, 0, 0, 0, .... <br>




# Calculae TTL:

In our algorithm, the size histograms (get_diff_size_hist, last_diff_size_hist) are used to calculate TTL.<br>

Using the histogram, we calculate the cost for each TTL over 1H, 2H, etc.<br>
If the TTL is larger than the time in the histogram, then it means we stored the for the time difference between the GETs and we had a cache hit<br>
The cost is calculated by multiplying (Histogram_time + 0.6H (Middle of the hour)) by the number of bytes stored in the histogram position.<br>
Otherwise, we had a cache miss, and the cost is equal to (TTL*storage + network cost) * bytes stored in the histogram position.<br>

For the last GET, we assume no more GET requests will be made, so the cost is TTL multiplied by the number of bytes stored in the histogram position.<br>

```
def calc_evict_cost(X, in_dict, teven_hours, net_cost):
    storage_cost_hour = net_cost / teven_hours
    cost_hist = [0] * (len(X)+1)
    for c in range(len(X)+1):
        TTL = c 
        for i in range(len(X)):
            if ((i+1)< TTL):
                cost_hist[c] += float(get_diff_size_hist[i])/GB * (i+0.6) * storage_cost_hour 
            else:
                cost_hist[c] += float(get_diff_size_hist[i]) / GB * ((TTL * storage_cost_hour) + net_cost)
            cost_hist[c] += float(last_diff_size_hist[i]) / GB * ((TTL * storage_cost_hour))

    return (cost_hist)
```

For choosing the right TTL, we find the TTL with the lowest cost in the training window time among all the possible TTLs.
