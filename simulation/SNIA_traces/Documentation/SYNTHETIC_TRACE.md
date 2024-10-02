# Generate Synthetic Traces (no region)

We can Create a synthetic traces<br>
Here is the link to the documantation<br>

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

#### Every 2 hours - OBJs 10000 / Gets 84 (over the week) /  PUT 12 (over the week) (get_put_ratio 1:7)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal two_hours.csv --gets 84 --objs 10000 --get_put_ratio 7 
```

#### Every 6 hours - OBJs 10000 / Gets 28 (over the week) /  PUT 4 (over the week) (get_put_ratio 1:7)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal six_hours.csv --gets 28 --objs 10000 --get_put_ratio 7 
```

#### Every 12 hours - OBJs 10000 / Gets 14 (over the week) /  PUT 2 (over the week) (get_put_ratio 1:7)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal half_day.csv --gets 14 --objs 10000 --get_put_ratio 7 
```

#### Every 24 hours - OBJs 10000 / Gets 7 (over the week) /  PUT 1 (over the week) (get_put_ratio 1:7)
```
python3 synthetic_trace.py ymal/synthetic_onehitwonder.ymal one_day.csv --gets 7 --objs 10000 --get_put_ratio 7 
```


