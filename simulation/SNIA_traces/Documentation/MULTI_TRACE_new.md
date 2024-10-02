# Generate a multi_region trace out of no_region:<br>

We can take a trace (IBM trace or synthetic trace) and transfer it to multi-cloud according to polic


## First step -> transfer to per obj dictionary
save the file in pickle format -> this stage can be done once to each trace

### Script name: parser.py<br>
input: csv file
output: pickle file
```
python3 parser_data.py IBMObjectStoreSample IBMObjectStoreSample.pickle
```

## Second step -> transfer to multi cloud trace according to the yaml configuration (The trace is not sorted)
### Script name: multi_cloud_trace_new.py<br>
input: pickle file
input: config yaml file
output: multi_cloud_trace according to the config -> unsorted
```
python3 multi_cloud_trace_new.py IBMObjectStoreSample.pickle  yaml/typeA.yaml IBMObjectStoreSample.mc.unsort
```

YAML example: 3 Regions, equal distribution for READ amd WRITE and typeA
```
EGIONS_WRITE: ["aws:us-east-1", "aws:us-west-2", "aws:eu-west-1"]
REGIONS_WRITE_PROB: [1, 1, 1]
REGIONS_READ: ["aws:us-east-1","aws:us-west-2", "aws:eu-west-1" ]
REGIONS_READ_PROB: [1, 1, 1]
TYPE: "typeA" # Random Puts and Gets

START_TIME: 0 days
END_TIME: 7 days
```

## Third step  -> sort the trace
 
```
sort -t, -k1,1n IBMObjectStoreSample.mc.unsort > IBMObjectStoreSample.mc
```


