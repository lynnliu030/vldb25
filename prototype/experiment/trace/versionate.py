import csv
from collections import defaultdict
from datetime import datetime
import pandas as pd
import csv

trace_path = "./test2.csv"
out_path = "./test2-ver.csv"
versions = defaultdict(int)


# trace_path = "../NSDI/029_2_reg/IBMObjectStoreTrace029Part0.typeE.typeE.2_regions.mc"
# out_path = "../NSDI/augmented_029_2_reg/IBMObjectStoreTrace029Part0.typeE.mc-aug"

def parse_timestamp(ts_string):
    return datetime.strptime(ts_string, '%Y-%m-%d %H:%M:%S')

def format_timedelta(td):
    return str(td.total_seconds()) if td else 'N/A'

# First pass: collect information
print("start first pass")
obj_accesses = defaultdict(list)
obj_access_region = {}
with open(trace_path, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        timestamp = int(row['timestamp'])
        obj_key = row['obj_key']
        issue_region = row['issue_region']
        obj_accesses[obj_key].append(timestamp)
        if obj_key not in obj_access_region:
            obj_access_region[obj_key] = defaultdict(list)
        obj_access_region[obj_key][issue_region].append(timestamp)
print("fin first pass")

# Sort accesses for each object
# for obj_key in obj_accesses:
#     obj_accesses[obj_key].sort()

# Second pass: calculate and write output
with open(trace_path, 'r') as infile, open(out_path, 'w', newline='') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['time_to_next_access', 'time_to_next_access_same_reg']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    current_index = defaultdict(int) # obj_key -> current index
    current_index_region = {} # obj_key -> region -> current index

    for row in reader:
        timestamp = int(row['timestamp'])
        obj_key = row['obj_key']
        issue_region = row['issue_region']
        
        accesses = obj_accesses[obj_key]
        accesses_reg = obj_access_region[obj_key][issue_region]
        idx = current_index[obj_key]
        if obj_key not in current_index_region:
            current_index_region[obj_key] = defaultdict(int)
        reg_idx = current_index_region[obj_key][issue_region]
        
        # Time to next access
        if idx < len(accesses) - 1:
            time_to_next = accesses[idx + 1]
        else:
            time_to_next = None
        
        # Time to next access in different region
        # Time to next access
        if reg_idx < len(accesses_reg) - 1:
            time_to_next_sm_reg = accesses_reg[reg_idx + 1]
        else:
            time_to_next_sm_reg = None
        
        current_index[obj_key] += 1
        current_index_region[obj_key][issue_region] += 1
        
        row['time_to_next_access'] = time_to_next if time_to_next is not None else -1
        row['time_to_next_access_same_reg'] = time_to_next_sm_reg if time_to_next_sm_reg is not None else -1
        
        writer.writerow(row)
    