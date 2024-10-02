import csv
import argparse
from typing import Dict, List
import yaml
import random
import parser_data
from collections import defaultdict
from datetime import datetime
import os

#######################################################################################################################
## This script transforms a basic trace into a multi-cloud trace
## It adds regions based on specifications from a YAML file.
## The script creates two output files:
##     - multi-cloud trace
##     - A trace of writes to create an initial state before the simulation begins.
#######################################################################################################################


MILLI = 1
SEC = 1000 * MILLI
MIN = 60 * SEC
HOUR = 60 * MIN
DAY = 24 * HOUR

# Setting a seed for the random number generator
random.seed(1)

###################################
##### Load the input trace ########
###################################


def time_in_milliseconds(t):
    milli = 0
    seconds = 0
    minutes = 0
    hours = 0
    days = 0
    t_list = t.split("+")

    for i in range(len(t_list)):
        if "day" in t_list[i]:
            days = float(t_list[i].split("day")[0])
            continue
        if "hour" in t_list[i]:
            hours = float(t_list[i].split("hour")[0])
            continue
        if "minute" in t_list[i]:
            minutes = float(t_list[i].split("minute")[0])
            continue
        if "second" in t_list[i]:
            seconds = float(t_list[i].split("second")[0])
            continue
        if "milli" in t_list[i]:
            milli = float(t_list[i].split("milli")[0])
            continue
        else:
            print(f"time is not correct {t} ->  see format")
            exit(0)
    return int(
        days * DAY + hours * HOUR + minutes * MIN + seconds * SEC + milli * MILLI
    )


###################################
##### Load the config file ########
###################################


def load_ymal(config_file):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


def add_region(op_dict, regions_list, regions_prob_list):
    for region in op_dict.keys():
        regions_list.append(region)
        regions_prob_list.append(float(op_dict[region].replace("%", "")))
    return 0


##############################################
#  Add a row to the output multi_cloud trace
##########################################


def add_row_to_mc_trace(input_dict, input_position, policy_dict, mc_output_dict):
    # regions configuration
    regions_list = []
    regions_prob_list = []

    for op in ["GET", "PUT", "HEAD", "DELETE", "COPY"]:
        if op in input_dict["op"][input_position]:
            add_region(policy_dict[op], regions_list, regions_prob_list)
    if "N/A" in regions_list[0]:
        return 0
    chosen_region = random.choices(regions_list, regions_prob_list, k=1)[0]
    for key in input_dict.keys():
        mc_output_dict[key].append(input_dict[key][input_position])
    mc_output_dict["issue_region"].append(chosen_region)


######################################
######## OBJECT CLASS ###############
######################################


class Object:
    def __init__(
        self, name: str, size: int, ops: list[str], access_times: list[int], config
    ):
        self.name = name
        self.size = size
        self.ops = ops
        self.timestamp = access_times  # Take same recency as the actual trace
        self.regions: List[str] = []

    def update_regions_list(self, type_obj):
        for i in range(len(self.ops)):
            if "PUT" in self.ops[i]:
                self.regions.append(type_obj.pick_region(self, True))
                continue
            if "GET" in self.ops[i]:
                self.regions.append(type_obj.pick_region(self, False))
                continue

    def add_init_put(self):
        if self.ops[0] not in "PUT":
            # need to add an initial PUT on time zero
            self.ops.insert(0, "PUT")
            self.timestamp.insert(0, "0")

    def write_obj(self, csv_writer):
        for i in range(len(self.ops)):
            csv_writer.writerow(
                [self.timestamp[i], self.ops[i], self.regions[i], self.name, self.size]
            )


######################################
######## Types CLASS ###############
######################################


class TraceType:
    """
    Template
    """

    def __init__(self, config):
        pass

    def pick_region(self):
        pass


class Group:
    def __init__(
        self, put_regions: list, put_prob: list, get_regions: list, get_prob: list
    ):
        self.put_regions = put_regions
        self.put_prob = put_prob
        self.get_regions = get_regions
        self.get_prob = get_prob


class TraceTypeA(TraceType):
    """
    Truly random for GETs and Puts
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        self.groups = {}

    def pick_region(self, _: Object, __: bool):
        return random.choice(self.regions)


class TraceTypeB(TraceType):
    """
    Puts for each objects in 1 regions, Gets for each object in other regions
    - self.group_prob is probability of getting group i
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        # config contains number of groups, prob of each group
        self.groups = {}
        self.obj_to_group = {}  # key -> group id (0-k)

    def pick_region(self, object: Object, isPut: bool):
        if object.name not in self.obj_to_group:
            put_region = random.choices(self.regions)
            get_region = random.choices(self.regions)
            self.obj_to_group[object.name] = Group(put_region, [1], get_region, [1])
        group = self.obj_to_group[object.name]
        if isPut:
            return random.choices(group.put_regions, weights=group.put_prob, k=1)[0]
        else:
            return random.choices(group.get_regions, weights=group.get_prob, k=1)[0]


class TraceTypeBManual(TraceType):
    """
    Puts for each objects in 1 regions, Gets for each object in other regions
    - self.group_prob is probability of getting group i
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        self.num_groups = config["NUM_GROUPS"]
        self.group_prob = config["GROUP_PROB"]
        # config contains number of groups, prob of each group
        self.groups = {}
        self.groups[0] = Group(["aws:us-east-1"], [1], ["gcp:us-west1-a"], [1])
        self.groups[1] = Group(["azure:westeurope"], [1], ["aws:us-east-1"], [1])
        self.groups[2] = Group(["gcp:us-west1-a"], [1], ["azure:westeurope"], [1])
        self.obj_to_group = {}  # key -> group id (0-k)

    def pick_region(self, object: Object, isPut: bool):
        if object.name not in self.obj_to_group:
            self.obj_to_group[object.name] = random.choices(
                list(range(self.num_groups)), weights=self.group_prob, k=1
            )[0]
        group = self.groups[self.obj_to_group[object.name]]
        if isPut:
            return random.choices(group.put_regions, weights=group.put_prob, k=1)[0]
        else:
            return random.choices(group.get_regions, weights=group.get_prob, k=1)[0]


class TraceTypeC(TraceType):
    """
    Puts for each objects in 1 regions, Gets for each object in other regions
    - self.group_prob is probability of getting group i
    - customize put region prob?
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        self.num_groups = config["NUM_GROUPS"]
        self.group_prob = config["GROUP_PROB"]
        # config contains number of groups, prob of each group
        self.groups = {}
        for k in range(self.num_groups):
            get_region = random.choices(
                self.regions
            )  # currently do not allow same region
            self.groups[k] = Group(
                self.regions, [1] * len(self.regions), get_region, [1]
            )
        self.obj_to_group = {}  # key -> group id (0-k)

    def pick_region(self, object: Object, isPut: bool):
        if object.name not in self.obj_to_group:
            self.obj_to_group[object.name] = random.choices(
                list(range(self.num_groups)), weights=self.group_prob, k=1
            )[0]
        group = self.groups[self.obj_to_group[object.name]]
        if isPut:
            return random.choices(group.put_regions, weights=group.put_prob, k=1)[0]
        else:
            return random.choices(group.get_regions, weights=group.get_prob, k=1)[0]


class TraceTypeD(TraceType):
    """
    Puts for each objects in 1 regions, Gets for each object in other regions
    - maybe add parameter to limit the gets to a subset of all regions
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        self.groups = {}

    def pick_region(self, object: Object, isPut: bool):
        if object.name not in self.groups:
            put_region = random.choices(self.regions)
            self.groups[object.name] = Group(
                put_region, [1], self.regions, [1] * len(self.regions)
            )
        group = self.groups[object.name]
        if isPut:
            return random.choices(group.put_regions, weights=group.put_prob, k=1)[0]
        else:
            return random.choices(group.get_regions, weights=group.get_prob, k=1)[0]


class TraceTypeE(TraceType):  # old type F (combined A-D)
    """
    Randomly choose a type.
    """

    def __init__(self, config):
        self.regions = config["POSSIBLE_REGIONS"]  # config contains POSSIBLE_REGIONS
        self.type_distribution = config["TYPE_DISTRIBUTION"]
        self.groups: Dict[str, Group] = {}
        A = TraceTypeA(config["TYPE_A"])
        B = TraceTypeB(config["TYPE_B"])
        C = TraceTypeC(config["TYPE_C"])
        D = TraceTypeD(config["TYPE_D"])
        self.groups[0] = A
        self.groups[1] = B
        self.groups[2] = C
        self.groups[3] = D
        self.obj_to_group = {}  # key -> group id (0-k)

    def pick_region(self, object: Object, isPut: bool):
        if object.name not in self.obj_to_group:
            self.obj_to_group[object.name] = random.choices(
                list(range(4)), weights=self.type_distribution, k=1
            )[0]
        group = self.groups[self.obj_to_group[object.name]]
        return group.pick_region(object, isPut)


########################################
### HELP Functions for obj format ######
########################################


def only_put_gets(obj_items: list):
    puts_gets = []
    for i in range(len(obj_items)):
        if "GET" in obj_items[i][1] or "PUT" in obj_items[i][1]:
            puts_gets.append(obj_items[i])
    return puts_gets


def list_of_timestamp_ops(obj_items: list):
    list_timestamp = []
    list_ops = []
    for i in range(len(obj_items)):
        list_timestamp.append(obj_items[i][0])
        list_ops.append(obj_items[i][1])
    return list_timestamp, list_ops


########## Main  ###########


def main():
    parser = argparse.ArgumentParser(
        description="Create a multi cloud trace from a trace"
    )
    parser.add_argument("obj_file", help="Path to the trace file in a pickle format")
    parser.add_argument("config_file", help="Path to the YAML config file")
    parser.add_argument(
        "save_mc_file", default=None, help="The output PATH of the multi cloud trace"
    )
    parser.add_argument(
        "--with_range_rd", action="store_true", help="add range read to the trace"
    )
    parser.add_argument(
        "--augment", default=False, help="also create an augmented trace"
    )
    args = parser.parse_args()

    file_handler = open(args.save_mc_file + "temp", "w")
    csv_writer = csv.writer(file_handler)
    #    csv_writer.writerow(["timestamp", "ops" , "issue_region" , "obj_key", "size"])
    print("loading input trace")
    obj_dict, tot_op = parser_data.load_obj_dict(args.obj_file)
    print("finish loading input trace")
    config_dict = load_ymal(args.config_file)
    print(config_dict)

    type_obj: TraceType = TraceType(config_dict)
    trace_type = config_dict["TYPE"]
    if trace_type == "typeA":
        type_obj = TraceTypeA(config_dict)
    elif trace_type == "typeB":
        type_obj = TraceTypeB(config_dict)
    elif trace_type == "typeC":
        type_obj = TraceTypeC(config_dict)
    elif trace_type == "typeD":
        type_obj = TraceTypeD(config_dict)
    elif trace_type == "typeE":
        type_obj = TraceTypeE(config_dict)
    elif trace_type == "typeBManual":
        type_obj = TraceTypeBManual(config_dict)
    else:
        raise Exception("Wrong trace type")

    file_handler.write("timestamp,op,issue_region,obj_key,size\n")

    for key in obj_dict.keys():
        name = key
        puts_gets = only_put_gets(obj_dict[key])
        if len(puts_gets) == 0:
            continue
        size = puts_gets[0][2]
        list_timestamp, list_ops = list_of_timestamp_ops(puts_gets)
        config_obj = config_dict.copy()
        # if config_dict["TYPE"] == "typeF":
        #     # choose one type for this OBJ
        #     type_obj = random.choices(config_dict["TYPES"], config_dict["TYPES_PROB"], k=1)[0]
        #    config_obj = fill_config_obj(config_obj, type_obj)
        obj = Object(
            name=name,
            size=size,
            ops=list_ops,
            access_times=list_timestamp,
            config=config_obj,
        )
        obj.add_init_put()
        obj.update_regions_list(type_obj)
        obj.write_obj(csv_writer)
    file_handler.close()

    with open(args.save_mc_file + "temp", mode="r", newline="") as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header
        data = list(reader)  # Read the rest of the data

    # Sort the data by the timestamp in the first column
    data.sort(key=lambda row: float(row[0]))

    # Write the sorted data back to a new CSV file
    with open(args.save_mc_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)  # Write the header
        writer.writerows(data)  # Write the sorted data

    # delete args.save_mc_file + "temp"
    os.remove(args.save_mc_file + "temp")

    if args.augment:  # augment if needed
        # First pass: collect information
        print("start first pass")
        obj_accesses = defaultdict(list)
        obj_access_region = {}
        with open(args.save_mc_file, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                timestamp = int(row["timestamp"])
                obj_key = row["obj_key"]
                issue_region = row["issue_region"]
                obj_accesses[obj_key].append(timestamp)
                if obj_key not in obj_access_region:
                    obj_access_region[obj_key] = defaultdict(list)
                obj_access_region[obj_key][issue_region].append(timestamp)
        print("fin first pass")

        # Second pass: calculate and write output
        with open(args.save_mc_file, "r") as infile, open(
            args.save_mc_file + ".aug", "w", newline=""
        ) as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + [
                "time_to_next_access",
                "time_to_next_access_same_reg",
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            current_index = defaultdict(int)  # obj_key -> current index
            current_index_region = {}  # obj_key -> region -> current index

            for row in reader:
                timestamp = int(row["timestamp"])
                obj_key = row["obj_key"]
                issue_region = row["issue_region"]

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

                row["time_to_next_access"] = (
                    time_to_next if time_to_next is not None else -1
                )
                row["time_to_next_access_same_reg"] = (
                    time_to_next_sm_reg if time_to_next_sm_reg is not None else -1
                )

                writer.writerow(row)


def parse_timestamp(ts_string):
    return datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")


def format_timedelta(td):
    return str(td.total_seconds()) if td else "N/A"


if __name__ == "__main__":
    main()
