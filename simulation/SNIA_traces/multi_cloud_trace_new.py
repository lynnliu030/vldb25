import csv
import argparse
from typing import List
import pandas as pd
import yaml
import random
from multi_cloud_trace import add_region
import parser_data

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
        self.possible_regions = config["possibleRegions"].copy()
        self.wr_regions = config["REGIONS_WRITE"].copy()
        self.wr_regions_prob = config["REGIONS_WRITE_PROB"].copy()
        self.rd_regions = config["REGIONS_READ"].copy()
        self.rd_regions_prob = config["REGIONS_READ_PROB"].copy()
        self.type = config["TYPE"]

    def choose_region(self, regions: str, regions_prob):
        return random.choices(regions, regions_prob, k=1)[0]

    def update_regions_list(self):
        for i in range(len(self.ops)):
            if "PUT" in self.ops[i]:
                self.regions.append(
                    self.choose_region(self.wr_regions, self.wr_regions_prob)
                )
                continue
            if "GET" in self.ops[i]:
                self.regions.append(
                    self.choose_region(self.rd_regions, self.rd_regions_prob)
                )
                continue

    def add_init_put(self):
        if self.ops[0] not in "PUT":
            # need to add an initial PUT on time zero
            self.ops.insert(0, "PUT")
            self.timestamp.insert(0, "0")

    def typeA(self):
        #  Random Puts and Gets
        self.update_regions_list()

    def typeB(self):
        # Puts in one region, Gets in one regions (obj level)
        self.rd_regions = [self.choose_region(self.rd_regions, self.rd_regions_prob)]
        self.rd_regions_prob = [1]
        self.wr_regions = [self.choose_region(self.wr_regions, self.wr_regions_prob)]
        self.wr_regions_prob = [1]
        self.update_regions_list()

    def typeC(self):
        # Random Puts, Gets in one other region
        self.rd_regions = [self.choose_region(self.rd_regions, self.rd_regions_prob)]
        self.rd_regions_prob = [1]
        #        del_index = self.wr_regions.index(self.rd_regions[0])
        #        del self.wr_regions[del_index]
        #        del self.wr_regions_prob[del_index]
        self.update_regions_list()

    def typeD(self):
        # Puts in one region, Gets distributed across regions
        self.wr_regions = [self.choose_region(self.wr_regions, self.wr_regions_prob)]
        self.wr_regions_prob = [1]
        #        del_index = self.rd_regions.index(self.wr_regions[0])
        #        del self.rd_regions[del_index]
        #        del self.rd_regions_prob[del_index]
        self.update_regions_list()

    def typeE(self):
        # All the puts in a Single Region, the Gets  on other
        self.update_regions_list()
        return 0

    def typeF(self):
        return 0

    type_mapping = {
        "typeA": typeA,
        "typeB": typeB,
        "typeC": typeC,
        "typeD": typeD,
        "typeE": typeE,
    }  # Random GETs and PUTs

    def type_handler(self):
        type_to_call = self.type_mapping.get(
            self.type, lambda: print("No function for this type")
        )
        type_to_call(self)

    def write_obj(self, csv_writer):
        for i in range(len(self.ops)):
            csv_writer.writerow(
                [self.timestamp[i], self.ops[i], self.regions[i], self.name, self.size]
            )


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


def fill_config_obj(config, type_obj):
    config_obj = {}
    for tyob in config[type_obj]:
        config_obj[tyob] = config[type_obj][tyob]
    config_obj["TYPE"] = type_obj
    #    print (config_obj)
    return config_obj


########## Main  ###########


def main():
    parser = argparse.ArgumentParser(
        description="Create a multi cloud trace from a trace"
    )
    parser.add_argument("obj_file", help="Path to the trace filei in a pickle format")
    parser.add_argument("config_file", help="Path to the YAML config file")
    parser.add_argument(
        "save_mc_file", default=None, help="The output PATH of the multi cloud trace"
    )
    parser.add_argument(
        "--with_range_rd", action="store_true", help="add range read to the trace"
    )
    args = parser.parse_args()

    file_handler = open(args.save_mc_file, "w")
    csv_writer = csv.writer(file_handler)
    #    csv_writer.writerow(["timestamp", "ops" , "issue_region" , "obj_key", "size"])
    print("loading input trace")
    obj_dict, tot_op = parser_data.load_obj_dict(args.obj_file)
    print("finish loading input trace")
    config_dict = load_ymal(args.config_file)
    for key in obj_dict.keys():
        name = key
        puts_gets = only_put_gets(obj_dict[key])
        if len(puts_gets) == 0:
            continue
        size = puts_gets[0][2]
        list_timestamp, list_ops = list_of_timestamp_ops(puts_gets)
        config_obj = config_dict.copy()
        if config_dict["TYPE"] == "typeF":
            # choose one type for this OBJ
            type_obj = random.choices(
                config_dict["TYPES"], config_dict["TYPES_PROB"], k=1
            )[0]
            config_obj = fill_config_obj(config_obj, type_obj)
        obj = Object(
            name=name,
            size=size,
            ops=list_ops,
            access_times=list_timestamp,
            config=config_obj,
        )
        obj.add_init_put()
        obj.type_handler()
        #        print (obj.regions)
        obj.write_obj(csv_writer)
    file_handler.close()

    df = pd.read_csv(args.save_mc_file)
    # Step 2: Sort the DataFrame by the first column ('timestamp')
    df_sorted = df.sort_values(by=df.columns[0])
    # Step 3: Write the sorted DataFrame back to a CSV file
    df_sorted.to_csv(args.save_mc_file, index=False)


if __name__ == "__main__":
    main()
