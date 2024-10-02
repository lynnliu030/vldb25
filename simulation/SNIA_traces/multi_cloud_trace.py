import csv
import argparse
import yaml
import random
import time

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
##### Load the input trace ########
###################################
def load_trace_to_dict(csv_file):
    custom_column_names = [
        "timestamp",
        "op",
        "obj_key",
        "size",
        "range_rd_begin",
        "range_rd_end",
    ]
    delimiter = " "  # Set the delimiter to space

    data_dict = {
        col_name: [] for col_name in custom_column_names
    }  # Create a dictionary with custom column names

    with open(csv_file, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=delimiter)
        for row in csvreader:
            # Assign values to custom column names if available
            if "timestamp" in row[0]:
                continue
            for i, col_name in enumerate(custom_column_names):
                if i < len(row):
                    data_dict[col_name].append(row[i])
                else:
                    data_dict[col_name].append("")  # Add None for missing values

    return data_dict


###################################
##### Load the config file ########
###################################


def load_ymal(config_file):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


#######################
##### help funcions ###
#######################
##
## add obj to the unique objects for the initial state
##
def add_obj_uniq(
    input_dict, input_position, chosen_policy_dict, obj_uniq_dict, init_state_conf
):
    obj = input_dict["obj_key"][input_position]
    op = input_dict["op"][input_position]
    size = input_dict["size"][input_position]

    regions_list = []
    regions_prob_list = []
    add_region(init_state_conf["PUT"], regions_list, regions_prob_list)
    chosen_region = random.choices(regions_list, regions_prob_list, k=1)[0]

    if obj not in obj_uniq_dict.keys():
        obj_uniq_dict[obj] = {}
        obj_uniq_dict[obj]["size"] = size
        obj_uniq_dict[obj]["issue_region"] = chosen_region
        if ("PUT" not in op) or ("N/A" in chosen_policy_dict["PUT"]):
            obj_uniq_dict[obj]["uniq"] = True
        else:
            obj_uniq_dict[obj]["uniq"] = False


##
## add a region according the probability in the config file
##
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


################################################
# Add a specific part to the multi_cloud trace
###############################################


def add_part_to_mc_trace(
    input_dict, part, config_dict, obj_uniq_dict, obj_policy_dict, mc_output_dict
):
    part_dict = config_dict[part]
    start_time = time_in_milliseconds(part_dict["START_TIME"])
    end_time = time_in_milliseconds(part_dict["END_TIME"])

    policy_keys_list = []
    policy_prob_list = []

    # The policies of this part
    for policy in part_dict.keys():
        if "POLICY_OBJ" in policy:
            if "%" in part_dict[policy]["OBJ%"]:
                # start new obj policy + op policy
                obj_policy_dict = {}
                policy_keys_list.append(policy)
                policy_prob_list.append(
                    float(part_dict[policy]["OBJ%"].replace("%", ""))
                )
            else:
                # use previous obj policy + new op policy
                obj_part = part_dict[policy]["OBJ%"]
                policy_keys_list.append(policy)
                policy_prob_list.append(
                    float(config_dict[obj_part][policy]["OBJ%"].replace("%", ""))
                )

    # Transfer each row in the range according the chosen policy
    st = time.time()
    for i in range(len(input_dict["op"])):
        if i % 1_000 == 0:
            print(
                f'Transfer no_region to multi_region {i}/{len(input_dict["op"])}, time of transfer {time.time() - st}'
            )
        timestamp = float(input_dict["timestamp"][i])
        if timestamp >= start_time and timestamp < end_time:
            obj = input_dict["obj_key"][i]
            if obj in obj_policy_dict.keys():
                # use the policy that already chosen
                policy = obj_policy_dict[obj]["POLICY"]
                add_row_to_mc_trace(
                    input_dict, i, config_dict[part][policy], mc_output_dict
                )
            else:
                chosen_policy = random.choices(policy_keys_list, policy_prob_list, k=1)[
                    0
                ]
                obj_policy_dict[obj] = {"EPOCH": part, "POLICY": chosen_policy}
                add_obj_uniq(
                    input_dict,
                    i,
                    part_dict[chosen_policy],
                    obj_uniq_dict,
                    config_dict["INIT_STATE"],
                )
                add_row_to_mc_trace(
                    input_dict, i, part_dict[chosen_policy], mc_output_dict
                )


########## Main  ###########


def main():
    parser = argparse.ArgumentParser(
        description="Create a multi cloud trace from a trace"
    )
    parser.add_argument("trace_file", help="Path to the trace file")
    parser.add_argument("config_file", help="Path to the YAML config file")
    parser.add_argument(
        "save_mc_file", default=None, help="The output PATH of the multi cloud trace"
    )
    parser.add_argument(
        "--with_range_rd", action="store_true", help="add range read to the trace"
    )
    args = parser.parse_args()

    print("loading input trace")
    input_dict = load_trace_to_dict(args.trace_file)
    print("loading config trace")
    config_dict = load_ymal(args.config_file)

    # output dict
    mc_output_dict = {}
    for key in input_dict.keys():
        mc_output_dict[key] = []
    mc_output_dict["issue_region"] = []

    # unique objects list
    obj_uniq_dict = {}
    obj_policy_dict = {}

    for part in config_dict.keys():
        if "EPOCH" in part:
            print(f"epoch name {part}")
            add_part_to_mc_trace(
                input_dict,
                part,
                config_dict,
                obj_uniq_dict,
                obj_policy_dict,
                mc_output_dict,
            )

    header = ["timestamp", "op", "issue_region", "obj_key", "size"]
    if args.with_range_rd:
        header += ["range_rd_begin", "range_rd_end"]

    ###### write multi cloud trace ###########
    with open(args.save_mc_file, mode="w", newline="") as csv_file:
        print("Write init file")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)
        for obj in obj_uniq_dict.keys():
            if obj_uniq_dict[obj]["uniq"] is True:
                row = []
                for key in header:
                    if "timestamp" in key:
                        row.append("0")
                        continue
                    if "op" in key:
                        row.append("PUT")
                        continue
                    if "issue_region" in key:
                        row.append(obj_uniq_dict[obj]["issue_region"])
                        continue
                    if "obj_key" in key:
                        row.append(obj)
                        continue
                    if "size" in key:
                        row.append(obj_uniq_dict[obj]["size"])
                        continue
                csv_writer.writerow(row)

        print("Write multi region file")
        # Create a CSV writer object
        for i in range(len(mc_output_dict["issue_region"])):
            row = []
            for key in header:
                row.append(mc_output_dict[key][i])

            csv_writer.writerow(row)


if __name__ == "__main__":
    main()
