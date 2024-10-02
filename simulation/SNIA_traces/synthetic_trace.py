import csv
import argparse
import yaml
import random
import time
import numpy as np

#######################################################################################################################
## This script create a synthetic_trace according to YMAL
#######################################################################################################################

MILLI = 1
SEC = 1000 * MILLI
MIN = 60 * SEC
HOUR = 60 * MIN
DAY = 24 * HOUR

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB

K = 1000
M = 1000 * K
B = 1000 * M

# Setting a seed for the random number generator
random.seed(1)

#######################
##### help funcions ###
#######################


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
    return int(
        days * DAY + hours * HOUR + minutes * MIN + seconds * SEC + milli * MILLI
    )


def size_in_bytes(size):
    if "KB" in size:
        return float(size.split("KB")[0]) * KB
    if "MB" in size:
        return float(size.split("MB")[0]) * MB
    if "GB" in size:
        return float(size.split("GB")[0]) * GB
    if "TB" in size:
        return float(size.split("TB")[0]) * TB
    return int(size)


def num_in_units(number):
    if "K" in number:
        return float(number.split("K")[0]) * K
    if "M" in number:
        return float(number.split("M")[0]) * M
    if "B" in number:
        return float(number.split("B")[0]) * B
    return int(number)


def policy_size(size_policy):
    size_list = []
    size_prob_list = []
    for r in size_policy.keys():
        size_list.append(r)
        size_prob_list.append(int(size_policy[r]["%"].split("%")[0]))
    return size_list, size_prob_list


def calc_size(size_list, size_prob_list, size_policy):
    chosen_range = random.choices(size_list, size_prob_list, k=1)[0]
    start_size = size_in_bytes(str(size_policy[chosen_range]["START_SIZE"]))
    end_size = size_in_bytes(str(size_policy[chosen_range]["END_SIZE"]))
    return int(random.uniform(start_size, end_size))


def add_row(trace_dict, ts, op, obj_key, size):
    "timestamp", "op", "obj_key", "size"
    trace_dict["timestamp"].append(ts)
    trace_dict["op"].append(op)
    trace_dict["obj_key"].append(obj_key)
    trace_dict["size"].append(size)
    trace_dict["range_rd_begin"].append("")
    trace_dict["range_rd_end"].append("")


###### Distribution function ############
def generate_poisson_events(total_time, num_events):
    while True:
        # Generate event times
        event_times = np.random.exponential(total_time / num_events, num_events)

        # Cumulative sum to get the actual times of events
        event_times_cumulative = np.cumsum(event_times)

        # Check if the last event is within the time limit
        if event_times_cumulative[-1] <= total_time:
            return event_times_cumulative


def generate_poisson_events(total_time, num_events):
    while True:
        # Generate event times
        event_times = np.random.exponential(total_time / num_events, num_events)

        # Cumulative sum to get the actual times of events
        event_times_cumulative = np.cumsum(event_times)

        # Check if the last event is within the time limit
        if event_times_cumulative[-1] <= total_time:
            return event_times_cumulative


def generate_fixtime_events(start_time, end_time, fix_time):
    fix_time_min = 60 * 1000 * int(fix_time)
    i = 0
    event_times = []
    while True:
        print()
        if start_time + 50 * 1000 * 60 + fix_time_min * i > end_time:
            return np.array(event_times)
        event_times.append(start_time + 50 * 1000 * 60 + fix_time_min * i)
        i += 1


def add_rows_to_obj_poisson(
    obj_key,
    size,
    start_time,
    end_time,
    policy_dict,
    trace_dict,
    num_gets,
    num_puts,
    total_ops,
    fix_time,
):
    if fix_time is None:
        event_times = generate_poisson_events(end_time - start_time, total_ops)
    else:
        event_times = generate_fixtime_events(start_time, end_time, fix_time)

    ops_dict = {"get": 0, "put": 0}
    for t in event_times:
        if num_puts == 0:
            ops = "REST.GET.OBJECT"
        else:
            ops = chosen_ops(num_gets, num_puts, ops_dict)
        print(ops)
        cur_time = int(start_time + t)
        add_row(trace_dict, cur_time, ops, obj_key, size)


def calc_get_put_num(policy_dict):
    num_gets = num_in_units(str(policy_dict["GET_STATS"]["GET_NUM"]))
    put_get_ratio = policy_dict["GET_STATS"]["GET_PUT_RATIO"]
    if "N/A" in str(put_get_ratio):
        put_get_ratio = 0
        num_puts = 0
    else:
        put_get_ratio = num_in_units(str(put_get_ratio))
        num_puts = int(num_gets / put_get_ratio)
    total_ops = num_gets + num_puts
    return num_gets, num_puts, total_ops


#############################################
##### Add Epoch to Trace:  OBJs LOOP  #######
#############################################


def add_epoch_to_trace_objs_loop(config_dict, epoch, trace_dict, fix_time):
    epoch_dict = config_dict[epoch]
    start_time = time_in_milliseconds(epoch_dict["START_TIME"])
    end_time = time_in_milliseconds(epoch_dict["END_TIME"])
    # The policies of this part
    for policy in epoch_dict.keys():
        if "POLICY" in policy:
            obj_num = num_in_units(str(epoch_dict[policy]["OBJ"]))
            size_list, size_prob_list = policy_size(epoch_dict[policy]["SIZE"])
            start_obj_key = epoch_dict[policy]["START_OBJ_KEY"]
            label = epoch_dict[policy]["LABEL"]
            num_gets, num_puts, total_ops = calc_get_put_num(epoch_dict[policy])
            for i in range(int(obj_num)):
                if i % 100 == 0:
                    print(f"calculate {i} from {obj_num} OBJ")
                obj_key = label + "_" + str(start_obj_key + i)
                size = calc_size(size_list, size_prob_list, epoch_dict[policy]["SIZE"])
                add_rows_to_obj_poisson(
                    obj_key,
                    size,
                    start_time,
                    end_time,
                    epoch_dict[policy],
                    trace_dict,
                    num_gets,
                    num_puts,
                    total_ops,
                    fix_time,
                )


#############################################
##### Add Epoch to Trace:  OBJs LOOP  #######
#############################################

####
#### SIZE: from YAML
#### AGGREGATE: from YAML
#### START_OBJ_KEY: from YAML
#### END_OBJ_KEY: START_OBJ_KEY + AGGREGATE/SIZE-1
#### GET_OPS: from YAML
#### GET_PUT_RATIO: from YAML
#### PUT_OPS: GET_OPS/GET_PUT_RATIO
#### TOTAL_OPS: GET_OPS+PUT_OPS
#### START_TIME: from YAML
#### END_TIME: from YAML
#### Timestamp: poisson distribution from start to end time with average of (END_TIME-START_TIME)/TOTAL_OPS

#### Number of Objecs
#### AGG 0.1TB / size 100 KB - 1M Obj
#### AGG 1 TB / size 100 KB - 10M Obj
#### AGG 10 TB / size 100 KB - 100M Obj

#### AGG 0.1 TB / size 1 KB - 100M Obj
#### AGG 1 TB / size 1 KB - 1B Obj
#### AGG 10TB / size 1 KB - 10B Obj

# Ratio:
# GET_OPS = 100 M
# GET_PUT_RATIO: 1:1 -> TOT_OPS: 200 M
# GET_PUT_RATIO: 1:30 -> TOT_OPS: 103.3 M


def chosen_ops(num_gets, num_puts, ops_dict):
    if ops_dict["get"] >= num_gets:
        ops_dict["put"] += 1
        return "REST.PUT.OBJECT"
    if ops_dict["put"] >= num_puts:
        ops_dict["get"] += 1
        return "REST.GET.OBJECT"

    ops = random.choices(["get", "put"], [num_gets / num_puts, 1], k=1)[0]
    if ops == "get":
        ops_dict["get"] += 1
        return "REST.GET.OBJECT"
    else:
        ops_dict["put"] += 1
        return "REST.PUT.OBJECT"


def chosen_obj(start_obj_key, end_obj_key):
    return random.randint(start_obj_key, end_obj_key)


def add_epoch_to_trace_gets_loop(config_dict, epoch, trace_dict, csv_writer):
    epoch_dict = config_dict[epoch]
    start_time = time_in_milliseconds(epoch_dict["START_TIME"])
    end_time = time_in_milliseconds(epoch_dict["END_TIME"])
    total_time = end_time - start_time

    size = int(size_in_bytes(str(epoch_dict["SIZE"])))
    agg = size_in_bytes(str(epoch_dict["AGGREGATE"]))
    start_obj_key = num_in_units(str(epoch_dict["START_OBJ_KEY"]))
    end_obj_key = start_obj_key + int(agg / size) - 1

    get_put_ratio = num_in_units(str(epoch_dict["GET_PUT_RATIO"]))
    num_gets = num_in_units(str(epoch_dict["GET_OPS"]))
    num_puts = int(num_gets / get_put_ratio)
    tot_ops = num_gets + num_puts

    ops_dict = {"get": 0, "put": 0}

    # Generate random samples from a Poisson distribution
    st = time.time()
    event_times = generate_poisson_events(int(total_time), int(tot_ops))
    print(f"The time for poisson distribution is ({time.time()-st})")

    for i in range(len(event_times)):
        if i % 1_000_000 == 0:
            print(f"{i} from {tot_ops}")
        obj_key = chosen_obj(start_obj_key, end_obj_key)
        ts = int(event_times[i])
        ops = chosen_ops(num_gets, num_puts, ops_dict)
        #        column_names = ["timestamp", "op", "obj_key", "size"]
        csv_writer.writerow([ts, ops, obj_key, size])
    #        print (obj_key)
    #        print (ts)
    #        print (ops)

    return 0


###################################
##### Load the config file ########
###################################


def load_ymal(config_file):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


def overlead_config_dict(config_dict, args):
    for epoch in config_dict.keys():
        if "GETS_LOOP" in config_dict[epoch]["LABEL"]:
            if args.size:
                config_dict[epoch]["SIZE"] = args.size

            if args.agg:
                config_dict[epoch]["AGGREGATE"] = args.agg

            if args.get_put_ratio:
                config_dict[epoch]["GET_PUT_RATIO"] = args.get_put_ratio

            if args.gets:
                config_dict[epoch]["GET_OPS"] = args.gets

        if "OBJS_LOOP" in config_dict[epoch]["LABEL"]:
            for policy in config_dict[epoch].keys():
                if "POLICY_OBJ" in policy:
                    print(config_dict[epoch][policy])
                    if args.objs:
                        config_dict[epoch][policy]["OBJ"] = args.objs

                    if args.get_put_ratio:
                        config_dict[epoch][policy]["GET_STATS"][
                            "GET_PUT_RATIO"
                        ] = args.get_put_ratio

                    if args.gets:
                        config_dict[epoch][policy]["GET_STATS"]["GET_NUM"] = args.gets

    return 0


########## Main  ###########


def main():
    parser = argparse.ArgumentParser(
        description="Create a multi cloud trace from a trace"
    )
    parser.add_argument("config_file", help="Path to the YAML config file")
    parser.add_argument(
        "trace_name", default=None, help="The prefix name of the output trace file"
    )
    #    parser.add_argument("--size", default=None, help="Change the size on the config file")
    parser.add_argument(
        "--get_put_ratio", default=None, help="Change the get_put_ratio"
    )
    parser.add_argument("--agg", default=None, help="Change the Aggregate")
    parser.add_argument(
        "--gets", default=None, help="Change the number of get operations"
    )
    parser.add_argument("--objs", default=None, help="Change the number Objects")
    parser.add_argument("--fix_time", default=None, help="iwhat is the fix time in min")

    args = parser.parse_args()

    column_names = [
        "timestamp",
        "op",
        "obj_key",
        "size",
        "range_rd_begin",
        "range_rd_end",
    ]
    config_dict = load_ymal(args.config_file)

    overlead_config_dict(config_dict, args)

    trace_dict = {}
    for col in column_names:
        trace_dict[col] = []

    filename = args.trace_name
    for epoch in config_dict.keys():
        if "GETS_LOOP" in config_dict[epoch]["LABEL"]:
            for epoch in config_dict.keys():
                epoch_dict = config_dict[epoch]
                filename += (
                    "_SIZE_"
                    + str(epoch_dict["SIZE"])
                    + "_AGG_"
                    + str(epoch_dict["AGGREGATE"])
                    + "_RATIO_"
                    + str(epoch_dict["GET_PUT_RATIO"])
                    + "_GET_OPS_"
                    + str(epoch_dict["GET_OPS"])
                )
                break
    filename += ".csv"

    if "GETS_LOOP" in config_dict[epoch]["LABEL"]:
        file_dis = open(filename, mode="w", newline="")
        csv_writer = csv.writer(file_dis, delimiter=" ")
    #        csv_writer.writerow(column_names)

    for epoch in config_dict.keys():
        if "GETS_LOOP" in config_dict[epoch]["LABEL"]:
            add_epoch_to_trace_gets_loop(config_dict, epoch, trace_dict, csv_writer)
        else:
            if "OBJS_LOOP" in config_dict[epoch]["LABEL"]:
                add_epoch_to_trace_objs_loop(
                    config_dict, epoch, trace_dict, args.fix_time
                )
            else:
                print(f"EPOCH LABEL should be GETS_LOOP or OBJS_LOOP")
                exit(0)

    if "GETS_LOOP" in config_dict[epoch]["LABEL"]:
        print(f"close file: {filename}")
        file_dis.close()
        exit(0)

    if "OBJS_LOOP" in config_dict[epoch]["LABEL"]:
        ###### write single cloud trace ###########
        with open(args.trace_name, mode="w", newline="") as csv_file:
            # Create a CSV writer object
            csv_writer = csv.writer(csv_file, delimiter=" ")
            #            csv_writer.writerow(column_names)
            rows = []
            for i in range(len(trace_dict["timestamp"])):
                row = []
                for key in column_names:
                    row.append(trace_dict[key][i])
                rows.append(row)
            print("sort according to timestemp")
            rows.sort(key=lambda x: x[0])

            for i in range(len(rows)):
                csv_writer.writerow(rows[i])


if __name__ == "__main__":
    main()
