import csv
import argparse
import parser_data
import statistics as st
from collections import OrderedDict
import os

#######################################################################################################################
## This script performs basic statistical analysis on the trace.
## It loads a pickle file, which is the output of the parser_data.py script
## It is designed to process one trace at a time. For running analyses on multiple traces, use the run_stats.py script.
#######################################################################################################################

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
SEC = 1000
MIN = 60 * SEC
HOUR = 60 * MIN
DAY = 24 * HOUR
LAST_TIME = 7 * DAY


############ REQ stats #############################################
########## how many reads, heads, writes, copys, other #############
########## how many objects have been read once #############
def obj_reads_writes(obj_dict):
    get_once = 0
    write_only = 0
    reads = 0
    heads = 0
    writes = 0
    copys = 0
    deletes = 0
    other = 0
    for obj in obj_dict.keys():
        flag_get = 0
        for op in range(len(obj_dict[obj])):
            # check if op is get
            if "GET" in obj_dict[obj][op][1]:
                reads += 1
                flag_get += 1
            else:
                if "HEAD" in obj_dict[obj][op][1]:
                    heads += 1
                else:
                    if "PUT" in obj_dict[obj][op][1]:
                        writes += 1
                        if len(obj_dict[obj]) == 1:
                            write_only += 1
                    else:
                        if "DELETE" in obj_dict[obj][op][1]:
                            deletes += 1
                            # write once is also wrote and then delete
                            if len(obj_dict[obj]) == 2 and "PUT" in obj_dict[obj][0][1]:
                                write_only += 1
                        else:
                            if "COPY" in obj_dict[obj][op][1]:
                                copys += 1
                            else:
                                other += 1
        if flag_get == 1:
            get_once += 1
    return get_once, write_only, reads, heads, writes, deletes, copys, other


############ HEAD stats ####################################
#### how many time there is head without get ###############
#### how many time there is head and then get ############
#### if there is a head and then get what is the avg time ##
def head_stats(obj_dict):
    head_no_reads = 0
    one_head_with_read = 0
    num_few_heads_with_reads = 0
    count_few_heads_with_reads = 0
    head_get_diff_time = []
    avg_head_get_time = 0
    median_head_get_time = 0
    for obj in obj_dict.keys():
        flag_head = 0
        for op in range(len(obj_dict[obj])):
            # check if op is head
            if "HEAD" in obj_dict[obj][op][1]:
                flag_head += 1
                start_head_time = int(obj_dict[obj][op][0])
            if "GET" in obj_dict[obj][op][1]:
                if flag_head >= 1:
                    if flag_head == 1:
                        one_head_with_read += 1
                    else:
                        num_few_heads_with_reads += flag_head
                        count_few_heads_with_reads += 1
                    flag_head = 0
                    head_get_diff_time.append(
                        int(obj_dict[obj][op][0]) - start_head_time
                    )
        if flag_head >= 1:
            head_no_reads += flag_head
    if (one_head_with_read + num_few_heads_with_reads) > 0:
        avg_head_get_time = sum(head_get_diff_time) / max(len(head_get_diff_time), 1)
        median_head_get_time = st.median(head_get_diff_time)
    return (
        head_no_reads,
        one_head_with_read,
        num_few_heads_with_reads,
        count_few_heads_with_reads,
        avg_head_get_time,
        median_head_get_time,
    )


########### Size stats ########################
def size_stats(obj_dict):
    size_dict = OrderedDict(
        [
            ("size_up_1MB", 0),
            ("size_1MB_10MB", 0),
            ("size_10MB_100MB", 0),
            ("size_100MB_1GB", 0),
            ("size_1GB_10GB", 0),
            ("size_above_10GB", 0),
        ]
    )
    size_list = []
    zero_size = 0
    for obj in obj_dict.keys():
        if "DELETE" in obj_dict[obj][0][1]:
            if len(obj_dict[obj]):
                continue
            else:
                print("There is an obj after delete, please go check")
                exit(0)
        cur_size = int(obj_dict[obj][0][2])
        size_list.append(cur_size)
        if cur_size == 0:
            zero_size += 1
        if cur_size < 1 * MB:
            size_dict["size_up_1MB"] += 1
        if cur_size >= 1 * MB and cur_size < 10 * MB:
            size_dict["size_1MB_10MB"] += 1
        if cur_size >= 10 * MB and cur_size < 100 * MB:
            size_dict["size_10MB_100MB"] += 1
        if cur_size >= 100 * MB and cur_size < 1 * GB:
            size_dict["size_100MB_1GB"] += 1
        if cur_size >= 1 * GB and cur_size < 10 * GB:
            size_dict["size_1GB_10GB"] += 1
        if cur_size >= 10 * GB:
            size_dict["size_above_10GB"] += 1

    avg_size = sum(size_list) / max(len(size_list), 1)
    median_size = st.median(size_list)
    print(zero_size)
    return avg_size, median_size, size_dict, zero_size


def update_time_diff_get(get_dict, time_diff):
    if time_diff < 1 * SEC:
        get_dict["diff_less_1sec"] += 1
    if time_diff >= 1 * SEC and time_diff < 1 * MIN:
        get_dict["diff_1sec_1m"] += 1
    if time_diff >= 1 * MIN and time_diff < 1 * HOUR:
        get_dict["diff_1m_1h"] += 1
    if time_diff >= 1 * HOUR and time_diff < 24 * HOUR:
        get_dict["diff_1h_24h"] += 1
    if time_diff >= 24 * HOUR and time_diff < 3 * DAY:
        get_dict["diff_1day_3days"] += 1
    if time_diff >= 3 * DAY:
        get_dict["diff_above_3days"] += 1


####### Get stats #####
# number of reads to each obj that have more then one access
# avg fopuency to each object between two repeated reads
def get_stats(obj_dict):
    obj_with_gets = 0
    per_obj_num_reads = OrderedDict(
        [
            ("reads_eq_0", 0),
            ("reads_eq_1", 0),
            ("reads_2_10", 0),
            ("reads_10_100", 0),
            ("reads_100_1K", 0),
            ("reads_1K_10K", 0),
            ("reads_10K_100K", 0),
            ("reads_100K_1M", 0),
            ("reads_above_1M", 0),
        ]
    )

    per_obj_avg_time_diff = OrderedDict(
        [
            ("diff_less_1sec", 0),
            ("diff_1sec_1m", 0),
            ("diff_1m_1h", 0),
            ("diff_1h_24h", 0),
            ("diff_1day_3days", 0),
            ("diff_above_3days", 0),
        ]
    )

    per_obj_median_time_diff = OrderedDict(
        [
            ("diff_less_1sec", 0),
            ("diff_1sec_1m", 0),
            ("diff_1m_1h", 0),
            ("diff_1h_24h", 0),
            ("diff_1day_3days", 0),
            ("diff_above_3days", 0),
        ]
    )

    per_get_time_diff = OrderedDict(
        [
            ("diff_less_1sec", 0),
            ("diff_1sec_1m", 0),
            ("diff_1m_1h", 0),
            ("diff_1h_24h", 0),
            ("diff_1day_3days", 0),
            ("diff_above_3days", 0),
        ]
    )

    last_get_time_diff = OrderedDict(
        [
            ("diff_less_1sec", 0),
            ("diff_1sec_1m", 0),
            ("diff_1m_1h", 0),
            ("diff_1h_24h", 0),
            ("diff_1day_3days", 0),
            ("diff_above_3days", 0),
        ]
    )

    for obj in obj_dict.keys():
        flag_first_get = 0
        tmp_reads = 0
        tmp_get_diff = []

        for op in range(len(obj_dict[obj]) - 1, -1, -1):
            if "GET" in obj_dict[obj][op][1]:
                obj_with_gets += 1
                last_get_diff = LAST_TIME - int(obj_dict[obj][op][0])
                update_time_diff_get(last_get_time_diff, last_get_diff)
                break

        for op in range(len(obj_dict[obj])):
            # check if op is get
            if "GET" in obj_dict[obj][op][1]:
                if flag_first_get == 0:
                    flag_first_get = 1
                    tmp_reads += 1
                    start_get_time = int(obj_dict[obj][op][0])
                else:
                    tmp_reads += 1
                    get_diff_time = int(obj_dict[obj][op][0]) - start_get_time
                    tmp_get_diff.append(get_diff_time)
                    update_time_diff_get(per_get_time_diff, get_diff_time)
        if len(tmp_get_diff) > 0:
            update_time_diff_get(
                per_obj_avg_time_diff, sum(tmp_get_diff) / max(len(tmp_get_diff), 1)
            )
            update_time_diff_get(per_obj_median_time_diff, st.median(tmp_get_diff))
        if tmp_reads == 0:
            per_obj_num_reads["reads_eq_0"] += 1
        if tmp_reads == 1:
            per_obj_num_reads["reads_eq_1"] += 1
        if tmp_reads >= 2 and tmp_reads < 10:
            per_obj_num_reads["reads_2_10"] += 1
        if tmp_reads >= 10 and tmp_reads < 100:
            per_obj_num_reads["reads_10_100"] += 1
        if tmp_reads >= 100 and tmp_reads < 1000:
            per_obj_num_reads["reads_100_1K"] += 1
        if tmp_reads >= 1000 and tmp_reads < 10 * 1000:
            per_obj_num_reads["reads_1K_10K"] += 1
        if tmp_reads >= 10 * 1000 and tmp_reads < 100 * 1000:
            per_obj_num_reads["reads_10K_100K"] += 1
        if tmp_reads >= 100 * 1000 and tmp_reads < 1000 * 1000:
            per_obj_num_reads["reads_100K_1M"] += 1
        if tmp_reads >= 1000 * 1000:
            per_obj_num_reads["reads_above_1M"] += 1

    return (
        obj_with_gets,
        per_get_time_diff,
        last_get_time_diff,
        per_obj_num_reads,
        per_obj_avg_time_diff,
        per_obj_median_time_diff,
    )


########## Main  ###########


def main():
    # custom_column_names = ["timestamp", "op", "obj_key", "size", "range_rd_begin", "range_rd_end"]
    # obj_dict[obj_key][0] - "timestamp"
    # obj_dict[obj_key][1] - "op"
    # obj_dict[obj_key][2] - "size"
    # obj_dict[obj_key][3] - "range_rd_begin"
    # obj_dict[obj_key][4] - "range_rd_end"
    parser = argparse.ArgumentParser(description="run the stats on the Trace")
    parser.add_argument(
        "obj_file", help="Path to obj_dict in a pickle format (the outpur od parser.py)"
    )
    parser.add_argument("save_res", help="Path to file that saves the result in csv")
    parser.add_argument(
        "--add_keys",
        action="store_true",
        help="add the keys in addition to the res file",
    )
    args = parser.parse_args()
    res = OrderedDict()

    # Load obj_dict from file
    # Each obj has a key and the value in this order:
    # "["timestamp", "op", "obj_key", "size", "range_rd_begin", "range_rd_end"]
    obj_dict, tot_op = parser_data.load_obj_dict(args.obj_file)
    uniq_obj = len(obj_dict.keys())
    print(
        f"The number of opuests are {tot_op} and the number of objects are {uniq_obj}, which is {uniq_obj/max(tot_op,1)*100:.2f}%"
    )
    (
        get_once,
        write_only,
        reads,
        heads,
        writes,
        deletes,
        copys,
        other,
    ) = obj_reads_writes(obj_dict)
    print(
        f"The number of reads {reads}, which is {reads/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of writes {writes}, which is {writes/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of heads {heads}, which is {heads/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of copys {copys}, which is {copys/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of deletes {deletes}, which is {deletes/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of other {other}, which is {other/max(tot_op,1)*100:.2f}% from the op"
    )
    print(
        f"The number of get_once {get_once} and the number of objects are {uniq_obj}, which is {get_once/max(uniq_obj,1)*100:.2f}%"
    )
    print(
        f"The number of write_only {write_only} and the number of objects are {uniq_obj}, which is {write_only/max(uniq_obj,1)*100:.2f}%"
    )
    print(
        f"The number of write_only {write_only} and the of all the writes are {writes}, which is {write_only/max(writes,1)*100:.2f}%"
    )

    (
        head_no_reads,
        one_head_with_read,
        num_few_heads_with_reads,
        count_few_heads_with_reads,
        avg_head_get_time,
        median_head_get_time,
    ) = head_stats(obj_dict)
    print(
        f"The number of heads with no reads {head_no_reads}, The number of heads with one read {one_head_with_read} the number of a few heads with read {num_few_heads_with_reads} it happened {count_few_heads_with_reads} The number of heads {heads}.  The avg of diff time between head and get is {avg_head_get_time:.2f}[ms] while the median is {median_head_get_time}[ms] "
    )
    avg_size, median_size, size_dict, zero_size = size_stats(obj_dict)
    print(
        f'avg_size {avg_size} median_size {median_size} / up_1MB {size_dict["size_up_1MB"]}, 1MB_10MB {size_dict["size_1MB_10MB"]}, 10MB_100MB {size_dict["size_10MB_100MB"]}, 100MB_1GB {size_dict["size_100MB_1GB"]}, 1GB_10GB {size_dict["size_1GB_10GB"]}, above_10GB {size_dict["size_above_10GB"]}'
    )
    (
        obj_with_gets,
        per_get_time_diff,
        last_get_time_diff,
        per_obj_num_reads,
        per_obj_avg_time_diff,
        per_obj_median_time_diff,
    ) = get_stats(obj_dict)
    print(
        f'per_obj_num_reads: reads_eq_0 {per_obj_num_reads["reads_eq_0"]} reads_eq_1 {per_obj_num_reads["reads_eq_1"]} reads_2_10 {per_obj_num_reads["reads_2_10"]}, reads_10_100 {per_obj_num_reads["reads_10_100"]}, reads_100_1K {per_obj_num_reads["reads_100_1K"]}, reads_1K_10K {per_obj_num_reads["reads_1K_10K"]}, reads_10K_100K {per_obj_num_reads["reads_10K_100K"]}, reads_100K_1M {per_obj_num_reads["reads_100K_1M"]}, reads_above_1M {per_obj_num_reads["reads_above_1M"]}'
    )

    res["name"] = prefix = os.path.splitext(args.obj_file)[0]
    res["tot_op"] = tot_op
    res["tot_op[M]"] = tot_op / 1000 / 1000
    res["uniq_obj"] = uniq_obj
    res["%uniq_obj/tot_op"] = uniq_obj / max(tot_op, 1) * 100
    res["reads"] = reads
    res["writes"] = writes
    res["heads"] = heads
    res["copys"] = copys
    res["deletes"] = deletes
    res["other"] = other
    res["%reads/tot_op"] = reads / max(tot_op, 1) * 100
    res["%writes/tot_op"] = writes / max(tot_op, 1) * 100
    res["%heads/tot_op"] = heads / max(tot_op, 1) * 100
    res["%copys/tot_op"] = copys / max(tot_op, 1) * 100
    res["%delete/tot_op"] = deletes / max(tot_op, 1) * 100
    res["%other/tot_op"] = other / max(tot_op, 1) * 100
    res["get_once"] = get_once
    res["%get_once/obj"] = get_once / max(uniq_obj, 1) * 100
    res["%get_once/reads"] = get_once / max(reads, 1) * 100
    res["write_only"] = write_only
    res["%write_only/obj"] = write_only / max(uniq_obj, 1) * 100
    res["head_no_reads"] = head_no_reads
    res["one_head_with_read"] = one_head_with_read
    res["num_few_heads_with_reads"] = num_few_heads_with_reads
    res["count_few_heads_with_reads"] = count_few_heads_with_reads
    res["avg_head_get_time[ms]"] = avg_head_get_time
    res["median_head_get_time[ms]"] = median_head_get_time

    res["avg_size"] = avg_size
    res["median_size"] = median_size
    res["zero_size"] = zero_size
    res["size_up_1MB"] = size_dict["size_up_1MB"]
    res["size_1MB_10MB"] = size_dict["size_1MB_10MB"]
    res["size_10MB_100MB"] = size_dict["size_10MB_100MB"]
    res["size_100MB_1GB"] = size_dict["size_100MB_1GB"]
    res["size_1GB_10GB"] = size_dict["size_1GB_10GB"]
    res["size_above_10GB"] = size_dict["size_above_10GB"]

    res["obj_with_gets"] = obj_with_gets
    res["%obj_with_gets/uniq_obj"] = obj_with_gets / max(uniq_obj, 1) * 100

    res["per_obj_num_reads_eq_0"] = per_obj_num_reads["reads_eq_0"]
    res["per_obj_num_reads_eq_1"] = per_obj_num_reads["reads_eq_1"]
    res["per_obj_num_reads_2_10"] = per_obj_num_reads["reads_2_10"]
    res["per_obj_num_reads_10_100"] = per_obj_num_reads["reads_10_100"]
    res["per_obj_num_reads_100_1K"] = per_obj_num_reads["reads_100_1K"]
    res["per_obj_num_reads_1K_10K"] = per_obj_num_reads["reads_1K_10K"]
    res["per_obj_num_reads_10K_100K"] = per_obj_num_reads["reads_10K_100K"]
    res["per_obj_num_reads_100K_1M"] = per_obj_num_reads["reads_100K_1M"]
    res["per_obj_num_reads_above_1M"] = per_obj_num_reads["reads_above_1M"]

    res["per_get_time_diff_less_1sec"] = per_get_time_diff["diff_less_1sec"]
    res["per_get_time_diff_1sec_1m"] = per_get_time_diff["diff_1sec_1m"]
    res["per_get_time_diff_1m_1h"] = per_get_time_diff["diff_1m_1h"]
    res["per_get_time_diff_1h_24h"] = per_get_time_diff["diff_1h_24h"]
    res["per_get_time_diff_1day_3days"] = per_get_time_diff["diff_1day_3days"]
    res["per_get_time_diff_above_3days"] = per_get_time_diff["diff_above_3days"]

    res["last_get_time_diff_less_1sec"] = last_get_time_diff["diff_less_1sec"]
    res["last_get_time_diff_1sec_1m"] = last_get_time_diff["diff_1sec_1m"]
    res["last_get_time_diff_1m_1h"] = last_get_time_diff["diff_1m_1h"]
    res["last_get_time_diff_1h_24h"] = last_get_time_diff["diff_1h_24h"]
    res["last_get_time_diff_1day_3days"] = last_get_time_diff["diff_1day_3days"]
    res["last_get_time_diff_above_3days"] = last_get_time_diff["diff_above_3days"]

    res["per_obj_avg_time_diff_less_1sec"] = per_obj_avg_time_diff["diff_less_1sec"]
    res["per_obj_avg_time_diff_1sec_1m"] = per_obj_avg_time_diff["diff_1sec_1m"]
    res["per_obj_avg_time_diff_1m_1h"] = per_obj_avg_time_diff["diff_1m_1h"]
    res["per_obj_avg_time_diff_1h_24h"] = per_obj_avg_time_diff["diff_1h_24h"]
    res["per_obj_avg_time_diff_1day_3days"] = per_obj_avg_time_diff["diff_1day_3days"]
    res["per_obj_avg_time_diff_above_3days"] = per_obj_avg_time_diff["diff_above_3days"]

    res["per_obj_median_time_diff_less_1sec"] = per_obj_median_time_diff[
        "diff_less_1sec"
    ]
    res["per_obj_median_time_diff_1sec_1m"] = per_obj_median_time_diff["diff_1sec_1m"]
    res["per_obj_median_time_diff_1m_1h"] = per_obj_median_time_diff["diff_1m_1h"]
    res["per_obj_median_time_diff_1h_24h"] = per_obj_median_time_diff["diff_1h_24h"]
    res["per_obj_median_time_diff_1day_3days"] = per_obj_median_time_diff[
        "diff_1day_3days"
    ]
    res["per_obj_median_time_diff_above_3days"] = per_obj_median_time_diff[
        "diff_above_3days"
    ]

    with open(args.save_res, mode="a", newline="") as csv_file:
        # Create a CSV writer object
        csv_writer = csv.writer(csv_file)

        # Write the ordered dictionary as a row in the CSV file
        if args.add_keys:
            csv_writer.writerow(res.keys())
        csv_writer.writerow(res.values())
    print(res)
    return res


if __name__ == "__main__":
    main()
