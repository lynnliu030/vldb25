import argparse
import csv
from collections import OrderedDict

#######################################################################################################################
## This script performs more advance clustring on the raw statistics from stats.py
## It also merge buckets that have more than one trace
#######################################################################################################################

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB
K = 1000
M = 1000 * K
B = 1000 * M


### Merge rows that were splited #########
def merge_rows(obj_dict):
    # merge splits buckets
    row_len = len(obj_dict["name"])
    bucket_dict = OrderedDict()
    tmp_dict = OrderedDict()
    for col in obj_dict.keys():
        bucket_dict[col] = []
        if col == "name":
            tmp_dict[col] = ""
        else:
            tmp_dict[col] = 0

    flag_merge_bucket = 0
    for i in range(row_len - 1, -1, -1):
        if "Part0" not in obj_dict["name"][i] or flag_merge_bucket:
            flag_merge_bucket = 1
            for col in obj_dict.keys():
                if col == "name":
                    tmp_dict["name"] += "_" + obj_dict[col][i][-1]
                else:
                    tmp_dict[col] += float(obj_dict[col][i])
            #                if (("%" in col) or ("avg" in col) or ("median" in col):

            if "Part0" in obj_dict["name"][i]:
                flag_merge_bucket = 0
                name = obj_dict["name"][i][:-1] + tmp_dict["name"]
                tmp_dict["name"] = name
                for col in obj_dict.keys():
                    if (
                        (
                            ("%" in col)
                            or ("midian" in col)
                            or ("avg" in col)
                            or ("get_once" in col)
                            or ("write_only" in col)
                        )
                        or ("uniq_obj" in col)
                        or ("last" in col)
                    ):
                        bucket_dict[col].append("N/A")
                    else:
                        bucket_dict[col].append(tmp_dict[col])
                    if col == "name":
                        tmp_dict[col] = ""
                    else:
                        tmp_dict[col] = 0

        else:
            for col in obj_dict.keys():
                bucket_dict[col].append(obj_dict[col][i])

    for col in bucket_dict.keys():
        bucket_dict[col].reverse()

    return bucket_dict


############### categorize Request type ###################
def cat_req_type(bucket_dict, attribute_dict):
    row_len = len(bucket_dict["name"])
    attribute_dict["Object_req_Category"] = []
    req_vhigh = 97
    req_high = 90
    req_mid = 50
    req_wr_high = 60
    for i in range(row_len):
        reads = float(bucket_dict["reads"][i]) / float(bucket_dict["tot_op"][i]) * 100
        writes = float(bucket_dict["writes"][i]) / float(bucket_dict["tot_op"][i]) * 100
        heads = float(bucket_dict["heads"][i]) / float(bucket_dict["tot_op"][i]) * 100
        deletes = (
            float(bucket_dict["deletes"][i]) / float(bucket_dict["tot_op"][i]) * 100
        )
        float(bucket_dict["copys"][i]) / float(bucket_dict["tot_op"][i]) * 100
        if reads > req_vhigh:
            attribute_dict["Object_req_Category"].append(
                f"Very Heavy Reads > {req_vhigh}"
            )
            #               print (f"vheavy reads {reads}")
            continue

        if heads > req_vhigh:
            attribute_dict["Object_req_Category"].append(
                f"Very Heavy Heads > {req_vhigh}"
            )
            #               print (f"vheavy heads {heads}")
            continue

        if heads + reads > req_vhigh:
            attribute_dict["Object_req_Category"].append(
                f"Very Heavy Reads+Heads > {req_vhigh}"
            )
            #               print (f"vheavy reads+heads {heads} {reads}")
            continue

        if heads + reads > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Reads+Heads > {req_high}"
            )
            #               print (f"vheavy reads+heads {heads} {reads}")
            continue

        if writes > req_wr_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes > {req_wr_high}"
            )
            #               print (f"vheavy writes {writes}")
            continue
        if writes + reads > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes+Reads > {req_high}"
            )
            #               print (f"Heavy writes+reads {writes} {reads}")
            continue
        if writes + deletes > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes+Deletes > {req_high}"
            )
            #               print (f"Heavy writes+reads {writes} {deletes}")
            continue
        if writes + heads > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes+Heads > {req_high}"
            )
            #               print (f"Heavy writes {writes} {heads}")
            continue
        if writes + reads + deletes > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes+Reads+Deletes > {req_high}"
            )
            #               print (f"Heavy writes {writes} {heads} {deletes} ")
            continue
        if writes + reads + heads > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Writes+Reads+Heads > {req_high}"
            )
            #               print (f"Heavy writes {writes} {heads} {heads} ")
            continue
        if reads + heads + deletes > req_high:
            attribute_dict["Object_req_Category"].append(
                f"Heavy Reads+Heads_Deletes > {req_high}"
            )
            #               print (f"Heavy writes {writes} {heads} {heads} ")
            continue
        if heads > req_mid:
            attribute_dict["Object_req_Category"].append(f"Mostly Heads > {req_mid}")
            #               print (f"Mostly Heads {heads}")
            continue
        if heads + deletes > req_mid:
            attribute_dict["Object_req_Category"].append(
                f"Mostly Heads+Deletes > {req_mid}"
            )
            #               print (f"Mostly Heads {heads}")
            continue
        else:
            #               print (f"Not supposed to be herei: writes + reads + heads + deletes + copys {writes} {reads} {heads} {deletes} {copys}")
            exit(0)


######### Operations Category: #########################
def cat_op_volume(bucket_dict, attribute_dict):
    row_len = len(bucket_dict["name"])
    op_ct_huge_vol = 100  # above
    op_ct_high_vol = 40  # above
    op_ct_medium_vol = 10  # above
    op_ct_low_vol = 10  # below
    op_ct_tiny_vol = 1  # below

    attribute_dict["Operations_Category"] = []
    for i in range(row_len):
        tot_op = int(bucket_dict["tot_op"][i])
        if tot_op > op_ct_huge_vol * M:
            attribute_dict["Operations_Category"].append(
                f"Huge Volume >{op_ct_huge_vol}M requests"
            )
        else:
            if tot_op > op_ct_high_vol * M:
                attribute_dict[f"Operations_Category"].append(
                    f"High Volume {op_ct_high_vol}M-{op_ct_huge_vol}M requests"
                )
            else:
                if tot_op > op_ct_medium_vol * M:
                    attribute_dict["Operations_Category"].append(
                        f"Medium Volume {op_ct_medium_vol}M-{op_ct_high_vol}M requests"
                    )
                else:
                    if tot_op < op_ct_tiny_vol * M:
                        attribute_dict["Operations_Category"].append(
                            f"Tiny Volume  <{op_ct_tiny_vol}M requests "
                        )
                    else:
                        attribute_dict["Operations_Category"].append(
                            f"Low Volume {op_ct_tiny_vol}M-{op_ct_medium_vol}M requests"
                        )


######### Object Size Category: #########################
# This is per obj not per req ################
def cat_obj_size(bucket_dict, attribute_dict):
    row_len = len(bucket_dict["name"])
    attribute_dict["Object_Size_Category"] = []
    above_99 = 99
    above_20 = 20
    above_3 = 3
    for i in range(row_len):
        size_up_1M = float(bucket_dict["size_up_1MB"][i])
        size_1M_10M = float(bucket_dict["size_1MB_10MB"][i])
        size_10M_100M = float(bucket_dict["size_10MB_100MB"][i])
        size_100M_1GB = float(bucket_dict["size_100MB_1GB"][i])
        size_1GB_10GB = float(bucket_dict["size_1GB_10GB"][i])
        size_above_10GB = float(bucket_dict["size_above_10GB"][i])
        tot_obj = (
            size_up_1M + size_1M_10M + size_10M_100M + size_100M_1GB + size_above_10GB
        )
        if size_up_1M / tot_obj * 100 > above_99:
            attribute_dict["Object_Size_Category"].append(
                f"Obj size tiny, [{above_99}% < 1MB]"
            )
        else:
            if (size_up_1M + size_1M_10M) * 1.0 / tot_obj * 100 > above_99:
                attribute_dict["Object_Size_Category"].append(
                    f"Obj size low [{above_99}% < 10MB]"
                )
            else:
                if (
                    size_up_1M + size_1M_10M + size_10M_100M
                ) / tot_obj * 100 > above_99:
                    attribute_dict["Object_Size_Category"].append(
                        f"Obj size low-medium [{above_99}% < 100MB]"
                    )
                else:
                    if (
                        size_100M_1GB + size_1GB_10GB + size_above_10GB
                    ) / tot_obj * 100 > above_20:
                        attribute_dict["Object_Size_Category"].append(
                            f"Obj size high size [{above_20}% > 100MB]"
                        )
                    else:
                        if (
                            size_100M_1GB + size_1GB_10GB + size_above_10GB
                        ) / tot_obj * 100 > above_3:
                            attribute_dict["Object_Size_Category"].append(
                                f"Obj size medium size [{above_3}% > 100MB]"
                            )
                        else:
                            attribute_dict["Object_Size_Category"].append(
                                f"Obj size medium-high size [{int(100.0-above_99)}-{above_3}% > 100MB])"
                            )


def cat_head_get(bucket_dict, attribute_dict):
    ######### Head Get Category: #########################
    row_len = len(bucket_dict["name"])
    heads_with_reads_vhigh_th = 99  # very high
    heads_with_reads_high_th = 90  #  high
    heads_with_reads_high_quarter = 75  #  high
    heads_with_reads_mid_high_quarter = 50  #  mid-high
    heads_with_reads_mid_low_quarter = 25  #  mid-low
    heads_sufficient = 10
    attribute_dict["Heads_with_reads"] = []

    for i in range(row_len):
        heads_with_reads = float(bucket_dict["one_head_with_read"][i]) + float(
            bucket_dict["num_few_heads_with_reads"][i]
        )
        heads = float(bucket_dict["heads"][i])
        tot_op = float(bucket_dict["tot_op"][i])
        if heads / tot_op * 100 < heads_sufficient:
            attribute_dict["Heads_with_reads"].append(
                f"Not enough Heads less than {heads_sufficient}"
            )
            #               print (f"Not enough heads {i} {heads/tot_op*100}")
            continue
        else:
            if heads_with_reads / heads * 100 > heads_with_reads_vhigh_th:
                attribute_dict["Heads_with_reads"].append(
                    f"Very Heavy Heads with read > {heads_with_reads_vhigh_th}"
                )
                #                    print (f"Very Heavy Heads {heads_with_reads/heads*100}")
                continue
            if heads_with_reads / heads * 100 > heads_with_reads_high_th:
                attribute_dict["Heads_with_reads"].append(
                    f"Heavy Heads with read > {heads_with_reads_high_th}"
                )
                #                    print (f"Heavy Heads {heads_with_reads/heads*100}")
                continue
            if (heads_with_reads / heads * 100) < (100 - heads_with_reads_vhigh_th):
                attribute_dict["Heads_with_reads"].append(
                    f"Tiny Heads with read  < {100-heads_with_reads_vhigh_th}"
                )
                continue
            if (heads_with_reads / heads * 100) < (100 - heads_with_reads_high_th):
                attribute_dict["Heads_with_reads"].append(
                    f"Low Heads with read < {100-heads_with_reads_high_th}"
                )
                #                    print (f"Heavy Heads No Reads {heads_with_reads/heads*100}")
                continue
            else:
                if heads_with_reads / heads * 100 > heads_with_reads_high_quarter:
                    attribute_dict["Heads_with_reads"].append(
                        f"Heads with Reads [high] between {heads_with_reads_high_quarter}-{heads_with_reads_high_th}%"
                    )
                    #                       print (f"Heads with Reads high  {heads_with_reads/heads*100}%")
                    continue

                if heads_with_reads / heads * 100 > heads_with_reads_mid_high_quarter:
                    attribute_dict["Heads_with_reads"].append(
                        f"Heads with Reads [mid-high] between {heads_with_reads_mid_high_quarter}-{heads_with_reads_high_quarter}%"
                    )
                    #                       print (f"Heads with Reads mid high {heads_with_reads/heads*100}%")
                    continue

                if heads_with_reads / heads * 100 > heads_with_reads_mid_low_quarter:
                    attribute_dict["Heads_with_reads"].append(
                        f"Heads with Reads [mid-low] between {heads_with_reads_mid_low_quarter}-{heads_with_reads_mid_high_quarter}%"
                    )
                    #                       print (f"Heads with Reads mid low {heads_with_reads/heads*100}%")
                    continue
                else:
                    attribute_dict["Heads_with_reads"].append(
                        f"Heads with Reads [low] between {100-heads_with_reads_high_th}-{heads_with_reads_mid_low_quarter}%"
                    )


#                      print (f"Heads with Reads low {heads_with_reads/heads*100}%")


######### Write Only Category: #########################
def cat_wr_only(bucket_dict, attribute_dict):
    row_len = len(bucket_dict["name"])
    wron_vhigh_th = 40  # very high
    wron_high_th = 20  # high
    wron_med_h_th = 10  # medium-high
    wron_med_l_th = 5  # medium-low
    # between 1-5 low
    wron_vlow_th = 1  # very low
    attribute_dict["Write_Only_Category"] = []
    for i in range(row_len):
        if bucket_dict["write_only"][i] == "N/A" or bucket_dict["uniq_obj"][i] == "N/A":
            attribute_dict["Write_Only_Category"].append(f"N/A")
            continue
        uniq_obj = float(bucket_dict["uniq_obj"][i])

        write_only = float(bucket_dict["write_only"][i])
        if write_only / uniq_obj * 100 > wron_vhigh_th:
            attribute_dict["Write_Only_Category"].append(
                f"WrOnly Very High > {wron_vhigh_th}%"
            )
        else:
            if write_only / uniq_obj * 100 > wron_high_th:
                attribute_dict["Write_Only_Category"].append(
                    f"WrOnly High {wron_high_th}%-{wron_vhigh_th}%"
                )
            else:
                if write_only / uniq_obj * 100 > wron_med_h_th:
                    attribute_dict["Write_Only_Category"].append(
                        f"WrOnly Medium-High {wron_med_h_th}%-{wron_high_th}%"
                    )
                else:
                    if write_only / uniq_obj * 100 > wron_med_l_th:
                        attribute_dict["Write_Only_Category"].append(
                            f"WrOnly Medium-Low {wron_med_l_th}%-{wron_med_h_th}%"
                        )
                    else:
                        if write_only / uniq_obj * 100 < wron_vlow_th:
                            attribute_dict["Write_Only_Category"].append(
                                f"WrOnly Very Low <{1}%"
                            )
                        else:
                            attribute_dict["Write_Only_Category"].append(
                                f"WrOnly Low {5}%-{1}%"
                            )


######### one-hit wonder Category: #########################
def cat_1hit_wonder_only(bucket_dict, attribute_dict):
    row_len = len(bucket_dict["name"])
    one_hit_wonder_decile = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    one_hit_wonder_low = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    attribute_dict["One-Hit_Wonder_Decile[PER_OBJ]"] = []
    attribute_dict["One-Hit_Wonder_LOW[PER_OBJ]"] = []
    attribute_dict["One-Hit_Wonder_Decile[PER_GET]"] = []
    attribute_dict["One-Hit_Wonder_LOW[PER_GET]"] = []
    for i in range(row_len):
        if bucket_dict["get_once"][i] == "N/A":
            attribute_dict["One-Hit_Wonder_Decile[PER_OBJ]"].append(f"N/A")
            attribute_dict["One-Hit_Wonder_LOW[PER_OBJ]"].append(f"N/A")
            attribute_dict["One-Hit_Wonder_Decile[PER_GET]"].append(f"N/A")
            attribute_dict["One-Hit_Wonder_LOW[PER_GET]"].append(f"N/A")
            continue

        get_once = float(bucket_dict["get_once"][i])
        uniq_obj = float(bucket_dict["uniq_obj"][i])
        reads = float(bucket_dict["reads"][i])

        # per obj
        for decile in one_hit_wonder_decile:
            if get_once / uniq_obj * 100 <= decile:
                attribute_dict["One-Hit_Wonder_Decile[PER_OBJ]"].append(
                    f"One-hit Wonder {decile}th Percentile"
                )
                break

        if get_once / uniq_obj * 100 < 10:
            for low in one_hit_wonder_low:
                if get_once / uniq_obj * 100 < low:
                    attribute_dict["One-Hit_Wonder_LOW[PER_OBJ]"].append(
                        f"One-hit Wonder low between {low-1}-{low}"
                    )
                    break
        else:
            attribute_dict["One-Hit_Wonder_LOW[PER_OBJ]"].append(f"N/A")

        # per read
        for decile in one_hit_wonder_decile:
            if get_once * 1.0 / reads * 100 <= decile:
                attribute_dict["One-Hit_Wonder_Decile[PER_GET]"].append(
                    f"One-hit Wonder {decile}th Percentile"
                )
                break

        if get_once / reads * 100 < 10:
            for low in one_hit_wonder_low:
                if get_once / reads * 100 <= low:
                    attribute_dict["One-Hit_Wonder_LOW[PER_GET]"].append(
                        f"One-hit Wonder low between {low-1}-{low}"
                    )
                    break
        else:
            attribute_dict["One-Hit_Wonder_LOW[PER_GET]"].append(f"N/A")


def main():
    parser = argparse.ArgumentParser(description="add insights to the raw results")
    parser.add_argument("filename", help="Path to filename that have the raw results")
    parser.add_argument("save_res", help="Path to file that saves the results")
    args = parser.parse_args()
    obj_dict = OrderedDict()

    with open(args.filename, mode="r") as csv_file:
        csv_reader = csv.reader(csv_file)

        # Read the first row as keys
        keys = next(csv_reader)

        # Initialize the dictionary with empty lists
        for key in keys:
            obj_dict[key] = []

        # Read the remaining rows and populate the dictionary
        for row in csv_reader:
            # Use a dictionary comprehension to create key-value pairs
            # from the row and keys
            for key, value in zip(keys, row):
                obj_dict[key].append(value)

        bucket_dict = merge_rows(obj_dict)

        # Create an attribute_dict
        attribute_dict = OrderedDict()

        len(bucket_dict["name"])

        cat_req_type(bucket_dict, attribute_dict)
        cat_op_volume(bucket_dict, attribute_dict)
        cat_obj_size(bucket_dict, attribute_dict)
        cat_head_get(bucket_dict, attribute_dict)
        cat_wr_only(bucket_dict, attribute_dict)
        cat_1hit_wonder_only(bucket_dict, attribute_dict)

    ##### Write results ###########
    res = OrderedDict()
    for key in attribute_dict.keys():
        res[key] = attribute_dict[key]

    for key in bucket_dict:
        res[key] = bucket_dict[key]

    with open(args.save_res, mode="w", newline="") as csv_file:
        # Create a CSV writer object
        csv_writer = csv.writer(csv_file)

        # Write the ordered dictionary as a row in the CSV file
        csv_writer.writerow(res.keys())
        for row in range(len(res["Object_Size_Category"])):
            row_to_print = []
            for key in res.keys():
                row_to_print.append(res[key][row])
            csv_writer.writerow(row_to_print)
    return res


# TODO: add more than one part
# calculate the % and the avg, write once[N/A], median [N/A]
# remove total req in Millions

# Requests Category
# Head + get category -> fix bug head - head - get
# Read/ head/ write....

# Insights
# Read Once
# Write Once


if __name__ == "__main__":
    main()
