import csv
import argparse
import pickle
import os
import subprocess
import random

#######################################################################################################################
## This script parses a trace and converts it into a dictionary format,
## where each object is treated as a key, and the associated values represent the metadeta of the accesses to that key.
## The script then saves these results in a pickle file
#######################################################################################################################

##########  load obj dict  ########################
######### the value is a list that  contain ########
####  [ts, req,"size", "off_begin", "off_end"] #####
#########  Return: obj_dict           #############

########## Main  ###########

random.seed(1)


def main():
    parser = argparse.ArgumentParser(
        description="Load a CSV file without a header and convert it to a dictionary."
    )
    parser.add_argument("inputfile", help="Path to the CSV file")
    parser.add_argument("outfile", default=None, help="Save the obj dict in file")
    parser.add_argument("src_regions", nargs="*", help="The source regions")
    parser.add_argument("--dst_regions", nargs="*", help="dst regions")

    args = parser.parse_args()

    if not os.path.exists(args.inputfile):
        print("The file " + args.inputfile + " does not exist.")
        exit(0)

    print(f"src {args.src_regions}")
    print(f"dst {args.dst_regions}")
    # Run the grep command'
    #    assert len(args.src_regions) == len(args.dst_regions), "the size of src and dst regions are not eq"

    # check if src exists
    for src in args.src_regions:
        pattern_found = False

        try:
            result = subprocess.run(["grep", "-q", src, args.inputfile], check=True)
            pattern_found = True
        except subprocess.CalledProcessError:
            pattern_found = False

        if pattern_found is False:
            print("region: " + src + " not found in the file. ")
            exit(0)

    # Add timestamp if needed
    try:
        result = subprocess.run(["grep", "-q", "timestamp", args.inputfile], check=True)
        pattern_found = True
    except subprocess.CalledProcessError:
        pattern_found = False

    #    result = subprocess.run(['grep', , file_path], capture_output=True, text=True)
    src = args.src_regions
    dst = []
    for i in range(len(args.dst_regions)):
        cur_dst = args.dst_regions[i].split(",")
        dst.append(cur_dst)

    for i in range(len(dst) - 1):
        assert len(dst[i]) == len(dst[i + 1])

    dst_prob = [1] * len(dst[0])

    write_handler = open(args.outfile, "w")
    csv_writer = csv.writer(write_handler)
    with open(args.inputfile, newline="") as csvfile:
        custom_column_names = "timestamp,op,issue_region,obj_key,size"
        csvreader = csv.reader(csvfile, delimiter=",")
        for row in csvreader:
            # Assign values to custom column names if available
            if row[0] == "timestamp":
                csv_writer.writerow(row)
                continue
            for idx in range(len(src)):
                if src[idx] == row[2]:
                    chosen_region = random.choices(dst[idx], dst_prob, k=1)[0]
            csv_writer.writerow([row[0], row[1], chosen_region, row[3], row[4]])


if __name__ == "__main__":
    main()
