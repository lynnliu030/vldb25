import csv
import argparse
import pickle
import os

#######################################################################################################################
## This script parses a trace and converts it into a dictionary format,
## where each object is treated as a key, and the associated values represent the metadeta of the accesses to that key.
## The script then saves these results in a pickle file
#######################################################################################################################


########## load the Trace file #############
#########  input: csv_file     #############
#########  output: dict        #############


def count_ranges(csv_file, save_file):
    custom_column_names = [
        "timestamp",
        "op",
        "obj_key",
        "size",
        "range_rd_begin",
        "range_rd_end",
    ]
    delimiter = " "  # Set the delimiter to space
    name = os.path.basename(csv_file)
    data_dict = {
        col_name: [] for col_name in custom_column_names
    }  # Create a dictionary with custom column names
    count_gets = 0
    count_range = 0
    output_list = []
    with open(csv_file, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=delimiter)
        for row in csvreader:
            # Assign values to custom column names if available
            if "GET" in row[1]:
                count_gets += 1
                if len(row) > 4:
                    if int(row[3]) != (int(row[4]) + int(row[5]) + 1):
                        count_range += 1
    output_list.append(
        [name, count_gets, count_range, f"{count_range/count_gets * 100:.2f}%"]
    )

    with open(save_file, mode="a", newline="") as csv_file:
        # Create a CSV writer object
        csv_writer = csv.writer(csv_file)
        for i in range(len(output_list)):
            csv_writer.writerow(output_list[i])
    return count_gets, count_range


########## split the data_dict to obj_dict  #############
##########  objs the key and the value is
##### We can load from already obj_dict     #########
#########  Return: obj_dict                 #############


def create_obj_dict(data_dict, obj_dict):
    tot_op = len(data_dict["obj_key"])
    print(f"The number of Operations are {tot_op}")
    for i in range(tot_op):
        if i % 100000 == 0:
            print(f"process {i}/{tot_op}")
        key = data_dict["obj_key"][i]
        if key not in obj_dict:
            obj_dict[key] = []
        if data_dict["op"][i] == "REST.PUT.OBJECT":
            data_dict["op"][i] = "PUT"
        if data_dict["op"][i] == "REST.GET.OBJECT":
            data_dict["op"][i] = "GET"
        if data_dict["op"][i] == "REST.HEAD.OBJECT":
            data_dict["op"][i] = "HEAD"
        if data_dict["op"][i] == "REST.COPY.OBJECT":
            data_dict["op"][i] = "COPY"
        if data_dict["op"][i] == "REST.DELETE.OBJECT":
            data_dict["op"][i] = "DELETE"

        obj_dict[key].append(
            [
                data_dict["timestamp"][i],
                data_dict["op"][i],
                data_dict["size"][i],
                data_dict["range_rd_begin"][i],
                data_dict["range_rd_begin"][i],
            ]
        )
    return obj_dict


def save_obj_dict(obj_dict, save_file):
    # Save the dictionary to the Pickle file
    with open(save_file, "wb") as pickle_file:
        pickle.dump(obj_dict, pickle_file)


##########  load obj dict  ########################
######### the value is a list that  contain ########
####  [ts, req,"size", "off_begin", "off_end"] #####
#########  Return: obj_dict           #############


def load_obj_dict(obj_file):
    tot_op = 0
    with open(obj_file, "rb") as pickle_file:
        obj_dict = pickle.load(pickle_file)
        len(obj_dict.keys())

    for i in obj_dict.keys():
        tot_op += len(obj_dict[i])
    return obj_dict, tot_op


########## Main  ###########


def main():
    parser = argparse.ArgumentParser(
        description="Load a CSV file without a header and convert it to a dictionary."
    )
    parser.add_argument("filename", help="Path to the CSV file")
    parser.add_argument("save_file", default=None, help="Save the obj dict in file")
    args = parser.parse_args()

    filename = args.filename

    count_ranges(filename, args.save_file)


if __name__ == "__main__":
    main()
