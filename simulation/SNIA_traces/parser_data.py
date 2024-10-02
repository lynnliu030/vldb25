import csv
import argparse
import pickle

#######################################################################################################################
## This script parses a trace and converts it into a dictionary format,
## where each object is treated as a key, and the associated values represent the metadeta of the accesses to that key.
## The script then saves these results in a pickle file
#######################################################################################################################


########## load the Trace file #############
#########  input: csv_file     #############
#########  output: dict        #############


def load_csv_to_dict(csv_file):
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
            for i, col_name in enumerate(custom_column_names):
                if i < len(row):
                    data_dict[col_name].append(row[i])
                else:
                    data_dict[col_name].append("")  # Add None for missing values

    return data_dict


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
    parser.add_argument("save_obj", default=None, help="Save the obj dict in file")
    parser.add_argument(
        "--pre_obj_dict", default=None, help="load previous obj_dict from file"
    )
    args = parser.parse_args()

    filename = args.filename

    data_dict = load_csv_to_dict(filename)
    pre_obj_sum = 0
    if args.pre_obj_dict is not None:
        obj_dict, pre_obj_sum = load_obj_dict(args.preobjdict)
    else:
        obj_dict = {}
    obj_dict = create_obj_dict(data_dict, obj_dict)
    tot_op = len(data_dict["obj_key"])
    uniq_obj = len(obj_dict.keys())
    print(
        f"The number of requests are {tot_op+pre_obj_sum} (pre req {pre_obj_sum}) and the number of unique obj are {uniq_obj} which is {uniq_obj/(tot_op+pre_obj_sum)*100:.2f}%"
    )
    sum_req = 0
    for i in obj_dict.keys():
        sum_req += len(obj_dict[i])
    assert sum_req == tot_op + pre_obj_sum

    del data_dict
    if args.save_obj is not None:
        save_obj_dict(obj_dict, args.save_obj)


if __name__ == "__main__":
    main()
