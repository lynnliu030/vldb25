import csv
import argparse
import pickle
import os
import subprocess

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

    # Run the grep command'
    assert len(args.src_regions) == len(
        args.dst_regions
    ), "the size of src and dst regions are not eq"

    # Run the grep command
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

    input_file = args.inputfile
    sed_command = 'sed "'
    for i in range(len(args.src_regions)):
        sed_command += "s/" + args.src_regions[i] + "/" + args.dst_regions[i] + "/;"

    sed_command += ' "  ' + args.inputfile + " >  " + args.outfile
    try:
        subprocess.run(sed_command, check=True, shell=True)
        print(f"Pattern replaced successfully. New file created at: {args.outfile}")
    except subprocess.CalledProcessError:
        print("An error occurred while replacing the pattern.")


if __name__ == "__main__":
    main()
