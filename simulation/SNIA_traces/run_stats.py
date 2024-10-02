import subprocess
import argparse
import os

# Call main_script.py with the arguments using subprocess

#######################################################################################################################
## This script serves as a helping scrupt for executing 'stats.py' script over multiple traces.
## It utilizes a subprocess to execute 'stats.py'.
#######################################################################################################################


def run_stats(filename, save_res, add_keys):
    script_directory = os.path.dirname(__file__)
    cmd = ["python3", os.path.join(script_directory, "stats.py"), filename, save_res]
    if add_keys:
        cmd.append("--add_keys")
    completed_process = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout_output = completed_process.stdout.strip()
    stderr_output = completed_process.stderr.strip()

    # Check if the subprocess completed successfully
    if completed_process.returncode == 0:
        # Read the return value from the file
        print(f"Return value from stats.py: {stdout_output}")
    else:
        print(f"Error from stats.py: {stderr_output}")


def main():
    parser = argparse.ArgumentParser(
        description="Load a text file that have the files to run on ststs.py"
    )
    parser.add_argument("files", help="Path to list files that will be exe")
    parser.add_argument("save_res", help="Path to file that saves the result in csv")
    parser.add_argument(
        "--add_keys",
        action="store_true",
        help="add the leys in addition to the res file",
    )
    args = parser.parse_args()

    with open(args.files, "r") as file:
        flag_first = True
        # Iterate over each line in the file
        for line in file:
            # Remove any leading or trailing whitespace (e.g., newline characters)
            filename = line.strip()
            if filename.startswith("#"):
                continue
            print(filename)
            if args.add_keys is not True:
                flag_first = False
            run_stats(filename, args.save_res, flag_first)
            if flag_first is True:
                flag_first = False


if __name__ == "__main__":
    main()
