import json
import typer
from typing import Dict, List
from skyplane import compute
from skyplane.cli.experiments.provision import provision
from skyplane.compute.const_cmds import make_sysctl_tcp_tuning_command
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.compute.aws.aws_auth import AWSAuthentication
import csv
import time
from datetime import datetime
import json
import asyncio

from tqdm import tqdm  # Import tqdm for the progress bar
from collections import defaultdict

all_aws_regions = compute.AWSCloudProvider.region_list()
all_azure_regions = compute.AzureCloudProvider.region_list()
all_gcp_regions = compute.GCPCloudProvider.region_list()
all_gcp_regions_standard = compute.GCPCloudProvider.region_list_standard()
all_ibmcloud_regions = compute.IBMCloudProvider.region_list()


def aws_credentials():
    auth = AWSAuthentication()
    access_key, secret_key = auth.get_credentials()
    return access_key, secret_key
    
def create_instance(
    aws_region_list=None,
    azure_region_list=None,
    gcp_region_list=None,
    gcp_standard_region_list=None,
    ibmcloud_region_list=None,
    enable_aws=True,
    enable_azure=True,
    enable_gcp=True,
    enable_gcp_standard=True,
    enable_ibmcloud=False,
    aws_instance_class="m5.8xlarge",
    azure_instance_class="Standard_D32ps_v5",
    gcp_instance_class="n2-standard-32",
    ibmcloud_instance_class="bx2-2x8",
):
    def check_stderr(tup):
        assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

    # validate AWS regions
    aws_region_list = aws_region_list if enable_aws else []
    azure_region_list = azure_region_list if enable_azure else []
    gcp_region_list = gcp_region_list if enable_gcp else []
    ibmcloud_region_list = ibmcloud_region_list if enable_ibmcloud else []
    if not enable_aws and not enable_azure and not enable_gcp and not enable_ibmcloud:
        logger.error("At least one of -aws, -azure, -gcp, -ibmcloud must be enabled.")
        raise typer.Abort()

    # validate AWS regions
    if not enable_aws:
        aws_region_list = []
    elif not all(r in all_aws_regions for r in aws_region_list):
        logger.error(f"Invalid AWS region list: {aws_region_list}")
        raise typer.Abort()

    # validate Azure regions
    if not enable_azure:
        azure_region_list = []
    elif not all(r in all_azure_regions for r in azure_region_list):
        logger.error(f"Invalid Azure region list: {azure_region_list}")
        raise typer.Abort()

    # validate GCP regions
    gcp_region_list = ["us-west1-a"]
    gcp_standard_region_list = ["us-west1-a"]
    
    assert (
        not enable_gcp_standard or enable_gcp
    ), "GCP is disabled but GCP standard is enabled"
    if not enable_gcp:
        gcp_region_list = []
    elif not all(r in all_gcp_regions for r in gcp_region_list):
        logger.error(f"Invalid GCP region list: {gcp_region_list}")
        raise typer.Abort()

    # validate GCP standard instances
    if not enable_gcp_standard:
        gcp_standard_region_list = []
    if not all(r in all_gcp_regions_standard for r in gcp_standard_region_list):
        logger.error(f"Invalid GCP standard region list: {gcp_standard_region_list}")
        raise typer.Abort()

    # validate IBM Cloud regions
    if not enable_ibmcloud:
        ibmcloud_region_list = []
    elif not all(r in all_ibmcloud_regions for r in ibmcloud_region_list):
        logger.error(f"Invalid IBM Cloud region list: {ibmcloud_region_list}")
        raise typer.Abort()

    # provision servers
    aws = compute.AWSCloudProvider()
    azure = compute.AzureCloudProvider()
    gcp = compute.GCPCloudProvider()
    ibmcloud = compute.IBMCloudProvider()

    aws_instances, azure_instances, gcp_instances, ibmcloud_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        ibmcloud=ibmcloud,
        aws_regions_to_provision=aws_region_list,
        azure_regions_to_provision=azure_region_list,
        gcp_regions_to_provision=gcp_region_list,
        ibmcloud_regions_to_provision=ibmcloud_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        ibmcloud_instance_class=ibmcloud_instance_class,
        aws_instance_os="ubuntu",
        gcp_instance_os="ubuntu",
        gcp_use_premium_network=True,
    )
    # instance_list: List[compute.Server] = [i for ilist in aws_instances.values() for i in ilist]
    instances_dict: Dict[str, compute.Server] = {}

    # AWS instances
    for region, ilist in aws_instances.items():
        for instance in ilist:
            instances_dict[f"aws:{region}"] = instance

    # Azure instances
    for region, ilist in azure_instances.items():
        for instance in ilist:
            instances_dict[f"azure:{region}"] = instance

    # GCP instances
    for region, ilist in gcp_instances.items():
        for instance in ilist:
            instances_dict[f"gcp:{region}"] = instance

    # TODO: change this to the actual server address
    server_addr = "xxxx"
    get_policy = "cheapest"
    put_policy = "always_store"
    # put_policy = "always_evict"
    # put_policy = "skystore"
    
    # setup instances
    def setup(server: compute.Server):
        print("Setting up instance: ", server.region_tag)
        
        if server.region_tag.startswith("aws"):
            username = "ubuntu"
        else:
            username = "skyplane"
        
        config_content = {
            "init_regions": [f"aws:{region}" for region in aws_region_list]
            + [f"azure:{region}" for region in azure_region_list]
            + [f"gcp:{region}" for region in gcp_region_list]
            + [f"ibmcloud:{region}" for region in ibmcloud_region_list],
            "client_from_region": server.region_tag,
            "skystore_bucket_prefix": "skystore-update",
            "put_policy": put_policy,
            "get_policy": get_policy,
            "server_addr": server_addr,  # TODO: change this to the actual server address
        }
        
        print(f"Setup config content: {config_content}")
        config_file_path = f"/home/{username}/init_config_{server.region_tag}.json"
        check_stderr(
            server.run_command(
                f"echo '{json.dumps(config_content)}' > {config_file_path}"
            )
        )
        
        check_stderr(
            server.run_command(
                "echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections"
            )
        )
        server.run_command(
            "sudo apt remove python3-apt -y; sudo apt autoremove -y; \
            sudo apt autoclean; sudo apt install python3-apt -y; \
            (sudo apt-get update && sudo apt-get install python3-pip -y && sudo pip3 install awscli);\
            sudo apt install python3.9 python3-apt pkg-config libssl-dev -y\
            sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1;\
            sudo update-alternatives --config python3"
        )
        server.run_command(make_sysctl_tcp_tuning_command(cc="cubic"))
        server.run_command("cd ~")
        
        clone_cmd = f"git clone https://github.com/skyplane-project/skystore.git; cd skystore; git switch policy; git pull;"
        cmd1 = f"sudo apt remove python3-apt -y; sudo apt autoremove -y; \
                sudo apt autoclean; sudo apt install python3-apt -y; sudo apt-get update; \
                sudo apt install python3.9 -y; sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1; \
                sudo update-alternatives --config python3; \
                sudo pip3 install awscli; \
                curl https://sh.rustup.rs -sSf | sh -s -- -y; source $HOME/.cargo/env;\
                {clone_cmd} "

        cmd2 = f'/home/{username}/.cargo/bin/cargo install just --force; \
                sudo apt install pkg-config libssl-dev; \
                cd /home/{username}/skystore; \
                curl -sSL https://install.python-poetry.org | python3 -; \
                /home/{username}/.local/bin/poetry install; python3 -m pip install pip==23.2.1; \
                export PATH="/home/{username}/.local/bin:$PATH"; pip3 install -e .; cd store-server; \
                sudo apt-get install libpq-dev -y; sudo apt-get install python3.9-dev -y; \
                pip3 install -r requirements.txt; \
                cd ../s3-proxy; \
                  /home/{username}/.cargo/bin/cargo install --force \
                --git https://github.com/Nugine/s3s \
                --rev 0cc49cf24c05eeb6a809882d1a7b76e953822c0d \
                --bin s3s-fs \
                --features="binary" \
                s3s-fs; \
                cd ..; \
                skystore exit; '
        cmd3 = f"cd /home/{username}/skystore; \
                /home/{username}/.cargo/bin/cargo build --release; \
                nohup python3 send.py --server_addr {server_addr} > send_output 2>&1 & \
                nohup /home/{username}/.local/bin/skystore init --config {config_file_path} > data_plane_output 2>&1 &"
        server.run_command(cmd1)
        server.run_command(cmd2)
        server.run_command(cmd3)
        
    def setup_simple(server: compute.Server):
        if server.region_tag.startswith("aws"):
            username = "ubuntu"
        else:
            username = "skyplane"
            
        print("Setting up instance: ", server.region_tag)
        server.run_command(make_sysctl_tcp_tuning_command(cc="cubic"))
        
        # Set up other stuff
        cmd2 = f'cd skystore; /home/{username}/.local/bin/skystore exit; rm s3-proxy-output.log; rm data_plane_output; rm metrics.json'
        
        config_content = {
            "init_regions": [f"aws:{region}" for region in aws_region_list]
            + [f"azure:{region}" for region in azure_region_list]
            + [f"gcp:{region}" for region in gcp_region_list]
            + [f"ibmcloud:{region}" for region in ibmcloud_region_list],
            "client_from_region": server.region_tag,
            "skystore_bucket_prefix": "skystore-update",
            "put_policy": put_policy,
            "get_policy": get_policy,
            "server_addr": server_addr,  
        }
        print(f"Setup config content: {config_content}")
        config_file_path = f"/home/{username}/init_config_{server.region_tag}.json"
        check_stderr(
            server.run_command(
                f"echo '{json.dumps(config_content)}' > {config_file_path}"
            )
        )
        
        cmd3 = f"cd /home/{username}/skystore; \
                /home/{username}/.cargo/bin/cargo build --release; \
                nohup python3 send.py --server_addr {server_addr} > send_output 2>&1 & \
                nohup /home/{username}/.local/bin/skystore init --config {config_file_path} > data_plane_output 2>&1 &"

        server.run_command(cmd2)
        server.run_command(cmd3)

    do_parallel(setup_simple, list(instances_dict.values()), spinner=True, n=-1, desc="Setup")
    return instances_dict


def extract_regions_from_trace(trace_file_path: str) -> Dict[str, List[str]]:
    regions = {
        "aws": set(),
        "azure": set(),
        "gcp": set(),
    }

    with open(trace_file_path, "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            provider, region = row[2].split(":")
            regions[provider].add(region)

    for provider in regions:
        regions[provider] = list(regions[provider])

    return regions

async def generate_file_on_server(server, size, filename):
    cmd = f"test -f {filename} && echo 'File exists' || (dd if=/dev/urandom of={filename} bs={size} count=1 && echo 'File generated')"
    stdout, stderr = await async_run_command(server, cmd)
    
    if "File exists" in stdout:
        print(f"File {filename} already exists on {server.region_tag}, skipping generation.")
    else:
        print(f"File {filename} was generated on {server.region_tag}.")

async def generate_files_on_servers(instances_dict: Dict[str, compute.Server], trace_file_path: str):
    """
    Generates necessary files on the servers before starting the trace operations.
    """ 
    tasks = []  

    with open(trace_file_path, "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader) 
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size, answer_region = row
            server = instances_dict.get(issue_region)
            if server and op == "PUT": 
                filename = f"{data_id}.data"
                tasks.append(generate_file_on_server(server, size, filename))

    await asyncio.gather(*tasks)
    

put_events: Dict[str, asyncio.Event] = {}
previous_put: Dict[str, bool] = {}
def on_task_completion(fut, data_id):
    success = fut.result()
    if success:
        previous_put[data_id] = True
        put_events[data_id].set()  # Signal that PUT is done
        print(f"PUT completed for object {data_id} and event set.")
    else:
        print(f"PUT failed for object {data_id}.")

async def async_run_command(server, cmd, retries=3, delay=5):
    print(f"\nExecuting on {server} with {cmd}", flush=True)
    try: 
        stdout, stderr = await asyncio.to_thread(server.run_command, cmd)
        print(f"\n{cmd} finishes on {server}: \nstdout: {stdout}\nstderr: {stderr}")
        return stdout, stderr
    except Exception as e:
        print(f"Error executing command: {cmd} on {server}: {e}")
        return None, str(e)

import asyncio
import time
from collections import defaultdict

first_get_events: Dict[str, asyncio.Event] = {}

async def issue_requests(trace_file_path: str, instances_dict: Dict[str, compute.Server]):
    s3_args = "--endpoint-url http://127.0.0.1:8002 --no-verify-ssl"

    read_op = "GET"
    write_op = "PUT"

    pending_tasks = [] 
    requests_by_timestamp = defaultdict(list)

    with open(trace_file_path, "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader) 
        tot_reqs = 0
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size, answer_region = row
            tot_reqs += 1 
            requests_by_timestamp[int(timestamp_str)].append((op, issue_region, data_id, size, answer_region))
    print(f"Total requests: {tot_reqs}")

    sorted_timestamps = sorted(requests_by_timestamp.keys())
    
    last_issue_time = None 
    start_time = time.time()
    progress = 0
    print_batch_size = 1000
    print("Starting to issue requests...")

    for i, current_timestamp in enumerate(sorted_timestamps):
        current_requests = requests_by_timestamp[current_timestamp]
        
        if last_issue_time is not None:
            previous_timestamp = sorted_timestamps[i - 1]
            timestamp_diff = (current_timestamp - previous_timestamp) * 30 * 3  
            physical_elapsed_time = time.time() - last_issue_time 
            remaining_wait_time = (timestamp_diff / 1000.0) - physical_elapsed_time
            if remaining_wait_time > 0:
                try:
                    done, pending = await asyncio.wait(pending_tasks, timeout=remaining_wait_time)
                    pending_tasks = list(pending) 
                    terminate_time = time.time() - last_issue_time
                    if terminate_time > remaining_wait_time:
                        print(f"TIMEOUT: Physical time exceeded timestamp difference. Waiting for {remaining_wait_time} seconds before issuing timestamp {current_timestamp}...")
                    else:
                        print(f"EXECUTION: Waiting for {terminate_time:.3f} seconds before issuing timestamp {current_timestamp}...")
                        
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for previous timestamp tasks. Proceeding with timestamp {current_timestamp}.")
            else:
                print(f"Physical time exceeded timestamp difference. Proceeding with timestamp {current_timestamp}.")
        

        concurrent_tasks = []

        for row in current_requests:
            op, issue_region, data_id, size, answer_region = row
            server_key = issue_region
            server = instances_dict.get(server_key)

            if server:
                filename = f"{data_id}.data"
                if op == write_op:
                    if data_id not in put_events:
                        put_events[data_id] = asyncio.Event()

                    cmd = f"time aws s3api {s3_args} put-object --bucket default-skybucket --key {data_id} --body {filename} --cli-read-timeout 6000 --cli-connect-timeout 6000"
                    task = asyncio.create_task(async_run_command(server, cmd))
                    task.add_done_callback(lambda fut, data_id=data_id: on_task_completion(fut, data_id))
                    concurrent_tasks.append(task)

                elif op == read_op:
                    cmd = f"time aws s3api {s3_args} get-object --bucket default-skybucket --key {data_id} {filename} --cli-read-timeout 6000 --cli-connect-timeout 6000"

                    if data_id in put_events:
                        
                        async def wait_for_put_and_get(data_id, server):
                            print(f"Waiting for PUT completion for {data_id}...")
                            await put_events[data_id].wait()  
                            print(f"PUT completed for {data_id}. Now issuing GET...")
                            await async_run_command(server, cmd)

                        concurrent_tasks.append(asyncio.create_task(wait_for_put_and_get(data_id, server)))
                    else:
                        concurrent_tasks.append(asyncio.create_task(async_run_command(server, cmd)))
                else:
                    raise ValueError(f"Invalid operation: {op}")
            else:
                print(f"No server found for region: {issue_region}")

        pending_tasks.extend(concurrent_tasks)
        last_issue_time = time.time()
        progress += len(current_requests)
        if progress % print_batch_size == 0:
            takes_mins = (time.time() - start_time) / 60
            print(f"Progress: {progress}/{tot_reqs} = {progress/tot_reqs*100:.2f}% = {takes_mins:.2f} mins")
    
    if pending_tasks:
        await asyncio.gather(*pending_tasks, return_exceptions=True)
        
    print(f"All request finishes in mins (total time): {(time.time() - start_time) / 60:.3f}")
    
         
def main(trace_file_path: str):
    regions_dict = extract_regions_from_trace(trace_file_path)
    print("Extracted regions: ", regions_dict)

    enable_aws = len(regions_dict["aws"]) > 0
    enable_gcp = len(regions_dict["gcp"]) > 0
    enable_azure = len(regions_dict["azure"]) > 0
    print(
        f"enable_aws: {enable_aws}, enable_gcp: {enable_gcp}, enable_azure: {enable_azure}"
    )
    instances_dict = create_instance(
        aws_region_list=regions_dict.get("aws", []),
        azure_region_list=regions_dict.get("azure", []),
        gcp_region_list=regions_dict.get("gcp", []),
        enable_aws=enable_aws,
        enable_azure=enable_azure,
        enable_gcp=enable_gcp,
        enable_gcp_standard=enable_gcp,
        enable_ibmcloud=False,
        aws_instance_class="m5.8xlarge",
        azure_instance_class="Standard_D32ps_v5",   
        gcp_instance_class="n2-standard-32",
    )

    print("Create instance finished.", flush=True)

    print("Generating files on servers...")
    generate_files_on_servers(instances_dict, trace_file_path)
    asyncio.run(generate_files_on_servers(instances_dict, trace_file_path))
    
    print("Issuing requests right now concurrently", flush=True)
    asyncio.run(issue_requests(trace_file_path, instances_dict))

if __name__ == "__main__":
    typer.run(main)