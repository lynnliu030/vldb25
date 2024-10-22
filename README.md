# Cloud Storage 

## Prototype Code 
This repository contains our prototype code implementing GlobalStore, a global object store built on top of the object stores in the cloud. It currently support S3, Azure Blob, and GCS. Users interact with SkyStore through S3 API.

This repository contains:
1. Multi-cloud traces and workloads  
2. S3-Proxy: a fully compatible web server that speaks S3 protocol, communciates with Store-Server, and talks to Azure Blob, S3, and GCS underneath the hood
3. Store-Server: a FastAPI server that maintains namespace mappings and various policies. It speaks to the database (e.g. Postgres / SQLite) via SQLAlchemy model.

This repository is structured as follows:
* `/prototype/experiment/trace` - the IBM traces used in our benchmarks 
* `/prototype/s3-proxy/src` - main implementation of S3-proxy written in Rust
* `/prototype/store-server` - the store-server implementations of virtual bucket and virtual object abstractions, along with placement and eviction policies

Prerequisites: see `requirements.txt` in `/prototype/store-server`

### Test S3-Proxy and Store-Server Locally 

To setup the environment:

- Ensure that you have Python and Rust toolchain installed.
- `cargo install just`. We use `just` as a task runner.

```bash
cd store-server
pip install -r requirements.txt
```

```bash
cd s3-proxy
just install-local-s3

# run the following commands in separate windows as they are blocking.
just run-skystore-server
just run-local-s3
just run
```

The S3 proxy should now be serving requests at `http://localhost:8002`.

You can use the AWS CLI or any S3 client to interact with the proxy. Do note that you will need to set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to some dummy values. Checkout `s3-proxy/justfile` for reference.

Run some sample commands using the AWS CLI:

```bash
cd s3-proxy
just run-cli-create-bucket
just run-cli-list-buckets 
just run-cli-put
just run-cli-get
just run-cli-list
just run-cli-multipart
```

Test the server
```
cd store-server
just test
```

### Setting Up Store-Server and S3-Proxy in remote VMs 
* End-to-end benchmark is run in this [script](https://github.com/lynnliu030/storage/blob/main/prototype/run_client.py)

## Simulation code 
This repository contains our simulation code implementing various policies in `/simulation` folder, the instructions is in README.md inside this folder.

The folder structured as:
* `/simulation/SNIA_traces`: scripts containing how we generate multi-cloud traces
* `/simulation/benchmark`: scripts on benchmarking real object stores
* `/simulation/config`: various policy config
* `/simulation/src`: poilcy implementation - our policy is named `tevict_new` and `tevict_ranges_new` 

### Usage 
```
python main.py --config [CONFIG] --trace [TRACE] --vm [NUM_VMS] --setbase [SET_BASE] --simversion [SIM_VERSION]
```
### 2,3,6,9-region traces
The traces we use in our cost estimation is in `/traces` folder. Containing 2, 3, 6, 9 region, 5 SNIA IBM traces, with different types of workloads generated when number of regions is greater than 2. 
