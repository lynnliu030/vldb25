#!/bin/bash

# Check if num_gpus is provided; if not, default to 1
if [ $# -eq 0 ]; then
    echo "Number of GPUs not specified, defaulting to 1"
    num_gpus=1
else
    num_gpus=$1
fi
echo "Number of GPUs to be used: $num_gpus"

# Base port number for API servers
base_port=8000
model_name="meta-llama/Meta-Llama-3-8B-Instruct"

# Output is stored in logs directory
log_dir="logs"
mkdir -p $log_dir

# Iterate over the number of GPUs and start an API server for each
# NOTE: enable prefix caching for now 
for gpu_id in $(seq 0 $(($num_gpus - 1)))
do
    port=$(($base_port + $gpu_id))
    log_file="$log_dir/server_${port}.log"
    
    echo "Starting API server on port $port using GPU $gpu_id. Logs will be stored in $log_file"
    CUDA_VISIBLE_DEVICES=$gpu_id python -m vllm.entrypoints.openai.api_server --model $model_name --dtype auto --port $port --enable-prefix-caching > $log_file 2>&1 &
done

wait