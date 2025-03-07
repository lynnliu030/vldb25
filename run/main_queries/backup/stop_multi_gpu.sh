#!/bin/bash

# Check if num_gpus is provided; if not, default to 1
if [ $# -eq 0 ]; then
    echo "Number of GPUs not specified, defaulting to 1"
    num_gpus=1
else
    num_gpus=$1
fi
echo "Number of GPUs to be stopped: $num_gpus"

base_port=8000
for gpu_id in $(seq 0 $(($num_gpus - 1)))
do
    port=$(($base_port + $gpu_id))
    
    # Find the process ID (PID) using the port and kill it
    pid=$(lsof -t -i:$port)
    if [ -z "$pid" ]; then
        echo "No process found running on port $port"
    else
        echo "Killing process $pid running on port $port"
        kill -9 $pid
    fi
done
