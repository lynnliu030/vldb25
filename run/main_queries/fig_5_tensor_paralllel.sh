start_multi_gpu() {
    num_gpus=$1
    dataset=$2
    algorithm=$3
    query=$4  
    echo "Number of GPUs to be used: $num_gpus, tensor parallism"

    base_port=8000
    model_name="meta-llama/Meta-Llama-3-70B-Instruct"

    log_dir="logs/fig5/server"
    mkdir -p $log_dir

    log_file="${log_dir}/${query}_${dataset}_server_${algorithm}_${base_port}_70B_vllm_tensorparallel.log"

    # Always enable prefix cache 
    python3 -m vllm.entrypoints.openai.api_server --model $model_name --dtype auto --port $base_port --gpu-memory-utilization 0.85 --enable-prefix-caching --tensor-parallel-size $num_gpus > $log_file 2>&1 &
}

stop_multi_gpu() {
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
        port=$(($base_port + $gpu_id*100))
        pid=$(lsof -t -i:$port)
        if [ -z "$pid" ]; then
            echo "No process found running on port $port"
        else
            echo "Killing process $pid running on port $port"
            kill -9 $pid
        fi
    done
    sleep 10  
}

clear_gpu_processes() {
    echo "Clearing all GPU processes..."
    pids=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits)
    if [ -z "$pids" ]; then
        echo "No GPU processes found."
    else
        for pid in $pids; do
            echo "Killing GPU process with PID: $pid"
            kill -9 $pid
        done
    fi
}

run_tasks_on_gpus() {
    num_gpus=$1
    shift  # Remove the number of GPUs from the argument list

    query_log_dir="logs/fig5/query"
    mkdir -p $query_log_dir

    # Parse any remaining flags but skip their use, as they are for tensor parallelism
    while [[ "$1" == "--"* ]]; do
        shift
    done

    tasks=("$@")  # Remaining arguments are the tasks

    echo "Running tasks sequentially on a multi-GPU (tensor-parallel) setup with $num_gpus GPUs."

    # Iterate through each task sequentially
    for task in "${tasks[@]}"; do
        # Extract information from the task
        dataset=$(echo "$task" | grep -oP '(?<=-d\s)[^\s]+')
        algorithm=$(echo "$task" | grep -oP '(?<=-a\s)[^\s]+')
        query=$(echo "$task" | sed -E 's|.*/([a-zA-Z0-9]+)\.py.*|\1|')

        echo "Starting multi-GPU server for Dataset: $dataset, Algorithm: $algorithm, Query: $query"
        start_multi_gpu "$num_gpus" "$dataset" "$algorithm" "$query"

        sleep 120  # Wait for the server to start

        # Port is fixed to the base_port since all tasks run sequentially
        base_port=8000
        modified_task=$(echo "$task" | sed "s/>/-p $base_port >/")  # Substitute the port in the command

        # append log 
        dataset=$(echo "$task" | grep -oP '(?<=-d\s)[^\s]+')
        algorithm=$(echo "$task" | grep -oP '(?<=-a\s)[^\s]+')
        query=$(echo "$task" | sed -E 's|.*/([a-zA-Z0-9]+)\.py.*|\1|')

        modified_task="$modified_task > ${query_log_dir}/${query}_${dataset}_${algorithm}_70B_output_vllm.txt 2>&1"
        
        echo "Running task: $modified_task on port $base_port"

        # Run the task and wait for it to complete
        eval $modified_task &
        wait $!  # Wait for the task to finish

        # Stop the multi-GPU server once all tasks are completed
        echo "Stopping multi-GPU server."
        stop_multi_gpu "$num_gpus"
        clear_gpu_processes
        wait 20
    done
}


######################################################### 
# This test for tensor parallelism
# Hardware: 2x4 L4
# Baselines: naive and GGR
# Models: meta-llama/Meta-Llama-3-70B-Instruct
#########################################################

# num_gpus=8
num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)

# assert 8 gpus are available to continue 
if [ $num_gpus -ne 8 ]; then
    echo "This script requires 8 GPUs to run. Detected $num_gpus GPUs."
    exit 1
fi

stop_multi_gpu $num_gpus
clear_gpu_processes


# Defining the datasets for each query type
declare -A query_datasets=(
    [filter]="movies products BIRD PDMX beer"
)

algorithms=("naive" "quick_greedy_colmerging")

# Query types
queries=("filter")

# Initialize tasks array
tasks=()

# Constructing the tasks list
for query in "${queries[@]}"; do
    for dataset in ${query_datasets[$query]}; do
        for algorithm in "${algorithms[@]}"; do
            tasks+=("python src/pyspark/${query}.py -d ${dataset} -a ${algorithm}")
        done
    done
done

run_tasks_on_gpus $num_gpus "${tasks[@]}" # vLLM (With Cache)