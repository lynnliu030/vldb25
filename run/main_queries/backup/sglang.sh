start_multi_gpu_sglang() {
    if [ $# -eq 0 ]; then
        echo "Number of GPUs not specified, defaulting to 1"
        num_gpus=1
    else
        num_gpus=$1
    fi
    echo "Number of GPUs to be used: $num_gpus"

    base_port=8000
    model_name="meta-llama/Meta-Llama-3-8B-Instruct"
    mem_fraction="0.8" 

    log_dir="logs"
    mkdir -p $log_dir

    for gpu_id in $(seq 0 $(($num_gpus - 1)))
    do
        port=$(($base_port + $gpu_id * 100))

        # Check if any process is already using this port and kill it
        pid=$(lsof -t -i:$port)
        if [ -z "$pid" ]; then
            echo "No process found running on port $port"
        else
            echo "Killing process $pid running on port $port"
            kill -9 $pid
        fi

        log_file="$log_dir/server_${port}.log"
        
        # echo "Starting API server on port $port using GPU $gpu_id with $mem_fraction memory fraction. Logs will be stored in $log_file"
        echo "CUDA_VISIBLE_DEVICES=$gpu_id python -m sglang.launch_server --model-path $model_name --port $port --mem-fraction-static $mem_fraction > $log_file 2>&1 &"
        CUDA_VISIBLE_DEVICES=$gpu_id python -m sglang.launch_server --model-path $model_name --port $port --mem-fraction-static $mem_fraction > $log_file 2>&1 &

        sleep 40

        # Check for errors in the log file
        if grep -q "ERROR" "$log_file"; then
            echo "---> Error detected in log file $log_file. Restarting process on port $port."
            
            # Kill the process if there's an error in the log file
            pid=$(lsof -t -i:$port)
            if [ -z "$pid" ]; then
                echo "No process found running on port $port"
            else
                echo "Killing process $pid running on port $port"
                kill -9 $pid
            fi

            # Restart the server
            echo "Restarting API server on port $port using GPU $gpu_id with $mem_fraction memory fraction. Logs will be stored in $log_file"
            CUDA_VISIBLE_DEVICES=$gpu_id python3 -m sglang.launch_server --model-path $model_name --port $port --mem-fraction-static $mem_fraction > $log_file 2>&1 &
            sleep 30 
        fi

    done
}

start_multi_gpu() {
    if [ $# -eq 0 ]; then
        echo "Number of GPUs not specified, defaulting to 1"
        num_gpus=1
    else
        num_gpus=$1
    fi
    echo "Number of GPUs to be used: $num_gpus"

    base_port=8000
    model_name="meta-llama/Meta-Llama-3-8B-Instruct"

    log_dir="logs"
    mkdir -p $log_dir

    for gpu_id in $(seq 0 $(($num_gpus - 1)))
    do
        port=$(($base_port + $gpu_id * 100))

        # Check if any process is already using this port and kill it
        pid=$(lsof -t -i:$port)
        if [ -z "$pid" ]; then
            echo "No process found running on port $port"
        else
            echo "Killing process $pid running on port $port"
            kill -9 $pid
        fi

        log_file="$log_dir/server_${port}.log"
        
        echo "Starting API server on port $port using GPU $gpu_id. Logs will be stored in $log_file"
        CUDA_VISIBLE_DEVICES=$gpu_id python -m vllm.entrypoints.openai.api_server --model $model_name --dtype auto --port $port --enable-prefix-caching > $log_file 2>&1 &

        sleep 30 
    done
}

start_one_gpu_sglang() {
    gpu_id=$1
    dataset=$2
    algorithm=$3
    query=$4  # Pass q1 or q2 here
    no_cache=$5  # Pass no-cache option here
    echo "GPU ID: $gpu_id, Dataset: $dataset, Algorithm: $algorithm, Query: $query, No Cache: $no_cache"

    base_port=8000
    model_name="meta-llama/Meta-Llama-3-8B-Instruct"
    mem_fraction="0.88"

    log_dir="logs"
    mkdir -p "$log_dir"
    
    port=$(($base_port + $gpu_id * 100))

    # Check if any process is already using this port and kill it
    pid=$(lsof -t -i:$port)
    if [ -z "$pid" ]; then
        echo "No process found running on port $port"
    else
        echo "Killing process $pid running on port $port"
        kill -9 $pid
    fi

    # Update log file name and command based on no_cache option
    cache_suffix=""
    cache_flag=""
    if [ "$no_cache" = true ]; then
        cache_suffix="_no_cache"
        cache_flag="--disable-radix-cache"
    fi

    # Ensure variables are properly formatted and avoid any unintended expansion
    log_file="${log_dir}/${query}_${dataset}_server_${algorithm}_${port}_sglang${cache_suffix}.log"
    
    echo "CUDA_VISIBLE_DEVICES=$gpu_id python3 -m sglang.launch_server --model-path $model_name --host 0.0.0.0 --port $port --mem-fraction-static $mem_fraction --disable-cuda-graph --chunked-prefill-size 2048 --schedule-conservativeness 0.2 --schedule-policy fcfs $cache_flag > $log_file 2>&1 &"
    echo ""
    CUDA_VISIBLE_DEVICES=$gpu_id python3 -m sglang.launch_server --model-path $model_name --host 0.0.0.0 --port $port --mem-fraction-static $mem_fraction --disable-cuda-graph --chunked-prefill-size 2048 --schedule-conservativeness 0.2 --schedule-policy fcfs $cache_flag > $log_file 2>&1 &
}


start_one_gpu_vllm() {
    gpu_id=$1
    dataset=$2
    algorithm=$3
    query=$4  # Pass q1 or q2 here
    no_cache=$5  # Pass no-cache option here
    echo "GPU ID: $gpu_id, Dataset: $dataset, Algorithm: $algorithm, Query: $query, No Cache: $no_cache"

    base_port=8000
    model_name="meta-llama/Meta-Llama-3-8B-Instruct"
    log_dir="logs"
    mkdir -p $log_dir

    port=$(($base_port + $gpu_id*100))

    # Check if any process is already using this port and kill it
    pid=$(lsof -t -i:$port)
    if [ -z "$pid" ]; then
        echo "No process found running on port $port"
    else
        echo "Killing process $pid running on port $port"
        kill -9 $pid
    fi

    # Update log file name and command based on no_cache option
    cache_suffix=""
    cache_flag="--enable-prefix-caching"  # Default: with cache
    if [ "$no_cache" = true ]; then
        cache_suffix="_no_cache"
        cache_flag=""  # Remove cache flag if no_cache is true
    fi

    # NOTE: with cache 
    log_file="${log_dir}/${query}_${dataset}_server_${algorithm}_${port}_vllm${cache_suffix}.log"


    echo "CUDA_VISIBLE_DEVICES=$gpu_id python -m vllm.entrypoints.openai.api_server --model $model_name --dtype auto --port $port $cache_flag > $log_file 2>&1 &"

    CUDA_VISIBLE_DEVICES=$gpu_id python -m vllm.entrypoints.openai.api_server --model $model_name --dtype auto --port $port $cache_flag > $log_file 2>&1 &
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

    use_sglang=false  # Default to using vLLM
    no_cache=false  # Default to using cache

    # Check for flags
    if [[ "$1" == "--sglang" ]]; then
        use_sglang=true
        shift
    fi
    if [[ "$1" == "--no-cache" ]]; then
        no_cache=true
        shift
    fi

    tasks=("$@")  # Remaining arguments are the tasks

    echo "Using sglang: $use_sglang, No Cache: $no_cache"

    task_idx=0
    base_port=8000
    while [ $task_idx -lt ${#tasks[@]} ]; do
        declare -a pids=()  # Initialize empty array for PIDs
        declare -A port_to_algorithm=()  # Initialize empty dictionary for port to algorithm mapping

        # Loop to start GPU processes
        mimic_task_idx=$task_idx
        for gpu_id in $(seq 0 $(($num_gpus - 1))); do
            if [ $mimic_task_idx -lt ${#tasks[@]} ]; then
                port=$(($base_port + $gpu_id*100))
                task="${tasks[$mimic_task_idx]}"

                # Extract dataset, algorithm, and query with improved regex for clarity
                dataset=$(echo "$task" | grep -oP '(?<=-d\s)[^\s]+')
                algorithm=$(echo "$task" | grep -oP '(?<=-a\s)[^\s]+')
                query=$(echo "$task" | sed -E 's|.*/([a-zA-Z0-9]+)\.py.*|\1|')

                # Verify extracted values for debugging
                echo "Extracted values - Dataset: $dataset, Algorithm: $algorithm, Query: $query"

                if [ "$use_sglang" = true ]; then
                    echo "Using SGLang with dataset $dataset, algorithm $algorithm, query $query on GPU $gpu_id, port $port, No Cache: $no_cache"
                    start_one_gpu_sglang $gpu_id "$dataset" "$algorithm" "$query" "$no_cache"
                else
                    echo "Using vLLM with dataset $dataset, algorithm $algorithm, query $query on GPU $gpu_id, port $port, No Cache: $no_cache"
                    start_one_gpu_vllm $gpu_id "$dataset" "$algorithm" "$query" "$no_cache"
                fi

                mimic_task_idx=$((mimic_task_idx + 1))
            fi
        done

        sleep 100  # Wait for servers to start

        # Run all tasks once servers are started
        for g in $(seq 0 $(($num_gpus - 1))); do
            if [ $task_idx -lt ${#tasks[@]} ]; then
                port=$(($base_port + $g*100))
                task="${tasks[$task_idx]}"
                
                # Substitute the port in the task command
                modified_task=$(echo "$task" | sed "s/>/-p $port >/")  # Adjusted command for clarity
                echo "Running task: $modified_task on GPU $g (port $port)"
                eval $modified_task &
                pids[$g]=$!  # Track process PID for waiting
                task_idx=$((task_idx + 1))
            fi
        done

        # Wait for each task to complete
        for pid in "${pids[@]}"; do
            wait $pid
        done

        # Stop and clear GPU processes
        stop_multi_gpu $num_gpus
        clear_gpu_processes
        sleep 15
    done
}


num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
echo "Number of GPUs available: $num_gpus"
stop_multi_gpu $num_gpus
clear_gpu_processes

# Defining the datasets for each query type
declare -A query_datasets=(
    [filter]="movies products BIRD PDMX beer"
    [projection]="movies products BIRD PDMX beer"
    [multi_llm]="movies products"
    [aggregation]="movies products"
    [rag]="fever squad"
)

# Algorithms available
algorithms=("naive" "quick_greedy_colmerging")

# Query types
queries=("filter" "projection" "multi_llm" "aggregation" "rag")

# Initialize tasks array
tasks=()

# Constructing the tasks list
for query in "${queries[@]}"; do
    for dataset in ${query_datasets[$query]}; do
        for algorithm in "${algorithms[@]}"; do
            tasks+=("python3.9 src/pyspark/${algorithm}.py -d ${dataset} -a ${algorithm} > ${query}_${dataset}_${algorithm}_output_vllm.txt 2>&1")
        done
    done
done

run_tasks_on_gpus $num_gpus "${tasks[@]}" # vLLM 