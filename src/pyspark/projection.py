from src.core.udfs import llm_naive, llm_dedup
from src.core.algos.quick_greedy import QuickGreedy
from src.core.utils import clean_df, convert_df, prepend_col_name, read_yaml
import argparse
import time
import pandas as pd
import os
import asyncio
from pyspark.sql import functions as F

from src.core.utils import prepend_col_name
from pyspark.sql.functions import col, lit
from typing import List
from src.prompts.q1p import MOVIES_PROMPT_SHORT, PRODUCTS_PROMPT_SHORT, PDMX_PROMPT_SHORT, BEER_PROMPT_SHORT, BIRD_PROMPT_SHORT
import numpy as np
from pathlib import Path

import concurrent.futures

simulate_runtime = 0


async def call_LLM_UDF(aggregated_df, cols, prompt_prefix: str, dedup: bool = False, guided_choice: List[str] = None, port: int = 8000):
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        llm_s = time.time()
        if dedup:
            result_df = await loop.run_in_executor(
                pool,
                lambda: aggregated_df.select(
                    llm_dedup(lit(prompt_prefix), col("contexts"), lit(cols), lit(guided_choice), lit(port)).alias("results")
                ),
            )
        else:
            result_df = await loop.run_in_executor(
                pool,
                lambda: aggregated_df.select(
                    llm_naive(lit(prompt_prefix), col("contexts"), lit(cols), lit(guided_choice), lit(port)).alias("results")
                ),
            )
        result_exploded = await loop.run_in_executor(pool, lambda: result_df.withColumn("results", F.explode("results")).cache())
        output_length = await loop.run_in_executor(pool, lambda: result_exploded.count())
        llm_e = time.time()
        llm_time = llm_e - llm_s

        return llm_time, output_length


async def single_select(
    dataset_config: str = "movies",
    output_path: str = None,
    dedup: bool = False,
    guided_choice: List[str] = None,
    algo_config: str = "naive",
    num_gpus: int = 1,  # Data parallelism
    assigned_port: int = None,
    default_distinct_value_threshold: float = 0.8,
):
    sql_operator_time = 0
    algo_runtime = 0

    sql_s = time.time()
    # Step 1: Read the dataset and algorithm config
    base_path = os.path.dirname(os.path.abspath(__file__))  # Identify the current directory

    # dataset config in YAML
    dataset_config_path = os.path.join(base_path, "dataset_configs", f"{dataset_config}.yaml")
    data_config = read_yaml(dataset_config_path)
    dataset_name = data_config["filename"]
    dataset_path = os.path.join(Path(__file__).resolve().parent.parent.parent, "datasets", f"{dataset_name}")

    # Step 2: Reorder the input dataframe
    one_deps = [tuple(dep) for dep in data_config.get("one_deps", [])]

    if "movies" in dataset_name:
        prompt_prefix = MOVIES_PROMPT_SHORT
    elif "products" in dataset_name:
        prompt_prefix = PRODUCTS_PROMPT_SHORT
    elif "BIRD" in dataset_name:
        prompt_prefix = BIRD_PROMPT_SHORT
    elif "PDMX" in dataset_name:
        prompt_prefix = PDMX_PROMPT_SHORT
    elif "beer" in dataset_name:
        prompt_prefix = BEER_PROMPT_SHORT
    else:
        raise ValueError(f"Dataset name {dataset_name} not recognized")

    # algorithm config in YAML
    solver_config_path = dataset_config_path = os.path.join(base_path, "solver_configs", f"{algo_config}.yaml")
    solver_config = read_yaml(solver_config_path)
    algo = solver_config["algorithm"]
    merged_cols = [] if not solver_config.get("colmerging", True) else data_config["merged_columns"]
    sql_operator_time += time.time() - sql_s

    print(f"Merging columns: {merged_cols}")

    sql_s = time.time()
    # Read the dataset
    df = pd.read_csv(dataset_path)
    df = prepend_col_name(clean_df(df))

    sql_e = time.time()
    sql_operator_time += sql_e - sql_s
    num_total = df.shape[0]
    print(f"Dataset Characterisitcs: number of rows: {num_total}, number of columns: {df.shape[1]}")
    print(f"Column Orders: {df.columns}")

    # Step 2: ALGORITHM (reorder)
    algo_s = time.time()
    if "greedy" in algo:
        # Greedy reordering
        df, _ = QuickGreedy().reorder(
            df,
            early_stop=solver_config["early_stop"],
            # enable_fixed_reorder=True,
            col_merge=merged_cols,
            one_way_dep=one_deps,
            distinct_value_threshold=solver_config.get("distinct_value_threshold", default_distinct_value_threshold),
        )
    else:
        # No reordering
        df = df.sample(frac=1)

    algo_runtime = time.time() - algo_s

    print(f"Algorithm runtime: {algo_runtime:.3f}", flush=True)

    # Step 3: Run LLMs, and actually collect the data
    sql_s = time.time()
    cols = list(df.columns)

    # Step 3.1: plit DF into num_gpus partitions
    df_partitions = np.array_split(df, num_gpus)
    partition_aggregates = []
    for _, df_partition in enumerate(df_partitions):
        df_new = convert_df(df_partition)
        sample_df_with_contexts = df_new.withColumn("context", F.struct(cols))
        # Group the entire dataframe into one row, and collect contexts into a list
        aggregated_df = sample_df_with_contexts.groupBy().agg(F.collect_list(col("context")).alias("contexts"))
        partition_aggregates.append(aggregated_df)
    sql_e = time.time()
    sql_operator_time += sql_e - sql_s
    print(f"Number of GPUs: {num_gpus}")

    end_to_end_llm_s = time.time()
    llm_port_to_time = {}
    llm_port_to_output_len = {}
    starting_port = 8000

    async def execute_llm_task(i, df_partition):
        if num_gpus == 1 and assigned_port:
            port = assigned_port
        else:
            # Just assign sequentially
            port = starting_port + i

        # Step 3.2: Executing LLM
        print(f"Executing LLM on port {port}")
        llm_time, output_length = await call_LLM_UDF(df_partition, cols, prompt_prefix, dedup, guided_choice, port=port)
        print(f"Port {port} finished, LLM time: {llm_time:.3f}, output length: {output_length}")
        llm_port_to_time[port] = llm_time
        llm_port_to_output_len[port] = output_length

    # Create a list of tasks and run them concurrently
    tasks = [execute_llm_task(i, partition_aggregates[i]) for i in range(num_gpus)]
    await asyncio.gather(*tasks)

    assert sum(llm_port_to_output_len.values()) == num_total

    end_to_end_llm_e = time.time()
    llm_time = max(llm_port_to_time.values())
    actual_end_to_end = end_to_end_llm_e - end_to_end_llm_s

    print(f"LLM port to time: {llm_port_to_time}")
    print(f"Actual end-to-end LLM time: {actual_end_to_end:.3f}, take max of LLM times: {llm_time:.3f}")

    rps = num_total / llm_time
    tot_time = sql_operator_time + algo_runtime + llm_time

    # Step 5: Output statistics if needed
    if output_path:
        with open(output_path, "w") as f:
            print(f"Algorithm: {algo}")
            print(f"Number of rows: {num_total}", file=f)
            print(f"Algorithm Runtime: {algo_runtime}", file=f)
            print(f"LLM time: {llm_time}", file=f)
            print(f"SQL Operators time: {sql_operator_time}", file=f)
            print(f"Total time: {tot_time}", file=f)
            print(f"Requests per Second (RPS): {rps}", file=f)
    else:
        print("*" * 25 + "Result" + "*" * 25)
        print(f"Algorithm: {algo}")
        print(f"Number of rows: {num_total}")
        print(f"Algorithm Runtime: {algo_runtime}")
        print(f"LLM time: {llm_time}")
        print(f"SQL Operators time: {sql_operator_time}")
        print(f"Total time: {tot_time}")
        print(f"Requests per Second (RPS): {rps}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dataset_config",
        default="movies",
        help="Name of dataset config file (without .yaml suffix)",
    )
    parser.add_argument("--output_path", help="Saves statistics output to a file")

    # NOTE: query specific arguments
    parser.add_argument(
        "-g",
        "--guided_choice",
        nargs="+",
        help="Constained decoding output options",
    )

    # NOTE: algorithm specific arguments
    parser.add_argument(
        "-a",
        "--algo_config",
        default="naive",
        help="Name for solver config file (without .yaml suffix)",
    )
    parser.add_argument("--dedup", action="store_true", help="Dedup")

    # NOTE: system specific arguments
    parser.add_argument("--num-gpus", type=int, default=1, help="Number of GPUs to use")
    parser.add_argument("-p", "--port", type=int, default=None, help="Assigned port (num_gpu = 1)")

    args = parser.parse_args()
    guided_choice = [str(x) for x in args.guided_choice] if args.guided_choice else None
    print(f"Num GPUs: {args.num_gpus}, Assigned Port: {args.port}")

    tot_time = 0
    asyncio.run(
        single_select(
            dataset_config=args.dataset_config,
            output_path=args.output_path,
            dedup=args.dedup,
            guided_choice=guided_choice,
            algo_config=args.algo_config,
            num_gpus=args.num_gpus,
            assigned_port=args.port,
        )
    )
