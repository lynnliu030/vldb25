from src.core.udfs import llm_naive, llm_dedup
from src.core.algos.quick_greedy import QuickGreedy
from src.core.utils import clean_df, convert_df, prepend_col_name, read_yaml
import argparse
import time
import pandas as pd
import os
import asyncio
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.core.utils import prepend_col_name
from pyspark.sql.functions import upper, col, lit, row_number
from typing import List
from src.prompts.q3p import SENTIMENT_PROMPT_SHORT, MOVIES_PROMPT_SHORT, PRODUCTS_PROMPT_SHORT
import numpy as np
from pathlib import Path

import concurrent.futures

simulate_runtime = 0


def split_dataframe(df, n):
    # Step 1: Add an index column (starting from 0)
    indexed_df = df.withColumn("index", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1)

    # Step 2: Get total number of rows
    total_rows = df.count()

    # Step 3: Calculate the number of rows per split
    rows_per_split = total_rows // n
    extra_rows = total_rows % n  # For handling cases where total_rows isn't divisible by n

    # Step 4: Create list of DataFrames for each split
    split_dfs = []
    for i in range(n):
        start_index = i * rows_per_split
        end_index = start_index + rows_per_split
        if i == n - 1:  # Last partition gets any remaining rows
            end_index += extra_rows

        # Filter rows for the current partition
        split_dfs.append(indexed_df.filter((F.col("index") >= start_index) & (F.col("index") < end_index)).drop("index"))

    return split_dfs


async def call_LLM_UDF(
    aggregated_df,
    cols,
    prompt_prefix: str,
    dedup: bool = False,
    context_col: str = "contexts",
    guided_choice: List[str] = None,
    port: int = 8000,
):
    if guided_choice is None and prompt_prefix == SENTIMENT_PROMPT_SHORT:
        guided_choice = []

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        llm_s = time.time()
        if dedup:
            result_df = await loop.run_in_executor(
                pool,
                lambda: aggregated_df.select(
                    llm_dedup(lit(prompt_prefix), col(context_col), lit(cols), lit(guided_choice), lit(port)).alias("results")
                ),
            )
        else:
            result_df = await loop.run_in_executor(
                pool,
                lambda: aggregated_df.select(
                    llm_naive(lit(prompt_prefix), col(context_col), lit(cols), lit(guided_choice), lit(port)).alias("results")
                ),
            )
        result_exploded = await loop.run_in_executor(pool, lambda: result_df.withColumn("results", F.explode("results")).cache())
        output_length = await loop.run_in_executor(pool, lambda: result_exploded.count())
        llm_e = time.time()
        llm_time = llm_e - llm_s

        return llm_time, output_length, result_exploded


async def multi_invoke(
    dataset_config: str = "movies",
    output_path: str = None,
    dedup: bool = False,
    sql_opt: bool = False,
    guided_choice: List[str] = None,
    algo_config: str = "naive",
    num_gpus: int = 1,  # Data parallelism
    assigned_port: int = 8000,
    default_distinct_value_threshold: float = 0.8,
):
    sql_operator_time = 0
    algo_runtime = 0

    # Step 1: Read the dataset (FROM MLSys)
    sql_read_s = time.time()
    base_path = os.path.dirname(os.path.abspath(__file__))  # Identify the current directory
    dataset_config_path = os.path.join(base_path, "dataset_configs", f"{dataset_config}.yaml")  # Construct the correct dataset path
    data_config = read_yaml(dataset_config_path)
    dataset_name = data_config["filename"]
    if "movies" in dataset_name:
        prompt_prefix = MOVIES_PROMPT_SHORT
    elif "products" in dataset_name:
        prompt_prefix = PRODUCTS_PROMPT_SHORT

    dataset_path = os.path.join(Path(__file__).resolve().parent.parent.parent, "datasets", f"{dataset_name}")
    one_deps = [tuple(dep) for dep in data_config.get("one_deps", [])]

    solver_config_path = dataset_config_path = os.path.join(base_path, "solver_configs", f"{algo_config}.yaml")
    solver_config = read_yaml(solver_config_path)
    algo = solver_config["algorithm"]
    merged_cols = [] if not solver_config.get("colmerging", True) else data_config["merged_columns"]
    sentiment_field = data_config["filtering_field"]
    sql_operator_time = time.time() - sql_read_s

    print(f"Merging columns: {merged_cols}")

    sql_s = time.time()
    df = pd.read_csv(dataset_path)
    df = clean_df(df)
    if sql_opt:
        if "movies" in dataset_name:
            df = df[df["reviewtype"] == "Fresh"]
        else:
            df = df[df["rating"] == "5.0"]
    df = prepend_col_name(df)
    num_total = df.shape[0]
    sql_e = time.time()
    sql_operator_time += sql_e - sql_s

    # Step 2: Reorder the input dataframe
    algo_s = time.time()
    sentiment_prompt = SENTIMENT_PROMPT_SHORT
    if "movies" in dataset_name:
        prompt_prefix = MOVIES_PROMPT_SHORT
    elif "products" in dataset_name:
        prompt_prefix = PRODUCTS_PROMPT_SHORT

    algo_s = time.time()
    if "greedy" in algo:
        df, _ = QuickGreedy().reorder(
            df,
            early_stop=solver_config["early_stop"],
            row_stop=solver_config.get("row_stop", None),
            col_stop=solver_config.get("col_stop", None),
            col_merge=merged_cols,
            one_way_dep=one_deps,
            distinct_value_threshold=solver_config.get("distinct_value_threshold", default_distinct_value_threshold),
        )
    else:
        # No reordering
        df = df.sample(frac=1)
    algo_e = time.time()
    algo_runtime += algo_e - algo_s

    print(f"Algorithm runtime: {algo_runtime:.3f}", flush=True)

    # Step 4: Run LLMs, and actually collect the data
    sql_s = time.time()
    cols = list(df.columns)
    df_partitions = np.array_split(df, num_gpus)  # Split DF into num_gpus partitions
    partition_aggregates = []
    for _, df_partition in enumerate(df_partitions):
        df_new = convert_df(df_partition)
        sample_df_with_contexts = df_new.withColumn("context_one", F.struct(col(sentiment_field)))
        sample_df_with_contexts = sample_df_with_contexts.withColumn("context_two", F.struct(cols))
        # Group the entire dataframe into one row, and collect contexts into a list
        aggregated_df = sample_df_with_contexts.groupBy().agg(F.collect_list(col("context_one")).alias("contexts"))
        partition_aggregates.append(aggregated_df)
    sql_e = time.time()
    sql_operator_time += sql_e - sql_s

    print(f"Number of GPUs: {num_gpus}")

    end_to_end_llm_s = time.time()
    llm_port_to_time = {}
    llm_port_to_output_len = {}
    result_dfs = {}
    starting_port = 8000

    async def execute_first_llm_task(i, df_partition):
        if num_gpus == 1 and assigned_port:
            print(f"Use assigned port: {assigned_port}", flush=True)
            port = assigned_port
        else:
            # Just assign sequentially
            port = starting_port + i * 100

        # Step 4.2: Executing LLM
        llm_time, output_length, result_exploded = await call_LLM_UDF(
            df_partition, [sentiment_field], sentiment_prompt, dedup, guided_choice=guided_choice, context_col="contexts", port=port
        )
        print(f"Port {port} finished, LLM time: {llm_time:.3f}, output length: {output_length}")
        llm_port_to_time[port] = llm_time
        llm_port_to_output_len[port] = output_length
        result_dfs[port] = result_exploded

    # Create a list of tasks and run them concurrently
    tasks = [execute_first_llm_task(i, partition_aggregates[i]) for i in range(num_gpus)]
    await asyncio.gather(*tasks)

    assert sum(llm_port_to_output_len.values()) == num_total

    end_to_end_llm_e = time.time()
    first_llm_time = max(llm_port_to_time.values())
    actual_end_to_end = end_to_end_llm_e - end_to_end_llm_s

    print(f"LLM port to time: {llm_port_to_time}")
    print(f"Actual end-to-end LLM time: {actual_end_to_end:.3f}, take max of LLM times: {first_llm_time:.3f}")

    first_rps = num_total / first_llm_time
    llm_time = first_llm_time

    result_dfs = list(result_dfs.values())
    if not result_dfs:
        if output_path:
            with open(output_path, "w") as f:
                print(f"Algorithm: {algo}")
                print(f"Number of rows: {num_total}", file=f)
                print(f"LLM time: {first_llm_time}", file=f)
                print(f"SQL Operators time: {sql_operator_time}", file=f)
                print(f"Total time: {llm_time + algo_runtime + sql_operator_time}", file=f)
                print(f"Requests per Second (RPS): {first_rps}", file=f)
        else:
            print("*" * 25 + "Result" + "*" * 25)
            print(f"Algorithm: {algo}")
            print(f"Number of rows: {num_total}")
            print(f"LLM time: {first_llm_time}")
            print(f"SQL Operators time: {sql_operator_time}")
            print(f"Total time: {llm_time + algo_runtime + sql_operator_time}")
            print(f"Requests per Second (RPS): {first_rps}")

    result_df_combined = result_dfs[0]
    if len(result_dfs) > 1:
        for df in result_dfs[1:]:
            result_df_combined = result_df_combined.union(df)

    w = Window().orderBy(lit("A"))
    temp = result_df_combined.withColumn("rn", row_number().over(w))
    result_df = sample_df_with_contexts.withColumn("rn", row_number().over(w))
    result_df = result_df.join(temp, ["rn"]).drop("rn")

    s = time.time()
    result_df = result_df.filter(upper(col("results")) == "POSITIVE")
    e = time.time()
    sql_operator_time += e - s

    df_partitions = split_dataframe(result_df, n=num_gpus)  # Split DF into num_gpus partitions
    partition_aggregates = []
    sql_s = time.time()
    for _, df_partition in enumerate(df_partitions):
        aggregated_df = df_partition.groupBy().agg(F.collect_list(col("context_two")).alias("contexts"))
        partition_aggregates.append(aggregated_df)
    sql_e = time.time()
    sql_operator_time += sql_e - sql_s

    end_to_end_llm_s = time.time()
    llm_port_to_time = {}
    llm_port_to_output_len = {}
    result_dfs = {}
    starting_port = 8000

    async def execute_second_llm_task(i, df_partition):
        if num_gpus == 1 and assigned_port:
            print(f"Use assigned port: {assigned_port}", flush=True)
            port = assigned_port
        else:
            # Just assign sequentially
            port = starting_port + i * 100

        # Step 4.2: Executing LLM
        llm_time, output_length, result_exploded = await call_LLM_UDF(
            df_partition, cols, prompt_prefix, dedup, guided_choice=guided_choice, context_col="contexts", port=port
        )
        print(f"Port {port} finished, LLM time: {llm_time:.3f}, output length: {output_length}")
        llm_port_to_time[port] = llm_time
        llm_port_to_output_len[port] = output_length
        result_dfs[port] = result_exploded

    # Create a list of tasks and run them concurrently
    tasks = [execute_second_llm_task(i, partition_aggregates[i]) for i in range(num_gpus)]
    await asyncio.gather(*tasks)

    end_to_end_llm_e = time.time()
    second_llm_time = max(llm_port_to_time.values())
    actual_end_to_end = end_to_end_llm_e - end_to_end_llm_s

    print(f"Second LLM port to time: {llm_port_to_time}")
    print(f"Actual end-to-end second LLM time: {actual_end_to_end:.3f}, take max of LLM times: {second_llm_time:.3f}")

    second_rps = num_total / second_llm_time
    tot_time = sql_operator_time + algo_runtime + first_llm_time + second_llm_time

    # Step 5: Output statistics if needed
    if output_path:
        with open(output_path, "w") as f:
            print(f"Algorithm: {algo}")
            print(f"Number of rows: {num_total}", file=f)
            print(f"Algorithm Runtime: {algo_runtime}", file=f)
            print(f"First LLM time: {first_llm_time}", file=f)
            print(f"Second LLM time: {second_llm_time}", file=f)
            print(f"SQL Operators time: {sql_operator_time}", file=f)
            print(f"Total time: {tot_time}", file=f)
            print(f"First Requests per Second (RPS): {first_rps}", file=f)
            print(f"Second Requests per Second (RPS): {second_rps}", file=f)
    else:
        print("*" * 25 + "Result" + "*" * 25)
        print(f"Algorithm: {algo}")
        print(f"Number of rows: {num_total}")
        print(f"Algorithm Runtime: {algo_runtime}")
        print(f"First LLM time: {first_llm_time}")
        print(f"Second LLM time: {second_llm_time}")
        print(f"SQL Operators time: {sql_operator_time}")
        print(f"Total time: {tot_time}")
        print(f"First Requests per Second (RPS): {first_rps}")
        print(f"Second Requests per Second (RPS): {second_rps}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dataset_config",
        default="movies",
        help="Choose movies or products dataset (default movies)",
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
    parser.add_argument("-r", "--reorder", action="store_true", help="Reorder inputs (row level)")
    parser.add_argument(
        "-a",
        "--algo_config",
        default="naive",
        help="Choose fixed or greedy algorithm for reordering",
    )
    parser.add_argument("--dedup", action="store_true", help="Dedup")
    parser.add_argument("-s", "--sql_opt", action="store_true", help="SQL Opt")

    # NOTE: distributed execution
    parser.add_argument("--num-gpus", type=int, default=1, help="Number of GPUs to use")
    parser.add_argument("-p", "--port", type=int, default=None, help="Assigned port (num_gpu = 1)")

    args = parser.parse_args()
    guided_choice = [str(x) for x in args.guided_choice] if args.guided_choice else None

    # Run the query end-to-end, df.collect() is within the function
    asyncio.run(
        multi_invoke(
            dataset_config=args.dataset_config,
            output_path=args.output_path,
            dedup=args.dedup,
            sql_opt=args.sql_opt,
            guided_choice=guided_choice,
            algo_config=args.algo_config,
            num_gpus=args.num_gpus,
            assigned_port=args.port,
        )
    )
