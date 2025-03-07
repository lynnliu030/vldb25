from dataclasses import asdict
import gc
import torch

import os
import json


def _generate_prompt(tokenizer, user_prompt: str, system_prompt: str) -> str:
    messages = [
        {"role": "system", "content": system_prompt},
    ]

    messages.append({"role": "user", "content": user_prompt})
    successful_prompt_generation = False
    while not successful_prompt_generation:
        try:
            # Construct a prompt for the chosen model given OpenAI style messages.
            prompt = tokenizer.apply_chat_template(conversation=messages, tokenize=False, add_generation_prompt=True)
        except Exception as e:
            if messages[0]["role"] == "system":
                # Try again without system prompt
                messages = messages[1:]
            else:
                raise e
        else:
            successful_prompt_generation = True

    return prompt

def create_row(d: dict, merged_cols: list):
    result = {}
    for col_string in d.values():
        col_string = str(col_string)
        parsed = col_string.split(": ", 1)
        col_name = parsed[0]
        assert len(parsed) == 2
        
        merged_group = None
        for group in merged_cols:
            if col_name in group:
                merged_group = group
                break
        if merged_group:
            current_index = 0
            for key in merged_group:
                # Find the index of the key in the string
                start_index = col_string.find(f"{key}: ", current_index)
                if start_index == -1:
                    # Key not found, set value to None
                    result[key] = None
                else:
                    # Find the end of the value (next key or end of string)
                    if merged_group.index(key) + 1 >= len(merged_group):
                        end_index = -1
                    else:
                        end_index = col_string.find(f"{merged_group[merged_group.index(key)+1]}: ", start_index + 1)
                        if end_index == -1:
                            # Last key, find the end of the string
                            end_index = len(col_string)
                    # Extract the value and add it to the result dictionary
                    value = col_string[start_index + len(f"{key}: "):end_index]
                    result[key] = value
                current_index = start_index + 1
        else:
            result[parsed[0]] = parsed[1]
    return result


def main(args):
    DEFAULT_SYSTEM_PROMPT = """
    You are a helpful data analyst. You will receive JSON data containing various fields and their corresponding values, representing different attributes. Use these fields to provide an answer to the user query. The user query will indicate which fields to use for your response. Your response should contain only the answer and no additional formatting.
    """

    if args.dataset == "movies":
        # Movies
        prompt = "Given the following fields, answer in ONE word, 'YES' or 'NO', whether the movie would be suitable for kids.  Answer with ONLY 'YES' or 'NO'."
        if args.reordered:
            path = "./datasets/movies_reordered_sampled_labelled.csv"
        else:
            path = "./datasets/movies_sampled_labelled.csv"
        merge_cols = [["movieinfo", "rottentomatoeslink", "movietitle"]]
    elif args.dataset == "products":
        # Products
        prompt = "Given the following fields determine if the review speaks positively ('POSITIVE'), negatively ('NEGATIVE'), or netural ('NEUTRAL') about the product. Answer only 'POSITIVE', 'NEGATIVE', or 'NEUTRAL', nothing else."
        if args.reordered:
            path = "./datasets/products_reordered_sampled_labelled.csv"
        else:
            path = "./datasets/products_sampled_labelled.csv"
        merge_cols = [["product_title", "parent_asin"]]
    elif args.dataset == "bird":
        # Bird
        prompt = "Given the following fields related to posts in an online codebase community, answer whether the post is related to statistics. Answer with only 'YES' or 'NO'."
        if args.reordered:
            path = "./datasets/posts_reordered_sampled_labelled.csv"
        else:
            path = "./datasets/posts_sampled_labelled.csv"
        merge_cols = [["Body", "PostId"]]
    elif args.dataset == "beer":
        # Beer
        prompt = "Based on the beer descriptions, does this beer have European origin? Answer 'YES' if it does or 'NO' if it doesn't."
        if args.reordered:
            path = "./datasets/beer_reordered_sampled_labelled.csv"
        else:
            path = "./datasets/beer_sampled_labelled.csv"
        merge_cols = [['beer/name', 'beer/beerId']]
    elif args.dataset == "pdmx":
        # PDMX
        prompt = "Based on following fields, answer 'YES' or 'NO' if any of the song information references a specific individual. Answer only 'YES' or 'NO', nothing else."
        if args.reordered:
            path = "./datasets/pdmx_reordered_sampled_labelled.csv"
        else:
            path = "./datasets/pdmx_sampled_labelled.csv"
        
        merge_cols = [['path', 'metadata'], ['isuserpublisher', 'hasannotations', 'hasmetadata', 'isofficial', 'subsetall', 'isdraft']]

    from vllm import EngineArgs, SamplingParams, LLM

    if args.model == "llama3-8b":
        engine_args = EngineArgs(model="meta-llama/Meta-Llama-3-8B-Instruct", enable_prefix_caching=True)
    elif args.model == "llama3-70b":
        engine_args = EngineArgs(model="meta-llama/Meta-Llama-3-70B-Instruct", enable_prefix_caching=True, tensor_parallel_size=8, gpu_memory_utilization=0.85)

    sampling_params = SamplingParams(temperature=0.0, seed=42)
        

    def query(df, merged_cols, col_ordering):
        llm = LLM(**asdict(engine_args))

        records = df.to_dict(orient="records")
        assert list(records[0].keys()) == list(col_ordering)

        fields_json_list = [json.dumps(create_row(record, merged_cols)) for record in records]

        user_prompt_template = f"Answer the below query:\n{prompt}\ngiven the following data:\n"
        user_prompt_template += "{{fields_json}}"

        user_prompts = [user_prompt_template.replace("{{fields_json}}", fields_json) for fields_json in fields_json_list]

        tokenizer = llm.get_tokenizer()
        prompts = [
            _generate_prompt(tokenizer, user_prompt=user_prompt, system_prompt=DEFAULT_SYSTEM_PROMPT)
            for user_prompt in user_prompts
        ]

        print("************")
        print(prompts[0])
        print("-----------")
        print(prompts[1])
        print("**************")

        request_outputs = llm.generate(prompts=prompts, sampling_params=sampling_params)
        assert len(request_outputs) == len(df)

        del llm.llm_engine.model_executor.driver_worker
        del llm
        gc.collect()
        torch.cuda.empty_cache()
        return [output.outputs[-1].text for output in request_outputs]

    import pandas as pd

    df = pd.read_csv(path, index_col=0)
    if "manual_labels" in df.columns:
        new_df = df.drop(columns=["manual_labels"])
    else:
        new_df = df
    if "gpt4o" in new_df.columns:
        new_df = new_df.drop(columns=["gpt4o"])
    if "llama3-8b" in new_df.columns:
        new_df = new_df.drop(columns=["llama3-8b"])
    if "llama3-70b" in new_df.columns:
        new_df = new_df.drop(columns=["llama3-70b"])

    results = query(new_df, merged_cols=merge_cols, col_ordering=new_df.columns)

    result_lists = []

    df[args.model] = results
    df.to_csv(path)


if __name__ == "__main__":
    import argparse

    # Create the parser
    parser = argparse.ArgumentParser(description="A simple argparse example.")

    # Add arguments
    parser.add_argument("--huggingface-hub-token", type=str, required=True)
    parser.add_argument("--model", type=str, choices=["llama3-8b", "llama3-70b"], required=True)
    parser.add_argument("--dataset", type=str, choices=["movies", "products", "bird", "beer", "pdmx"], required=True)
    parser.add_argument("--reordered", action="store_true")

    # Parse the arguments
    args = parser.parse_args()

    os.environ["HUGGING_FACE_HUB_TOKEN"] = args.huggingface_hub_token
    main(args)
