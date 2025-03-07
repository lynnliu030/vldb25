from dataclasses import asdict
import openai
from tqdm import tqdm

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

def create_row(record):
    output = {}
    for v in record.values():
        parsed = v.split(": ", 1)
        output[parsed[0]] = parsed[1]
    return output

def main(args):
    DEFAULT_SYSTEM_PROMPT = """
    You are a helpful data analyst. You will receive JSON data containing various fields and their corresponding values, representing different attributes. Use these fields to provide an answer to the user query. The user query will indicate which fields to use for your response. Your response should contain only the answer and no additional formatting.
    """

    prompt = "You are given 4 pieces of evidence as {evidence1}, {evidence2}, {evidence3}, and {evidence4}. You are also given a claim as {claim}. Answer SUPPORTS if the pieces of evidence support the given {claim}, REFUTES if the evidence refutes the given {claim}, or NOT ENOUGH INFO if there is not enough information to answer. Your answer should just be SUPPORTS, REFUTES, or NOT ENOUGH INFO and nothing else."

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
    
    if args.reordered:
        path = "./datasets/fever_reordered.csv"
    else:
        path = "./datasets/fever_with_evidence_5.csv"
    df = pd.read_csv(path, index_col=0)
    new_df = df.drop(columns=["label"])

    result_lists = []

    results = query(new_df, col_ordering=new_df.columns)
    new_df[args.model] = results
    new_df["label"] = match
    new_df.to_csv(path)


if __name__ == "__main__":
    import argparse

    # Create the parser
    parser = argparse.ArgumentParser(description="A simple argparse example.")

    # Add arguments
    parser.add_argument("--huggingface-hub-token", type=str, required=True)
    parser.add_argument("--model", type=str, choices=["llama3-8b", "llama3-70b"], required=True)
    parser.add_argument("--reordered", action="store_true")

    # Parse the arguments
    args = parser.parse_args()

    os.environ["HUGGING_FACE_HUB_TOKEN"] = args.huggingface_hub_token
    main(args)
