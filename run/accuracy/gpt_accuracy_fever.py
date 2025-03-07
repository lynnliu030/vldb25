from dataclasses import asdict
import openai
from tqdm import tqdm

import os
import json


def _generate_prompt(user_prompt: str, system_prompt: str) -> str:
    messages = [
        {"role": "system", "content": system_prompt},
    ]
    messages.append({"role": "user", "content": user_prompt})

    return messages


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

    def query(df, col_ordering):
        client = openai.OpenAI(base_url="https://api.openai.com/v1", api_key=args.openai_api_key)

        records = df.to_dict(orient="records")
        fields_json_list = [json.dumps(create_row(record)) for record in records]

        user_prompt_template = f"Answer the below query:\n{prompt}\ngiven the following data:\n"
        user_prompt_template += "{{fields_json}}"

        user_prompts = [user_prompt_template.replace("{{fields_json}}", fields_json) for fields_json in fields_json_list]
        system_prompt = DEFAULT_SYSTEM_PROMPT

        messages = [
            _generate_prompt(user_prompt=user_prompt, system_prompt=system_prompt, few_shots=few_shots) for user_prompt in user_prompts
        ]

        print("************")
        print(messages[0])
        print("-----------")
        print(messages[1])
        print("**************")

        output = []
        for message in tqdm(messages):
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=message,
                temperature=0.0,
                seed=42,
            )
            output.append(response.choices[0].message.content)
        return output

    import pandas as pd

    if args.reordered:
        path = "./datasets/fever_reordered.csv"
    else:
        path = "./datasets/fever_with_evidence_5.csv"
    df = pd.read_csv(path, index_col=0)
    new_df = df.drop(columns=["label"])

    result_lists = []

    results = query(new_df, col_ordering=new_df.columns)
    new_df["gpt4o"] = results
    new_df["label"] = match
    new_df.to_csv(path)


if __name__ == "__main__":
    import argparse

    # Create the parser
    parser = argparse.ArgumentParser(description="A simple argparse example.")

    # Add arguments
    parser.add_argument("--openai-api-key", type=str, required=True)
    parser.add_argument("--reordered", action="store_true")

    # Parse the arguments
    args = parser.parse_args()

    main(args)
