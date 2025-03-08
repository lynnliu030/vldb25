import json
from typing import List
from src.core.model_loader import LLMModel
from openai import OpenAI

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
import requests
from src.prompts.systemp import SYSTEM_PROMPT
import pickle

openai_api_key = "EMPTY"


def post_http_request(
    model: str,
    prompts: List[str],
    temperature: float = 0,
    port: int = 8000,
    guided_choice: List[str] = None,
    max_tokens: int = 1024,
) -> requests.Response:
    api_url = f"http://localhost:{port}/v1/completions"
    if guided_choice is not None:
        pload = {
            "model": model,
            "prompt": prompts,
            "temperature": temperature,
            "max_tokens": 5,
            "guided_choice": guided_choice,
        }
    else:
        pload = {
            "model": model,
            "prompt": prompts,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
    headers = {"Content-Type": "application/json"}
    response = requests.post(api_url, headers=headers, data=json.dumps(pload))
    return response


def generate_prompt(user_prompt: str, system_prompt: str) -> str:
    tokenizer = LLMModel.get_tokenizer()
    messages = [
        {"role": "system", "content": system_prompt},
    ]
    messages.append({"role": "user", "content": user_prompt})

    successful_prompt_generation = False
    while not successful_prompt_generation:
        try:
            prompt = tokenizer.apply_chat_template(conversation=messages, tokenize=False, add_generation_prompt=True)
        except Exception as e:
            if messages[0]["role"] == "system":
                messages = messages[1:]
            else:
                raise e
        else:
            successful_prompt_generation = True

    return prompt


def process_col(col_string, all_columns, merged_columns):
    col_names = []
    field_vals = []

    # Step 1: Find the first occurrence of ": " in the string to get the first column
    first_col_pos = col_string.find(": ")
    if first_col_pos == -1:
        return ["N/A"], ["None"]

    # Extract the first column name
    first_col = col_string[:first_col_pos]

    # Step 2: Check if the first column is part of a merged group
    merged_group = None
    for group in merged_columns:
        if first_col in group:
            merged_group = group
            break

    # Step 3: If it is a merged column group, extract values for all columns in the group
    if merged_group:
        current_pos = 0
        for merged_col in merged_group:
            search_str = f"{merged_col}: "
            start = col_string.find(search_str, current_pos)

            if start == -1:
                continue  # Skip this column if it is not found

            start += len(search_str)

            # Find where the next column from the merged group starts
            next_col_pos = len(col_string)  # Default to end of the string
            for next_col in merged_group:
                if next_col != merged_col:
                    next_search_str = f"{next_col}: "
                    if next_search_str in col_string[start:]:
                        curr_col_pos = col_string.index(next_search_str, start)
                        if curr_col_pos < next_col_pos:
                            next_col_pos = curr_col_pos

            # Extract the value for this merged column
            value = col_string[start:next_col_pos]
            col_names.append(merged_col)
            field_vals.append(value)

            # Move the current position forward
            current_pos = next_col_pos
    else:
        # Step 4: If it's a single column, extract the value directly
        parsed = col_string.split(": ", 1)
        if len(parsed) == 2:
            col_names.append(parsed[0])
            field_vals.append(parsed[1])
        else:
            col_names.append("N/A")
            field_vals.append("None")

    return col_names, field_vals


def save_prompts_to_csv(prompts, filename="prompts.pkl"):
    with open(filename, mode="wb") as f:
        pickle.dump(prompts, f)
    print("Prompts saved")


@udf(returnType=ArrayType(StringType()))
def llm_naive(query: str, contexts: List, fields: List, guided_choice: List[str] = None, port: int = 8000):
    """
    TODO: add description here
    """
    prompts = []
    user_prompts = []

    delimiter = "_"
    merged_columns = []
    for col in fields:
        if delimiter in col:
            merged_columns.append(col.split(delimiter))  # Split merged column names into a list

    for i, entry in enumerate(contexts):
        fields_json = {}
        for i in range(len(fields)):
            field_val = entry[i] if entry[i] else "None"
            col_names, field_vals = process_col(str(field_val), fields, merged_columns)
            for col_name, field_val in zip(col_names, field_vals):
                fields_json[col_name] = field_val
        user_prompt = f"Answer the below query:\n{query}\n Given the following data:\n {fields_json}"
        prompt = generate_prompt(user_prompt=user_prompt, system_prompt=SYSTEM_PROMPT)
        prompts.append(prompt)
        user_prompts.append(user_prompt)

    openai_api_base = f"http://localhost:{port}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    models = client.models.list()
    model = models.data[0].id

    # NOTE: don't use constrained decoding, but use constrained max tokens instead
    if guided_choice is not None and guided_choice == []:
        max_tokens = 5
        outputs = json.loads(post_http_request(model, prompts, temperature=0, max_tokens=max_tokens, guided_choice=None, port=port).content)
    else:
        outputs = json.loads(post_http_request(model, prompts, temperature=0, guided_choice=guided_choice, port=port).content)

    input_token_length = outputs["usage"]["prompt_tokens"]
    output_token_length = outputs["usage"]["completion_tokens"]
    total_tokens = outputs["usage"]["total_tokens"]

    return [output["text"] for output in outputs["choices"]]


@udf(returnType=ArrayType(StringType()))
def llm_dedup(query: str, contexts: List, fields: List, guided_choice: List[str] = None, port: int = 8000):
    """
    TODO: add description here
    """
    prompts = []

    seen_prompts = {}
    num_unique_prompts = 0
    prompt_mappings = {}

    prompts = []
    delimiter = "|||"
    merged_columns = []
    for col in fields:
        if delimiter in col:
            merged_columns.append(col.split(delimiter))  # Split merged column names into a list
    for j, entry in enumerate(contexts):
        fields_json = {}
        for i in range(len(fields)):
            field_val = entry[i] if entry[i] else "None"
            col_names, field_vals = process_col(str(field_val), fields, merged_columns)
            for col_name, field_val in zip(col_names, field_vals):
                fields_json[col_name] = field_val
        user_prompt = f"Answer the below query:\n{query}\n Given the following data:\n {fields_json}"
        if user_prompt in seen_prompts:
            prompt_mappings[j] = seen_prompts[user_prompt]
        else:
            prompt = generate_prompt(user_prompt=user_prompt, system_prompt=SYSTEM_PROMPT)
            if prompt is not None:
                prompts.append(prompt)
                prompt_mappings[j] = num_unique_prompts
                seen_prompts[user_prompt] = num_unique_prompts
                num_unique_prompts += 1

    openai_api_base = f"http://localhost:{port}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    models = client.models.list()
    model = models.data[0].id
    outputs = json.loads(post_http_request(model, prompts, temperature=0, guided_choice=guided_choice, port=port).content)

    input_token_length = outputs["usage"]["prompt_tokens"]
    output_token_length = outputs["usage"]["completion_tokens"]
    total_tokens = outputs["usage"]["total_tokens"]
    udf_outputs = []

    for i in range(len(contexts)):
        prompt_index = prompt_mappings[i]
        udf_outputs.append(outputs["choices"][prompt_index]["text"])

    return udf_outputs


@udf(returnType=ArrayType(StringType()))
def llm_average_dedup(query: str, contexts: List, fields: List, guided_choice: List[str] = None, port: int = 8000):
    prompts = []

    seen_prompts = {}
    num_unique_prompts = 0
    prompt_mappings = {}

    delimiter = "|||"
    merged_columns = []
    for col in fields:
        if delimiter in col:
            merged_columns.append(col.split(delimiter))  # Split merged column names into a list

    for j, entry in enumerate(contexts):
        fields_json = {}
        for i in range(len(fields)):
            field_val = entry[i] if entry[i] else "None"
            col_names, field_vals = process_col(str(field_val), fields, merged_columns)
            for col_name, field_val in zip(col_names, field_vals):
                fields_json[col_name] = field_val
        user_prompt = f"Answer the below query:\n{query}\n Given the following data:\n {fields_json}"
        if user_prompt in seen_prompts:
            prompt_mappings[j] = seen_prompts[user_prompt]
        else:
            prompt = generate_prompt(user_prompt=user_prompt, system_prompt=SYSTEM_PROMPT)
            if prompt is not None:
                prompts.append(prompt)
                prompt_mappings[j] = num_unique_prompts
                seen_prompts[user_prompt] = num_unique_prompts
                num_unique_prompts += 1

    openai_api_base = f"http://localhost:{port}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    models = client.models.list()
    model = models.data[0].id
    outputs = json.loads(post_http_request(model, prompts, temperature=0, guided_choice=guided_choice, port=port).content)
    input_token_length = outputs["usage"]["prompt_tokens"]
    output_token_length = outputs["usage"]["completion_tokens"]
    total_tokens = outputs["usage"]["total_tokens"]
    udf_outputs = []

    for i in range(len(contexts)):
        prompt_index = prompt_mappings[i]
        output = outputs["choices"][prompt_index]["text"]
        score = [int(s) for s in output.split() if s.isdigit()]
        if score:
            udf_outputs.append(score[0])
        else:
            udf_outputs.append(3)

    return udf_outputs


@udf(returnType=ArrayType(IntegerType()))
def llm_average_naive(query: str, contexts: List, fields: List, guided_choice: List[str] = None, port: int = 8000):
    """
    TODO: add description here
    """
    prompts = []
    user_prompts = []

    delimiter = "_"
    merged_columns = []
    for col in fields:
        if delimiter in col:
            merged_columns.append(col.split(delimiter))  # Split merged column names into a list

    for i, entry in enumerate(contexts):
        fields_json = {}
        for i in range(len(fields)):
            field_val = entry[i] if entry[i] else "None"
            col_names, field_vals = process_col(str(field_val), fields, merged_columns)
            for col_name, field_val in zip(col_names, field_vals):
                fields_json[col_name] = field_val
        user_prompt = f"Answer the below query:\n{query}\n Given the following data:\n {fields_json}"
        prompt = generate_prompt(user_prompt=user_prompt, system_prompt=SYSTEM_PROMPT)
        prompts.append(prompt)
        user_prompts.append(user_prompt)

    openai_api_base = f"http://localhost:{port}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    models = client.models.list()
    model = models.data[0].id
    if guided_choice is not None and guided_choice == []:
        max_tokens = 5
        outputs = json.loads(post_http_request(model, prompts, temperature=0, max_tokens=max_tokens, guided_choice=None, port=port).content)
    else:
        outputs = json.loads(post_http_request(model, prompts, temperature=0, guided_choice=guided_choice, port=port).content)
    input_token_length = outputs["usage"]["prompt_tokens"]
    output_token_length = outputs["usage"]["completion_tokens"]
    total_tokens = outputs["usage"]["total_tokens"]

    udf_outputs = []
    for i in range(len(contexts)):
        output = outputs["choices"][i]["text"]
        score = [int(s) for s in output.split() if s.isdigit()]
        if score:
            udf_outputs.append(score[0])
        else:
            udf_outputs.append(3)

    return udf_outputs
