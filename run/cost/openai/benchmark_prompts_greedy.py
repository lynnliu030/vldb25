import openai
import anthropic
import time
import csv
import pickle
import re
import os 
from transformers import AutoTokenizer

openai.api_key = os.getenv("OPENAI_API_KEY")
tokenizer = AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
anthropic_client = anthropic.Anthropic()

rows_with_more_than_1024_tokens = 0


########################################
########### Benchmark Helper ###########
def call_openai_with_caching(system_instruction, prompt):
    """Make an OpenAI API call with a prompt and system instruction."""
    response = openai.chat.completions.create(
        model="gpt-4o-mini", messages=[{"role": "system", "content": system_instruction}, {"role": "user", "content": prompt}]
    )

    prompt_tokens = response.usage.prompt_tokens
    completion_tokens = response.usage.completion_tokens
    cached_tokens = response.usage.prompt_tokens_details.cached_tokens
    return prompt_tokens, completion_tokens, cached_tokens


def call_anthropic_with_caching(system_instruction, cached_prompt, rest_prompt):
    response = anthropic_client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=300,
        system=[
            {"type": "text", "text": system_instruction, "cache_control": {"type": "ephemeral"}},
        ],
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": cached_prompt, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": rest_prompt if rest_prompt else "."},
                ],
            },
        ],
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
    )
    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    input_tokens_cache_read = getattr(response.usage, "cache_read_input_tokens", "---")
    input_tokens_cache_create = getattr(response.usage, "cache_creation_input_tokens", "---")
    return input_tokens, output_tokens, input_tokens_cache_read, input_tokens_cache_create


def load_prompts_from_csv(csv_file_path, max_prompts=100000, top_n=1000):
    """Load and format the first `max_prompts` from the given CSV file."""
    prompts = []

    # Load all prompts from the CSV
    with open(csv_file_path, mode="r") as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i > 8:
                user_prompt = row[0]
                prompts.append(user_prompt)

    prompts = [duplicate_evidence_fields(prompt) for prompt in prompts]
    sorted_prompts = prompts[:top_n]

    print(f"Loaded {len(sorted_prompts)} prompts from {csv_file_path}.")
    return sorted_prompts


def duplicate_evidence_fields(prompt):
    # Use regex to capture 'evidenceX' fields, ensuring both key and value are duplicated
    matches = re.findall(r"('(context\d+|question)':\s*.*?)(?='context\d+':|'question':|}$)", prompt, re.DOTALL)

    # Duplicate each evidence field and insert it back into the prompt
    for match, _ in matches:
        prompt.replace(match, f"{match} {match} {match} {match} {match} {match} {match} {match} {match}")

    return prompt


def load_prompts_from_pkl(pkl_file_path):
    with open(pkl_file_path, "rb") as file:
        data = pickle.load(file)

    system_instruction = None
    prompts = []

    for item in data:
        if "<<SYS>>" in item and system_instruction == None:
            system_instruction = item.split("<<SYS>>")[1].split("<</SYS>>")[0].strip()
        else:
            prompts.append(item)

    if not system_instruction:
        raise ValueError("System instruction not found in the provided .pkl file.")

    print(f"Numer of available prompts: {len(prompts)}")
    return system_instruction, prompts


########################################
############## Benchmark ###############
def process_csv_file(system_instruction, csv_file, max_prompts=100, anthropic: bool = False):
    """Process a single CSV file and calculate total duration, latency, and TPS."""
    global rows_with_more_than_1024_tokens
    prompts = load_prompts_from_csv(csv_file, max_prompts)
    total_time = 0
    total_input_token_length = 0
    prompt_tokens = 0
    completion_tokens = 0
    cached_tokens = 0
    cached_write_tokens = 0
    if anthropic:
        print("ANTHROPIC")
    else:
        print("OPENAI")
    matched_tokens = 0
    seen_prefixes = {}

    for i, prompt in enumerate(prompts):
        print(f"Processing prompt {i + 1}/{len(prompts)} from {csv_file}...", end="\r")
        tokens = tokenizer(prompt, return_tensors="pt", truncation=False)["input_ids"].squeeze()

        # Split tokens into two parts
        first_part_tokens = tokens[:2200]
        second_part_tokens = tokens[2200:] if len(tokens) > 2200 else []
        # first_part_tokens = tokens[:1024]
        # second_part_tokens = tokens[1024:]

        # Decode tokens back to strings
        first_part_prompt = tokenizer.decode(first_part_tokens, skip_special_tokens=True)
        second_part_prompt = tokenizer.decode(second_part_tokens, skip_special_tokens=True)

        # Display or use the two parts
        # print("First part (up to 1024 tokens):", first_part_prompt)
        # print("Second part (remaining tokens):", second_part_prompt)

        start_time = time.time()
        if anthropic:
            prompt_tokens_x, completion_tokens_x, cached_tokens_x, cached_write_tokens_x = call_anthropic_with_caching(
                system_instruction, first_part_prompt, second_part_prompt
            )
            cached_write_tokens += cached_write_tokens_x
        else:
            prompt_tokens_x, completion_tokens_x, cached_tokens_x = call_openai_with_caching(system_instruction, prompt)
        total_time += time.time() - start_time
        prompt_tokens += prompt_tokens_x
        completion_tokens += completion_tokens_x
        cached_tokens += cached_tokens_x

        token_length = len(tokenizer.encode(prompt))
        total_input_token_length += token_length

        if token_length > 1024:
            rows_with_more_than_1024_tokens += 1

    avg_latency = total_time / len(prompts)
    tps = total_input_token_length / total_time

    return total_time, avg_latency, tps, prompt_tokens, completion_tokens, cached_tokens, cached_write_tokens


########################################
################ Main ##################
def main(system_instruction, csv_files, output_file, anthropic: bool = False):
    """Process all CSV files and save results to the output file."""
    global rows_with_more_than_1024_tokens

    with open(output_file, "w") as outfile:
        for csv_file in csv_files:
            print(f"Processing {csv_file}...")
            total_time, avg_latency, tps, prompt_tokens, completion_tokens, cached_tokens, cached_write_tokens = process_csv_file(
                system_instruction, csv_file, anthropic=anthropic
            )

            outfile.write(f"Results for {csv_file}:\n")
            outfile.write(f"Total Duration: {total_time:.2f} seconds\n")
            outfile.write(f"Average Latency: {avg_latency:.2f} s/request\n")
            outfile.write(f"TPS: {tps:.2f} tokens/second\n")
            outfile.write(f"Rows With More Than 1024 Tokens: {rows_with_more_than_1024_tokens:.2f} \n")
            outfile.write(f"prompt_tokens: {prompt_tokens}:\n")
            outfile.write(f"completion_tokens: {completion_tokens}:\n")
            outfile.write(f"cached_tokens: {cached_tokens}:\n")
            outfile.write(f"cached_write_tokens (anthropic): {cached_write_tokens}:\n")

    print("\nProcessing completed. Results saved to", output_file)


# OpenAI
csv_files = ["./run/cost/openai/fever_greedy_user_prompts_1000.csv"]
output_file = "./run/cost/openai/results/openai_fever_greedy_user_prompts.txt"
system_instruction = "You are a data analyst. Use the provided JSON data to answer the user query based on the specified fields. Respond with only the answer, no extra formatting."
main(system_instruction, csv_files, output_file, anthropic=False)

# Anthropic
rows_with_more_than_1024_tokens = 0
csv_files = ["./run/cost/openai/fever_greedy_user_prompts_1000.csv"]
output_file = "./run/cost/openai/results/anthropic_fever_greedy_user_prompts.txt"
system_instruction = "You are a data analyst. Use the provided JSON data to answer the user query based on the specified fields. Respond with only the answer, no extra formatting."
main(system_instruction, csv_files, output_file, anthropic=True)
