import random
import time
import string
from transformers import AutoTokenizer
import openai

############# OpenAI SetUp #############
openai.api_key = "<your key>"
cost_per_million_tokens = 2.50  # $2.50 per 1 million input tokens


def call_openai_with_caching(prompt):
    # Note: Caching is enabled automatically for prompts that are 1024 tokens or longer.
    # Source: https://platform.openai.com/docs/guides/prompt-caching/how-it-works

    instruction = "You will get random characters and should count the occurrences of lowercase a's and b's."
    response = openai.chat.completions.create(
        model="gpt-4o", messages=[{"role": "system", "content": instruction}, {"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


########################################


########### Benchmark Helper ###########
tokenizer = AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")


def generate_random_string(token_length: int) -> str:
    random.seed(0)

    # Ensure pad_token_id is defined
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token_id = tokenizer.convert_tokens_to_ids(tokenizer.pad_token)

    random_string = "".join(random.choices(string.ascii_letters + string.digits + " ", k=10000))
    tokenized_output = tokenizer.encode(random_string, add_special_tokens=False)

    if len(tokenized_output) > token_length:
        truncated_output = tokenized_output[:token_length]
    else:
        truncated_output = tokenized_output + [tokenizer.pad_token_id] * (token_length - len(tokenized_output))

    decoded_string = tokenizer.decode(truncated_output, skip_special_tokens=False)
    return decoded_string


def generate_unique_prefix(base_text, index):
    index_str = str(index)
    if len(index_str) > len(base_text):
        raise ValueError("Index is too large to fit into the base prefix.")

    # Replace beginning of the base_text with index_str
    return index_str + base_text[len(index_str) :]


########################################


############## Benchmark ###############
def test_prefix_caching(
    num_prefix,
    num_samples_per_prefix,
    prefix_length,
    input_length,
    reorder=False,
    prefix=True,
):
    tot_num_reqs = num_prefix * num_samples_per_prefix

    base_prefix = generate_random_string(prefix_length)
    suffix_length = input_length - prefix_length
    suffix = generate_random_string(suffix_length)

    tokenized_suffix_length = len(tokenizer.encode(suffix)) - 1
    assert (
        tokenized_suffix_length == suffix_length
    ), f"Suffix token length: {tokenized_suffix_length}, Expected suffix token length: {suffix_length}"

    total_time = 0
    total_output_token_length = 0
    total_input_token_length = 0

    prompt_list = []
    propmt_pos_list = []
    input_token_list = []

    # Generate unique prefixes
    for i in range(num_prefix):
        unique_prefix = generate_unique_prefix(base_prefix, i)
        print(f"Unique prefix {i}: {unique_prefix}")
        prompt = unique_prefix + suffix

        prefix_token_length = len(tokenizer.encode(unique_prefix)) - 1
        assert (
            prefix_token_length == prefix_length
        ), f"Prefix token length: {prefix_token_length}, Expected prefix token length: {prefix_length}"

        # prefix_token_length = None
        input_token_length = len(tokenizer.encode(prompt))
        total_input_token_length += input_token_length

        prompt_list.append(prompt)
        propmt_pos_list.append(prefix_token_length)
        input_token_list.append(input_token_length)

    # Test with prefix 1,2,3,...,N, 1,2,3,...,N, 1,2,3,...,N, ...
    if not prefix:
        print("Sequential, no prefix")
        prompt_inputs = prompt_list * num_samples_per_prefix
    else:
        if not reorder:
            print("Testing with prefix 1,2,3,...,N, 1,2,3,...,N, 1,2,3,...,N, ...")
            prompt_inputs = prompt_list * num_samples_per_prefix
        else:
            print("Reordering prompts...")
            prompt_inputs = []
            for i in range(num_prefix):
                prompt_inputs.extend([prompt_list[i]] * num_samples_per_prefix)

    assert len(prompt_inputs) == tot_num_reqs, f"Prompt input length: {len(prompt_inputs)}, Expected length: {tot_num_reqs}"

    print("\n START")
    st_time = time.time()
    for index, prompt in enumerate(prompt_inputs, start=1):
        print(f"Processing prompt {index}/{len(prompt_inputs)}...", end="\r")
        call_openai_with_caching(prompt)
    end_time = time.time()

    total_time += end_time - st_time
    avg_latency = total_time / tot_num_reqs
    rps = tot_num_reqs / total_time
    tps = (total_input_token_length + total_output_token_length) / total_time
    estimated_cost = (total_input_token_length / 1_000_000) * cost_per_million_tokens

    print("\n---------------------------------------------")
    if reorder:
        print("With Reordering:")
    else:
        print("Without Reordering:")
    print(f"Average Latency: {avg_latency:.2f} s/request")
    print(f"Throughput: {rps:.2f} requests/s")
    print(f"TPS: {tps:.2f} tokens/s")
    print(f"Total number of requests: {tot_num_reqs}")
    print(f"Total number of processed token: {total_input_token_length}")
    print(f"Estimated Cost: ${estimated_cost:.2f}")
    print("---------------------------------------------\n")


########################################


################ Main ##################
num_prefix = 10
num_samples_per_prefix = 15  # total number of request = num_prefix * num_samples_per_prefix
prefix_length = 1024
input_length = 1424  # input_length > 1424 for caching to be activated
reorder = False

print(
    f"Total number of unique prefixes: {num_prefix}, Total number of requests: {num_prefix * num_samples_per_prefix}, Number of samples for each unique prefix: {num_samples_per_prefix}"
)
print(f"Prefix length: {prefix_length}, Input length: {input_length}")

test_prefix_caching(num_prefix, num_samples_per_prefix, prefix_length, input_length, reorder=reorder)
