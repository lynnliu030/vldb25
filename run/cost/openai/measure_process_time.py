import time
import openai
from transformers import AutoTokenizer

openai.api_key = "<your key>"
tokenizer = AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")


def call_openai(system_instruction, prompt):
    """Make an OpenAI API call with a prompt and system instruction."""
    response = openai.chat.completions.create(
        model="gpt-4o", messages=[{"role": "system", "content": system_instruction}, {"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


def get_token_byte_size(prompt):
    """Calculate the byte size of each token in the prompt."""
    print("\n--\n")
    tokens = tokenizer.encode(prompt)
    total_bytes = sum(len(tokenizer.decode([token]).encode("utf-8")) for token in tokens)
    avg_token_size = total_bytes / len(tokens) if tokens else 0
    print(f"Total Bytes: {total_bytes}, Number of Tokens: {len(tokens)}, Avg Bytes per Token: {avg_token_size:.2f}")


def measure_tokens_per_second(prompts, system_instruction):
    total_time = 0
    total_tokens = 0

    for prompt in prompts:
        token_count = len(tokenizer.encode(prompt))
        total_tokens += token_count

    for i, prompt in enumerate(prompts):
        print(f"Processing prompt {i + 1}/{len(prompts)}...", end="\r")
        start_time = time.time()
        answer = call_openai(system_instruction, prompt)
        print(f"Prompt {i} - Answer: {answer}")
        total_time += time.time() - start_time

    tokens_per_second = total_tokens / total_time
    print(f"\nTotal Time: {total_time:.2f} seconds")
    print(f"Tokens Processed Total: {total_tokens}")
    print(f"Tokens Per Query: {len(tokenizer.encode(prompt))}")
    print(f"Tokens per Second: {tokens_per_second:.2f}")
    return tokens_per_second


prompts = ["Apple " * 500] * 100
system_instruction = "Respond with 'Yes' if the prompt contains 500 times the word Apple. Otherwise, respond with 'No'"

get_token_byte_size(prompts[0])

tokens_per_second = measure_tokens_per_second(prompts, system_instruction)
