def calculate_cost(cached_percent):
    cached_percent /= 100
    uncached_percent = 1 - cached_percent
    return cached_percent / 2 + uncached_percent


def calculate_anthropic_cost(cached_percent):
    cached_percent /= 100
    uncached_percent = 1 - cached_percent
    return cached_percent / 10 + uncached_percent + 1.25 * uncached_percent


def ratio(naive, ggr):
    return calculate_cost(naive) / calculate_cost(ggr)


def anthropic_ratio(naive, ggr):
    return calculate_anthropic_cost(naive) / calculate_anthropic_cost(ggr)


# Numbers collected from vLLM PHR
datasets_naive_ggr_phr = {
    "Movies": (34.6, 85.7),
    "Products": (26.7, 83.3),
    "BIRD": (10.4, 84.8),
    "PDMX": (11.8, 56.6),
    "Beer": (49.9, 80.1),
    "FEVER": (11.2, 67.4),
    "SQuAD": (11.0, 69.7),
}

for dataset, (naive, ggr) in datasets_naive_ggr_phr.items():
    openai_savings = round((ratio(naive, ggr) - 1) / ratio(naive, ggr) * 100)
    anthropic_savings = round((anthropic_ratio(naive, ggr) - 1) / anthropic_ratio(naive, ggr) * 100)
    print(f"{dataset} - OpenAI GGR savings v.s. Original = {openai_savings}%")
    print(f"{dataset} - Anthropic GGR savings v.s. Original = {anthropic_savings}%")
    print()
