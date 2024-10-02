import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
import math

sns.set(style="whitegrid")


def parse_metrics(file_path):
    metrics = {}
    with open(file_path, "r") as file:
        lines = file.readlines()[-13:]
        for line in lines:
            match = re.search(r"\|\s+([^|]+)\s+\|\s+([^|]+)\s+\|", line)
            if match:
                key, value = match.groups()
                key = key.strip()
                try:
                    # Improved handling of numbers, including scientific notation
                    value = float(value.strip())
                except ValueError:
                    pass  # Keep the original string if conversion to float fails
                metrics[key] = value
    return metrics


def is_log_scale_needed(values):
    return any(v > 0 and math.log10(v) < -4 for v in values)


base_dir = "./"
config_files = ["all", "skystore", "single", "spanstore"]
metrics_data = {}

for config in config_files:
    output_file = f"{config}_error.out"
    if os.path.isfile(os.path.join(base_dir, output_file)):
        metrics_data[config.replace("_", " ").title()] = parse_metrics(
            os.path.join(base_dir, output_file)
        )

df_data = {"Metric": []}
for key in metrics_data.values():
    for metric in key.keys():
        if metric not in df_data["Metric"]:
            df_data["Metric"].append(metric)

for config, metrics in metrics_data.items():
    df_data[config] = [metrics.get(metric, "N/A") for metric in df_data["Metric"]]

df = pd.DataFrame(df_data)
for column in df.columns:
    if "latency" in column or "cost" in column:
        df[column] = pd.to_numeric(df[column], errors="coerce")

# Plotting with Seaborn
for metric in df["Metric"]:
    fig, ax = plt.subplots(figsize=(8, 6))
    metric_row = df[df["Metric"] == metric]
    metric_data = metric_row.iloc[0, 1:].to_dict()
    metric_data = {k: pd.to_numeric(v, errors="coerce") for k, v in metric_data.items()}

    # Check if "Pull" is a key and change it to "SkyStore"
    if "Pull" in metric_data:
        metric_data["SkyStore"] = metric_data.pop("Skystore")

    metric_data = {
        k: v for k, v in metric_data.items() if pd.api.types.is_numeric_dtype(type(v))
    }
    use_log_scale = is_log_scale_needed(metric_data.values())

    font_size = 14
    font_weight = "bold"
    if metric_data:
        sns.barplot(
            x=list(metric_data.keys()),
            y=list(metric_data.values()),
            ax=ax,
            palette="viridis",
        )  # Using Seaborn's barplot

        # Apply title method to metric for the plot title
        ax.set_title(metric.title(), fontsize=font_size, fontweight=font_weight)

        ax.set_xlabel("Policy", fontsize=font_size, fontweight=font_weight)
        # ax.set_ylabel("Value", fontsize=font_size, fontweight=font_weight)
        if use_log_scale:
            ax.set_yscale("log")
            ax.set_ylabel(
                "Value (log scale)", fontsize=font_size, fontweight=font_weight
            )
        else:
            ax.set_ylabel("Value", fontsize=font_size, fontweight=font_weight)

        for p in ax.patches:
            height = p.get_height()
            if height > 0:
                ax.annotate(
                    f"{height:.4f}",
                    (p.get_x() + p.get_width() / 2.0, height),
                    ha="center",
                    va="bottom",
                )
            else:
                ax.annotate(
                    "0",
                    (p.get_x() + p.get_width() / 2.0, height),
                    ha="center",
                    va="bottom",
                )

        plt.savefig(f"{metric}_bar_seaborn.pdf", dpi=300)
    else:
        print(f"No numeric data available for plotting the metric: {metric}")
