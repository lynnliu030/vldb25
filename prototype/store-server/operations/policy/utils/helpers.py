import yaml
import pandas as pd
import networkx as nx
from ..model.config import Config
import os
import json
from ..utils.definitions import (
    aws_instance_throughput_limit,
    gcp_instance_throughput_limit,
    azure_instance_throughput_limit,
)
import statistics
import csv

GB = 1024 * 1024 * 1024


def refine_string(s):
    parts = s.split("-")[:4]
    refined = parts[0] + ":" + "-".join(parts[1:])
    return refined


def convert_hyphen_to_colon(s):
    return s.replace(":", "-", 1)


def load_config(config_path: str) -> Config:
    with open(get_full_path(config_path), "r") as f:
        config_data = yaml.safe_load(f)
        return Config(**config_data)


def get_full_path(relative_path: str):
    # Move up to the SkyStore-Simulation directory from helpers.py
    base_path = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    full_path = os.path.join(base_path, relative_path)
    return full_path


def load_profile(file_name: str):
    return pd.read_csv(f"src/profiles/{file_name}")


def make_csv(filename="manual.csv"):
    path = os.path.dirname(os.path.abspath(__file__))
    data = pd.read_csv(os.path.join(path, "profiles", filename))

    # remove all lines with `op` == `write`
    data = data[data["op"] != "write"]
    return data


def get_min_network_cost(G: nx.DiGraph):
    filtered_edges_with_cost = [
        (u, v, d["cost"])
        for u, v, d in G.edges(data=True)
        if d["cost"] and d["cost"] > 0
    ]
    return (
        min(filtered_edges_with_cost, key=lambda x: x[2])[2]
        if filtered_edges_with_cost
        else None
    )


def get_median_network_cost(G: nx.DiGraph):
    filtered_edges_with_cost = [
        d["cost"] for u, v, d in G.edges(data=True) if d["cost"] and d["cost"] > 0
    ]
    return (
        statistics.median(filtered_edges_with_cost)
        if filtered_edges_with_cost
        else None
    )


def get_avg_network_cost(G: nx.DiGraph):
    filtered_edges_with_cost = [
        (u, v, d["cost"])[2]
        for u, v, d in G.edges(data=True)
        if d["cost"] and d["cost"] > 0
    ]
    return sum(filtered_edges_with_cost) / len(filtered_edges_with_cost)


def get_avg_storage_cost(G: nx.DiGraph):
    storages = [G.nodes[node]["priceStorage"] for node in G.nodes]
    return sum(storages) / len(storages)


def read_timestamps_from_csv(file_path: str = "test3.csv") -> list:
    timestamp_column = "timestamp"

    path = os.path.dirname(os.path.abspath(__file__))
    data = pd.read_csv(os.path.join(path, "profiles", file_path))

    return data[timestamp_column].tolist()


def make_nx_graph(
    cost_path=None,
    throughput_path=None,
    latency_path=None,
    storage_cost_path=None,
    num_vms=1,
):
    """
    Default graph with capacity constraints and cost info
    nodes: regions, edges: links
    per edge:
        throughput: max tput achievable (gbps)
        cost: $/GB
        flow: actual flow (gbps), must be < throughput, default = 0
    """
    path = os.path.dirname(os.path.abspath(__file__))
    if cost_path is None:
        cost = pd.read_csv(os.path.join(path, "profiles", "cost.csv"))
    else:
        cost = pd.read_csv(cost_path)

    if throughput_path is None:
        throughput = pd.read_csv(os.path.join(path, "profiles", "throughput.csv"))
    else:
        throughput = pd.read_csv(throughput_path)

    if latency_path is None:
        complete_latency = pd.read_csv(
            os.path.join(path, "profiles", "complete_latency.csv")
        )
    else:
        with open(latency_path, "r") as f:
            latency = json.load(f)

    if storage_cost_path is None:
        storage = pd.read_csv(os.path.join(path, "profiles", "storage.csv"))
        storage_gcp = pd.read_csv(
            os.path.join(path, "profiles", "gcp_storage_pricing.csv")
        )
    else:
        storage = pd.read_csv(storage_cost_path)

    G = nx.DiGraph()
    for _, row in throughput.iterrows():
        if row["src_region"] == row["dst_region"]:
            continue
        G.add_edge(
            row["src_region"],
            row["dst_region"],
            cost=None,
            throughput=num_vms * row["throughput_sent"] / GB,
        )

    for _, row in cost.iterrows():
        if row["src"] in G and row["dest"] in G[row["src"]]:
            G[row["src"]][row["dest"]]["cost"] = row["cost"]
            G[row["src"]][row["dest"]]["latency"] = 1

    # some pairs not in the cost grid
    no_cost_pairs = []
    for edge in G.edges.data():
        src, dst = edge[0], edge[1]
        if edge[-1]["cost"] is None:
            no_cost_pairs.append((src, dst))
    print("Unable to get egress costs for: ", no_cost_pairs)

    no_storage_cost = set()
    for _, row in storage.iterrows():
        region = row["Vendor"] + ":" + row["Region"]
        if region in G:
            if row["Group"] == "storage" and (
                row["Tier"] == "General Purpose" or row["Tier"] == "Hot"
            ):
                G.nodes[region]["priceStorage"] = row["PricePerUnit"]
        else:
            no_storage_cost.add(region)

    for _, row in complete_latency.iterrows():
        src_region = row["src_region"]
        dst_region = row["dst_region"]
        if (
            G.has_edge(src_region, dst_region)
            and row["dst_tier"] == "PREMIUM"
            and row["src_tier"] == "PREMIUM"
        ):
            G[src_region][dst_region]["latency"] = row["avg_rtt"]

    for node in G.nodes:
        if not G.has_edge(node, node):
            ingress_limit = (
                aws_instance_throughput_limit[1]
                if node.startswith("aws")
                else (
                    gcp_instance_throughput_limit[1]
                    if node.startswith("gcp")
                    else azure_instance_throughput_limit[1]
                )
            )
            G.add_edge(
                node, node, cost=0, throughput=num_vms * ingress_limit, latency=1
            )

    return G
