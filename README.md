# LLM-SQL: Artifact Evaluation [MLSys 25] 
This document contains instruction for MLSys 2025 artifact evaluation for the paper *Optimizing LLM Queries in Relational Data Analytics Workloads*. 

## Artifact Overview 
In this artifact, we include the implementation of the algorithm (GGR) proposed by the paper to reorder inputs to maximize KV cache reuse when performing LLM serving for batch analytics tasks. 

We also include the scripts for evaluating 5 different query types (LLM filter, LLM projection, Multi-LLM invocations, LLM aggregation, RAG) and a benchmark suite including 7 different datasets (Movies, Products, BIRD, PDMX, Beer, FEVER, SQuAD). 

## Directory Structure 
```

├── datasets/
├── run/
├── src/
│   ├── core/
│   └── prompts/
│   └── pyspark/
└── tests/
```
* `/datasets`: 7 dataset csvs. Detailed instructions for large file download see [AE_README.md](https://github.com/lynnliu030/artifact-eval/blob/llmsql/AE_README.md). 
* `/run`: scripts to reproduce experiments in the paper. 
* `/src/core`: scripts for the algorithm implementations are in this folder.
    * `quick_greedy.py` implements the core GGR algorithm.
    * `opt.py` implements the oracle OHPR algorithm that maximizes the prefix hit. 
* `/src/pyspark`: scripts to experiment with different queries; for each dataset, a variety of queries are run, including projection, filtering, multi-LLM invocations, AVG, and RAG queries.


## Run Instructions 
Instruction to run and reprodcue the main results for the paper are in [AE_README.md](https://github.com/lynnliu030/artifact-eval/blob/llmsql/AE_README.md).  
