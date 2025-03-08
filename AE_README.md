# Artifact Evaluation Instructions 
## Installation 

Clone and create a new python environment, installing dependencies and ensuring Python version >= 3.10.
```
git clone -b llmsql https://github.com/lynnliu030/artifact-eval.git
cd artifact-eval

python -m venv artifact_env
source artifact_env/bin/activate
pip install -e .
sudo apt-get update; sudo apt install default-jdk
```

Put the following into `~/.bashrc` as necessary. Please update the following paths depending on your python installation.
```
export JAVA_HOME=/usr
export PATH=$JAVA_HOME/bin:$PATH

export PYSPARK_PYTHON=/artifact_env/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/artifact_env/bin/python3.10
```

Login with huggingface, using a token that access to the gated Meta-Llama models. Specifically, access to meta-llama/Meta-Llama-3-8B-Instruct is required.
```
huggingface-cli login
```

## Dataset download
Most datasets are located in `/datasets` folder. For larger dataset we use in our experiments, download it with S3. 
```
bash download_dataset.sh
```

## Main Experiments (Fig 3, Fig 4, Tab 2, Tab 5)
This executes Filter, Projection, RAG, Multi-LLM invocation, and Aggregation queries with Meta-Llama-3-8B-Instruct model, over Movies, Products, BIRD, PDMX, Beer, FEVER, and SQuAD datasets with three different algorithms including No Cache, Cache (Original), and Cache (GGR). 

### Runtime and Solver Time (s) (Fig 3, Fig 4, Tab 5) 
Run the following command to reproduce  results from Fig 3, Fig 4, and Tab 5 on Nvidia L4 instances. 
```
bash run/main_queries/fig_3_4_run_script.sh 
```

In folder `logs/fig3-4/query`, find the results in log file in this format `{query}_${dataset}_${algorithm}_output_vllm.txt`. 

The results of end-to-end query runtime(s) are as shown in Fig 3, Fig 4. Example as follows. The reported results in Figure 3, Figure 4 from the paper are in the field `Total time`. 
```
*************************Result*************************
Algorithm: quick_greedy
Number of rows: 15000
Algorithm Runtime: 7.329204082489014
LLM time: 1837.8235006332397
SQL Operators time: 7.1459527015686035
Total time: 1852.2986574172974
Requests per Second (RPS): 8.16182837733417
```
The results of solver runtimes (s) in Tab 5 are also logged in this folder. Example as follows. 
```
Algorithm runtime: 8.372
```
### Prefix Hit Rate (Tab 2) 

After the experiment is done, in the folder `logs/fig3-4/server`, find the result log file in this format `{query}_${dataset}_server_${algorithm}_${port}_vllm${cache_suffix}.log`. 
- `cache_suffix` indicates whether prefix cache is enabled in vLLM engine or not.
- The results of prefix hit rate (PHR) shown in Tab 2 are logged in vLLM server side. Example as follows. 
```
INFO 03-07 20:33:07 metrics.py:367] Prefix cache hit rate: GPU: 26.05%, CPU: 0.00%
```

## Larger Model (Fig 5)
Run the following commands to reproduce Meta-Llama-3-70B-Instruct results from Fig 5 on 8 x Nvidia L4 instances (e.g., g2-standard-96 on GCP). 
```
bash /run/main_queries/fig_5_tensor_parallel.sh 
```

Results for the end-to-end runtime can be seen in `logs/fig5/query` as shown before. 

## OpenAI and Anthropic Costs (Tab 3) 

First, set the Anthropic key:
```bash
export ANTHROPIC_API_KEY="your-anthropic-api-key"
```

Second, set the OpenAI key:
```bash
export OPENAI_API_KEY="your-openai-api-key"
```

Next, run the test with the naive approach via:
```bash
python ./run/cost/openai/benchmark_prompts_naive.py
```

The expected output gets stored in the files:

- results_openai_fever_naive_user_prompts.txt
- results_anthropic_fever_naive_user_prompts.txt


Next, we need to either change the API key for both Anthropic and OpenAI, or wait for ~2 hours for the cache to invalidate. Afterward, run the test with the greedy approach via:

```bash
python ./run/cost/openai/benchmark_prompts_greedy.py
```

The expected output gets stored in the files:

- results_openai_fever_greedy_user_prompts.txt
- results_anthropic_fever_greedy_user_prompts.txt

### Estimated Cost (Tab 4)
This script takes in the PHR calculated from main experiment runs and estimate the cost savings on OpenAI and Anthropic using our algorithm GGR compared to naive vLLM cache algorithm. The expected outputs are shown in Table 4 from the original paper. 
```
python /run/cost/estimate_cost_savings.py
```

## Accuracy Experiments (Fig 6) 
All the scripts for accuracy experiments in Figure 6 are contained in the `/run/accuracy` folder. 

```
cd /run/accuracy 
```
### All Datasets except FEVER

For all datasets except FEVER, we have randomly sampled the same 100 rows from both the original dataset and the column reordered version of the dataset, and manually labelled them. The files for the sampled original dataset and the sampled reordered dataset with the manual labels are in the `./datasets` directory.

To reproduce the accuracy results, first generate predictions for each row using either Llama models or GPT4o by following the instructions below.

#### Llama Models
Run `python llama_accuracy.py --huggingface-hub-token=<INSERT_HUGGINGFACE_API_KEY> --dataset=<DATASET> --model=<MODEL>` to run inference on the original non-reordered dataset using the model of your choice. This will add a new column to the input dataset CSV with the inference outputs and write it back to the same location (inside `datasets` directory).

To run inference on the reordered dataset, simply add the `--reordered` flag when running the command.

You can run `python llama_accuracy.py --help` to see the full list of supported models and datasets. Note that for Llama-70B, the scripts are currently setup to run with 8-way tensor parallelism, which requires 8 GPUs on a node. You can update the script to change the tp factor when initializing the vLLM engine, but all of our experiments were run iwht `tp=8`.

#### OpenAI GPT4o
Run `python gpt_accuracy.py --openai-api-key=<INSERT_OPENAI_API_KEY> --dataset=<DATASET> --model=<MODEL>` to run inference on the original non-reordered dataset using the model of your choice. This will add a new column to the input dataset CSV with the inference outputs and write it back to the same location (inside `datasets` directory).

To run inference on the reordered dataset, simply add the `--reordered` flag when running the command.

You can run `python gpt_accuracy.py --help` to see the full list of supported datasets.

### FEVER

For Fever, we have the ground truth labels for every row in the dataset. The files are too large to upload to git, so we read them from cloud storage.

#### Llama Models
Run `python llama_accuracy_fever.py --huggingface-hub-token=<INSERT_HUGGINGFACE_API_KEY> --model=<MODEL>` to run inference on the original non-reordered dataset using the model of your choice. This will add a new column to the input dataset CSV with the inference outputs and write it back to the same location (inside `datasets` directory).

To run inference on the reordered dataset, simply add the `--reordered` flag when running the command.

You can run `python llama_accuracy_fever.py --help` to see the full list of supported models. Note that for Llama-70B, the scripts are currently setup to run with 8-way tensor parallelism, which requires 8 GPUs on a node. You can update the script to change the tp factor when initializing the vLLM engine, but all of our experiments were run iwht `tp=8`.

#### OpenAI GPT4o
Run `python gpt_accuracy_fever.py --openai-api-key=<INSERT_OPENAI_API_KEY>` to run inference on the original non-reordered dataset using the model of your choice. This will add a new column to the input dataset CSV with the inference outputs and write it back to the same location (inside `datasets` directory).

To run inference on the reordered dataset, simply add the `--reordered` flag when running the command.

### Performing Bootstrapping
Once you have generated the inference results for all the datasets, you can run `bootstrapping.ipynb` to execute the bootstrapping steps and get the accuracy percentiles for both the original and reordered datasets. Simply modify the path in the notebook to point to which dataset you want to get accuracy results on.

