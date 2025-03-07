# Artifact Evaluation Instructions 
## Installation 

Clone and create a new conda environment, installing dependencies 
```
git clone -b llmsql https://github.com/lynnliu030/artifact-eval.git
cd artifact-eval

conda create -n artifact_env python=3.10
conda activate artifact_env
pip install -e .
sudo apt-get update; sudo apt install default-jdk
```

Put the following into `~/.bashrc` as necessary.
```
export JAVA_HOME=/usr
export PATH=$JAVA_HOME/bin:$PATH

export PYSPARK_PYTHON=/opt/conda/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python3.10
```

## Dataset download
Most datasets are located in `/datasets` folder. For larger dataset we use in our experiments, download it with S3 
```
# download fever.csv and squad.csv for RAG 

# download for accuracy experiments 
```

## Main Experiments (Fig 3, Fig 4, Tab 2, Tab 5)
This executes Filter, Projection, RAG, Multi-LLM invocation, and Aggregation queries with Meta-Llama-3-8B-Instruct model, over Movies, Products, BIRD, PDMX, Beer, FEVER, and SQuAD datasets with three different algorithms including No Cache, Cache (Original), and Cache (GGR). 

### Runtime and Solver Time (s) (Fig 3, Fig 4, Tab 5) 
Run the following command to reproduce  results from Fig 3, Fig 4, and Tab 5 on Nvidia L4 instances. 
```
bash /run/main_queries/fig_3_4_run_script.sh 
```

In folder `logs/fig3-4/query`, find the results in log file in this format `{query}_${dataset}_${algorithm}_output_vllm.txt`. 

The results of end-to-end query runtime(s) are as shown in Fig 3, Fig 4. Example as follows. 
```
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

## Cost Estimation (Tab 3, Tab 4) 
Run 
```
```

Expected output 
```
```
## Accuracy Experiments (Fig 6) 
Run 
```
```

Expected output 
```
```

