import os
from typing import List

from langchain_community.embeddings import HuggingFaceHubEmbeddings
from langchain_community.vectorstores import FAISS
from transformers import AutoTokenizer

os.environ["TOKENIZERS_PARALLELISM"] = "false"

def vllm_model(
    model_type: str = "meta-llama/Llama-2-7b-chat-hf",
    tensor_parallel_size: int = None,
):
    if tensor_parallel_size is None:
        return LLM(model=model_type)
    else:
        return LLM(model=model_type, tensor_parallel_size=tensor_parallel_size)


def sampling_params(
    temperature: float = 0,
    spaces_between_special_tokens: bool = False,
    max_tokens: int = None,
):
    if max_tokens is None:
        return SamplingParams(
            temperature=temperature,
            spaces_between_special_tokens=spaces_between_special_tokens,
        )
    else:
        return SamplingParams(
            temperature=temperature,
            spaces_between_special_tokens=spaces_between_special_tokens,
            max_tokens=max_tokens,
        )


class LLMModel:
    _instance = None
    _sampling_params = None
    _tokenizer = None

    @classmethod
    def get_instance(cls):
        return cls._instance

    @classmethod
    def get_sampling_params(
        cls,
        temperature: float = 0,
        spaces_between_special_tokens: bool = False,
        max_tokens: int = None,
    ):
        if cls._sampling_params is None:
            cls._sampling_params = sampling_params(
                temperature=temperature,
                spaces_between_special_tokens=spaces_between_special_tokens,
                max_tokens=max_tokens,
            )
        return cls._sampling_params

    @classmethod
    def get_tokenizer(cls):
        if cls._tokenizer is None:
            cls._tokenizer = AutoTokenizer.from_pretrained("meta-llama/Llama-3.1-8B-Instruct")
        return cls._tokenizer


def vector_db(texts: List[str]):
    embeddings = HuggingFaceHubEmbeddings(model="http://localhost:8082")
    return FAISS.from_text(texts, embeddings)


class VectorDB:
    _instance = None

    @classmethod
    def get_instance(cls, texts: List[str]):
        if cls._instance is None:
            cls._instance = vector_db(texts)
        return cls._instance
