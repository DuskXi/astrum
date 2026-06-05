from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except ImportError:
    Console = None
    Panel = None
    Table = None

from astrum import F, AstrumConfig, Ref
from astrum.decorators import SchedulerRegistry


PIP_INSTALL_HINT = "pip install python-dotenv litellm rich numpy scipy"
RUNTIME_PACKAGES = {
    "dotenv": "python-dotenv",
    "litellm": "litellm",
    "rich": "rich",
    "numpy": "numpy",
    "scipy": "scipy",
}
KNOWN_API_KEY_ENV_NAMES = (
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "COHERE_API_KEY",
    "TOGETHERAI_API_KEY",
    "AZURE_API_KEY",
    "VOYAGE_API_KEY",
    "NVIDIA_NIM_API_KEY",
    "DEEPINFRA_API_KEY",
    "FIREWORKS_API_KEY",
)


class PlainConsole:
    """Fallback console for plan-only runs when Rich is not installed."""

    def print(self, *values: Any, **_: Any) -> None:
        print(*values)

    def log(self, *values: Any, **_: Any) -> None:
        print(*values)


console = Console() if Console is not None else PlainConsole()


@dataclass(frozen=True)
class RetrieverSettings:
    """All tunable knobs come from environment variables loaded by dotenv."""

    provider: str
    api_key: str
    api_key_env: str
    api_base: str | None
    completion_model: str
    embedding_model: str
    rerank_model: str
    astrum_concurrency: int | None
    embed_max_concurrency: int
    keyword_top_n: int
    chunk_window_size: int
    chunk_stride: int
    weight_keyword_bm25: float
    weight_query_bm25: float
    weight_query_embedding: float
    weight_keyword_embedding: float
    weight_query_chunk: float
    weight_keyword_chunk: float
    prefilter_top_k: int
    prefilter_threshold: float
    rerank_input_top_n: int
    rerank_top_k: int
    rerank_threshold: float


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_optional_str(name: str) -> str | None:
    value = _env_str(name)
    return value or None


def _env_int(name: str, default: int) -> int:
    value = _env_str(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None


def _env_optional_int(name: str) -> int | None:
    value = _env_str(name)
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None
    return parsed if parsed > 0 else None


def _env_float(name: str, default: float) -> float:
    value = _env_str(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"{name} must be a float, got {value!r}") from None


def _default_api_key_env(provider: str) -> str:
    normalized = provider.strip().lower().replace("-", "_")
    if normalized == "openai":
        return "OPENAI_API_KEY"
    if normalized == "cohere":
        return "COHERE_API_KEY"
    if normalized in {"together", "together_ai", "togetherai"}:
        return "TOGETHERAI_API_KEY"
    if normalized == "anthropic":
        return "ANTHROPIC_API_KEY"
    if normalized == "azure":
        return "AZURE_API_KEY"
    if normalized == "voyage":
        return "VOYAGE_API_KEY"
    return f"{normalized.upper()}_API_KEY" if normalized else "OPENAI_API_KEY"


def load_settings() -> RetrieverSettings:
    provider = _env_str("RETRIEVER_PROVIDER", "openai")
    api_key_env = _env_str("RETRIEVER_API_KEY_ENV", _default_api_key_env(provider))
    api_key = _env_str("RETRIEVER_API_KEY") or _env_str(api_key_env)

    if api_key:
        os.environ[api_key_env] = api_key

    return RetrieverSettings(
        provider=provider,
        api_key=api_key,
        api_key_env=api_key_env,
        api_base=_env_optional_str("RETRIEVER_API_BASE"),
        completion_model=_env_str("RETRIEVER_COMPLETION_MODEL", "openai/gpt-5-mini"),
        embedding_model=_env_str("RETRIEVER_EMBEDDING_MODEL", "openai/text-embedding-3-small"),
        rerank_model=_env_str("RETRIEVER_RERANK_MODEL", "cohere/rerank-v3.5"),
        astrum_concurrency=_env_optional_int("RETRIEVER_ASTRUM_CONCURRENCY"),
        embed_max_concurrency=max(1, _env_int("RETRIEVER_EMBED_MAX_CONCURRENCY", 8)),
        keyword_top_n=max(1, _env_int("RETRIEVER_KEYWORD_TOP_N", 8)),
        chunk_window_size=max(1, _env_int("RETRIEVER_CHUNK_WINDOW_SIZE", 3)),
        chunk_stride=max(1, _env_int("RETRIEVER_CHUNK_STRIDE", 1)),
        weight_keyword_bm25=_env_float("RETRIEVER_WEIGHT_KEYWORD_BM25", 0.16),
        weight_query_bm25=_env_float("RETRIEVER_WEIGHT_QUERY_BM25", 0.18),
        weight_query_embedding=_env_float("RETRIEVER_WEIGHT_QUERY_EMBEDDING", 0.24),
        weight_keyword_embedding=_env_float("RETRIEVER_WEIGHT_KEYWORD_EMBEDDING", 0.18),
        weight_query_chunk=_env_float("RETRIEVER_WEIGHT_QUERY_CHUNK", 0.12),
        weight_keyword_chunk=_env_float("RETRIEVER_WEIGHT_KEYWORD_CHUNK", 0.12),
        prefilter_top_k=max(1, _env_int("RETRIEVER_PREFILTER_TOP_K", 8)),
        prefilter_threshold=_env_float("RETRIEVER_PREFILTER_THRESHOLD", 0.0),
        rerank_input_top_n=max(1, _env_int("RETRIEVER_RERANK_INPUT_TOP_N", 6)),
        rerank_top_k=max(1, _env_int("RETRIEVER_RERANK_TOP_K", 5)),
        rerank_threshold=_env_float("RETRIEVER_RERANK_THRESHOLD", 0.0),
    )


def missing_packages() -> list[str]:
    missing: list[str] = []
    for import_name, package_name in RUNTIME_PACKAGES.items():
        if importlib.util.find_spec(import_name) is None:
            missing.append(package_name)
    return missing


def has_any_api_key(settings: RetrieverSettings) -> bool:
    if settings.api_key:
        return True
    return any(bool(os.getenv(name)) for name in KNOWN_API_KEY_ENV_NAMES)


def require_runtime_ready(settings: RetrieverSettings) -> None:
    missing = missing_packages()
    errors: list[str] = []
    if missing:
        errors.append(f"Missing packages: {', '.join(missing)}")
    if not has_any_api_key(settings):
        errors.append(
            "Missing API key. Set RETRIEVER_API_KEY or provider-native keys "
            f"such as {settings.api_key_env}."
        )
    if not settings.completion_model:
        errors.append("Missing RETRIEVER_COMPLETION_MODEL.")
    if not settings.embedding_model:
        errors.append("Missing RETRIEVER_EMBEDDING_MODEL.")
    if not settings.rerank_model:
        errors.append("Missing RETRIEVER_RERANK_MODEL.")

    if not errors:
        return

    sample_env = f"""
RETRIEVER_PROVIDER=openai
RETRIEVER_API_KEY_ENV=OPENAI_API_KEY
RETRIEVER_API_KEY=sk-...
RETRIEVER_COMPLETION_MODEL=openai/gpt-5-mini
RETRIEVER_EMBEDDING_MODEL=openai/text-embedding-3-small
RETRIEVER_RERANK_MODEL=cohere/rerank-v3.5
RETRIEVER_EMBED_MAX_CONCURRENCY=8
RETRIEVER_PREFILTER_TOP_K=8
RETRIEVER_RERANK_TOP_K=5
""".strip()
    message = "\n".join(errors)
    raise RuntimeError(
        f"{message}\n\nInstall example dependencies with:\n  {PIP_INSTALL_HINT}\n\n"
        f"Example .env:\n{sample_env}"
    )


def tokenize(text: str) -> list[str]:
    """中文: 轻量分词供 BM25 示例使用。English: simple tokenization for the BM25 demo."""

    return re.findall(r"[\w]+", text.lower(), flags=re.UNICODE)


def bm25_scores(candidates: list[str], query_terms: list[str]) -> list[float]:
    """中文: 自实现 BM25，避免为示例再引入 rank_bm25。English: local BM25 to keep this example dependency-light."""

    tokenized_docs = [tokenize(text) for text in candidates]
    if not tokenized_docs:
        return []

    doc_count = len(tokenized_docs)
    avg_doc_len = sum(len(doc) for doc in tokenized_docs) / max(doc_count, 1)
    avg_doc_len = avg_doc_len or 1.0
    k1 = 1.5
    b = 0.75

    document_frequency: dict[str, int] = {}
    for doc in tokenized_docs:
        for term in set(doc):
            document_frequency[term] = document_frequency.get(term, 0) + 1

    scores: list[float] = []
    for doc in tokenized_docs:
        term_frequency: dict[str, int] = {}
        for term in doc:
            term_frequency[term] = term_frequency.get(term, 0) + 1

        score = 0.0
        doc_len = len(doc) or 1
        for term in query_terms:
            if not term:
                continue
            freq = term_frequency.get(term, 0)
            if freq == 0:
                continue
            df = document_frequency.get(term, 0)
            idf = math.log(1.0 + (doc_count - df + 0.5) / (df + 0.5))
            numerator = freq * (k1 + 1.0)
            denominator = freq + k1 * (1.0 - b + b * doc_len / avg_doc_len)
            score += idf * numerator / denominator
        scores.append(float(score))
    return scores


def min_max_normalize(scores: list[float]) -> list[float]:
    if not scores:
        return []
    minimum = min(scores)
    maximum = max(scores)
    if math.isclose(maximum, minimum):
        return [0.0 for _ in scores]
    return [(score - minimum) / (maximum - minimum) for score in scores]


def cosine_scores(query_vectors: list[list[float]], candidate_vectors: list[list[float]]) -> list[float]:
    """中文: 用 scipy 的 cosine 距离转相似度。English: convert scipy cosine distance to similarity."""

    import numpy as np
    from scipy.spatial.distance import cdist

    if not query_vectors or not candidate_vectors:
        return [0.0 for _ in candidate_vectors]

    query_matrix = np.asarray(query_vectors, dtype=float)
    candidate_matrix = np.asarray(candidate_vectors, dtype=float)
    similarities = 1.0 - cdist(candidate_matrix, query_matrix, metric="cosine")
    similarities = np.nan_to_num(similarities, nan=0.0, posinf=0.0, neginf=0.0)
    return similarities.max(axis=1).astype(float).tolist()


def centroid(vectors: list[list[float]]) -> list[float]:
    import numpy as np

    if not vectors:
        return []
    matrix = np.asarray(vectors, dtype=float)
    return matrix.mean(axis=0).astype(float).tolist()


async def embed_text(text: str, settings: RetrieverSettings) -> list[float]:
    """中文: LiteLLM embedding 封装。English: small wrapper around LiteLLM aembedding."""

    from litellm import aembedding

    kwargs: dict[str, Any] = {
        "model": settings.embedding_model,
        "input": [text],
    }
    if settings.api_base:
        kwargs["api_base"] = settings.api_base

    response = await aembedding(**kwargs)
    data = getattr(response, "data", None)
    if data is None and isinstance(response, dict):
        data = response.get("data")
    if not data:
        raise RuntimeError("Embedding response did not contain data.")

    first = data[0]
    embedding = getattr(first, "embedding", None)
    if embedding is None and isinstance(first, dict):
        embedding = first.get("embedding")
    if embedding is None:
        raise RuntimeError("Embedding response did not contain an embedding vector.")
    return [float(value) for value in embedding]


async def embed_many(texts: list[str], settings: RetrieverSettings) -> list[list[float]]:
    """中文: 内部并发受宏控制。English: inner fan-out concurrency is controlled by env."""

    semaphore = asyncio.Semaphore(settings.embed_max_concurrency)

    async def run_one(text: str) -> list[float]:
        async with semaphore:
            return await embed_text(text, settings)

    return await asyncio.gather(*(run_one(text) for text in texts))


def extract_message_content(response: Any) -> str:
    """中文: 兼容 LiteLLM 的对象和 dict 响应。English: accept object-style and dict-style LiteLLM responses."""

    choices = getattr(response, "choices", None)
    if choices is None and isinstance(response, dict):
        choices = response.get("choices")
    if not choices:
        return ""

    first_choice = choices[0]
    message = getattr(first_choice, "message", None)
    if message is None and isinstance(first_choice, dict):
        message = first_choice.get("message")

    content = getattr(message, "content", None)
    if content is None and isinstance(message, dict):
        content = message.get("content")
    return content or ""


def parse_keywords(text: str, top_n: int) -> list[str]:
    """中文: 优先解析 JSON 列表，失败时回退到逗号/换行分割。English: prefer JSON, then split text."""

    cleaned = text.strip()
    if not cleaned:
        return []

    json_candidates = [cleaned]
    match = re.search(r"\[[\s\S]*\]", cleaned)
    if match:
        json_candidates.insert(0, match.group(0))

    for candidate in json_candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            keywords = [str(item).strip() for item in parsed if str(item).strip()]
            return dedupe_preserve_order(keywords)[:top_n]

    rough_parts = re.split(r"[,;\n]+", cleaned)
    keywords = [part.strip(" -'\"") for part in rough_parts if part.strip(" -'\"")]
    return dedupe_preserve_order(keywords)[:top_n]


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def build_embedding_windows(
    candidates: list[str],
    candidate_embeddings: list[list[float]],
    window_size: int,
    stride: int,
) -> list[dict[str, Any]]:
    """中文: 用候选项 embedding 构建滑动窗口。English: build sliding windows over candidate embeddings."""

    if not candidates or not candidate_embeddings:
        return []

    windows: list[dict[str, Any]] = []
    last_start = max(0, len(candidates) - window_size)
    starts = list(range(0, len(candidates), stride))
    if last_start not in starts:
        starts.append(last_start)

    for start in sorted(set(starts)):
        end = min(start + window_size, len(candidates))
        if start >= end:
            continue
        vectors = candidate_embeddings[start:end]
        windows.append(
            {
                "start": start,
                "end": end,
                "candidate_indexes": list(range(start, end)),
                "embedding": centroid(vectors),
                "text": "\n".join(candidates[start:end]),
            }
        )
    return windows


def pool_window_scores(windows: list[dict[str, Any]], window_scores: list[float], candidate_count: int) -> list[float]:
    scores = [0.0 for _ in range(candidate_count)]
    for window, score in zip(windows, window_scores):
        for candidate_index in window["candidate_indexes"]:
            scores[candidate_index] = max(scores[candidate_index], float(score))
    return scores


def blend_results(
    candidates: list[str],
    normalized_scores: dict[str, list[float]],
    settings: RetrieverSettings,
) -> list[dict[str, Any]]:
    weights = {
        "keyword_bm25": settings.weight_keyword_bm25,
        "query_bm25": settings.weight_query_bm25,
        "query_embedding": settings.weight_query_embedding,
        "keyword_embedding": settings.weight_keyword_embedding,
        "query_chunk": settings.weight_query_chunk,
        "keyword_chunk": settings.weight_keyword_chunk,
    }
    total_weight = sum(max(weight, 0.0) for weight in weights.values()) or 1.0

    blended: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates):
        channel_scores = {
            name: normalized_scores.get(name, [0.0 for _ in candidates])[index]
            for name in weights
        }
        score = sum(channel_scores[name] * max(weight, 0.0) for name, weight in weights.items()) / total_weight
        if score >= settings.prefilter_threshold:
            blended.append(
                {
                    "index": index,
                    "text": candidate,
                    "blend_score": float(score),
                    "channels": channel_scores,
                }
            )

    blended.sort(key=lambda item: item["blend_score"], reverse=True)
    return blended[: settings.prefilter_top_k]


def parse_rerank_results(response: Any, prefilter_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """中文: 解析 Cohere-style rerank 结果。English: parse Cohere-style rerank results from LiteLLM."""

    raw_results = getattr(response, "results", None)
    if raw_results is None and isinstance(response, dict):
        raw_results = response.get("results")
    if raw_results is None:
        raw_results = []

    parsed: list[dict[str, Any]] = []
    for position, raw in enumerate(raw_results):
        local_index = getattr(raw, "index", None)
        relevance_score = getattr(raw, "relevance_score", None)
        if isinstance(raw, dict):
            local_index = raw.get("index", local_index)
            relevance_score = raw.get("relevance_score", relevance_score)

        if local_index is None:
            local_index = position
        if relevance_score is None:
            relevance_score = 0.0

        if 0 <= int(local_index) < len(prefilter_results):
            base = dict(prefilter_results[int(local_index)])
            base["rerank_score"] = float(relevance_score)
            parsed.append(base)

    if parsed:
        parsed.sort(key=lambda item: item["rerank_score"], reverse=True)
    return parsed


async def call_keyword_llm(query: str, settings: RetrieverSettings) -> list[str]:
    from litellm import acompletion

    prompt = (
        "Generate concise associative retrieval keywords for the user query. "
        "Return only a JSON array of strings, with no markdown.\n\n"
        f"Query: {query}\n"
        f"Max keywords: {settings.keyword_top_n}"
    )
    kwargs: dict[str, Any] = {
        "model": settings.completion_model,
        "messages": [
            {
                "role": "system",
                "content": "You create search expansion keywords for a text retriever.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    if settings.api_base:
        kwargs["api_base"] = settings.api_base

    response = await acompletion(**kwargs)
    return parse_keywords(extract_message_content(response), settings.keyword_top_n)


async def call_reranker(
    query: str,
    prefilter_results: list[dict[str, Any]],
    settings: RetrieverSettings,
) -> list[dict[str, Any]]:
    from litellm import arerank

    selected = prefilter_results[: settings.rerank_input_top_n]
    if not selected:
        return []

    documents = [item["text"] for item in selected]
    kwargs: dict[str, Any] = {
        "model": settings.rerank_model,
        "query": query,
        "documents": documents,
        "top_n": min(settings.rerank_top_k, len(documents)),
    }
    if settings.api_base:
        kwargs["api_base"] = settings.api_base

    response = await arerank(**kwargs)
    reranked = parse_rerank_results(response, selected)
    if not reranked:
        reranked = [dict(item, rerank_score=item["blend_score"]) for item in selected]

    filtered = [item for item in reranked if item["rerank_score"] >= settings.rerank_threshold]
    filtered.sort(key=lambda item: item["rerank_score"], reverse=True)
    return filtered[: settings.rerank_top_k]


def build_retriever_scheduler(
    candidates: list[str],
    query: str,
    settings: RetrieverSettings,
) -> tuple[Any, dict[str, Any]]:
    namespace = f"stateless_text_retriever_{uuid.uuid4().hex}"
    workflow = SchedulerRegistry(namespace)
    result_holder: dict[str, Any] = {
        "prefilter_results": [],
        "final_results": [],
        "score_summary": [],
    }

    @workflow.task("load_input")
    async def load_input() -> dict[str, Any]:
        """中文: 注入本次调用输入。English: inject per-call input without keeping global state."""

        return {"candidates": candidates, "query": query}

    # 中文: T1 对候选项逐条 embedding，内部并发由环境变量控制。
    # English: T1 embeds candidates with inner concurrency controlled by env.
    @workflow.task("T1_embed_candidates")
    async def embed_candidates(
        candidate_texts: Ref[list, F("load_input", "candidates")],
    ) -> dict[str, Any]:
        vectors = await embed_many(candidate_texts, settings)
        console.log(f"T1 embedded {len(vectors)} candidates")
        return {"candidate_embeddings": vectors}

    # 中文: T2 用候选 embedding 做滑动窗口切块。
    # English: T2 builds embedding windows over the candidate list.
    @workflow.task("T2_build_embedding_windows")
    async def build_windows(
        candidate_texts: Ref[list, F("load_input", "candidates")],
        candidate_embeddings: Ref[list, F("T1_embed_candidates", "candidate_embeddings")],
    ) -> dict[str, Any]:
        windows = build_embedding_windows(
            candidate_texts,
            candidate_embeddings,
            settings.chunk_window_size,
            settings.chunk_stride,
        )
        console.log(f"T2 built {len(windows)} embedding windows")
        return {"embedding_windows": windows}

    # 中文: T3 用 LLM 为 query 生成联想关键词。
    # English: T3 asks an LLM for associative query keywords.
    @workflow.task("T3_generate_keywords")
    async def generate_keywords(
        query_text: Ref[str, F("load_input", "query")],
    ) -> dict[str, Any]:
        keywords = await call_keyword_llm(query_text, settings)
        console.log(f"T3 generated keywords: {', '.join(keywords) or '-'}")
        return {"keywords": keywords}

    # 中文: T4 为 query 本体生成 embedding。
    # English: T4 embeds the original query.
    @workflow.task("T4_embed_query")
    async def embed_query(
        query_text: Ref[str, F("load_input", "query")],
    ) -> dict[str, Any]:
        vector = await embed_text(query_text, settings)
        console.log("T4 embedded query")
        return {"query_embedding": vector}

    # 中文: T5 为联想关键词并发生成 embedding。
    # English: T5 embeds the generated keywords concurrently.
    @workflow.task("T5_embed_keywords")
    async def embed_keywords(
        keywords: Ref[list, F("T3_generate_keywords", "keywords")],
    ) -> dict[str, Any]:
        vectors = await embed_many(keywords, settings) if keywords else []
        console.log(f"T5 embedded {len(vectors)} keywords")
        return {"keyword_embeddings": vectors}

    # 中文: T6 用关键词列表和候选项计算 BM25 分数。
    # English: T6 scores candidates with keyword-expanded BM25.
    @workflow.task("T6_keyword_bm25_scores")
    async def keyword_bm25(
        candidate_texts: Ref[list, F("load_input", "candidates")],
        keywords: Ref[list, F("T3_generate_keywords", "keywords")],
    ) -> dict[str, Any]:
        query_terms = [term for keyword in keywords for term in tokenize(keyword)]
        return {"keyword_bm25_scores": bm25_scores(candidate_texts, query_terms)}

    # 中文: T7 用 query 本体计算 BM25 分数。
    # English: T7 scores candidates with original-query BM25.
    @workflow.task("T7_query_bm25_scores")
    async def query_bm25(
        candidate_texts: Ref[list, F("load_input", "candidates")],
        query_text: Ref[str, F("load_input", "query")],
    ) -> dict[str, Any]:
        return {"query_bm25_scores": bm25_scores(candidate_texts, tokenize(query_text))}

    # 中文: T8 用 query embedding 与候选 embedding 计算余弦相似度。
    # English: T8 computes query-candidate cosine similarity.
    @workflow.task("T8_query_embedding_scores")
    async def query_embedding_scores(
        query_embedding: Ref[list, F("T4_embed_query", "query_embedding")],
        candidate_embeddings: Ref[list, F("T1_embed_candidates", "candidate_embeddings")],
    ) -> dict[str, Any]:
        scores = cosine_scores([query_embedding], candidate_embeddings)
        return {"query_embedding_scores": scores}

    # 中文: T9 用关键词 embedding 与候选 embedding 计算多向量相似度。
    # English: T9 computes keyword-candidate multi-vector similarity.
    @workflow.task("T9_keyword_embedding_scores")
    async def keyword_embedding_scores(
        keyword_embeddings: Ref[list, F("T5_embed_keywords", "keyword_embeddings")],
        candidate_embeddings: Ref[list, F("T1_embed_candidates", "candidate_embeddings")],
    ) -> dict[str, Any]:
        scores = cosine_scores(keyword_embeddings, candidate_embeddings)
        return {"keyword_embedding_scores": scores}

    # 中文: T12 把 query embedding 与窗口 centroid 比较，并回填到候选项。
    # English: T12 scores query-to-window centroids and max-pools back to candidates.
    @workflow.task("T12_query_window_scores")
    async def query_window_scores(
        embedding_windows: Ref[list, F("T2_build_embedding_windows", "embedding_windows")],
        query_embedding: Ref[list, F("T4_embed_query", "query_embedding")],
        candidate_texts: Ref[list, F("load_input", "candidates")],
    ) -> dict[str, Any]:
        window_vectors = [window["embedding"] for window in embedding_windows]
        window_scores = cosine_scores([query_embedding], window_vectors)
        scores = pool_window_scores(embedding_windows, window_scores, len(candidate_texts))
        return {"query_window_scores": scores}

    # 中文: T13 把关键词 embedding 与窗口 centroid 比较，并回填到候选项。
    # English: T13 scores keyword-to-window centroids and max-pools back to candidates.
    @workflow.task("T13_keyword_window_scores")
    async def keyword_window_scores(
        embedding_windows: Ref[list, F("T2_build_embedding_windows", "embedding_windows")],
        keyword_embeddings: Ref[list, F("T5_embed_keywords", "keyword_embeddings")],
        candidate_texts: Ref[list, F("load_input", "candidates")],
    ) -> dict[str, Any]:
        window_vectors = [window["embedding"] for window in embedding_windows]
        window_scores = cosine_scores(keyword_embeddings, window_vectors)
        scores = pool_window_scores(embedding_windows, window_scores, len(candidate_texts))
        return {"keyword_window_scores": scores}

    # 中文: T14 对所有分数通道归一化，为混排和可视化做准备。
    # English: T14 normalizes all score channels for blending and inspection.
    @workflow.task("T14_normalize_score_channels")
    async def normalize_score_channels(
        keyword_bm25_scores: Ref[list, F("T6_keyword_bm25_scores", "keyword_bm25_scores")],
        query_bm25_scores: Ref[list, F("T7_query_bm25_scores", "query_bm25_scores")],
        query_embedding_scores_value: Ref[list, F("T8_query_embedding_scores", "query_embedding_scores")],
        keyword_embedding_scores_value: Ref[list, F("T9_keyword_embedding_scores", "keyword_embedding_scores")],
        query_window_scores_value: Ref[list, F("T12_query_window_scores", "query_window_scores")],
        keyword_window_scores_value: Ref[list, F("T13_keyword_window_scores", "keyword_window_scores")],
    ) -> dict[str, Any]:
        raw_channels = {
            "keyword_bm25": keyword_bm25_scores,
            "query_bm25": query_bm25_scores,
            "query_embedding": query_embedding_scores_value,
            "keyword_embedding": keyword_embedding_scores_value,
            "query_chunk": query_window_scores_value,
            "keyword_chunk": keyword_window_scores_value,
        }
        normalized = {name: min_max_normalize(scores) for name, scores in raw_channels.items()}
        summary = [
            {
                "channel": name,
                "min": min(scores) if scores else 0.0,
                "max": max(scores) if scores else 0.0,
                "avg": sum(scores) / len(scores) if scores else 0.0,
            }
            for name, scores in raw_channels.items()
        ]
        result_holder["score_summary"] = summary
        return {"normalized_scores": normalized, "score_channel_summary": summary}

    # 中文: T10 混合所有通道，排序、阈值过滤并截断。
    # English: T10 blends score channels, filters by threshold, and truncates top-k.
    @workflow.task("T10_blend_prefilter")
    async def blend_prefilter(
        candidate_texts: Ref[list, F("load_input", "candidates")],
        normalized_scores: Ref[dict, F("T14_normalize_score_channels", "normalized_scores")],
    ) -> dict[str, Any]:
        prefilter_results = blend_results(candidate_texts, normalized_scores, settings)
        result_holder["prefilter_results"] = prefilter_results
        console.log(f"T10 kept {len(prefilter_results)} prefilter candidates")
        return {"prefilter_results": prefilter_results}

    # 中文: T11 只对预筛范围调用 reranker，输出最终排序。
    # English: T11 reranks only the prefiltered range and emits final results.
    @workflow.task("T11_rerank")
    async def rerank_results(
        query_text: Ref[str, F("load_input", "query")],
        prefilter_results: Ref[list, F("T10_blend_prefilter", "prefilter_results")],
    ) -> dict[str, Any]:
        final_results = await call_reranker(query_text, prefilter_results, settings)
        result_holder["final_results"] = final_results
        console.log(f"T11 returned {len(final_results)} reranked candidates")
        return {"final_results": final_results}

    scheduler = workflow.build_scheduler(
        target_tasks=["T11_rerank"],
        config=AstrumConfig(
            concurrency_limit=settings.astrum_concurrency,
            silence=True,
            silence_warnings=True,
            skip_type_check=True,
        ),
    )
    return scheduler, result_holder


async def retrieve(candidates: list[str], query: str) -> list[dict[str, Any]]:
    """中文: 无状态召回入口。English: stateless retrieval entry point."""

    settings = load_settings()
    require_runtime_ready(settings)
    scheduler, result_holder = build_retriever_scheduler(candidates, query, settings)
    render_stage_plan(scheduler)
    report = await scheduler.execute()
    render_execution_report(report)
    render_score_summary(result_holder.get("score_summary", []))
    render_result_table("Prefilter top-k", result_holder.get("prefilter_results", []), "blend_score")
    render_result_table("Rerank final top-k", result_holder.get("final_results", []), "rerank_score")

    if report.execution_state != "completed":
        raise RuntimeError(f"Astrum execution failed: {report.error_summary}")
    return result_holder.get("final_results", [])


def render_config(settings: RetrieverSettings, plan_only: bool) -> None:
    if Table is None or Panel is None:
        console.print(f"Provider: {settings.provider}")
        console.print(f"Completion model: {settings.completion_model}")
        console.print(f"Embedding model: {settings.embedding_model}")
        console.print(f"Rerank model: {settings.rerank_model}")
        console.print(f"API key present: {has_any_api_key(settings)}")
        return

    table = Table(title="Retriever configuration")
    table.add_column("Name")
    table.add_column("Value")
    rows = [
        ("provider", settings.provider),
        ("api_key_env", settings.api_key_env),
        ("api_key_present", "yes" if has_any_api_key(settings) else "no"),
        ("api_base", settings.api_base or "-"),
        ("completion_model", settings.completion_model),
        ("embedding_model", settings.embedding_model),
        ("rerank_model", settings.rerank_model),
        ("astrum_concurrency", str(settings.astrum_concurrency or "unlimited")),
        ("embed_max_concurrency", str(settings.embed_max_concurrency)),
        ("chunk_window_size", str(settings.chunk_window_size)),
        ("chunk_stride", str(settings.chunk_stride)),
        ("plan_only", str(plan_only)),
    ]
    for name, value in rows:
        table.add_row(name, value)
    console.print(Panel(table, title="Stateless Text Retriever", border_style="cyan"))


def render_stage_plan(scheduler: Any) -> None:
    plan = scheduler.get_execute_timeline()
    if Table is None:
        console.print(plan.get_visualization_table())
        return

    table = Table(title="Astrum execution stages")
    table.add_column("Stage", justify="right")
    table.add_column("Start tasks")
    table.add_column("Wait for")
    table.add_column("Parallel view")
    for stage in plan.stages:
        table.add_row(
            str(stage.stage_id),
            ", ".join(stage.start_tasks) or "-",
            ", ".join(stage.wait_for_tasks) or "-",
            ", ".join(stage.parallel_tasks) or "-",
        )
    console.print(table)


def render_execution_report(report: Any) -> None:
    if Table is None:
        console.print(f"state={report.execution_state}, completed={report.successful_tasks}/{report.total_tasks}")
        for stat in report.task_statistics:
            console.print(f"{stat.task_name}: {stat.status} ({stat.duration:.3f}s)")
        return

    table = Table(title="Task execution report")
    table.add_column("Task")
    table.add_column("Stage", justify="right")
    table.add_column("Status")
    table.add_column("Duration", justify="right")
    table.add_column("Attempts", justify="right")
    for stat in report.task_statistics:
        table.add_row(
            stat.task_name,
            str(stat.stage_id),
            stat.status,
            f"{stat.duration:.3f}s",
            str(stat.attempt_count),
        )
    console.print(table)
    console.print(f"state={report.execution_state}, completed={report.successful_tasks}/{report.total_tasks}")
    if report.error_summary:
        console.print("[bold red]Errors[/bold red]")
        for message in report.error_summary:
            console.print(message)


def render_score_summary(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    if Table is None:
        console.print(rows)
        return

    table = Table(title="Score channel summary")
    table.add_column("Channel")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Avg", justify="right")
    for row in rows:
        table.add_row(
            row["channel"],
            f"{row['min']:.4f}",
            f"{row['max']:.4f}",
            f"{row['avg']:.4f}",
        )
    console.print(table)


def render_result_table(title: str, rows: list[dict[str, Any]], score_key: str) -> None:
    if not rows:
        console.print(f"{title}: no results")
        return
    if Table is None:
        console.print(title)
        for row in rows:
            console.print(f"{row['index']}: {row.get(score_key, 0.0):.4f} {row['text']}")
        return

    table = Table(title=title)
    table.add_column("Rank", justify="right")
    table.add_column("Index", justify="right")
    table.add_column("Score", justify="right")
    table.add_column("Text")
    for rank, row in enumerate(rows, start=1):
        text = row["text"].replace("\n", " ")
        if len(text) > 100:
            text = text[:97] + "..."
        table.add_row(str(rank), str(row["index"]), f"{row.get(score_key, 0.0):.4f}", text)
    console.print(table)


def sample_candidates() -> list[str]:
    return [
        "Astrum schedules independent async tasks in the same stage and waits only when dependencies require it.",
        "A product analytics dashboard groups signups, activations, retention, and revenue into daily cohorts.",
        "The refund policy allows customers to request a return within 30 days when the product is unused.",
        "Vector search systems often combine BM25, embeddings, query expansion, and reranking for robust retrieval.",
        "A coffee shop workflow can grind beans, steam milk, and prepare cups in parallel before assembling a drink.",
        "Kubernetes controllers reconcile desired state by watching resources and applying changes continuously.",
        "A stateless retriever should avoid keeping candidate embeddings in process-level caches between requests.",
        "Rerank models can reorder a short candidate list after a cheaper recall stage has reduced the search space.",
        "Sliding windows over neighboring passages help capture context that single isolated chunks may miss.",
        "An invoice approval workflow may fan out to fraud checks, budget checks, and vendor verification.",
        "Embedding cosine similarity is useful for semantic matching even when documents use different wording.",
        "BM25 remains a strong lexical baseline because exact term overlap still carries important retrieval signal.",
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Astrum stateless text retriever example")
    parser.add_argument("--plan-only", action="store_true", help="Build and print the Astrum DAG without API calls.")
    parser.add_argument(
        "--query",
        default="How can I build a stateless retriever with embeddings, BM25, and reranking?",
        help="Query string for the default sample candidate set.",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    settings = load_settings()
    candidates = sample_candidates()
    render_config(settings, args.plan_only)

    if args.plan_only:
        scheduler, _ = build_retriever_scheduler(candidates, args.query, settings)
        render_stage_plan(scheduler)
        missing = missing_packages()
        if missing:
            console.print(
                "\nPlan-only skipped API calls. To run retrieval, install missing packages: "
                + ", ".join(missing)
                + f"\n  {PIP_INSTALL_HINT}"
            )
        return

    try:
        await retrieve(candidates, args.query)
    except RuntimeError as exc:
        console.print(str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    asyncio.run(async_main())
