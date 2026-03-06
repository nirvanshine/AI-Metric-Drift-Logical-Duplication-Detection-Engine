"""
Advanced Metric Clustering Engine (FR4).

Replaces naive exact-string clustering with a hybrid approach:
  - Structural similarity via sqlglot AST (with SSRS fallback)
  - Semantic similarity via sentence-transformers embeddings
  - Context similarity via filter/join/source overlap
  - Hybrid score: S = 0.55*struct + 0.30*sem + 0.15*ctx
  - Top-K candidate retrieval instead of full N×N comparison
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

from .models import MetricCluster, MetricInstance

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Semantic Embedding Generation (Local HuggingFace model)
# ---------------------------------------------------------------------------

_EMBEDDING_MODEL = None


def _get_embedding_model():
    """Lazy-load the sentence-transformers model on first use."""
    global _EMBEDDING_MODEL
    if _EMBEDDING_MODEL is None:
        try:
            from sentence_transformers import SentenceTransformer
            _EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Loaded local embedding model: all-MiniLM-L6-v2")
        except ImportError:
            logger.warning(
                "sentence-transformers not installed. "
                "Semantic similarity will fall back to 0.0. "
                "Install with: pip install sentence-transformers"
            )
            _EMBEDDING_MODEL = None
    return _EMBEDDING_MODEL


def _build_embedding_text(metric: MetricInstance) -> str:
    """Build a rich text document from a MetricInstance for embedding.

    Includes metric_name, expression_signature, grain, filters,
    join_path_signature, and source_objects for full context capture.
    """
    parts = [
        f"Metric: {metric.metric_name}",
        f"Formula: {metric.expression_signature}",
        f"Grain: {metric.grain}",
        f"Filters: {', '.join(metric.filters) if metric.filters else 'none'}",
        f"Joins: {metric.join_path_signature}",
        f"Sources: {', '.join(metric.source_objects) if metric.source_objects else 'none'}",
    ]
    return " | ".join(parts)


def get_metric_embeddings(metrics: List[MetricInstance]) -> np.ndarray:
    """Generate embedding vectors for a list of metrics.

    Returns an (N, D) numpy array where N = len(metrics) and D = embedding dim.
    If the embedding model is unavailable, returns a zero matrix.
    """
    model = _get_embedding_model()
    texts = [_build_embedding_text(m) for m in metrics]

    if model is not None:
        vectors = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
        return vectors
    else:
        # Fallback: zero vectors (semantic score will be 0.0)
        return np.zeros((len(metrics), 1))


# ---------------------------------------------------------------------------
# 2. Structural Similarity (sqlglot AST + SSRS fallback)
# ---------------------------------------------------------------------------

def _try_sqlglot_parse(expr: str) -> Optional[list]:
    """Attempt to parse a SQL expression into sqlglot AST tokens."""
    try:
        import sqlglot
        parsed = sqlglot.parse_one(expr)
        # Flatten the AST into a token list for comparison
        tokens = [node.key for node in parsed.walk()]
        return tokens
    except Exception:
        return None


def _normalize_ssrs_expression(expr: str) -> List[str]:
    """Lightweight SSRS expression normalization and tokenization.

    Handles expressions like:
      =Sum(Fields!SalesAmount.Value)
      =IIf(Fields!Status.Value="Active", 1, 0)

    Normalizes by:
      - Lowercasing
      - Stripping leading '='
      - Extracting function names and field references
      - Removing whitespace noise
    """
    normalized = expr.strip().lower()
    if normalized.startswith("="):
        normalized = normalized[1:]

    # Tokenize: split on non-alphanumeric characters, keep meaningful tokens
    tokens = re.findall(r'[a-z_][a-z0-9_]*(?:\![a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)?', normalized)
    return tokens


def calculate_structural_similarity(expr1: str, expr2: str) -> float:
    """Calculate structural similarity between two metric expressions.

    Primary: Uses sqlglot AST token comparison.
    Fallback: Uses lightweight SSRS tokenization if sqlglot fails.

    Returns a score between 0.0 and 1.0.
    """
    # Try sqlglot first
    tokens1 = _try_sqlglot_parse(expr1)
    tokens2 = _try_sqlglot_parse(expr2)

    if tokens1 is not None and tokens2 is not None:
        return _jaccard_similarity(tokens1, tokens2)

    # Fallback to SSRS normalization
    tokens1 = _normalize_ssrs_expression(expr1)
    tokens2 = _normalize_ssrs_expression(expr2)
    return _jaccard_similarity(tokens1, tokens2)


def _jaccard_similarity(tokens1: list, tokens2: list) -> float:
    """Jaccard similarity between two token lists."""
    set1 = set(tokens1)
    set2 = set(tokens2)
    if not set1 and not set2:
        return 1.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


# ---------------------------------------------------------------------------
# 3. Context Similarity (filter/join/source overlap)
# ---------------------------------------------------------------------------

def calculate_context_similarity(m1: MetricInstance, m2: MetricInstance) -> float:
    """Calculate context similarity based on filters, joins, and source objects.

    Returns a score between 0.0 and 1.0.
    """
    scores = []

    # Filter overlap
    f1 = set(f.lower().strip() for f in m1.filters)
    f2 = set(f.lower().strip() for f in m2.filters)
    if f1 or f2:
        scores.append(_jaccard_similarity(list(f1), list(f2)))
    else:
        scores.append(1.0)  # Both have no filters → identical context

    # Join path comparison (exact match gives 1.0, else 0.0 for simplicity)
    join_score = 1.0 if m1.join_path_signature.lower().strip() == m2.join_path_signature.lower().strip() else 0.0
    scores.append(join_score)

    # Source objects overlap
    s1 = set(s.lower().strip() for s in m1.source_objects)
    s2 = set(s.lower().strip() for s in m2.source_objects)
    if s1 or s2:
        scores.append(_jaccard_similarity(list(s1), list(s2)))
    else:
        scores.append(1.0)

    return sum(scores) / len(scores)


# ---------------------------------------------------------------------------
# 4. Hybrid Scoring Engine
# ---------------------------------------------------------------------------

# Weights as specified by user
W_STRUCTURAL = 0.55
W_SEMANTIC = 0.30
W_CONTEXT = 0.15


def calculate_hybrid_similarity(
    structural_score: float,
    semantic_score: float,
    context_score: float,
) -> float:
    """Compute hybrid similarity score.

    S = 0.55 * structural + 0.30 * semantic + 0.15 * context
    """
    return (
        W_STRUCTURAL * structural_score
        + W_SEMANTIC * semantic_score
        + W_CONTEXT * context_score
    )


def _cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    norm1 = np.linalg.norm(v1)
    norm2 = np.linalg.norm(v2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(np.dot(v1, v2) / (norm1 * norm2))


# ---------------------------------------------------------------------------
# 5. Top-K Candidate Clustering
# ---------------------------------------------------------------------------

SIMILARITY_THRESHOLD = 0.85
TOP_K = 10
# Singletons: no pair comparison is possible, so assign a neutral baseline confidence
SINGLETON_CONFIDENCE = 0.65


def cluster_metrics_advanced(
    metrics: List[MetricInstance],
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    top_k: int = TOP_K,
) -> List[MetricCluster]:
    """Advanced metric clustering using hybrid similarity scoring.

    Instead of a full N×N comparison, this uses a two-phase approach:
      Phase 1 (Blocking): Use semantic embeddings for fast top-K retrieval.
      Phase 2 (Scoring): Apply the full hybrid scoring only on top candidates.

    Returns a list of MetricCluster objects.
    """
    n = len(metrics)
    if n == 0:
        return []

    logger.info(f"Clustering {n} metrics using advanced hybrid similarity...")

    # Phase 0: Generate all embeddings in batch
    embeddings = get_metric_embeddings(metrics)

    # Phase 1: Build semantic similarity matrix for fast candidate retrieval
    # Using numpy vectorized cosine similarity for efficiency
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)  # Avoid division by zero
    normalized = embeddings / norms

    # Full cosine similarity matrix (efficient with numpy dot product)
    sim_matrix = np.dot(normalized, normalized.T)

    # Phase 2: For each metric, retrieve top-K candidates and apply hybrid scoring
    assigned = [False] * n
    clusters: List[MetricCluster] = []
    cluster_idx = 0

    for i in range(n):
        if assigned[i]:
            continue

        # Get top-K semantic neighbors (excluding self)
        semantic_scores = sim_matrix[i]
        candidate_indices = np.argsort(-semantic_scores)  # Descending order

        # Take top_k candidates (skip self at position 0)
        candidates = []
        for j in candidate_indices:
            if j == i or assigned[j]:
                continue
            if len(candidates) >= top_k:
                break
            candidates.append(int(j))

        # Apply full hybrid scoring on candidates
        cluster_members = [metrics[i].metric_id]
        cluster_scores = []

        for j in candidates:
            struct_score = calculate_structural_similarity(
                metrics[i].expression_signature,
                metrics[j].expression_signature,
            )
            sem_score = float(semantic_scores[j])
            ctx_score = calculate_context_similarity(metrics[i], metrics[j])

            hybrid = calculate_hybrid_similarity(struct_score, sem_score, ctx_score)

            if hybrid >= similarity_threshold:
                cluster_members.append(metrics[j].metric_id)
                cluster_scores.append(hybrid)
                assigned[j] = True

        assigned[i] = True
        cluster_idx += 1

        confidence = (
            sum(cluster_scores) / len(cluster_scores) if cluster_scores else SINGLETON_CONFIDENCE
        )

        clusters.append(
            MetricCluster(
                cluster_id=f"CL-{cluster_idx:04d}",
                members=cluster_members,
                confidence_score=round(confidence, 4),
            )
        )

    logger.info(f"Created {len(clusters)} clusters from {n} metrics.")
    return clusters
