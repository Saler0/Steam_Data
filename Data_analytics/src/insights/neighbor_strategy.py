"""Utilities to select competitor neighbors combining intra-cluster and cross-cluster recall."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import math

import numpy as np

# Optional dependency: scikit-learn is part of the project; if unavailable, micro-segmentation falls back gracefully.
try:
    from sklearn.metrics.pairwise import cosine_similarity
except ImportError:  # pragma: no cover
    cosine_similarity = None  # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_unit_vector(vec: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vec))
    if norm == 0.0:
        return vec
    return vec / norm


def _as_float32(vec: Iterable[float]) -> np.ndarray:
    return np.asarray(list(vec), dtype=np.float32)


def _normalize_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _normalize_collection(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = str(value).replace(";", ",").replace("|", ",").split(",")
    result = []
    for item in items:
        token = _normalize_string(item)
        if token:
            result.append(token)
    return result


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = _normalize_string(value)
    return text in {"1", "true", "yes", "y", "t"}


def _coerce_float(value: Any, default: float = math.nan) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return default
    try:
        return float(str(value).strip())
    except Exception:  # noqa: BLE001
        return default


@dataclass
class MetadataView:
    genres: List[str]
    tags: List[str]
    categories: List[str]
    modes: List[str]
    is_free: Optional[bool]
    price: float
    primary_genre: Optional[str]
    name: Optional[str]


class MetadataAccessor:
    """Caches normalized metadata rows for quick reuse."""

    def __init__(self, df):
        self._df = df
        self._cache: Dict[str, MetadataView] = {}

    def get(self, appid: str) -> MetadataView:
        if appid in self._cache:
            return self._cache[appid]
        if self._df.empty:
            view = MetadataView([], [], [], [], None, math.nan, None)
            self._cache[appid] = view
            return view
        sub = self._df[self._df['appid'].astype(str) == appid]
        if sub.empty:
            view = MetadataView([], [], [], [], None, math.nan, None)
            self._cache[appid] = view
            return view
        row = sub.iloc[0]
        genres = _normalize_collection(row.get('genres'))
        tags = _normalize_collection(row.get('tags')) or _normalize_collection(row.get('steamspy_tags'))
        categories = _normalize_collection(row.get('categories'))
        modes = self._infer_modes(categories + _normalize_collection(row.get('modes')))
        is_free = row.get('is_free')
        price = _coerce_float(row.get('price'))
        if math.isnan(price) and row.get('final_price') is not None:
            price = _coerce_float(row.get('final_price')) / 100 if _coerce_float(row.get('final_price')) > 5 else math.nan
        raw_name = row.get('name')
        if isinstance(raw_name, float) and math.isnan(raw_name):
            raw_name = None
        view = MetadataView(
            genres=genres,
            tags=tags,
            categories=categories,
            modes=modes,
            is_free=None if is_free is None else _is_true(is_free),
            price=price,
            primary_genre=genres[0] if genres else None,
            name=None if raw_name is None else str(raw_name),
        )
        self._cache[appid] = view
        return view

    @staticmethod
    def _infer_modes(tokens: Sequence[str]) -> List[str]:
        if not tokens:
            return []
        modes = set()
        for raw in tokens:
            token = _normalize_string(raw)
            if not token:
                continue
            if 'pvp' in token:
                modes.add('pvp')
            if 'pve' in token or 'player vs environment' in token:
                modes.add('pve')
            if 'coop' in token or 'co-op' in token or 'cooperative' in token:
                modes.add('coop')
            if 'multiplayer' in token:
                modes.add('multiplayer')
            if 'single' in token:
                modes.add('singleplayer')
        return sorted(modes)


@dataclass
class EmbeddingIndex:
    ids: List[str]
    matrix: np.ndarray
    id_to_idx: Dict[str, int]

    @classmethod
    def from_dataframe(cls, df) -> 'EmbeddingIndex':
        if df.empty:
            return cls([], np.zeros((0, 0), dtype=np.float32), {})
        ids = df['appid'].astype(str).tolist()
        vectors = [_as_float32(vec if isinstance(vec, (list, tuple, np.ndarray)) else []) for vec in df['embedding']]
        if not vectors:
            return cls([], np.zeros((0, 0), dtype=np.float32), {})
        matrix = np.vstack(vectors).astype(np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0.0] = 1.0
        matrix = matrix / norms
        id_to_idx = {aid: idx for idx, aid in enumerate(ids)}
        return cls(ids, matrix, id_to_idx)

    def similarity_vector(self, query: np.ndarray) -> np.ndarray:
        if self.matrix.size == 0:
            return np.zeros(0, dtype=np.float32)
        q = _ensure_unit_vector(query.astype(np.float32))
        return self.matrix @ q

    def vector_for(self, appid: str) -> Optional[np.ndarray]:
        idx = self.id_to_idx.get(appid)
        if idx is None:
            return None
        return self.matrix[idx]


DEFAULT_CONFIG: Dict[str, Any] = {
    'target_total': 40,
    'same_cluster_only': True,
    'allow_cross_cluster': False,
    'k_in': 50,
    'k_out': 20,
    'min_similarity_in': 0.82,
    'min_similarity_out': 0.80,
    'max_out_ratio': 0.30,
    'business_filters': {
        'genre_match': True,
        'min_shared_tags': 2,
        'monetization_compatible': True,
        'mode_match': [],
        'price_tolerance_ratio': 2.5,
    },
    'rerank': {
        'alpha_cosine': 1.0,
        'beta_cluster_penalty': 0.15,
        'gamma_tag_overlap': 0.10,
        'delta_monetization': 0.05,
        'epsilon_mode_match': 0.05,
    },
    'microsegmentation': {
        'enabled': True,
        'cosine_distance_threshold': 0.25,
        'min_neighbors': 10,
    },
    'dilution_checks': {
        'silhouette_proxy_threshold': 0.15,
        'min_in_neighbors': 15,
        'min_avg_similarity': 0.80,
    },
}


def _merge_configs(user_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = {**DEFAULT_CONFIG}
    for key in ['business_filters', 'rerank', 'microsegmentation', 'dilution_checks']:
        cfg[key] = {**DEFAULT_CONFIG[key], **user_cfg.get(key, {})} if user_cfg.get(key) else DEFAULT_CONFIG[key].copy()
    for key, value in user_cfg.items():
        if key not in cfg:
            cfg[key] = value
        elif not isinstance(value, dict):
            cfg[key] = value
    return cfg


def _prepare_medoids(raw: Optional[Dict[str, Iterable[float]]]) -> Dict[str, np.ndarray]:
    if not raw:
        return {}
    medoids = {}
    for cid, vec in raw.items():
        try:
            medoids[str(cid)] = _ensure_unit_vector(_as_float32(vec))
        except Exception:  # noqa: BLE001
            continue
    return medoids


def _business_filters_pass(candidate: MetadataView, query: MetadataView, cfg: Dict[str, Any]) -> bool:
    if cfg.get('genre_match') and query.primary_genre and candidate.primary_genre:
        if candidate.primary_genre != query.primary_genre:
            return False
    min_shared_tags = cfg.get('min_shared_tags') or 0
    if min_shared_tags:
        if len(set(candidate.tags) & set(query.tags)) < int(min_shared_tags):
            return False
    if cfg.get('monetization_compatible'):
        cand_free = candidate.is_free
        query_free = query.is_free
        if cand_free is not None and query_free is not None and cand_free != query_free:
            return False
        if (cand_free is False or cand_free is None) and (query_free is False or query_free is None):
            price_tol = float(cfg.get('price_tolerance_ratio') or 2.5)
            if not math.isnan(candidate.price) and not math.isnan(query.price) and query.price > 0:
                ratio = candidate.price / query.price
                if ratio > price_tol or ratio < (1 / price_tol):
                    return False
    desired_modes = [_normalize_string(m) for m in cfg.get('mode_match') or [] if _normalize_string(m)]
    if desired_modes:
        query_modes = set(query.modes)
        candidate_modes = set(candidate.modes)
        relevant = {m for m in desired_modes if m in query_modes}
        if relevant and candidate_modes.isdisjoint(relevant):
            return False
    return True


def _compute_cluster_distance(query_cluster: Optional[int], candidate_cluster: Optional[int], medoids: Dict[str, np.ndarray]) -> float:
    if query_cluster is None or candidate_cluster is None:
        return 0.0
    if str(query_cluster) not in medoids or str(candidate_cluster) not in medoids:
        return 0.0
    q_vec = medoids[str(query_cluster)]
    c_vec = medoids[str(candidate_cluster)]
    similarity = float(np.dot(q_vec, c_vec))
    similarity = max(min(similarity, 1.0), -1.0)
    return 1.0 - similarity


def _microsegment_neighbors(query_vec: np.ndarray, candidates: List[Dict[str, Any]], threshold: float, min_neighbors: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not candidates:
        return candidates, {'applied': False}
    if len(candidates) < max(2, min_neighbors):
        return candidates, {'applied': False, 'reason': 'not_enough_neighbors'}
    if cosine_similarity is None:
        return candidates, {'applied': False, 'reason': 'sklearn_missing'}
    vectors = [query_vec] + [cand['vector'] for cand in candidates]
    matrix = np.vstack(vectors)
    sims = cosine_similarity(matrix)
    adjacency = sims >= (1.0 - threshold)
    visited = set()
    component = set()
    stack = [0]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        component.add(node)
        neighbors = np.where(adjacency[node])[0]
        for neigh in neighbors:
            if neigh not in visited:
                stack.append(int(neigh))
    # Remove query node (index 0)
    keep_indices = {idx - 1 for idx in component if idx != 0 and (idx - 1) < len(candidates)}
    if not keep_indices:
        return candidates, {'applied': False, 'reason': 'component_empty'}
    filtered = [cand for idx, cand in enumerate(candidates) if idx in keep_indices]
    return filtered, {
        'applied': True,
        'threshold': threshold,
        'original': len(candidates),
        'kept': len(filtered),
    }


def _silhouette_proxy(in_sims: Sequence[float], cross_sims: Sequence[float]) -> float:
    if not in_sims:
        return -1.0
    mean_in = float(np.mean(in_sims))
    if not cross_sims:
        return mean_in
    mean_out = float(np.mean(cross_sims))
    denom = max(mean_in, mean_out, 1e-6)
    return (mean_in - mean_out) / denom


def select_competitor_neighbors(
    query_vec: np.ndarray,
    query_metadata: Dict[str, Any],
    query_appid: Optional[str],
    query_cluster_id: Optional[int],
    embeddings: EmbeddingIndex,
    clusters_df,
    metadata_df,
    medoids: Optional[Dict[str, Iterable[float]]] = None,
    user_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Return a ranked list of competitor neighbors and diagnostics."""

    cfg = _merge_configs(user_cfg or {})
    medoid_vectors = _prepare_medoids(medoids)
    query_vec = _ensure_unit_vector(query_vec.astype(np.float32))

    cluster_map = {}
    if not clusters_df.empty:
        cluster_map = {
            str(row['appid']): int(row['cluster_id'])
            for _, row in clusters_df[['appid', 'cluster_id']].dropna().iterrows()
        }

    metadata_accessor = MetadataAccessor(metadata_df)

    query_genres = _normalize_collection(query_metadata.get('genres'))
    query_tags = _normalize_collection(query_metadata.get('tags'))
    query_categories = _normalize_collection(query_metadata.get('categories'))
    query_modes = MetadataAccessor._infer_modes(query_categories + _normalize_collection(query_metadata.get('modes')))
    query_price = _coerce_float(query_metadata.get('price'))
    query_is_free = query_metadata.get('is_free') if isinstance(query_metadata.get('is_free'), bool) else None
    if query_is_free is None and not math.isnan(query_price):
        query_is_free = query_price == 0
    query_view = MetadataView(
        genres=query_genres,
        tags=query_tags,
        categories=query_categories,
        modes=query_modes,
        is_free=query_is_free,
        price=query_price,
        primary_genre=query_genres[0] if query_genres else None,
        name=query_metadata.get('name'),
    )

    sims = embeddings.similarity_vector(query_vec)
    ids = embeddings.ids

    ordered = np.argsort(-sims)
    min_sim_in = float(cfg.get('min_similarity_in', 0.0))
    min_sim_out = float(cfg.get('min_similarity_out', 0.0))
    k_in = int(cfg.get('k_in', 50) or 0)
    k_out = int(cfg.get('k_out', 20) or 0)

    in_raw: List[Dict[str, Any]] = []
    out_raw: List[Dict[str, Any]] = []

    for idx in ordered:
        aid = ids[idx]
        if query_appid is not None and aid == query_appid:
            continue
        sim = float(sims[idx])
        candidate_cluster = cluster_map.get(aid)
        candidate_meta = metadata_accessor.get(aid)
        vector = embeddings.matrix[idx]
        entry = {
            'appid': aid,
            'similarity': sim,
            'cluster_id': candidate_cluster,
            'vector': vector,
            'meta': candidate_meta,
        }
        if candidate_cluster is not None and query_cluster_id is not None and candidate_cluster == query_cluster_id:
            if sim >= min_sim_in:
                in_raw.append(entry)
                if len(in_raw) >= max(k_in * 5, k_in + 20):
                    # Enough intra-cluster candidates gathered
                    pass
        else:
            if sim >= min_sim_out:
                out_raw.append(entry)
                if len(out_raw) >= max(k_out * 5, k_out + 50):
                    pass
        if len(in_raw) >= max(k_in * 5, k_in + 50) and len(out_raw) >= max(k_out * 5, k_out + 200):
            break

    in_candidates = in_raw[:k_in] if k_in else in_raw

    micro_cfg = cfg.get('microsegmentation', {})
    micro_info = {'applied': False}
    if micro_cfg.get('enabled', True) and in_candidates:
        threshold = float(micro_cfg.get('cosine_distance_threshold', 0.25))
        min_neighbors = int(micro_cfg.get('min_neighbors', 10))
        in_candidates, micro_info = _microsegment_neighbors(query_vec, in_candidates, threshold, min_neighbors)

    dilution_cfg = cfg.get('dilution_checks', {})
    silhouette_threshold = float(dilution_cfg.get('silhouette_proxy_threshold', 0.15))
    min_in_neighbors = int(dilution_cfg.get('min_in_neighbors', 15))
    min_avg_similarity = float(dilution_cfg.get('min_avg_similarity', 0.8))
    silhouette_val = _silhouette_proxy(
        [cand['similarity'] for cand in in_raw[:min(20, len(in_raw))]],
        [cand['similarity'] for cand in out_raw[:min(20, len(out_raw))]],
    )
    avg_in_sim = float(np.mean([cand['similarity'] for cand in in_candidates])) if in_candidates else 0.0
    diluted = (
        len(in_candidates) < min_in_neighbors
        or avg_in_sim < min_avg_similarity
        or silhouette_val < silhouette_threshold
    )

    allow_cross = bool(cfg.get('allow_cross_cluster')) or diluted

    out_candidates: List[Dict[str, Any]] = []
    if allow_cross and out_raw:
        business_cfg = cfg.get('business_filters', {})
        for cand in out_raw:
            if not _business_filters_pass(cand['meta'], query_view, business_cfg):
                continue
            out_candidates.append(cand)
            if len(out_candidates) >= k_out:
                break

    total_target = int(cfg.get('target_total', 40))
    max_out_ratio = float(cfg.get('max_out_ratio', 0.3))
    max_cross_allowed = max(0, int(round(total_target * max_out_ratio))) if allow_cross else 0

    rerank_cfg = cfg.get('rerank', {})
    alpha = float(rerank_cfg.get('alpha_cosine', 1.0))
    beta = float(rerank_cfg.get('beta_cluster_penalty', 0.15))
    gamma = float(rerank_cfg.get('gamma_tag_overlap', 0.1))
    delta = float(rerank_cfg.get('delta_monetization', 0.05))
    epsilon = float(rerank_cfg.get('epsilon_mode_match', 0.05))

    def compute_score(entry: Dict[str, Any]) -> float:
        similarity = entry['similarity']
        candidate_meta = entry['meta']
        candidate_cluster = entry.get('cluster_id')
        cluster_penalty = _compute_cluster_distance(query_cluster_id, candidate_cluster, medoid_vectors)
        tag_overlap = len(set(candidate_meta.tags) & set(query_view.tags))
        monetization_bonus = 1.0 if candidate_meta.is_free == query_view.is_free else 0.0
        mode_overlap = len(set(candidate_meta.modes) & set(query_view.modes))
        return (
            alpha * similarity
            - beta * cluster_penalty
            + gamma * tag_overlap
            + delta * monetization_bonus
            + epsilon * mode_overlap
        )

    for cand in in_candidates:
        cand['source'] = 'intra'
        cand['score'] = compute_score(cand)
    for cand in out_candidates:
        cand['source'] = 'cross'
        cand['score'] = compute_score(cand)

    pool = in_candidates + out_candidates
    pool.sort(key=lambda item: (item['score'], item['similarity']), reverse=True)

    selected: List[Dict[str, Any]] = []
    cross_selected = 0
    remaining_in_pool = [cand for cand in pool if cand['source'] == 'intra']
    remaining_cross_pool = [cand for cand in pool if cand['source'] == 'cross']

    for cand in pool:
        if cand['source'] == 'cross' and cross_selected >= max_cross_allowed and len(selected) < total_target:
            continue
        selected.append(cand)
        if cand['source'] == 'cross':
            cross_selected += 1
        if len(selected) >= total_target:
            break

    if len(selected) < total_target:
        # Attempt to top up with remaining candidates irrespective of ratio
        remaining = [cand for cand in pool if cand not in selected]
        for cand in remaining:
            selected.append(cand)
            if len(selected) >= total_target:
                break

    # Deduplicate preserving order
    final_neighbors: List[Dict[str, Any]] = []
    seen_ids = set()
    for cand in selected:
        aid = cand['appid']
        if aid in seen_ids:
            continue
        seen_ids.add(aid)
        final_neighbors.append(cand)
        if len(final_neighbors) >= total_target:
            break

    # Drop helper fields before returning
    output_neighbors: List[Dict[str, Any]] = []
    for cand in final_neighbors:
        output_neighbors.append({
            'appid': cand['appid'],
            'similarity': float(cand['similarity']),
            'cluster_id': cand.get('cluster_id'),
            'name': cand['meta'].name,
            'source': cand.get('source'),
            'score': float(cand.get('score', 0.0)),
        })

    diagnostics = {
        'target_total': total_target,
        'intra_candidates': len(in_candidates),
        'cross_candidates': len(out_candidates) if allow_cross else 0,
        'selected': len(output_neighbors),
        'cross_selected': sum(1 for cand in output_neighbors if cand.get('source') == 'cross'),
        'diluted': diluted,
        'silhouette_proxy': silhouette_val,
        'average_in_similarity': avg_in_sim,
        'microsegmentation': micro_info,
        'allow_cross_effective': allow_cross,
    }

    return output_neighbors, diagnostics


__all__ = [
    'EmbeddingIndex',
    'MetadataAccessor',
    'select_competitor_neighbors',
    'DEFAULT_CONFIG',
]
