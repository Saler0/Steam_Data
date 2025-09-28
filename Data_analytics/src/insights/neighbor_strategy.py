"""Utilities to select competitor neighbors combining intra-cluster and cross-cluster recall."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import math
import json
import re
from pathlib import Path

import numpy as np

# Optional dependency: scikit-learn is part of the project; if unavailable, micro-segmentation falls back gracefully.
try:
    from sklearn.metrics.pairwise import cosine_similarity
except ImportError:  # pragma: no cover
    cosine_similarity = None  # type: ignore

try:
    from src.utils.faiss_utils import load_faiss_index, search_faiss_index
except ImportError:  # pragma: no cover
    load_faiss_index = None  # type: ignore
    search_faiss_index = None  # type: ignore


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

def _vector_from_value(value: Any) -> Optional[np.ndarray]:
    if isinstance(value, np.ndarray):
        arr = value.astype(np.float32)
    elif isinstance(value, list):
        arr = np.asarray(value, dtype=np.float32)
    elif isinstance(value, str):
        try:
            arr = np.asarray(json.loads(value), dtype=np.float32)
        except Exception:
            return None
    else:
        return None
    if arr.size == 0:
        return None
    return _ensure_unit_vector(arr)


NAME_STOPWORDS: Set[str] = {
    'a', 'an', 'the', 'and', 'or', 'for', 'of', 'in', 'on', 'at', 'to', 'with', 'from', 'by',
    'vs', 'vs.', 'edition', 'edicion', 'game', 'collection', 'pack', 'bundle', 'redux',
    'la', 'el', 'los', 'las', 'lo', 'una', 'uno', 'unos', 'unas', 'un', 'y', 'en', 'con',
    'para', 'por', 'del', 'de', 'al', 'remastered', 'definitive', 'ultimate'
}

DEFAULT_NAME_PENALTY_CFG: Dict[str, Any] = {
    'w_unigram': 1.0,
    'w_ngram': 1.5,
    'w_prefix': 1.0,
    'ngram_sizes': (2, 3),
    'min_prefix_length': 6,
    'generic_whitelist': (
        'zombie', 'zombies', 'survival', 'survivor', 'survivors', 'horde', 'fps', 'coop',
        'co-op', 'online', 'protocol', 'arena', 'shooter', 'battle'
    ),
}


def _light_stem(token: str) -> str:
    if len(token) <= 3:
        return token
    if token.endswith('ies') and len(token) > 4:
        return token[:-3] + 'y'
    if token.endswith('ing') and len(token) > 5:
        return token[:-3]
    if token.endswith('ed') and len(token) > 4:
        return token[:-2]
    if token.endswith('es') and len(token) > 3:
        return token[:-2]
    if token.endswith('s') and len(token) > 3:
        return token[:-1]
    return token


def _normalize_name_text(text: str) -> str:
    cleaned = text.replace('co-op', 'coop').replace('co op', 'coop')
    cleaned = re.sub(r"\s+", ' ', cleaned)
    return cleaned.strip()


def _prepare_name_penalty_cfg(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = {
        'w_unigram': float(DEFAULT_NAME_PENALTY_CFG['w_unigram']),
        'w_ngram': float(DEFAULT_NAME_PENALTY_CFG['w_ngram']),
        'w_prefix': float(DEFAULT_NAME_PENALTY_CFG['w_prefix']),
        'min_prefix_length': int(DEFAULT_NAME_PENALTY_CFG['min_prefix_length']),
        'ngram_sizes': tuple(DEFAULT_NAME_PENALTY_CFG['ngram_sizes']),
    }
    raw_whitelist: List[str] = list(DEFAULT_NAME_PENALTY_CFG['generic_whitelist'])
    if user_cfg:
        if 'w_unigram' in user_cfg:
            cfg['w_unigram'] = float(user_cfg['w_unigram'])
        if 'w_ngram' in user_cfg:
            cfg['w_ngram'] = float(user_cfg['w_ngram'])
        if 'w_prefix' in user_cfg:
            cfg['w_prefix'] = float(user_cfg['w_prefix'])
        if 'min_prefix_length' in user_cfg:
            cfg['min_prefix_length'] = int(user_cfg['min_prefix_length'])
        if 'ngram_sizes' in user_cfg and user_cfg['ngram_sizes']:
            values = []
            for item in user_cfg['ngram_sizes']:
                try:
                    num = int(item)
                except Exception:
                    continue
                if num > 1:
                    values.append(num)
            if values:
                cfg['ngram_sizes'] = tuple(sorted(set(values)))
        if 'generic_whitelist' in user_cfg and user_cfg['generic_whitelist']:
            raw_whitelist.extend(str(item) for item in user_cfg['generic_whitelist'])
    if not cfg['ngram_sizes']:
        cfg['ngram_sizes'] = (2, 3)
    whitelist: Set[str] = set()
    for term in raw_whitelist:
        normalized = _normalize_string(term)
        if not normalized:
            continue
        normalized = _normalize_name_text(normalized)
        token = re.sub(r'[^a-z0-9]+', '', normalized)
        token = _light_stem(token)
        if token:
            whitelist.add(token)
    cfg['generic_whitelist'] = whitelist
    return cfg


def _build_name_signature(name: Optional[str], cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = '' if name is None else str(name)
    normalized_base = _normalize_string(raw)
    if not normalized_base:
        return {
            'raw': raw,
            'tokens': (),
            'token_set': set(),
            'ngram_map': {},
            'prefix_plain': '',
            'leading_fragment': '',
        }
    normalized_base = _normalize_name_text(normalized_base)
    tokens: List[str] = []
    whitelist: Set[str] = cfg.get('generic_whitelist', set())
    for token in re.findall(r'[a-z0-9]+', normalized_base):
        if not token:
            continue
        stemmed = _light_stem(token)
        if len(stemmed) <= 1:
            continue
        if stemmed in NAME_STOPWORDS or stemmed in whitelist:
            continue
        tokens.append(stemmed)
    token_set = set(tokens)
    ngram_map: Dict[int, Set[str]] = {}
    for size in cfg.get('ngram_sizes', (2, 3)):
        try:
            n = int(size)
        except Exception:
            continue
        if n <= 1 or len(tokens) < n:
            continue
        grams = {' '.join(tokens[idx: idx + n]) for idx in range(len(tokens) - n + 1)}
        if grams:
            ngram_map[n] = grams
    prefix_plain = re.sub(r'[^a-z0-9]+', '', normalized_base)
    fragment_source = re.split(r'[:\-]', normalized_base, maxsplit=1)[0]
    fragment_tokens = []
    for token in re.findall(r'[a-z0-9]+', fragment_source):
        stemmed = _light_stem(token)
        if stemmed in NAME_STOPWORDS or stemmed in whitelist:
            continue
        fragment_tokens.append(stemmed)
    leading_fragment = ''.join(fragment_tokens)
    return {
        'raw': raw,
        'tokens': tuple(tokens),
        'token_set': token_set,
        'ngram_map': ngram_map,
        'prefix_plain': prefix_plain,
        'leading_fragment': leading_fragment,
    }


def _longest_common_prefix(a: str, b: str) -> str:
    max_len = min(len(a), len(b))
    idx = 0
    while idx < max_len and a[idx] == b[idx]:
        idx += 1
    return a[:idx]


def _has_prefix_match(query_sig: Dict[str, Any], candidate_sig: Dict[str, Any], min_prefix_length: int) -> bool:
    if min_prefix_length <= 0:
        min_prefix_length = 1
    q_plain = query_sig.get('prefix_plain', '')
    c_plain = candidate_sig.get('prefix_plain', '')
    if q_plain and c_plain:
        shared = _longest_common_prefix(q_plain, c_plain)
        if len(shared) >= min_prefix_length:
            return True
    q_fragment = query_sig.get('leading_fragment', '')
    c_fragment = candidate_sig.get('leading_fragment', '')
    if q_fragment and c_fragment and q_fragment == c_fragment and len(q_fragment) >= max(3, min_prefix_length // 2):
        return True
    return False


def _compute_name_penalty(query_sig: Dict[str, Any], candidate_sig: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    if not query_sig:
        return 0.0
    query_tokens: Set[str] = query_sig.get('token_set', set())
    candidate_tokens: Set[str] = candidate_sig.get('token_set', set())
    if query_tokens:
        overlap_unigram = len(query_tokens & candidate_tokens) / float(len(query_tokens))
    else:
        overlap_unigram = 0.0
    ngram_overlap = 0.0
    total_query_ngrams = 0
    query_map = query_sig.get('ngram_map', {}) or {}
    candidate_map = candidate_sig.get('ngram_map', {}) or {}
    for size in cfg.get('ngram_sizes', (2, 3)):
        try:
            n = int(size)
        except Exception:
            continue
        q_set = query_map.get(n)
        if not q_set:
            continue
        total_query_ngrams += len(q_set)
        c_set = candidate_map.get(n)
        if c_set:
            ngram_overlap += len(q_set & c_set)
    if total_query_ngrams > 0:
        overlap_ngram = ngram_overlap / float(total_query_ngrams)
    else:
        overlap_ngram = 0.0
    prefix_match = 1.0 if _has_prefix_match(query_sig, candidate_sig, int(cfg.get('min_prefix_length', 6))) else 0.0
    penalty = (
        float(cfg.get('w_unigram', 1.0)) * overlap_unigram
        + float(cfg.get('w_ngram', 1.0)) * overlap_ngram
        + float(cfg.get('w_prefix', 1.0)) * prefix_match
    )
    return float(penalty)



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
        self._cache: Dict[str, MetadataView] = {}
        self._lookup: Dict[str, Dict[str, Any]] = {}
        if df is None or getattr(df, 'empty', True):
            return
        if 'appid' not in df.columns:
            return
        tmp = df.copy()
        tmp['appid'] = tmp['appid'].astype(str)
        self._lookup = {row['appid']: row.to_dict() for _, row in tmp.iterrows()}

    def get(self, appid: str) -> MetadataView:
        if appid in self._cache:
            return self._cache[appid]
        record = self._lookup.get(appid)
        if not record:
            view = MetadataView([], [], [], [], None, math.nan, None)
            self._cache[appid] = view
            return view
        genres = _normalize_collection(record.get('genres'))
        tags = _normalize_collection(record.get('tags')) or _normalize_collection(record.get('steamspy_tags'))
        categories = _normalize_collection(record.get('categories'))
        modes = self._infer_modes(categories + _normalize_collection(record.get('modes')))
        is_free = record.get('is_free')
        price = _coerce_float(record.get('price'))
        if math.isnan(price) and record.get('final_price') is not None:
            final_price_val = _coerce_float(record.get('final_price'))
            price = final_price_val / 100 if final_price_val > 5 else math.nan
        raw_name = record.get('name')
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
    faiss_index: Optional[Any] = None
    vector_map: Dict[str, np.ndarray] | None = None

    def __post_init__(self) -> None:
        if self.vector_map is None:
            self.vector_map = {}

    @property
    def uses_faiss(self) -> bool:
        return self.faiss_index is not None and search_faiss_index is not None

    def __len__(self) -> int:
        return len(self.ids)

    @classmethod
    def from_dataframe(
        cls,
        df,
        faiss_index_path: Optional[str] = None,
        faiss_ids_path: Optional[str] = None,
    ) -> 'EmbeddingIndex':
        if df.empty:
            return cls([], np.zeros((0, 0), dtype=np.float32), {})

        tmp = df.copy()
        tmp['appid'] = tmp['appid'].astype(str)
        vector_map: Dict[str, np.ndarray] = {}
        for _, row in tmp.iterrows():
            vec = _vector_from_value(row.get('embedding'))
            if vec is None:
                continue
            vector_map[row['appid']] = vec

        if not vector_map:
            return cls([], np.zeros((0, 0), dtype=np.float32), {})

        faiss_index = None
        ids: List[str] = list(vector_map.keys())
        idx_path = Path(faiss_index_path) if faiss_index_path else None
        ids_path = Path(faiss_ids_path) if faiss_ids_path else None
        if idx_path and ids_path and load_faiss_index is not None and search_faiss_index is not None:
            if idx_path.exists() and ids_path.exists():
                try:
                    faiss_index = load_faiss_index(str(idx_path))
                    faiss_ids = json.loads(ids_path.read_text(encoding="utf-8"))
                    ids = [str(item) for item in faiss_ids]
                    if faiss_index.ntotal != len(ids):
                        print(f"[WARN] FAISS index size ({faiss_index.ntotal}) no coincide con ids ({len(ids)}); ignorando base FAISS.")
                        faiss_index = None
                except Exception as exc:
                    print(f"[WARN] No se pudo cargar FAISS index '{idx_path}': {exc}")
                    faiss_index = None
            else:
                missing = []
                if idx_path and not idx_path.exists():
                    missing.append(str(idx_path))
                if ids_path and not ids_path.exists():
                    missing.append(str(ids_path))
                if missing:
                    joined = ', '.join(missing)
                    print(f"[WARN] FAISS solicitado pero faltan archivos: {joined}")
        first_vec = next(iter(vector_map.values()))
        dim = int(first_vec.shape[0])
        matrix = np.zeros((len(ids), dim), dtype=np.float32)

        for idx, aid in enumerate(ids):
            vec = vector_map.get(aid)
            if vec is None and faiss_index is not None:
                try:
                    vec = np.asarray(faiss_index.reconstruct(idx), dtype=np.float32)
                    vec = _ensure_unit_vector(vec)
                    vector_map[aid] = vec
                except Exception:
                    vec = None
            if vec is not None:
                matrix[idx] = vec

        id_to_idx = {aid: idx for idx, aid in enumerate(ids)}
        return cls(ids, matrix, id_to_idx, faiss_index=faiss_index, vector_map=vector_map)

    def similarity_vector(self, query: np.ndarray) -> np.ndarray:
        if self.matrix.size == 0:
            return np.zeros(0, dtype=np.float32)
        q = _ensure_unit_vector(query.astype(np.float32))
        return self.matrix @ q

    def top_similar(self, query: np.ndarray, top_k: int) -> List[Tuple[str, float, int]]:
        if not self.ids:
            return []
        top_k = max(0, min(top_k, len(self.ids)))
        if top_k == 0:
            return []
        q = _ensure_unit_vector(query.astype(np.float32))
        if self.uses_faiss:
            distances, indices = search_faiss_index(self.faiss_index, q.reshape(1, -1), top_k)
            results: List[Tuple[str, float, int]] = []
            for dist, idx in zip(distances[0], indices[0]):
                if idx < 0 or idx >= len(self.ids):
                    continue
                results.append((self.ids[idx], float(dist), int(idx)))
            return results
        sims = self.matrix @ q
        order = np.argsort(-sims)[:top_k]
        return [(self.ids[i], float(sims[i]), int(i)) for i in order]

    def vector_for(self, appid: str) -> Optional[np.ndarray]:
        idx = self.id_to_idx.get(appid)
        if idx is None:
            return None
        vec = None
        if self.vector_map:
            vec = self.vector_map.get(appid)
        if vec is None and self.matrix.size and idx < self.matrix.shape[0]:
            row = self.matrix[idx]
            if row.size and np.any(row):
                vec = row.astype(np.float32)
                if self.vector_map is not None:
                    self.vector_map[appid] = vec
        if vec is None and self.uses_faiss:
            try:
                arr = np.asarray(self.faiss_index.reconstruct(idx), dtype=np.float32)
                vec = _ensure_unit_vector(arr)
                if self.vector_map is not None:
                    self.vector_map[appid] = vec
                if self.matrix.size and idx < self.matrix.shape[0]:
                    self.matrix[idx] = vec
            except Exception:
                return None
        return vec
DEFAULT_CONFIG: Dict[str, Any] = {
    'target_total': 40,
    'same_cluster_only': True,
    'allow_cross_cluster': False,
    'k_in': 50,
    'k_out': 20,
    'min_similarity_in': 0.82,
    'min_similarity_out': 0.80,
    'max_out_ratio': 0.30,
    'max_cross_clusters': None,
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
        'zeta_name_penalty': 0.20,
        'name_penalty': DEFAULT_NAME_PENALTY_CFG,
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
        default_section = DEFAULT_CONFIG[key]
        override_section = user_cfg.get(key) if isinstance(user_cfg.get(key), dict) else None
        merged = {**default_section}
        if override_section:
            merged.update(override_section)
        if key == 'rerank':
            name_override = None
            if override_section and isinstance(override_section.get('name_penalty'), dict):
                name_override = override_section['name_penalty']
            merged['name_penalty'] = _prepare_name_penalty_cfg(name_override)
        cfg[key] = merged
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


def _closest_clusters(query_cluster: Optional[int], medoids: Dict[str, np.ndarray], limit: int) -> set[int]:
    if query_cluster is None or limit <= 0:
        return set()
    key = str(query_cluster)
    if key not in medoids:
        return set()
    query_vec = medoids[key]
    scores: List[Tuple[int, float]] = []
    for cid_str, vec in medoids.items():
        if cid_str == key:
            continue
        similarity = float(np.dot(query_vec, vec))
        scores.append((int(cid_str), similarity))
    scores.sort(key=lambda item: item[1], reverse=True)
    top = scores[:limit]
    return {cid for cid, _ in top}


def _microsegment_neighbors(query_vec: np.ndarray, candidates: List[Dict[str, Any]], threshold: float, min_neighbors: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    valid_candidates = [cand for cand in candidates if isinstance(cand.get('vector'), np.ndarray) and getattr(cand.get('vector'), 'size', 0) > 0]
    if len(valid_candidates) < len(candidates):
        if not valid_candidates:
            return candidates, {'applied': False, 'reason': 'missing_vectors'}
        candidates = valid_candidates
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

    rerank_cfg = cfg.get('rerank', {})
    name_penalty_cfg = rerank_cfg.get('name_penalty', {})
    zeta_name_penalty = float(rerank_cfg.get('zeta_name_penalty', 0.0))
    query_name_signature = _build_name_signature(query_view.name, name_penalty_cfg) if zeta_name_penalty > 0.0 else {}
    use_name_penalty = zeta_name_penalty > 0.0 and isinstance(query_name_signature, dict)

    total_embeddings = len(embeddings.ids)
    if total_embeddings == 0:
        return [], {
            'target_total': int(cfg.get('target_total', 0)),
            'intra_candidates': 0,
            'cross_candidates': 0,
            'selected': 0,
            'cross_selected': 0,
            'diluted': False,
            'silhouette_proxy': 0.0,
            'average_in_similarity': 0.0,
            'microsegmentation': {'applied': False},
            'allow_cross_effective': bool(cfg.get('allow_cross_cluster')),
            'scanned': 0,
            'gathered_in': 0,
            'gathered_cross': 0,
            'fallback_used': False,
            'faiss_used': embeddings.uses_faiss,
        }

    min_sim_in = float(cfg.get('min_similarity_in', 0.0))
    min_sim_out = float(cfg.get('min_similarity_out', 0.0))
    k_in = int(cfg.get('k_in', 50) or 0)
    k_out = int(cfg.get('k_out', 20) or 0)
    total_target = int(cfg.get('target_total', 40))
    business_cfg = cfg.get('business_filters', {})

    gather_in_limit = max(total_target * 3, (k_in or total_target) * 3, (k_in or total_target) + 40)
    gather_out_limit = max(total_target * 3, (k_out or total_target) * 3, (k_out or total_target) + 80)
    base_max_scan = max(gather_in_limit + gather_out_limit, total_target * 25, 2000)
    max_scan = min(base_max_scan, total_embeddings) if embeddings.uses_faiss else total_embeddings
    if max_scan <= 0:
        max_scan = total_embeddings
    gather_size = min(max_scan, max(total_target * 4, gather_in_limit + gather_out_limit, 128))
    if gather_size <= 0:
        gather_size = max_scan

    max_cross_clusters = int(cfg.get('max_cross_clusters') or 0)
    allowed_cross_clusters: Optional[set[int]] = None
    if max_cross_clusters > 0 and query_cluster_id is not None:
        allowed_cross_clusters = _closest_clusters(query_cluster_id, medoid_vectors, max_cross_clusters)
        if allowed_cross_clusters and query_cluster_id in allowed_cross_clusters:
            allowed_cross_clusters.discard(query_cluster_id)
        if not allowed_cross_clusters:
            allowed_cross_clusters = None

    in_raw: List[Dict[str, Any]] = []
    out_raw: List[Dict[str, Any]] = []
    in_all: List[Dict[str, Any]] = []
    out_all: List[Dict[str, Any]] = []

    seen_ids: set[str] = set()
    scanned = 0
    expansion = max(1, gather_size)
    while True:
        top_results = embeddings.top_similar(query_vec, expansion)
        added_any = False
        for aid, sim, idx in top_results:
            if aid in seen_ids:
                continue
            if query_appid is not None and aid == query_appid:
                continue
            seen_ids.add(aid)
            scanned += 1
            candidate_cluster = cluster_map.get(aid)
            candidate_meta = metadata_accessor.get(aid)
            vector = embeddings.vector_for(aid)
            name_signature = _build_name_signature(candidate_meta.name, name_penalty_cfg) if use_name_penalty else None
            entry = {
                'appid': aid,
                'similarity': float(sim),
                'cluster_id': candidate_cluster,
                'vector': vector,
                'meta': candidate_meta,
                'name_signature': name_signature,
            }

            same_cluster = (
                candidate_cluster is not None
                and query_cluster_id is not None
                and candidate_cluster == query_cluster_id
            )
            if same_cluster:
                in_all.append(entry)
                if sim >= min_sim_in:
                    in_raw.append(entry)
            else:
                cluster_allowed = (
                    allowed_cross_clusters is None
                    or candidate_cluster is None
                    or candidate_cluster in allowed_cross_clusters
                )
                if cluster_allowed:
                    out_all.append(entry)
                    if sim >= min_sim_out:
                        out_raw.append(entry)
            added_any = True

        if (
            (len(in_raw) >= gather_in_limit and len(out_raw) >= gather_out_limit)
            or expansion >= max_scan
            or not added_any
        ):
            break

        expansion = min(max_scan, max(expansion + total_target, expansion * 2))

    in_candidates = in_raw[:k_in] if k_in else list(in_raw)
    if k_in and len(in_candidates) < k_in and in_all:
        seen_in_ids = {cand['appid'] for cand in in_candidates}
        for cand in in_all:
            if cand['appid'] in seen_in_ids:
                continue
            in_candidates.append(cand)
            seen_in_ids.add(cand['appid'])
            if len(in_candidates) >= k_in:
                break

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
        for cand in out_raw:
            if not _business_filters_pass(cand['meta'], query_view, business_cfg):
                continue
            out_candidates.append(cand)
            if len(out_candidates) >= k_out:
                break
    if allow_cross and len(out_candidates) < k_out and out_all:
        seen_out_ids = {cand['appid'] for cand in out_candidates}
        for cand in out_all:
            if cand['appid'] in seen_out_ids:
                continue
            if not _business_filters_pass(cand['meta'], query_view, business_cfg):
                continue
            out_candidates.append(cand)
            seen_out_ids.add(cand['appid'])
            if len(out_candidates) >= k_out:
                break

    max_out_ratio = float(cfg.get('max_out_ratio', 0.3))
    max_cross_allowed = max(0, int(round(total_target * max_out_ratio))) if allow_cross else 0

    alpha = float(rerank_cfg.get('alpha_cosine', 1.0))
    beta = float(rerank_cfg.get('beta_cluster_penalty', 0.15))
    gamma = float(rerank_cfg.get('gamma_tag_overlap', 0.1))
    delta = float(rerank_cfg.get('delta_monetization', 0.05))
    epsilon = float(rerank_cfg.get('epsilon_mode_match', 0.05))
    zeta = float(rerank_cfg.get('zeta_name_penalty', zeta_name_penalty))

    def compute_score(entry: Dict[str, Any]) -> float:
        similarity = entry['similarity']
        candidate_meta = entry['meta']
        candidate_cluster = entry.get('cluster_id')
        cluster_penalty = _compute_cluster_distance(query_cluster_id, candidate_cluster, medoid_vectors)
        tag_overlap = len(set(candidate_meta.tags) & set(query_view.tags))
        monetization_bonus = 1.0 if candidate_meta.is_free == query_view.is_free else 0.0
        mode_overlap = len(set(candidate_meta.modes) & set(query_view.modes))
        name_penalty_value = 0.0
        if use_name_penalty and zeta > 0.0:
            candidate_signature = entry.get('name_signature')
            if candidate_signature is None:
                candidate_signature = _build_name_signature(candidate_meta.name, name_penalty_cfg)
                entry['name_signature'] = candidate_signature
            name_penalty_value = _compute_name_penalty(query_name_signature, candidate_signature, name_penalty_cfg)
        return (
            alpha * similarity
            - beta * cluster_penalty
            + gamma * tag_overlap
            + delta * monetization_bonus
            + epsilon * mode_overlap
            - zeta * name_penalty_value
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
    for cand in pool:
        if cand['source'] == 'cross' and cross_selected >= max_cross_allowed and len(selected) < total_target:
            continue
        selected.append(cand)
        if cand['source'] == 'cross':
            cross_selected += 1
        if len(selected) >= total_target:
            break

    if len(selected) < total_target:
        remaining = [cand for cand in pool if cand not in selected]
        for cand in remaining:
            selected.append(cand)
            if len(selected) >= total_target:
                break

    fallback_used = False
    if len(selected) < total_target:
        existing_ids = {cand['appid'] for cand in selected}
        fallback_pool: List[Dict[str, Any]] = []

        def add_fallback(entries: List[Dict[str, Any]], source: str) -> None:
            for cand in entries:
                appid = cand['appid']
                if appid in existing_ids:
                    continue
                if source == 'cross':
                    if not allow_cross:
                        continue
                    if allowed_cross_clusters is not None:
                        cluster_val = cand.get('cluster_id')
                        if cluster_val is not None and cluster_val not in allowed_cross_clusters:
                            continue
                candidate_copy = dict(cand)
                candidate_copy['source'] = source
                candidate_copy['score'] = compute_score(candidate_copy)
                fallback_pool.append(candidate_copy)

        add_fallback(in_all, 'intra')
        add_fallback(out_all, 'cross')

        if fallback_pool:
            fallback_used = True
            fallback_pool.sort(key=lambda item: (item['score'], item['similarity']), reverse=True)
            for cand in fallback_pool:
                appid = cand['appid']
                if appid in existing_ids:
                    continue
                selected.append(cand)
                existing_ids.add(appid)
                if len(selected) >= total_target:
                    break

    final_neighbors: List[Dict[str, Any]] = []
    seen_final = set()
    for cand in selected:
        aid = cand['appid']
        if aid in seen_final:
            continue
        seen_final.add(aid)
        final_neighbors.append(cand)
        if len(final_neighbors) >= total_target:
            break

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
        'scanned': scanned,
        'gathered_in': len(in_raw),
        'gathered_cross': len(out_raw),
        'fallback_used': fallback_used,
        'faiss_used': embeddings.uses_faiss,
    }

    return output_neighbors, diagnostics



__all__ = [
    'EmbeddingIndex',
    'MetadataAccessor',
    'select_competitor_neighbors',
    'DEFAULT_CONFIG',
]
