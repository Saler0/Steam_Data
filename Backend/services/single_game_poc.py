"""Utilities to run the single-game PoC from the backend."""
from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import yaml
from sentence_transformers import SentenceTransformer

BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent.parent
DATA_ANALYTICS_DIR = PROJECT_ROOT / "Data_analytics"

if str(DATA_ANALYTICS_DIR) not in sys.path:
    sys.path.append(str(DATA_ANALYTICS_DIR))

from src.pipelines.generate_embeddings import _build_doc
from src.utils.io import read_parquet_any
from src.insights.neighbor_strategy import DEFAULT_CONFIG, EmbeddingIndex, select_competitor_neighbors


class PoCExecutionError(RuntimeError):
    """Raised when the PoC pipeline cannot be executed."""


class PoCConfigurationError(PoCExecutionError):
    """Raised when required analytics artifacts are missing or invalid."""


def _load_yaml_required(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise PoCConfigurationError(f"Missing required configuration file: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise PoCConfigurationError(f"Could not parse YAML file: {path}") from exc
    if not isinstance(data, dict):
        raise PoCConfigurationError(f"Configuration file must contain a mapping: {path}")
    return data


def _load_yaml_optional(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _ensure_unit(vec: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vec))
    if norm == 0.0:
        return vec
    return vec / norm


def _embed_documents(
    model: SentenceTransformer,
    docs: Iterable[str],
    normalize: bool,
) -> List[np.ndarray]:
    embeddings = model.encode(list(docs), normalize_embeddings=normalize, show_progress_bar=False)
    vectors = [np.asarray(emb, dtype=np.float32) for emb in embeddings]
    if not normalize:
        vectors = [_ensure_unit(vec) for vec in vectors]
    return vectors


def _load_medoids(path: Path) -> Dict[str, np.ndarray]:
    if not path.exists():
        raise PoCConfigurationError(f"Medoid file not found: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PoCConfigurationError(f"Could not read medoid file: {path}") from exc
    if not isinstance(raw, dict):
        raise PoCConfigurationError(f"Medoid file must be a mapping: {path}")
    medoids: Dict[str, np.ndarray] = {}
    for key, value in raw.items():
        try:
            arr = np.asarray(value, dtype=np.float32)
        except Exception as exc:
            raise PoCConfigurationError(f"Invalid medoid vector for cluster {key}") from exc
        if arr.size == 0:
            continue
        medoids[str(key)] = _ensure_unit(arr)
    if not medoids:
        raise PoCConfigurationError(f"No medoids could be loaded from: {path}")
    return medoids


def _resolve_path(base: Path, candidate: Optional[str | Path]) -> Optional[Path]:
    if not candidate:
        return None
    path = Path(candidate)
    if not path.is_absolute():
        path = (base / path).resolve()
    return path


def _load_optional_df(path: Optional[Path]) -> pd.DataFrame:
    if not path or not path.exists():
        return pd.DataFrame()
    suffix = path.suffix.lower()
    try:
        if suffix in {".parquet", ".pq"}:
            return read_parquet_any(path)
        if suffix == ".csv":
            return pd.read_csv(path)
        if suffix == ".tsv":
            return pd.read_csv(path, sep="\t")
        if suffix == ".json":
            return pd.read_json(path)
        return read_parquet_any(path)
    except Exception:
        return pd.DataFrame()


def _prepare_lookup(df: pd.DataFrame, key: str, value: str) -> Dict[str, Any]:
    if df.empty or key not in df.columns or value not in df.columns:
        return {}
    subset = df[[key, value]].dropna()
    try:
        subset[key] = subset[key].astype(str)
    except Exception:
        subset[key] = subset[key].apply(lambda item: str(item) if item is not None else None)
    return {row[key]: row[value] for _, row in subset.iterrows()}


def _prepare_query_metadata(sample: Dict[str, Any]) -> Dict[str, Any]:
    def _clean(values: Any) -> List[str]:
        if not values:
            return []
        if isinstance(values, (list, tuple, set)):
            items = values
        else:
            items = str(values).split(",")
        result: List[str] = []
        for item in items:
            token = str(item).strip()
            if token:
                result.append(token)
        return result

    genres = _clean(sample.get("genres"))
    tags = _clean(sample.get("tags")) or genres
    categories = _clean(sample.get("categories"))
    price = sample.get("price")
    try:
        price_val = float(price) if price is not None else None
    except Exception:
        price_val = None
    is_free = None
    if price_val is not None:
        is_free = price_val == 0
    elif isinstance(sample.get("is_free"), bool):
        is_free = sample.get("is_free")

    modes: List[str] = []
    for token in categories:
        lower = token.lower()
        if "pvp" in lower:
            modes.append("pvp")
        if "pve" in lower:
            modes.append("pve")
        if "coop" in lower or "co-op" in lower:
            modes.append("coop")
        if "single" in lower:
            modes.append("singleplayer")
    modes = sorted(set(modes))

    return {
        "genres": genres,
        "tags": tags,
        "categories": categories,
        "modes": modes,
        "price": price_val,
        "is_free": is_free,
        "name": sample.get("name"),
    }


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
    return _ensure_unit(arr)


def _find_neighbors(
    sample_vec: np.ndarray,
    emb_df: pd.DataFrame,
    clusters_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    top_k: int,
    min_similarity: float,
) -> List[Dict[str, Any]]:
    if emb_df.empty or "embedding" not in emb_df.columns:
        return []
    df = emb_df.copy()
    if "appid" not in df.columns and "app_id" in df.columns:
        df = df.rename(columns={"app_id": "appid"})
    if "appid" not in df.columns:
        return []
    df["appid"] = df["appid"].astype(str)
    vectors: List[np.ndarray] = []
    ids: List[str] = []
    for _, row in df.iterrows():
        vec = _vector_from_value(row.get("embedding"))
        if vec is None:
            continue
        vectors.append(vec)
        ids.append(row.get("appid"))
    if not vectors:
        return []
    matrix = np.vstack(vectors)
    query = _ensure_unit(sample_vec.astype(np.float32))
    sims = matrix @ query
    order = np.argsort(-sims)
    clusters_lookup = _prepare_lookup(
        clusters_df.rename(columns={"app_id": "appid"}) if "app_id" in clusters_df.columns else clusters_df,
        "appid",
        "cluster_id",
    )
    names_lookup = _prepare_lookup(
        metadata_df.rename(columns={"app_id": "appid"}) if "app_id" in metadata_df.columns else metadata_df,
        "appid",
        "name",
    )
    neighbors: List[Dict[str, Any]] = []
    for idx in order:
        app_id = ids[idx]
        similarity = float(sims[idx])
        if similarity < min_similarity:
            continue
        neighbors.append(
            {
                "appid": app_id,
                "similarity": similarity,
                "cluster_id": clusters_lookup.get(app_id),
                "name": names_lookup.get(app_id),
            }
        )
        if len(neighbors) >= top_k:
            break
    return neighbors


def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> None:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value


def _build_strategy_cfg(params_cfg: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str], Optional[str]]:
    cfg = deepcopy(DEFAULT_CONFIG)
    file_cfg = params_cfg.get("neighbor_strategy") or {}
    if isinstance(file_cfg, dict):
        _merge_dict(cfg, file_cfg)
    legacy_cfg = (params_cfg.get("client_report") or {}).get("neighbors_config") or {}
    if isinstance(legacy_cfg, dict):
        _merge_dict(cfg, legacy_cfg)
    faiss_index = cfg.pop("faiss_index_path", None)
    faiss_ids = cfg.pop("faiss_ids_path", None)
    if not faiss_index:
        faiss_index = params_cfg.get("faiss_index_path")
    if not faiss_ids:
        faiss_ids = params_cfg.get("faiss_ids_path")
    return cfg, faiss_index, faiss_ids


class SingleGamePoCService:
    """Encapsulates the analytics steps required by the single-game PoC."""

    def __init__(
        self,
        base_path: Optional[Path] = None,
        embeddings_path: Optional[Path] = None,
        clusters_path: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
    ) -> None:
        self.base_path = (base_path or DATA_ANALYTICS_DIR).resolve()
        config_path = self.base_path / "configs" / "embeddings.yaml"
        config = _load_yaml_required(config_path)
        self.doc_fields = config.get("document_fields") or {}
        self.assemble_cfg = config.get("assemble")
        self.normalize = bool(config.get("normalize_embeddings", False))
        model_name = config.get("embedding_model")
        if not model_name:
            raise PoCConfigurationError("Embedding model name is not defined in embeddings.yaml")
        self.model = SentenceTransformer(model_name)

        medoids_path = self.base_path / "models" / "cluster_medoids.json"
        self.medoids = _load_medoids(medoids_path)

        params_path = self.base_path / "configs" / "params.yaml"
        params_cfg = _load_yaml_optional(params_path)
        strategy_cfg, faiss_index, faiss_ids = _build_strategy_cfg(params_cfg)
        self.strategy_cfg_template = strategy_cfg
        self.faiss_index_path = _resolve_path(self.base_path, faiss_index)
        self.faiss_ids_path = _resolve_path(self.base_path, faiss_ids)

        self.embeddings_path = _resolve_path(
            self.base_path,
            embeddings_path or self.base_path / "data" / "processed" / "embeddings" / "embeddings.parquet",
        )
        self.clusters_path = _resolve_path(
            self.base_path,
            clusters_path or self.base_path / "data" / "processed" / "clusters.parquet",
        )
        self.metadata_path = _resolve_path(
            self.base_path,
            metadata_path or self.base_path / "data" / "processed" / "game_metadata.parquet",
        )

        self.emb_df = _load_optional_df(self.embeddings_path)
        self.clusters_df = _load_optional_df(self.clusters_path)
        self.metadata_df = _load_optional_df(self.metadata_path)

        index_faiss_path = str(self.faiss_index_path) if self.faiss_index_path and self.faiss_ids_path else None
        index_faiss_ids = str(self.faiss_ids_path) if self.faiss_index_path and self.faiss_ids_path else None
        index = EmbeddingIndex.from_dataframe(
            self.emb_df,
            faiss_index_path=index_faiss_path,
            faiss_ids_path=index_faiss_ids,
        )
        self.embedding_index: Optional[EmbeddingIndex] = index if len(index) > 0 else None

    def run(
        self,
        sample: Dict[str, Any],
        *,
        neighbors: int = 20,
        min_similarity: float = 0.0,
    ) -> Dict[str, Any]:
        if not sample:
            raise PoCExecutionError("Sample payload is empty")
        enriched = dict(sample)
        if "price" not in enriched and "precio" in enriched:
            enriched["price"] = enriched.get("precio")

        doc = _build_doc(enriched, self.doc_fields, self.assemble_cfg)
        vector = _embed_documents(self.model, [doc], self.normalize)[0]

        medoid_scores = [
            (cluster_id, float(np.dot(vector, centroid)))
            for cluster_id, centroid in self.medoids.items()
        ]
        medoid_scores.sort(key=lambda item: item[1], reverse=True)
        if not medoid_scores:
            raise PoCExecutionError("No medoids available to score the sample")
        best_cluster, best_similarity = medoid_scores[0]

        sample_metadata = _prepare_query_metadata(enriched)
        strategy_cfg = deepcopy(self.strategy_cfg_template)
        strategy_cfg["target_total"] = int(neighbors)

        query_cluster_id: Optional[int] = None
        try:
            query_cluster_id = int(best_cluster)
        except Exception:
            query_cluster_id = None

        neighbors_list: List[Dict[str, Any]] = []
        if self.embedding_index is not None:
            neighbors_list = select_competitor_neighbors(
                query_vec=vector,
                query_metadata=sample_metadata,
                query_appid=None,
                query_cluster_id=query_cluster_id,
                embeddings=self.embedding_index,
                clusters_df=self.clusters_df,
                metadata_df=self.metadata_df,
                medoids=self.medoids,
                user_cfg=strategy_cfg,
            )
        if not neighbors_list:
            neighbors_list = _find_neighbors(
                vector,
                self.emb_df,
                self.clusters_df,
                self.metadata_df,
                top_k=neighbors,
                min_similarity=min_similarity,
            )


        return {
            "best_cluster_id": best_cluster,
            "best_cluster_similarity": best_similarity,
            "neighbors": neighbors_list[:neighbors],
        }

