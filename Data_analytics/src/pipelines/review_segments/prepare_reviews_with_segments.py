#!/usr/bin/env python
"""Prepare per-review dataset with experience labels and optional BERTopic topics (pandas only)."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd
import torch

THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config_utils import expand_env_in_obj

try:
    from pipelines.decision_rules.reglas_decision import experiencia_jugador
except Exception:  # pragma: no cover
    experiencia_jugador = None  # type: ignore

try:
    from bertopic import BERTopic  # type: ignore
    BER_TOPIC_AVAILABLE = True
except Exception:  # pragma: no cover
    BER_TOPIC_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import CountVectorizer
except ImportError:  # pragma: no cover
    CountVectorizer = None  # type: ignore

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore



def _coerce_mongo_number(value: Any) -> Optional[float]:
    if isinstance(value, dict):
        if "$numberLong" in value:
            try:
                return float(value["$numberLong"])
            except Exception:
                return None
        if "$numberInt" in value:
            try:
                return float(value["$numberInt"])
            except Exception:
                return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None

def _coerce_mongo_date(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]
    try:
        return pd.to_datetime(value, errors="coerce", utc=True)
    except Exception:
        return None

def _coerce_bool_series(series: pd.Series) -> pd.Series:
    return series.apply(_safe_bool)

def _ensure_column(df: pd.DataFrame, columns: Sequence[str]) -> Optional[str]:
    for name in columns:
        if name in df.columns:
            return name
    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, np.integer)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None




def _load_any_df(path_str: Optional[str]) -> pd.DataFrame:
    if not path_str:
        return pd.DataFrame()
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()
    suffix = path.suffix.lower()
    if suffix in {'.parquet', '.pq'}:
        return pd.read_parquet(path)
    if suffix == '.csv':
        return pd.read_csv(path)
    if suffix == '.json':
        return pd.read_json(path)
    return pd.read_parquet(path)

def _parse_json_arg(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception as exc:
        raise SystemExit(f"Could not decode JSON: {value} -> {exc}")


def _load_reviews_from_mongo(cfg: Dict[str, Any]) -> pd.DataFrame:
    if MongoClient is None:
        raise SystemExit("pymongo is required to pull reviews from MongoDB.")
    uri = cfg.get("uri")
    database = cfg.get("database") or cfg.get("db")
    collection = cfg.get("collection")
    if not uri or not database or not collection:
        raise SystemExit("Mongo configuration requires uri, database and collection.")
    query = cfg.get("query") or {}
    projection = cfg.get("projection")
    limit = cfg.get("limit")
    try:
        limit = int(limit) if limit is not None else None
    except Exception:
        limit = None

    client = MongoClient(uri)
    processed_chunks = []
    try:
        cursor = client[database][collection].find(query, projection, batch_size=5000)
        if limit:
            cursor = cursor.limit(limit)

        rows_chunk = []
        for row in cursor:
            rows_chunk.append(row)
            if len(rows_chunk) >= 5000:
                df_chunk = pd.DataFrame(rows_chunk)
                if "_id" in df_chunk.columns:
                    df_chunk = df_chunk.drop(columns=["_id"])
                if "author" in df_chunk.columns:
                    author_df = pd.json_normalize(df_chunk["author"]).add_prefix("author_")
                    df_chunk = df_chunk.drop(columns=["author"]).join(author_df)
                processed_chunks.append(df_chunk)
                rows_chunk = []

        if rows_chunk:
            df_chunk = pd.DataFrame(rows_chunk)
            if "_id" in df_chunk.columns:
                df_chunk = df_chunk.drop(columns=["_id"])
            if "author" in df_chunk.columns:
                author_df = pd.json_normalize(df_chunk["author"]).add_prefix("author_")
                df_chunk = df_chunk.drop(columns=["author"]).join(author_df)
            processed_chunks.append(df_chunk)
    finally:
        try:
            client.close()
        except Exception:
            pass

    if not processed_chunks:
        return pd.DataFrame()

    df = pd.concat(processed_chunks, ignore_index=True)


    convert_numeric = [
        "timestamp_created",
        "timestamp_updated",
        "votes_up",
        "votes_funny",
        "comment_count",
        "weighted_vote_score",
        "appid",
        "author_playtime_at_review",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "author_num_games_owned",
        "author_num_reviews"
    ]
    for col in convert_numeric:
        if col in df.columns:
            df[col] = df[col].apply(_coerce_mongo_number)

    convert_dates = [
        "timestamp_created_date",
        "timestamp_updated_date",
        "updated_at"
    ]
    for col in convert_dates:
        if col in df.columns:
            df[col] = df[col].apply(_coerce_mongo_date)

    convert_bool = [
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "voted_up",
        "primarily_steam_deck"
    ]
    for col in convert_bool:
        if col in df.columns:
            df[col] = df[col].apply(_safe_bool)

    return df


def _prepare_reviews(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    review_id_candidates = [cfg.get("review_id_column"), "review_id", "recommendationid", "id", "reviewid"]
    review_id_col = next((col for col in review_id_candidates if col and col in df.columns), None)
    if review_id_col:
        df["review_id"] = df[review_id_col].astype(str)
    else:
        df["review_id"] = np.arange(len(df)).astype(str)

    appid_candidates = [cfg.get("appid_column"), "appid"]
    appid_col = next((col for col in appid_candidates if col and col in df.columns), None)
    if not appid_col:
        raise SystemExit("Could not find appid column in reviews dataset.")

    def _to_appid_string(val: Any) -> str:
        coerced = _coerce_mongo_number(val)
        if coerced is not None and not np.isnan(coerced):
            try:
                return str(int(coerced))
            except Exception:
                return str(coerced)
        return str(val)

    df["appid"] = df[appid_col].apply(_to_appid_string)

    text_candidates = [cfg.get("text_column"), "review_clean", "review", "review_text", "body", "content"]
    text_col = next((col for col in text_candidates if col and col in df.columns), None)
    if text_col:
        df["review_text"] = df[text_col].fillna("").astype(str)
    else:
        df["review_text"] = ""

    date_candidates = [cfg.get("date_column"), "review_date", "timestamp_created_date", "timestamp_created", "timestamp_updated_date", "date"]
    date_col = next((col for col in date_candidates if col and col in df.columns), None)
    if not date_col:
        raise SystemExit("Could not find date column in reviews dataset.")
    df["review_date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df = df.dropna(subset=["review_date"])

    recommended_candidates = [cfg.get("recommended_column"), "recommended", "voted_up", "is_positive"]
    recommended_col = next((col for col in recommended_candidates if col and col in df.columns), None)
    if recommended_col:
        df["recommended"] = df[recommended_col].apply(_safe_bool)
    else:
        df["recommended"] = None

    playtime_candidates = [cfg.get("playtime_column"), "playtime_at_review", "author_playtime_at_review", "author_playtime_forever"]
    playtime_col = next((col for col in playtime_candidates if col and col in df.columns), None)
    if playtime_col:
        df["playtime_at_review"] = df[playtime_col].apply(_safe_float)
    else:
        df["playtime_at_review"] = None

    playtime_30d_candidates = [cfg.get("playtime_30d_column"), "playtime_since_review_30d", "author_playtime_last_two_weeks"]
    playtime_30d_col = next((col for col in playtime_30d_candidates if col and col in df.columns), None)
    if playtime_30d_col:
        df["playtime_since_review_30d"] = df[playtime_30d_col].apply(_safe_float)
    else:
        df["playtime_since_review_30d"] = None

    heuristic_cfg = cfg.get("abandon_heuristic", {}) or {}
    general_col_name = heuristic_cfg.get("general_column") or cfg.get("abandon_column") or "abandon_after_30d"
    review_col_name = heuristic_cfg.get("review_column") or "abandon_after_review"
    post_review_threshold = float(heuristic_cfg.get("post_review_minutes_threshold", 10.0))
    last_two_weeks_threshold = float(heuristic_cfg.get("last_two_weeks_threshold", post_review_threshold))

    # Normalizar unidades a horas si las entradas vienen en minutos
    time_units = str(cfg.get("time_units", "minutes")).lower()
    to_hours = (time_units.startswith("min"))

    abandon_candidates = []
    configured_abandon = cfg.get("abandon_column")
    if configured_abandon:
        abandon_candidates.append(configured_abandon)
    abandon_candidates.extend(["abandon_after_30d", "flag_abandon"])
    abandon_col = next((col for col in abandon_candidates if col and col in df.columns), None)

    author_forever = df.get("author_playtime_forever")
    author_forever = author_forever.apply(_safe_float) if author_forever is not None else None
    playtime_at_review_series = df.get("playtime_at_review")
    if to_hours:
        if playtime_at_review_series is not None:
            playtime_at_review_series = (pd.to_numeric(playtime_at_review_series, errors="coerce") / 60.0)
            df["playtime_at_review"] = playtime_at_review_series
    post_review_playtime = None
    if author_forever is not None and playtime_at_review_series is not None:
        post_review_playtime = (author_forever.fillna(0) - playtime_at_review_series.fillna(0)).clip(lower=0)
    last_two_weeks_series = None
    if "author_playtime_last_two_weeks" in df.columns:
        last_two_weeks_series = df["author_playtime_last_two_weeks"].apply(_safe_float).fillna(0)
        if to_hours:
            last_two_weeks_series = (pd.to_numeric(last_two_weeks_series, errors="coerce").fillna(0) / 60.0)
        if post_review_playtime is None:
            post_review_playtime = last_two_weeks_series

    if abandon_col:
        general_series = df[abandon_col].apply(_safe_bool)
        df[general_col_name] = general_series
        if general_col_name != "abandon_after_30d":
            df["abandon_after_30d"] = df[general_col_name]
        if post_review_playtime is None:
            df["post_review_playtime"] = None
            review_series = general_series
        else:
            # Convertir post_review_playtime a horas si procede
            df["post_review_playtime"] = (post_review_playtime / 60.0) if (to_hours and post_review_playtime is not None) else post_review_playtime
            review_series = (post_review_playtime <= post_review_threshold).astype(bool)
        df[review_col_name] = review_series
    else:
        df["post_review_playtime"] = (post_review_playtime / 60.0) if (to_hours and post_review_playtime is not None) else post_review_playtime
        if last_two_weeks_series is not None:
            general_series = (last_two_weeks_series <= last_two_weeks_threshold).astype(bool)
        elif post_review_playtime is not None:
            general_series = (post_review_playtime <= last_two_weeks_threshold).astype(bool)
        else:
            general_series = pd.Series([None] * len(df))

        if general_series is not None:
            df[general_col_name] = general_series
            if general_col_name != "abandon_after_30d":
                df["abandon_after_30d"] = df[general_col_name]
        else:
            df[general_col_name] = None
            if general_col_name != "abandon_after_30d":
                df["abandon_after_30d"] = None

        if post_review_playtime is not None:
            review_series = (post_review_playtime <= post_review_threshold).astype(bool)
            df[review_col_name] = review_series
        else:
            review_series = df[general_col_name]
            df[review_col_name] = review_series

    general_series = df[general_col_name] if general_col_name in df.columns else None
    review_series = df[review_col_name] if review_col_name in df.columns else None

    activity_cfg = heuristic_cfg.get("activity_segments", {}) or cfg.get("activity_segments", {}) or {}
    inactive_minutes = float(activity_cfg.get("inactive_hours", 0.0)) * 60.0
    low_minutes = float(activity_cfg.get("low_hours", 2.0)) * 60.0
    occasional_minutes = float(activity_cfg.get("occasional_hours", 10.0)) * 60.0
    frequent_minutes = float(activity_cfg.get("frequent_hours", 30.0)) * 60.0

    base_minutes = last_two_weeks_series if last_two_weeks_series is not None else post_review_playtime
    if base_minutes is not None:
        minutes = base_minutes.fillna(0) * (60.0 if not to_hours else 1.0)
        activity_segment = np.full(len(df), "muy_activo", dtype=object)
        mask = minutes <= inactive_minutes
        activity_segment[mask] = "inactivo"
        mask = (minutes > inactive_minutes) & (minutes <= low_minutes)
        activity_segment[mask] = "poco_activo"
        mask = (minutes > low_minutes) & (minutes <= occasional_minutes)
        activity_segment[mask] = "activo_ocasional"
        mask = (minutes > occasional_minutes) & (minutes <= frequent_minutes)
        activity_segment[mask] = "activo_frecuente"
    else:
        activity_segment = np.full(len(df), "desconocido", dtype=object)
    df["activity_segment"] = activity_segment

    recommended_bool = df["recommended"].fillna(False).astype(bool) if "recommended" in df.columns else pd.Series(False, index=df.index)
    abandon_reason = np.full(len(df), None, dtype=object)
    if review_series is not None:
        review_bool = pd.Series(review_series).fillna(False).astype(bool)
        positive_mask = review_bool & recommended_bool
        negative_mask = review_bool & ~recommended_bool
        abandon_reason[positive_mask.values] = "abandono_ajeno_al_juego"
        abandon_reason[negative_mask.values] = "abandono_por_resena"
    if general_series is not None:
        general_bool = pd.Series(general_series).fillna(False).astype(bool)
        mask_general = general_bool & pd.isna(abandon_reason)
        abandon_reason[mask_general.values] = "abandono_por_inactividad"
    mask_unknown = pd.isna(abandon_reason)
    if mask_unknown.any():
        abandon_reason[mask_unknown] = np.where(
            activity_segment[mask_unknown] == "desconocido",
            None,
            "actividad_" + activity_segment[mask_unknown]
        )
    df["abandon_reason"] = abandon_reason

    gifted_candidates = [cfg.get("gifted_column"), "received_for_free", "gifted"]
    gifted_col = next((col for col in gifted_candidates if col and col in df.columns), None)
    if gifted_col:
        df["gifted"] = df[gifted_col].apply(_safe_bool)
    else:
        df["gifted"] = None
    df["purchase_type"] = df["gifted"].apply(lambda x: "regalado" if x else ("comprado" if x is False else None))

    ea_candidates = [cfg.get("early_access_column"), "written_during_early_access", "early_access"]
    ea_col = next((col for col in ea_candidates if col and col in df.columns), None)
    if ea_col:
        df["early_access"] = df[ea_col].apply(_safe_bool)
    else:
        df["early_access"] = None
    df["review_phase"] = df["early_access"].apply(lambda x: "early_access" if x else ("post_lanzamiento" if x is False else None))

    if "review_phase" in df.columns and "post_launch" not in df.columns:
        df["post_launch"] = df["review_phase"].apply(lambda x: True if x == "post_lanzamiento" else (False if x == "early_access" else None))

    median_col = cfg.get("median_playtime_column") or "median_playtime_app"
    if median_col in df.columns:
        df["median_playtime_app"] = df[median_col].apply(_safe_float)
    else:
        medians = df.groupby("appid")["playtime_at_review"].median().rename("median_playtime_app")
        df = df.merge(medians, on="appid", how="left")

    if experiencia_jugador is not None:
        df["experience_label"] = df.apply(
            lambda row: experiencia_jugador(row.get("playtime_at_review"), row.get("median_playtime_app")), axis=1
        )
    else:
        df["experience_label"] = None

    def _experience_key(label: Any) -> Optional[str]:
        mapping = {
            "nuevo": "new",
            "intermedio": "intermediate",
            "experto": "expert",
            "veterano": "veteran",
        }
        if not label:
            return None
        return mapping.get(str(label).strip().lower())

    df["experience_key"] = df["experience_label"].apply(_experience_key)
    return df
def _write_output(df: pd.DataFrame, path: str) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base_columns = [
        "appid",
        "review_id",
        "review_date",
        "review_text",
        "recommended",
        "playtime_at_review",
        "playtime_since_review_30d",
        "post_review_playtime",
        "abandon_after_30d",
        "abandon_general",
        "abandon_after_review",
        "abandon_reason",
        "activity_segment",
        "gifted",
        "purchase_type",
        "early_access",
        "review_phase",
        "post_launch",
        "median_playtime_app",
        "experience_label",
        "experience_key"
    ]
    existing_columns = [col for col in base_columns if col in df.columns]
    if df.empty:
        if out_path.suffix.lower() == ".json":
            out_path.write_text("[]", encoding="utf-8")
        elif out_path.suffix.lower() == ".csv":
            pd.DataFrame(columns=existing_columns).to_csv(out_path, index=False)
        else:
            pd.DataFrame(columns=existing_columns).to_parquet(out_path, index=False)
        print(f"[WARN] Empty review dataset written to {out_path}")
        return
    subset = df.loc[:, existing_columns]
    if out_path.suffix.lower() == ".json":
        subset.to_json(out_path, orient="records", date_format="iso")
    elif out_path.suffix.lower() == ".csv":
        subset.to_csv(out_path, index=False)
    else:
        subset.to_parquet(out_path, index=False)
    print(f"[OK] Reviews with segments -> {out_path}")
def _fallback_topics(df: pd.DataFrame, topics_out: str) -> None:
    Path(topics_out).parent.mkdir(parents=True, exist_ok=True)
    columns = ["review_id", "topic_id", "topic_name", "share", "avg_sentiment", "snippet"]
    if df.empty or "review_text" not in df.columns:
        pd.DataFrame(columns=columns).to_parquet(topics_out, index=False)
        print(f"[WARN] Topics fallback -> {topics_out}")
        return
    if CountVectorizer is None:
        pd.DataFrame(columns=columns).to_parquet(topics_out, index=False)
        print(f"[WARN] CountVectorizer not available; topics fallback -> {topics_out}")
        return
    vec = CountVectorizer(max_features=25, stop_words="english")
    matrix = vec.fit_transform(df["review_text"])
    feature_names = vec.get_feature_names_out()
    records: List[Dict[str, Any]] = []
    for idx, review_id in enumerate(df["review_id"]):
        row = matrix[idx]
        if row.nnz == 0:
            continue
        counts = row.toarray()[0]
        top_idx = counts.argsort()[::-1][:3]
        words = [feature_names[i] for i in top_idx if counts[i] > 0]
        if not words:
            continue
        records.append({
            "review_id": review_id,
            "topic_id": idx,
            "topic_name": " ".join(words),
            "share": 1.0,
            "avg_sentiment": None,
            "snippet": df.iloc[idx]["review_text"][:160],
        })
    pd.DataFrame(records, columns=columns).to_parquet(topics_out, index=False)
    print(f"[WARN] Topics generated with simple keywords -> {topics_out}")


def _run_bertopic(df: pd.DataFrame, cfg: Dict[str, Any], topics_out: str) -> None:
    Path(topics_out).parent.mkdir(parents=True, exist_ok=True)
    if df.empty or "review_text" not in df.columns:
        _fallback_topics(df, topics_out)
        return
    if not BER_TOPIC_AVAILABLE:
        print("[WARN] BERTopic not available; using fallback topics.")
        _fallback_topics(df, topics_out)
        return

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device} for BERTopic")

    language = cfg.get("bertopic_language", "multilingual")
    min_topic_size = cfg.get("bertopic_min_topic_size", 20)
    topic_model = BERTopic(language=language, min_topic_size=min_topic_size, verbose=False)
    documents = df["review_text"].tolist()

    votes_series = pd.to_numeric(df.get("votes_up"), errors="coerce") if "votes_up" in df.columns else None
    weight_cap = int(cfg.get("bertopic_weight_cap", 10))
    if votes_series is not None:
        weights = votes_series.fillna(0).clip(lower=0)
        weights = weights.astype(int).clip(0, weight_cap) + 1
        weighted_docs: List[str] = []
        for text, repeat in zip(documents, weights):
            weighted_docs.extend([text] * int(max(1, repeat)))
        if len(weighted_docs) > len(documents):
            topic_model.fit(weighted_docs)
            topics, probs = topic_model.transform(documents)
        else:
            topics, probs = topic_model.fit_transform(documents)
    else:
        topics, probs = topic_model.fit_transform(documents)

    info = topic_model.get_topic_info().set_index("Topic")["Name"].to_dict()
    records: List[Dict[str, Any]] = []
    for idx, review_id in enumerate(df["review_id"]):
        topic_id = topics[idx]
        if topic_id == -1:
            continue
        name = info.get(topic_id, f"Topic {topic_id}")
        prob = None
        if probs is not None and len(probs) > idx:
            try:
                prob = float(np.max(probs[idx]))
            except Exception:
                prob = None
        records.append({
            "review_id": review_id,
            "topic_id": int(topic_id),
            "topic_name": name,
            "share": prob,
            "avg_sentiment": None,
            "snippet": df.iloc[idx]["review_text"][:160],
        })
    pd.DataFrame(records).to_parquet(topics_out, index=False)
    print(f"[OK] Topics by review -> {topics_out}")
def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    cfg_path = Path(path)
    if not cfg_path.exists():
        return {}
    try:
        if cfg_path.suffix.lower() == ".json":
            return expand_env_in_obj(json.loads(cfg_path.read_text()))
        import yaml  # type: ignore
        return expand_env_in_obj(yaml.safe_load(cfg_path.read_text()))
    except Exception as exc:
        print(f"[WARN] Could not read config {cfg_path}: {exc}")
        return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare reviews with experience segments and optional BERTopic topics")
    parser.add_argument("--reviews-source", default=None, help="Path to reviews dataset (csv/parquet/json).")
    parser.add_argument("--output", default="data/warehouse/reviews_with_segments.parquet", help="Output dataset with enriched reviews.")
    parser.add_argument("--topics-output", default="outputs/events/reviews_topics.parquet", help="Output parquet with topics per review.")
    parser.add_argument("--config", default="configs/review_segments.yaml", help="YAML/JSON configuration file.")
    parser.add_argument("--run-bertopic", action="store_true", help="Execute BERTopic if available.")
    parser.add_argument("--allow-empty", action="store_true", help="Generate empty datasets if no reviews are found.")
    parser.add_argument("--mongo-uri", help="MongoDB URI to pull reviews.")
    parser.add_argument("--mongo-db", help="MongoDB database name.")
    parser.add_argument("--mongo-collection", help="MongoDB collection name.")
    parser.add_argument("--mongo-query", help="MongoDB match query (JSON string).")
    parser.add_argument("--mongo-projection", help="MongoDB projection (JSON string).")
    parser.add_argument("--mongo-limit", type=int, help="Limit number of documents pulled from MongoDB.")
    args = parser.parse_args()

    cfg = load_config(args.config) or {}
    reviews_source = args.reviews_source or cfg.get("reviews_source")

    mongo_cfg = dict(cfg.get("mongo") or {})
    if args.mongo_uri:
        mongo_cfg["uri"] = args.mongo_uri
    if args.mongo_db:
        mongo_cfg["database"] = args.mongo_db
    if args.mongo_collection:
        mongo_cfg["collection"] = args.mongo_collection
    if args.mongo_query:
        mongo_cfg["query"] = _parse_json_arg(args.mongo_query)
    if args.mongo_projection:
        mongo_cfg["projection"] = _parse_json_arg(args.mongo_projection)
    if args.mongo_limit is not None:
        mongo_cfg["limit"] = args.mongo_limit
    if isinstance(mongo_cfg.get("query"), str):
        mongo_cfg["query"] = _parse_json_arg(mongo_cfg.get("query"))
    if isinstance(mongo_cfg.get("projection"), str):
        mongo_cfg["projection"] = _parse_json_arg(mongo_cfg.get("projection"))

    reviews_df = _load_any_df(reviews_source)
    if reviews_df.empty and mongo_cfg.get("uri"):
        print("[INFO] Loading reviews from MongoDB (pandas mode)...")
        reviews_df = _load_reviews_from_mongo(mongo_cfg)
        if reviews_df.empty:
            print("[WARN] MongoDB did not return reviews; dataset is empty.")
    if reviews_df.empty:
        if args.allow_empty:
            print("[WARN] No reviews found in configured sources; writing empty dataset.")
            _write_output(pd.DataFrame(), args.output)
            _fallback_topics(pd.DataFrame(), args.topics_output)
            return
        raise SystemExit("No reviews found in file or Mongo sources; use --allow-empty to continue.")

    reviews_df = _prepare_reviews(reviews_df, cfg)
    _write_output(reviews_df, args.output)

    if args.run_bertopic:
        _run_bertopic(reviews_df, cfg, args.topics_output)
    else:
        _fallback_topics(reviews_df, args.topics_output)


if __name__ == "__main__":
    main()

















