#!/usr/bin/env python
"""Prepare per-review dataset with experience labels and optional BERTopic topics."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from src.utils.config_utils import expand_env_in_obj

if os.path.exists(os.path.join(os.path.dirname(__file__), "..")):
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

try:
    from pyspark.sql import SparkSession, functions as F, types as T
    SPARK_AVAILABLE = True
except Exception:  # pragma: no cover
    SparkSession = None  # type: ignore
    F = None  # type: ignore
    T = None  # type: ignore
    SPARK_AVAILABLE = False


def _load_any_df(path_str: Optional[str]) -> pd.DataFrame:
    if not path_str:
        return pd.DataFrame()
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        return pd.read_json(path)
    return pd.read_parquet(path)


def _load_reviews_from_mongo(cfg: Dict[str, Any]) -> pd.DataFrame:
    if MongoClient is None:
        raise SystemExit("pymongo is not available; install it to pull reviews from MongoDB.")
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
    try:
        coll = client[database][collection]
        cursor = coll.find(query, projection)
        if limit:
            cursor = cursor.limit(limit)
        rows = list(cursor)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
    finally:
        try:
            client.close()
        except Exception:
            pass
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    return df


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


def _parse_json_arg(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception as exc:
        raise SystemExit(f"Could not parse JSON value: {value} -> {exc}")


def _experience_key(label: Any) -> Optional[str]:
    mapping = {
        "nuevo": "new",
        "new": "new",
        "intermedio": "intermediate",
        "intermediate": "intermediate",
        "experto": "expert",
        "expert": "expert",
        "veterano": "veteran",
        "veteran": "veteran",
    }
    if not label:
        return None
    return mapping.get(str(label).strip().lower())


def _prepare_reviews(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    id_col = cfg.get("review_id_column") or _ensure_column(df, ["review_id", "id", "reviewid"])
    if not id_col:
        df["review_id"] = np.arange(len(df))
        id_col = "review_id"
    df["review_id"] = df[id_col].astype(str)

    appid_col = cfg.get("appid_column") or _ensure_column(df, ["appid", "app_id", "appId"])
    if not appid_col:
        raise SystemExit("Could not find appid column in reviews dataset.")
    df["appid"] = df[appid_col].astype(str)

    text_col = cfg.get("text_column") or _ensure_column(df, ["review", "review_text", "body", "content"])
    if text_col:
        df["review_text"] = df[text_col].fillna("").astype(str)
    else:
        df["review_text"] = ""

    date_col = cfg.get("date_column") or _ensure_column(df, ["review_date", "timestamp_created", "date"])
    if not date_col:
        raise SystemExit("Could not find date column in reviews dataset.")
    df["review_date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df = df.dropna(subset=["review_date"])

    recommended_col = cfg.get("recommended_column") or _ensure_column(df, ["recommended", "voted_up", "is_positive"])
    df["recommended"] = df[recommended_col].apply(_safe_bool) if recommended_col else None

    playtime_col = cfg.get("playtime_column") or _ensure_column(df, ["playtime_at_review", "author_playtime_at_review", "author_playtime_forever"])
    df["playtime_at_review"] = df[playtime_col].apply(_safe_float) if playtime_col else None

    playtime_30d_col = cfg.get("playtime_30d_column") or _ensure_column(df, ["playtime_since_review_30d", "author_playtime_last_two_weeks"])
    if playtime_30d_col:
        df["playtime_since_review_30d"] = df[playtime_30d_col].apply(_safe_float)
        abandon_col = cfg.get("abandon_column") or _ensure_column(df, ["abandon_after_30d", "flag_abandon"])
        if abandon_col:
            df["abandon_after_30d"] = df[abandon_col].apply(_safe_bool)
        else:
            df["abandon_after_30d"] = df["playtime_since_review_30d"].apply(lambda x: x is not None and x <= 0.1)
    else:
        df["playtime_since_review_30d"] = None
        df["abandon_after_30d"] = None

    gifted_col = cfg.get("gifted_column") or _ensure_column(df, ["gifted", "steam_purchase", "received_for_free"])
    df["gifted"] = df[gifted_col].apply(_safe_bool) if gifted_col else None

    ea_col = cfg.get("early_access_column") or _ensure_column(df, ["early_access"])
    df["early_access"] = df[ea_col].apply(_safe_bool) if ea_col else None

    post_col = cfg.get("post_launch_column") or None
    if post_col and post_col in df.columns:
        df["post_launch"] = df[post_col].apply(_safe_bool)
    else:
        df["post_launch"] = None

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

    df["experience_key"] = df["experience_label"].apply(_experience_key)
    return df


def _spark_bool_expression(col_name: str) -> Any:
    lower = F.lower(F.col(col_name).cast("string"))
    return F.when(F.col(col_name).isNull(), F.lit(None).cast("boolean")) \
        .when(F.col(col_name).cast("boolean").isNotNull(), F.col(col_name).cast("boolean")) \
        .when(lower.isin("true", "t", "1", "yes", "y"), F.lit(True)) \
        .when(lower.isin("false", "f", "0", "no", "n"), F.lit(False)) \
        .otherwise(F.lit(None).cast("boolean"))


def _pick_column(columns: Sequence[str], preferred: Optional[str], fallbacks: Sequence[str]) -> Optional[str]:
    if preferred and preferred in columns:
        return preferred
    for name in fallbacks:
        if name in columns:
            return name
    return None


def _prepare_reviews_spark(
    source_path: Optional[str],
    mongo_cfg: Dict[str, Any],
    cfg: Dict[str, Any],
    spark_cfg: Dict[str, Any]
) -> pd.DataFrame:
    if not SPARK_AVAILABLE:
        raise SystemExit("Spark is not available in this environment; remove --use-spark or install pyspark.")

    builder = SparkSession.builder.appName("prepare_reviews_with_segments")
    extra_conf: Dict[str, Any] = spark_cfg.get("config", {}) if spark_cfg else {}
    for key, value in extra_conf.items():
        builder = builder.config(str(key), str(value))
    spark = builder.getOrCreate()
    try:
        if source_path and Path(source_path).exists():
            suffix = Path(source_path).suffix.lower()
            if suffix in {".parquet", ".pq"}:
                sdf = spark.read.parquet(source_path)
            elif suffix == ".csv":
                sdf = spark.read.option("header", True).csv(source_path)
            elif suffix == ".json":
                sdf = spark.read.json(source_path)
            else:
                sdf = spark.read.parquet(source_path)
        else:
            uri = mongo_cfg.get("uri")
            database = mongo_cfg.get("database") or mongo_cfg.get("db")
            collection = mongo_cfg.get("collection")
            if not uri or not database or not collection:
                raise SystemExit("Mongo configuration for Spark requires uri, database and collection.")
            reader = (
                spark.read.format("mongodb")
                .option("uri", uri)
                .option("database", database)
                .option("collection", collection)
            )
            projection = mongo_cfg.get("projection")
            if projection:
                reader = reader.option("projection", json.dumps(projection))
            pipeline = mongo_cfg.get("pipeline")
            query = mongo_cfg.get("query")
            if pipeline:
                reader = reader.option("pipeline", json.dumps(pipeline))
            elif query:
                reader = reader.option("pipeline", json.dumps([{ "$match": query }]))
            sdf = reader.load()
            limit = mongo_cfg.get("limit")
            if limit:
                sdf = sdf.limit(int(limit))

        columns = sdf.columns
        pick = lambda key, fallbacks: _pick_column(columns, cfg.get(key), fallbacks)

        review_id_col = pick("review_id_column", ["review_id", "id", "reviewid"])
        if review_id_col:
            sdf = sdf.withColumn("review_id", F.col(review_id_col).cast("string"))
        else:
            sdf = sdf.withColumn("review_id", F.monotonically_increasing_id().cast("string"))

        appid_col = pick("appid_column", ["appid", "app_id", "appId"])
        if not appid_col:
            raise SystemExit("Could not find appid column in Spark DataFrame.")
        sdf = sdf.withColumn("appid", F.col(appid_col).cast("string"))

        text_col = pick("text_column", ["review", "review_text", "body", "content"])
        if text_col:
            sdf = sdf.withColumn("review_text", F.col(text_col).cast("string"))
        else:
            sdf = sdf.withColumn("review_text", F.lit("").cast("string"))

        date_col = pick("date_column", ["review_date", "timestamp_created", "date"])
        if not date_col:
            raise SystemExit("Could not find date column in Spark DataFrame.")
        sdf = sdf.withColumn("_date_string", F.col(date_col).cast("string"))
        sdf = sdf.withColumn("_date_double", F.col(date_col).cast("double"))
        sdf = sdf.withColumn(
            "review_date",
            F.coalesce(
                F.col(date_col).cast("timestamp"),
                F.to_timestamp(F.col("_date_string")),
                F.to_timestamp(F.from_unixtime(F.col("_date_double")))
            )
        ).drop("_date_string", "_date_double")
        sdf = sdf.filter(F.col("review_date").isNotNull())

        recommended_col = pick("recommended_column", ["recommended", "voted_up", "is_positive"])
        if recommended_col:
            sdf = sdf.withColumn("recommended", _spark_bool_expression(recommended_col))
        else:
            sdf = sdf.withColumn("recommended", F.lit(None).cast("boolean"))

        playtime_col = pick("playtime_column", ["playtime_at_review", "author_playtime_at_review", "author_playtime_forever"])
        if playtime_col:
            sdf = sdf.withColumn("playtime_at_review", F.col(playtime_col).cast("double"))
        else:
            sdf = sdf.withColumn("playtime_at_review", F.lit(None).cast("double"))

        playtime_30d_col = pick("playtime_30d_column", ["playtime_since_review_30d", "author_playtime_last_two_weeks"])
        if playtime_30d_col:
            sdf = sdf.withColumn("playtime_since_review_30d", F.col(playtime_30d_col).cast("double"))
            abandon_col = pick("abandon_column", ["abandon_after_30d", "flag_abandon"])
            if abandon_col:
                sdf = sdf.withColumn("abandon_after_30d", _spark_bool_expression(abandon_col))
            else:
                sdf = sdf.withColumn(
                    "abandon_after_30d",
                    F.when(F.col("playtime_since_review_30d").isNull(), F.lit(None).cast("boolean"))
                     .when(F.col("playtime_since_review_30d") <= F.lit(0.1), F.lit(True))
                     .otherwise(F.lit(False))
                )
        else:
            sdf = sdf.withColumn("playtime_since_review_30d", F.lit(None).cast("double"))
            sdf = sdf.withColumn("abandon_after_30d", F.lit(None).cast("boolean"))

        gifted_col = pick("gifted_column", ["gifted", "steam_purchase", "received_for_free"])
        if gifted_col:
            sdf = sdf.withColumn("gifted", _spark_bool_expression(gifted_col))
        else:
            sdf = sdf.withColumn("gifted", F.lit(None).cast("boolean"))

        ea_col = pick("early_access_column", ["early_access"])
        if ea_col:
            sdf = sdf.withColumn("early_access", _spark_bool_expression(ea_col))
        else:
            sdf = sdf.withColumn("early_access", F.lit(None).cast("boolean"))

        post_col = pick("post_launch_column", ["post_launch"])
        if post_col:
            sdf = sdf.withColumn("post_launch", _spark_bool_expression(post_col))
        else:
            sdf = sdf.withColumn("post_launch", F.lit(None).cast("boolean"))

        median_col = cfg.get("median_playtime_column") or "median_playtime_app"
        if median_col in columns:
            sdf = sdf.withColumn("median_playtime_app", F.col(median_col).cast("double"))
        else:
            median_df = (
                sdf.groupBy("appid")
                .agg(F.expr("percentile_approx(playtime_at_review, 0.5, 100)").alias("median_playtime_app"))
            )
            sdf = sdf.join(median_df, on="appid", how="left")

        if experiencia_jugador is not None:
            experiencia_udf = F.udf(
                lambda play, median: experiencia_jugador(play, median),
                T.StringType()
            )
            sdf = sdf.withColumn("experience_label", experiencia_udf(F.col("playtime_at_review"), F.col("median_playtime_app")))
        else:
            sdf = sdf.withColumn("experience_label", F.lit(None).cast("string"))

        sdf = sdf.withColumn(
            "experience_key",
            F.when(F.col("experience_label").isNull(), F.lit(None).cast("string"))
             .otherwise(
                 F.when(F.lower(F.col("experience_label")) == "nuevo", F.lit("new"))
                  .when(F.lower(F.col("experience_label")) == "intermedio", F.lit("intermediate"))
                  .when(F.lower(F.col("experience_label")) == "experto", F.lit("expert"))
                  .when(F.lower(F.col("experience_label")) == "veterano", F.lit("veteran"))
                  .otherwise(F.lower(F.col("experience_label")))
             )
        )

        partitions = spark_cfg.get("partitions") or spark_cfg.get("num_partitions")
        partition_column = spark_cfg.get("partition_column", "appid")
        if partitions:
            try:
                partitions = int(partitions)
                if partition_column in sdf.columns:
                    sdf = sdf.repartition(partitions, F.col(partition_column))
                else:
                    sdf = sdf.repartition(partitions)
            except Exception:
                pass

        target_columns = [
            "appid",
            "review_id",
            "review_date",
            "review_text",
            "recommended",
            "playtime_at_review",
            "playtime_since_review_30d",
            "abandon_after_30d",
            "gifted",
            "early_access",
            "post_launch",
            "median_playtime_app",
            "experience_label",
            "experience_key",
        ]
        for col in target_columns:
            if col not in sdf.columns:
                if col in {"review_text", "experience_label", "experience_key"}:
                    sdf = sdf.withColumn(col, F.lit(None).cast("string"))
                elif col == "review_date":
                    sdf = sdf.withColumn(col, F.lit(None).cast("timestamp"))
                elif col in {"recommended", "abandon_after_30d", "gifted", "early_access", "post_launch"}:
                    sdf = sdf.withColumn(col, F.lit(None).cast("boolean"))
                else:
                    sdf = sdf.withColumn(col, F.lit(None).cast("double"))

        pdf = sdf.select(*target_columns).toPandas()
        return pdf
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _write_output(df: pd.DataFrame, path: str) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "appid", "review_id", "review_date", "review_text", "recommended",
        "playtime_at_review", "playtime_since_review_30d", "abandon_after_30d",
        "gifted", "early_access", "post_launch", "median_playtime_app",
        "experience_label", "experience_key"
    ]
    if df.empty:
        if out_path.suffix.lower() == ".json":
            out_path.write_text("[]", encoding="utf-8")
        elif out_path.suffix.lower() == ".csv":
            pd.DataFrame(columns=columns).to_csv(out_path, index=False)
        else:
            pd.DataFrame(columns=columns).to_parquet(out_path, index=False)
        print(f"[WARN] Empty review dataset written to {out_path}")
        return
    if out_path.suffix.lower() == ".json":
        df.to_json(out_path, orient="records", date_format="iso")
    elif out_path.suffix.lower() == ".csv":
        df.to_csv(out_path, index=False)
    else:
        df.to_parquet(out_path, index=False)
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
    if df.empty or "review_text" not in df.columns:
        _fallback_topics(df, topics_out)
        return
    if not BER_TOPIC_AVAILABLE:
        print("[WARN] BERTopic not available; using fallback topics.")
        _fallback_topics(df, topics_out)
        return
    language = cfg.get("bertopic_language", "multilingual")
    min_topic_size = cfg.get("bertopic_min_topic_size", 20)
    topic_model = BERTopic(language=language, min_topic_size=min_topic_size, verbose=False)
    texts = df["review_text"].tolist()
    topics, probs = topic_model.fit_transform(texts)
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
    parser.add_argument("--use-spark", action="store_true", help="Use Spark to load and preprocess reviews before pandas.")
    parser.add_argument("--spark-partitions", type=int, help="Number of Spark partitions when repartitioning by appid.")
    parser.add_argument("--spark-partition-column", default="appid", help="Column used to repartition in Spark mode.")
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

    spark_cfg: Dict[str, Any] = dict(cfg.get("spark") or {})
    if args.spark_partitions is not None:
        spark_cfg["partitions"] = args.spark_partitions
    if args.spark_partition_column:
        spark_cfg["partition_column"] = args.spark_partition_column

    use_spark = args.use_spark or bool(spark_cfg.get("enabled"))

    if use_spark:
        reviews_df = _prepare_reviews_spark(reviews_source, mongo_cfg, cfg, spark_cfg)
    else:
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
