#!/usr/bin/env python
from __future__ import annotations
"""Genera un perfil tematico por cluster usando BERTopic con tracking opcional en MLflow."""

import argparse
from pathlib import Path
from typing import Any, Dict, List

import mlflow
import pandas as pd

from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer

from src.utils.io import read_parquet_any, write_json_any


DEFAULT_TEXT_COLUMNS = [
    "name",
    "short_description",
    "detailed_description",
]


def _load_clusters(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No se encontro clusters.parquet en {path}")
    df = read_parquet_any(path)
    if df.empty:
        raise SystemExit("El fichero de clusters esta vacio.")
    if "cluster_id" not in df.columns or "appid" not in df.columns:
        raise SystemExit("clusters.parquet debe incluir columnas 'cluster_id' y 'appid'.")
    df["cluster_id"] = df["cluster_id"].astype(str)
    df["appid"] = df["appid"].astype(str)
    return df


def _load_metadata(path: str, text_columns: List[str]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No se encontro metadata en {path}")
    df = read_parquet_any(path) if p.suffix in {".parquet", ".pq"} or p.is_dir() else pd.read_csv(path)
    if df.empty:
        raise SystemExit("El fichero de metadata esta vacio.")
    if "appid" not in df.columns:
        raise SystemExit("La metadata debe contener columna 'appid'.")
    missing = [col for col in text_columns if col not in df.columns]
    if len(missing) == len(text_columns):
        raise SystemExit("La metadata no contiene ninguna de las columnas de texto especificadas.")
    df = df.dropna(subset=["appid"]).copy()
    df["appid"] = df["appid"].astype(str)
    return df


def _build_documents(df: pd.DataFrame, text_cols: List[str], min_chars: int) -> List[str]:
    docs: List[str] = []
    for _, row in df.iterrows():
        parts = []
        for col in text_cols:
            if col in row and isinstance(row[col], str):
                val = row[col].strip()
                if val:
                    parts.append(val)
        if not parts:
            continue
        doc = " ".join(parts)
        if len(doc) >= min_chars:
            docs.append(doc)
    return docs


def _topic_keywords(topic_model: BERTopic, topic_id: int, top_n: int) -> List[str]:
    words = topic_model.get_topic(topic_id)
    if not words:
        return []
    return [w for w, _ in words[:top_n]]


def _start_mlflow_run(args: argparse.Namespace) -> bool:
    if not args.mlflow_enabled:
        return False
    if args.mlflow_tracking_uri:
        mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    if args.mlflow_experiment:
        mlflow.set_experiment(args.mlflow_experiment)
    mlflow.start_run(run_name=args.mlflow_run_name)
    mlflow.log_params(
        {
            "embedding_model": args.embedding_model,
            "min_topic_size": args.min_topic_size,
            "min_docs": args.min_docs,
            "min_chars": args.min_chars,
            "top_n_words": args.top_n_words,
            "max_clusters": args.max_clusters if args.max_clusters is not None else "all",
        }
    )
    return True


def _log_mlflow_metrics(results: List[Dict[str, Any]],
                        clusters_considered: int,
                        skipped_min_docs: int,
                        failed_clusters: int,
                        out_path: Path) -> None:
    if not mlflow.active_run():
        return
    clusters_profiled = len(results)
    clusters_with_docs = clusters_considered - skipped_min_docs
    mlflow.log_metric("clusters_considered", float(clusters_considered))
    mlflow.log_metric("clusters_with_docs", float(clusters_with_docs))
    mlflow.log_metric("clusters_profiled", float(clusters_profiled))
    mlflow.log_metric("clusters_skipped_min_docs", float(skipped_min_docs))
    mlflow.log_metric("clusters_failed", float(failed_clusters))

    repr_docs = [r.get("repr_docs") for r in results if isinstance(r.get("repr_docs"), int)]
    if repr_docs:
        count = float(len(repr_docs))
        total = float(sum(repr_docs))
        mlflow.log_metric("avg_repr_docs", total / count)
        mlflow.log_metric("min_repr_docs", float(min(repr_docs)))
        mlflow.log_metric("max_repr_docs", float(max(repr_docs)))

    keywords_lengths = [len(r.get("keywords", [])) for r in results]
    if keywords_lengths:
        mlflow.log_metric("avg_keywords_reported", float(sum(keywords_lengths)) / len(keywords_lengths))

    if out_path.exists():
        mlflow.log_artifact(str(out_path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Perfila cada cluster con un tema principal usando BERTopic.")
    parser.add_argument("--clusters", default="data/processed/clusters.parquet", help="Ruta a clusters.parquet.")
    parser.add_argument("--metadata", default="data/processed/game_metadata.parquet", help="Metadata con campos de texto.")
    parser.add_argument("--text-columns", nargs="*", default=DEFAULT_TEXT_COLUMNS, help="Columnas de texto a concatenar.")
    parser.add_argument("--out", default="outputs/clustering/cluster_topics.json", help="Salida JSON.")
    parser.add_argument("--min-docs", type=int, default=30, help="Minimo de textos por cluster para ejecutar BERTopic.")
    parser.add_argument("--min-chars", type=int, default=100, help="Longitud minima de cada documento.")
    parser.add_argument("--embedding-model", default="paraphrase-multilingual-MiniLM-L12-v2", help="Modelo de SentenceTransformer.")
    parser.add_argument("--min-topic-size", type=int, default=10, help="min_topic_size de BERTopic.")
    parser.add_argument("--top-n-words", type=int, default=8, help="Numero de palabras clave a reportar por cluster.")
    parser.add_argument("--max-clusters", type=int, default=None, help="Limita el numero de clusters (para pruebas).")
    parser.add_argument("--mlflow-enabled", action="store_true", help="Activa el seguimiento en MLflow.")
    parser.add_argument("--mlflow-experiment", default=None, help="Nombre del experimento MLflow.")
    parser.add_argument("--mlflow-run-name", default="cluster_topics_profile", help="Nombre del run en MLflow.")
    parser.add_argument("--mlflow-tracking-uri", default=None, help="Tracking URI de MLflow.")
    args = parser.parse_args()

    run_active = _start_mlflow_run(args)

    try:
        clusters_df = _load_clusters(args.clusters)
        metadata_df = _load_metadata(args.metadata, args.text_columns)
        merged = clusters_df.merge(metadata_df, on="appid", how="left", suffixes=("", "_meta"))

        embedding_model = SentenceTransformer(args.embedding_model)
        vectorizer = CountVectorizer(stop_words="english", min_df=3)

        results: List[Dict[str, Any]] = []
        cluster_ids = merged["cluster_id"].unique()
        if args.max_clusters:
            cluster_ids = cluster_ids[: args.max_clusters]

        skipped_min_docs = 0
        failed_clusters = 0

        for cid in cluster_ids:
            subset = merged[merged["cluster_id"] == cid]
            documents = _build_documents(subset, args.text_columns, args.min_chars)
            if len(documents) < args.min_docs:
                skipped_min_docs += 1
                continue
            try:
                topic_model = BERTopic(
                    embedding_model=embedding_model,
                    vectorizer_model=vectorizer,
                    min_topic_size=args.min_topic_size,
                    calculate_probabilities=False,
                    verbose=False,
                )
                topic_model.fit_transform(documents)
                info = topic_model.get_topic_info()
                info = info[info.Topic >= 0]
                if info.empty:
                    failed_clusters += 1
                    continue
                top_row = info.iloc[0]
                topic_id = int(top_row.Topic)
                keywords = _topic_keywords(topic_model, topic_id, args.top_n_words)
                summary = {
                    "cluster_id": cid,
                    "topic_id": topic_id,
                    "repr_docs": int(top_row["Count"]),
                    "keywords": keywords,
                    "name": top_row.get("Name"),
                }
                results.append(summary)
            except Exception as exc:  # noqa: BLE001
                failed_clusters += 1
                print(f"[WARN] BERTopic fallo para cluster {cid}: {exc}")
                continue

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_any(results, out_path, indent=2)
        print(f"[OK] Perfil de topicos guardado en {out_path} ({len(results)} clusters).")

        if run_active:
            _log_mlflow_metrics(results, len(cluster_ids), skipped_min_docs, failed_clusters, out_path)
    finally:
        if run_active and mlflow.active_run():
            mlflow.end_run()


if __name__ == "__main__":
    main()
