#!/usr/bin/env python
from __future__ import annotations
"""Genera un perfil tem?tico por cl?ster usando BERTopic."""

import argparse
from pathlib import Path
from typing import Any, Dict, List

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
        raise FileNotFoundError(f"No se encontr? clusters.parquet en {path}")
    df = read_parquet_any(path)
    if df.empty:
        raise SystemExit("El fichero de cl?steres est? vac?o.")
    if "cluster_id" not in df.columns or "appid" not in df.columns:
        raise SystemExit("clusters.parquet debe incluir columnas 'cluster_id' y 'appid'.")
    df["cluster_id"] = df["cluster_id"].astype(str)
    df["appid"] = df["appid"].astype(str)
    return df


def _load_metadata(path: str, text_columns: List[str]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No se encontr? metadata en {path}")
    df = read_parquet_any(path) if p.suffix in {".parquet", ".pq"} or p.is_dir() else pd.read_csv(path)
    if df.empty:
        raise SystemExit("El fichero de metadata est? vac?o.")
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Perfila cada cl?ster con un tema principal usando BERTopic.")
    parser.add_argument("--clusters", default="data/processed/clusters.parquet", help="Ruta a clusters.parquet.")
    parser.add_argument("--metadata", default="data/processed/game_metadata.parquet", help="Metadata con campos de texto.")
    parser.add_argument("--text-columns", nargs="*", default=DEFAULT_TEXT_COLUMNS, help="Columnas de texto a concatenar.")
    parser.add_argument("--out", default="outputs/clustering/cluster_topics.json", help="Salida JSON.")
    parser.add_argument("--min-docs", type=int, default=30, help="M?nimo de textos por cl?ster para ejecutar BERTopic.")
    parser.add_argument("--min-chars", type=int, default=100, help="Longitud m?nima de cada documento.")
    parser.add_argument("--embedding-model", default="paraphrase-multilingual-MiniLM-L12-v2", help="Modelo de SentenceTransformer.")
    parser.add_argument("--min-topic-size", type=int, default=10, help="min_topic_size de BERTopic.")
    parser.add_argument("--top-n-words", type=int, default=8, help="N?mero de palabras clave a reportar por cl?ster.")
    parser.add_argument("--max-clusters", type=int, default=None, help="Limita el n?mero de cl?steres (para pruebas).")
    args = parser.parse_args()

    clusters_df = _load_clusters(args.clusters)
    metadata_df = _load_metadata(args.metadata, args.text_columns)
    merged = clusters_df.merge(metadata_df, on="appid", how="left", suffixes=("", "_meta"))

    embedding_model = SentenceTransformer(args.embedding_model)
    vectorizer = CountVectorizer(stop_words="english", min_df=3)

    results: List[Dict[str, Any]] = []
    cluster_ids = merged["cluster_id"].unique()
    if args.max_clusters:
        cluster_ids = cluster_ids[: args.max_clusters]

    for cid in cluster_ids:
        subset = merged[merged["cluster_id"] == cid]
        documents = _build_documents(subset, args.text_columns, args.min_chars)
        if len(documents) < args.min_docs:
            continue
        try:
            topic_model = BERTopic(
                embedding_model=embedding_model,
                vectorizer_model=vectorizer,
                min_topic_size=args.min_topic_size,
                calculate_probabilities=False,
                verbose=False,
            )
            topics, _ = topic_model.fit_transform(documents)
            info = topic_model.get_topic_info()
            info = info[info.Topic >= 0]
            if info.empty:
                continue
            top_row = info.iloc[0]
            topic_id = int(top_row.Topic)
            keywords = _topic_keywords(topic_model, topic_id, args.top_n_words)
            summary = {
                "cluster_id": cid,
                "topic_id": topic_id,
                "repr_docs": int(top_row['Count']),
                "keywords": keywords,
                "name": top_row.get('Name'),
            }
            results.append(summary)
        except Exception as exc:
            print(f"[WARN] BERTopic fall? para cluster {cid}: {exc}")
            continue

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    write_json_any(results, args.out, indent=2)
    print(f"[OK] Perfil de t?picos guardado en {args.out} ({len(results)} cl?steres).")


if __name__ == "__main__":
    main()
