#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Consume outputs/events/topics_prep.parquet y modela tópicos por (appid, event_year_month) usando BERTopic en paralelo con Ray.
Salida: outputs/events/topics.parquet (compatible con el pipeline actual).
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml
import pandas as pd
import ray
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer

from src.utils.io import read_parquet_any, write_parquet_any, makedirs_if_local


@ray.remote
def _model_topics_for_event(appid: str, event_ym: str, texts: list[str], cfg: dict):
    topic_cfg = cfg.get('bertopic', {})
    embedding_model = SentenceTransformer(topic_cfg.get('embedding_model', 'all-MiniLM-L6-v2'))
    vectorizer_model = CountVectorizer(stop_words="english", min_df=topic_cfg.get('min_df', 5))
    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        min_topic_size=topic_cfg.get('min_topic_size', 15),
        verbose=False
    )
    try:
        topics, _ = topic_model.fit_transform(texts)
        topic_info = topic_model.get_topic_info()
        top_n = topic_cfg.get('top_n_topics', 3)
        main_topics = topic_info[topic_info.Topic != -1].head(top_n)
        return {
            'appid': str(appid),
            'event_year_month': pd.to_datetime(event_ym),
            'topics': main_topics.to_dict(orient='records') if not main_topics.empty else []
        }
    except Exception as e:
        return {'appid': str(appid), 'event_year_month': pd.to_datetime(event_ym), 'topics': [], 'error': str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r'))
    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)
    prep_path = outdir / 'topics_prep.parquet'
    if not prep_path.exists():
        raise SystemExit('No existe outputs/events/topics_prep.parquet. Ejecuta topics_prep_spark primero.')

    df = read_parquet_any(prep_path)
    if df.empty:
        write_parquet_any(pd.DataFrame(), outdir / 'topics.parquet')
        return
    tasks = []
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    for _, row in df.iterrows():
        texts = [t for t in (row.get('texts') or []) if isinstance(t, str) and len(t) > 0]
        if not texts:
            continue
        tasks.append(_model_topics_for_event.remote(str(row['appid']), str(row['event_year_month']), texts, cfg))
    results = ray.get(tasks)
    ray.shutdown()
    out = pd.DataFrame([r for r in results if r is not None])
    write_parquet_any(out, outdir / 'topics.parquet')
    print(f"[OK] Tópicos generados en -> {outdir / 'topics.parquet'}")


if __name__ == '__main__':
    main()

