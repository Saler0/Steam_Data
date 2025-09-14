#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Etiqueta tópicos de BERTopic con un LLM a partir de outputs/events/topics.parquet.

Se separa de news_classifier para permitir paralelizar la clasificación de noticias
sin depender del modelado de tópicos.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml
import mlflow

from src.utils.io import read_parquet_any, write_parquet_any
from src.insights.news_classifier import label_topics as _label_topics


def main() -> None:
    ap = argparse.ArgumentParser(description="Etiqueta tópicos de BERTopic con LLM")
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML (events.yaml)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r', encoding='utf-8'))

    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)

    topics_input_path = cfg.get('topics_input_path') or str(outdir / 'topics.parquet')
    topics_path = Path(topics_input_path)
    if not topics_path.exists():
        raise FileNotFoundError(f"No se encontró topics.parquet en {topics_path}. Ejecuta 'topics' primero.")

    topics_df = read_parquet_any(topics_path)
    if topics_df.empty:
        print("[INFO] topics.parquet está vacío. Nada que etiquetar.")
        write_parquet_any(topics_df, outdir / 'topics_labeled.parquet')
        return

    llm_cfg = cfg.get('llm', {})

    mlflow.set_experiment(llm_cfg.get('mlflow_experiment') or cfg.get('mlflow', {}).get('experiment', 'Steam Analytics'))
    with mlflow.start_run(run_name='label_topics'):
        labeled = _label_topics(topics_df, llm_cfg)
        out_path = outdir / 'topics_labeled.parquet'
        write_parquet_any(labeled, out_path)
        mlflow.log_artifact(str(out_path), artifact_path='topics')
        print(f"[OK] Tópicos etiquetados guardados en -> {out_path}")


if __name__ == '__main__':
    main()

