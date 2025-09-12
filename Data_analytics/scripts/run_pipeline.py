#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Runner simple para disparar el pipeline en modo Local o Big Data.

Uso:
  python scripts/run_pipeline.py --mode local   [--appid 12345]
  python scripts/run_pipeline.py --mode bigdata [--appid 12345] [--data-root gs://bucket/prefix]

Notas:
- Local: usa DVC para orquestar (stages: embeddings, clustering, events, topics, news_classifier, enrich, ccf, report, editor_view)
- Big Data: usa scripts directos con variantes Spark (clustering_spark.yaml, events_spark, ccf_spark, topics_spark_prep + topics_ray)
  y ejecuta preaggregations antes de eventos.

Puedes definir DATA_ROOT para escribir/leer en gs:// o s3://.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys


def run(cmd: str, env: dict | None = None) -> None:
    print(f"$ {cmd}")
    res = subprocess.run(cmd, shell=True, env=env)
    if res.returncode != 0:
        sys.exit(res.returncode)


def run_local(appid: str | None, env: dict | None) -> None:
    # Orquestación vía DVC: etapas principales + report/editor_view
    stages = [
        "embeddings",
        "clustering",
        "events",
        "topics",
        "news_classifier",
        "enrich",
        "ccf",
    ]
    run("dvc repro " + " ".join(stages), env)
    # Reporte por juego (si se especifica appid) o usa el de params.yaml
    if appid:
        run(f"python src/insights/build_game_report.py --config configs/events.yaml --appid {appid} --top_k 15", env)
        run(f"python src/insights/compose_editor_payload.py --appid {appid}", env)
    else:
        run("dvc repro report editor_view", env)


def run_bigdata(appid: str | None, env: dict | None) -> None:
    # 1) Embeddings (igual config)
    run("python src/pipelines/generate_embeddings.py --config configs/embeddings.yaml", env)

    # 2) Clustering Spark (config dedicada)
    run("python src/pipelines/run_clustering.py --config configs/clustering_spark.yaml", env)

    # 3) Preaggregations (reviews, players)
    run("python src/pipelines/preaggregations/reviews_monthly.py --config configs/ccf_analysis.yaml", env)
    run("python src/pipelines/preaggregations/players_monthly.py --players_dir data/external/players --out data/warehouse/players_monthly.parquet", env)

    # 4) Eventos Spark y tópicos (Spark prep + Ray)
    run("python src/pipelines/event_detection/events_spark.py --config configs/events.yaml", env)
    run("python src/insights/topics_prep_spark.py --config configs/events.yaml", env)
    run("python src/insights/topics_from_prep_ray.py --config configs/events.yaml", env)

    # 5) Clasificador de noticias + Enriquecimiento
    run("python src/insights/news_classifier.py --config configs/events.yaml", env)
    run("python src/pipelines/event_detection/enrich_events.py --config configs/events.yaml", env)

    # 6) CCF Spark
    run("python src/pipelines/ccf_analysis/ccf_spark.py --config configs/ccf_analysis.yaml", env)

    # 7) Reportes
    if appid:
        run(f"python src/insights/build_game_report.py --config configs/events.yaml --appid {appid} --top_k 15", env)
        run(f"python src/insights/compose_editor_payload.py --appid {appid}", env)
    else:
        # por defecto usa APPID de params.yaml cuando se ejecuta vía DVC; aquí llamamos directo
        run("python src/insights/build_game_report.py --config configs/events.yaml --top_k 15", env)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "bigdata"], required=True)
    ap.add_argument("--appid", help="AppID para generar el reporte específico", default=None)
    ap.add_argument("--data-root", help="Prefijo de almacenamiento (gs://, s3://, o ruta local)", default=None)
    args = ap.parse_args()

    env = os.environ.copy()
    if args.data_root:
        env["DATA_ROOT"] = args.data_root

    if args.mode == "local":
        run_local(args.appid, env)
    else:
        run_bigdata(args.appid, env)


if __name__ == "__main__":
    main()

