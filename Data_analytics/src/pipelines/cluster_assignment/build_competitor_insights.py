#!/usr/bin/env python
from __future__ import annotations
"""Construye insights de vecinos tras asignar un juego a un cl?ster."""

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from src.utils.io import read_parquet_any, write_json_any
from src.pipelines.cluster_assignment.history_analysis import (
    StorytellingConfig,
    TimeSeriesConfig,
    SocialConfig,
    build_story,
    compute_activity_metrics,
    categorise_competitor,
)

SIM_COLUMNS = [
    "similarity",
    "sim_to_centroid",
    "sim_to_medoid",
    "sim_to_cluster",
    "cosine_sim",
]


def _load_clusters(path: str, cluster_id: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"No se encontr? clusters parquet en {path}")
    df = read_parquet_any(path)
    if df.empty:
        raise SystemExit("El fichero de cl?steres est? vac?o.")
    if "cluster_id" not in df.columns:
        raise SystemExit("clusters.parquet debe contener la columna cluster_id")
    df["cluster_id"] = df["cluster_id"].astype(str)
    df["appid"] = df["appid"].astype(str)
    return df[df["cluster_id"] == str(cluster_id)].copy()


def _similarity_from_row(row: pd.Series) -> float:
    for col in SIM_COLUMNS:
        if col in row and pd.notna(row[col]):
            try:
                return float(row[col])
            except Exception:
                continue
    return 0.5  # valor neutro si no hay similitud disponible


def _launch_overlap(ts: pd.DataFrame, launch_date: pd.Timestamp, window_days: int = 14) -> bool:
    if ts.empty or launch_date is None:
        return False
    start = launch_date - pd.Timedelta(days=window_days)
    end = launch_date + pd.Timedelta(days=window_days)
    df = ts.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    window = df[(df["date"] >= start) & (df["date"] <= end)]["value"]
    if window.empty:
        return False
    max_val = df["value"].max()
    if not max_val:
        return False
    return float(window.max()) >= 0.25 * float(max_val)


def _today_score(similarity: float, metrics: Dict[str, float], launch_overlap: bool) -> float:
    activity = 0.6 * metrics.get("r30", 0.0) + 0.4 * metrics.get("r7", 0.0)
    return 0.5 * similarity + 0.35 * activity + 0.15 * (1.0 if launch_overlap else 0.0)


def _recent_score(similarity: float, metrics: Dict[str, float]) -> float:
    return 0.6 * similarity + 0.4 * (0.7 * metrics.get("r90", 0.0) + 0.3 * metrics.get("d90", 0.0))


def _parse_date_arg(value: str | None) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() in {"none", "null"}:
        return None
    return pd.to_datetime(value)


def main() -> None:
    ap = argparse.ArgumentParser(description="Genera insights narrativos por competidor dentro de un cl?ster.")
    ap.add_argument("--cluster-id", required=True, help="ID de cl?ster a analizar.")
    ap.add_argument("--clusters", default="data/processed/clusters.parquet", help="Ruta al parquet de cl?steres.")
    ap.add_argument("--players", default="data/warehouse/players_monthly.parquet", help="Timeseries de jugadores.")
    ap.add_argument("--reviews", default=None, help="Dataset de rese?as con texto para BERTopic.")
    ap.add_argument("--events", default="outputs/events/events.parquet", help="Eventos precomputados (z-score, etc.).")
    ap.add_argument("--topics", default="outputs/events/topics.parquet", help="Tópicos BERTopic por evento precomputados.")
    ap.add_argument("--granger-summary", default="outputs/ccf_analysis/summary.parquet", help="Resultados de CCF/Granger.")
    ap.add_argument("--twitch", default=None, help="Dataset social de Twitch (opcional).")
    ap.add_argument("--youtube", default=None, help="Dataset social de YouTube (opcional).")
    ap.add_argument("--launch-date", default=None, help="Fecha de lanzamiento del cliente (YYYY-MM-DD).")
    ap.add_argument("--out", required=True, help="Ruta del JSON de salida.")
    ap.add_argument("--topic-window", type=int, default=14, help="Ventana ?d?as para t?picos.")
    ap.add_argument("--peak-window", type=int, default=30, help="Ventana de detecci?n de picos.")
    ap.add_argument("--min-topic-docs", type=int, default=300, help="M?nimo de rese?as para ejecutar BERTopic.")
    ap.add_argument("--bertopic-model", default="paraphrase-multilingual-MiniLM-L12-v2", help="Modelo de embeddings para BERTopic.")
    ap.add_argument("--start-date", default=None, help="Fecha de inicio de la ventana reciente (YYYY-MM-DD, opcional).")
    ap.add_argument("--end-date", default=None, help="Fecha de fin de la ventana reciente (YYYY-MM-DD, opcional).")
    ap.add_argument("--min-points", type=int, default=90, help="Número mínimo de observaciones diarias para usar la ventana reciente.")
    args = ap.parse_args()

    launch_str = (args.launch_date or '').strip() if args.launch_date else ''
    launch_date = None
    if launch_str and launch_str.lower() not in {'null', 'none'}:
        launch_date = pd.to_datetime(launch_str)
    start_date = _parse_date_arg(args.start_date)
    end_date = _parse_date_arg(args.end_date)
    if start_date and end_date and end_date < start_date:
        raise SystemExit('La ventana reciente debe tener end_date >= start_date.')
    cluster_id = str(args.cluster_id)
    clusters_df = _load_clusters(args.clusters, cluster_id)
    if clusters_df.empty:
        raise SystemExit(f"No se encontraron juegos para cluster_id={cluster_id}")

    storytelling_cfg = StorytellingConfig(
        ts=TimeSeriesConfig(
            players_path=args.players,
            reviews_path=args.reviews,
        ),
        social=SocialConfig(twitch_path=args.twitch, youtube_path=args.youtube) if args.twitch or args.youtube else None,
        events_path=args.events,
        topics_path=args.topics,
        granger_summary_path=args.granger_summary,
        t_ref=launch_date,
        start_date=start_date,
        end_date=end_date,
        min_points=args.min_points,
        peak_window_days=args.peak_window,
        topic_window_days=args.topic_window,
        min_topic_docs=args.min_topic_docs,
        bertopic_model=args.bertopic_model,
    )

    records_today, records_recent, records_historical = [], [], []

    for _, row in clusters_df.iterrows():
        appid = str(row["appid"])
        name = row.get("name")
        about = row.get("about")
        story = build_story(storytelling_cfg, appid)
        ts_global = pd.DataFrame(story.get("activity_timeseries", []))
        ts_recent = pd.DataFrame(story.get("activity_timeseries_recent", []))
        metrics_global = story.get("metrics") or compute_activity_metrics(ts_global, launch_date)
        metrics_recent = story.get("metrics_recent")
        recent_info = story.get("recent") or {}
        use_recent = bool((start_date or end_date) and recent_info and recent_info.get("status") == "ok" and metrics_recent)
        metrics_for_category = metrics_recent if use_recent else metrics_global
        similarity = _similarity_from_row(row)
        overlap_ts = ts_recent if use_recent and not ts_recent.empty else ts_global
        launch_overlap_flag = _launch_overlap(overlap_ts, launch_date) if launch_date is not None else False
        category = categorise_competitor(metrics_for_category, launch_overlap=launch_overlap_flag, launch_mode=bool(launch_date))
        scores = {
            "today": _today_score(similarity, metrics_for_category, launch_overlap_flag),
            "recent": _recent_score(similarity, metrics_for_category),
        }
        record = {
            "appid": appid,
            "name": name,
            "about": about,
            "category": category,
            "metrics": metrics_for_category,
            "metrics_global": metrics_global,
            "metrics_recent": metrics_recent,
            "scores": scores,
            "similarity": similarity,
            "launch_overlap": launch_overlap_flag,
            "story": story,
            "takeaways": story.get("takeaways", []),
        }
        if category == "today":
            records_today.append(record)
        elif category == "recent":
            records_recent.append(record)
        else:
            records_historical.append(record)


    records_today.sort(key=lambda r: r["scores"]["today"], reverse=True)
    records_recent.sort(key=lambda r: r["scores"]["recent"], reverse=True)
    records_historical.sort(key=lambda r: r["scores"]["recent"], reverse=True)

    window_info = {
        "start": start_date.strftime("%Y-%m-%d") if start_date is not None else None,
        "end": end_date.strftime("%Y-%m-%d") if end_date is not None else None,
        "min_points": args.min_points,
    }
    output = {
        "cluster_id": cluster_id,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window": window_info,
        "competitors": {
            "today": records_today,
            "recent": records_recent,
            "historical": records_historical,
        },
    }

    write_json_any(output, args.out, indent=2)
    print(f"[OK] Insights guardados en {args.out}")


if __name__ == "__main__":
    main()
