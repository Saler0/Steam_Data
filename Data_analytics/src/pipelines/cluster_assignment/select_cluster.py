#!/usr/bin/env python
from __future__ import annotations

"""Utility script to pick the most suitable cluster for downstream workflows."""

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import (
    read_csv_any,
    read_json_any,
    read_parquet_any,
    write_json_any,
)


class ColumnNotFoundError(RuntimeError):
    """Raised when a required metric column is missing in stats."""


def _load_frame(path: Any) -> pd.DataFrame:
    path_str = str(path)
    if path_str.endswith('.csv'):
        return read_csv_any(path)
    if path_str.endswith('.json'):
        return read_json_any(path)
    return read_parquet_any(path)


def _resolve_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for name in candidates:
        if name in df.columns:
            return name
    raise ColumnNotFoundError(f"Stats file is missing any of columns: {', '.join(candidates)}")


def _filter_candidates(
    stats: pd.DataFrame,
    density_col: str,
    size_col: str,
    borderline_col: str | None,
    *,
    min_density: float,
    min_size: int,
    max_size: int,
    max_borderline: float | None,
) -> pd.DataFrame:
    candidates = stats.copy()
    candidates = candidates[candidates[density_col] >= min_density]
    candidates = candidates[(candidates[size_col] >= min_size) & (candidates[size_col] <= max_size)]
    if borderline_col and max_borderline is not None and borderline_col in candidates.columns:
        candidates = candidates[candidates[borderline_col] <= max_borderline]
    return candidates


def _pick_best_cluster(
    stats: pd.DataFrame,
    density_col: str,
    size_col: str,
    borderline_col: str | None,
) -> pd.Series:
    sort_cols: list[str] = [density_col, size_col]
    ascending = [False, True]
    if borderline_col and borderline_col in stats.columns:
        sort_cols.append(borderline_col)
        ascending.append(True)
    ordered = stats.sort_values(sort_cols, ascending=ascending, kind="mergesort")
    return ordered.iloc[0]


def _infer_title_column(labels: pd.DataFrame) -> str | None:
    for name in ["title", "name", "app_name", "app_title"]:
        if name in labels.columns:
            return name
    return None


def _safe_sample(series: pd.Series, n: int) -> list[str]:
    values = series.dropna().astype(str).unique()
    if not len(values):
        return []
    n = min(n, len(values))
    if len(values) <= n:
        return sorted(values.tolist()) if hasattr(values, 'tolist') else sorted(values)
    sampled = pd.Series(values).sample(n=n, random_state=42).tolist()
    return sorted(str(v) for v in sampled)


def load_labels(path: Any) -> pd.DataFrame:
    labels = _load_frame(path)
    required = {"cluster_id", "appid"}
    missing = required.difference(labels.columns)
    if missing:
        raise SystemExit(f"Labels file must contain columns {sorted(missing)}.")
    labels["cluster_id"] = labels["cluster_id"].astype(str)
    labels["appid"] = labels["appid"].astype(str)
    return labels


def load_stats(path: Any) -> pd.DataFrame:
    stats = _load_frame(path)
    if "cluster_id" not in stats.columns:
        raise SystemExit("Stats file must contain column 'cluster_id'.")
    stats["cluster_id"] = stats["cluster_id"].astype(str)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Select the best performing cluster according to density and size rules.")
    parser.add_argument("--labels", required=True, help="Path to clusters.parquet with columns appid, cluster_id, and optionally title.")
    parser.add_argument("--stats", required=True, help="Path to cluster_stats.csv with metrics per cluster_id.")
    parser.add_argument("--outdir", default="outputs/clustering/selection", help="Directory where selected_cluster.json and appids_selected.txt will be stored.")
    parser.add_argument("--min-density", type=float, default=0.97, help="Minimum density threshold.")
    parser.add_argument("--min-size", type=int, default=30, help="Minimum cluster size allowed.")
    parser.add_argument("--max-size", type=int, default=150, help="Maximum cluster size allowed.")
    parser.add_argument("--max-borderline", type=float, default=0.30, help="Maximum borderline ratio (ignored if column missing).")
    parser.add_argument("--sample-size", type=int, default=15, help="How many titles to include as sample in summary.")
    args = parser.parse_args()

    labels = load_labels(args.labels)
    stats = load_stats(args.stats)

    density_candidates = [
        "density",
        "cluster_density",
        "mean_density",
        "mean_sim_to_centroid",
        "avg_similarity",
    ]
    size_candidates = ["size", "cluster_size", "count", "n_items", "n_games"]
    borderline_candidates = [
        "borderline",
        "borderline_ratio",
        "ratio_borderline",
        "borderline_rate",
        "pct_borderline",
        "borderline_pct",
    ]

    density_col = _resolve_column(stats, density_candidates)
    size_col = _resolve_column(stats, size_candidates)
    try:
        borderline_col = _resolve_column(stats, borderline_candidates)
    except ColumnNotFoundError:
        borderline_col = None

    filtered = _filter_candidates(
        stats,
        density_col,
        size_col,
        borderline_col,
        min_density=args.min_density,
        min_size=args.min_size,
        max_size=args.max_size,
        max_borderline=args.max_borderline if borderline_col else None,
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    summary_path = outdir / "selected_cluster.json"
    appids_path = outdir / "appids_selected.txt"

    if filtered.empty:
        write_json_any(
            {
                "cluster_id": None,
                "reason": "No cluster met the provided filters.",
                "filters": {
                    "min_density": args.min_density,
                    "min_size": args.min_size,
                    "max_size": args.max_size,
                    "max_borderline": args.max_borderline if borderline_col else None,
                },
            },
            summary_path,
            indent=2,
        )
        appids_path.write_text("", encoding="utf-8")
        print("[WARN] No cluster satisfied the selection criteria. Empty outputs were created.")
        return

    best = _pick_best_cluster(filtered, density_col, size_col, borderline_col)
    cluster_id = str(best["cluster_id"])
    labels_best = labels[labels["cluster_id"] == cluster_id]

    title_col = _infer_title_column(labels_best)
    sample_titles = _safe_sample(labels_best[title_col], args.sample_size) if title_col else []
    appids = labels_best["appid"].astype(str).tolist()

    summary = {
        "cluster_id": cluster_id,
        "size": int(best[size_col]),
        "density": float(best[density_col]),
    }
    if borderline_col and borderline_col in best.index:
        summary["borderline_ratio"] = float(best[borderline_col])
    if sample_titles:
        summary["sample_titles"] = sample_titles

    write_json_any(summary, summary_path, indent=2)
    appids_path.write_text("\n".join(appids), encoding="utf-8")

    print(f"[OK] Selected cluster {cluster_id} with {len(appids)} games -> {summary_path}")
    print(f"[OK] AppIDs exported to -> {appids_path}")


if __name__ == "__main__":
    main()
