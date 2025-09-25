#!/usr/bin/env python
"""Generate an interactive cluster size distribution dashboard using Altair/Vega-Lite."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

try:
    import altair as alt
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Altair is required for this visualization. Install it with `pip install altair vega_datasets`."
    ) from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a bar chart showing the frequency of clusters by size using Altair (Vega-Lite)."
    )
    parser.add_argument(
        "--stats",
        default="outputs/clustering/cluster_stats.csv",
        help="Path to cluster statistics CSV with at least 'cluster_id' and 'size'.",
    )
    parser.add_argument(
        "--metric",
        default="size",
        help="Numeric column to use for the size metric (default: size).",
    )
    parser.add_argument(
        "--max-bins",
        type=int,
        default=40,
        help="Maximum number of histogram bins (Altair's maxbins).",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use a logarithmic scale for the Y axis (useful for heavy-tailed distributions).",
    )
    parser.add_argument(
        "--title",
        default="Cluster Size Distribution",
        help="Title for the chart.",
    )
    parser.add_argument(
        "--theme",
        default="latimes",
        help="Altair theme name (e.g., latimes, quartz, urban).",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=900,
        help="Chart width in pixels.",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=480,
        help="Chart height in pixels.",
    )
    parser.add_argument(
        "--out-html",
        default="outputs/clustering/cluster_size_distribution.html",
        help="Output HTML file for the dashboard.",
    )
    return parser.parse_args()


def _load_stats(path: Path, metric: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Stats file not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise SystemExit("Cluster stats file is empty.")
    required_cols = {"cluster_id", metric}
    missing = required_cols - set(df.columns)
    if missing:
        raise SystemExit(f"Stats file must include columns: {', '.join(sorted(missing))}.")
    df = df.dropna(subset=[metric]).copy()
    df[metric] = pd.to_numeric(df[metric], errors="coerce")
    df = df.dropna(subset=[metric])
    df["cluster_id"] = df["cluster_id"].astype(str)
    if df.empty:
        raise SystemExit("No valid metric values found after cleaning.")
    return df


def _enable_theme(name: Optional[str]) -> None:
    if not name:
        return
    try:
        alt.themes.enable(name)
    except Exception:
        print(f"[WARN] Altair theme '{name}' is not available. Using default theme.")


def _build_chart(df: pd.DataFrame, metric: str, max_bins: int, log_y: bool, width: int, height: int, title: str) -> alt.Chart:
    alt.data_transformers.disable_max_rows()

    hover_selection = alt.selection(type="single", nearest=True, on="mouseover", fields=[metric], empty="none")

    base = alt.Chart(df, title=title).transform_bin(
        "metric_bin",
        field=metric,
        bin=alt.Bin(maxbins=max_bins, nice=True),
    ).transform_aggregate(
        cluster_count="count()",
        size_mean=f"mean({metric})",
        size_min=f"min({metric})",
        size_max=f"max({metric})",
        groupby=["metric_bin"],
    )

    bars = base.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("metric_bin:Q", title="Games per cluster", axis=alt.Axis(labelAngle=0)),
        y=alt.Y(
            "cluster_count:Q",
            title="Number of clusters",
            scale=alt.Scale(type="log" if log_y else "linear", nice=True, zero=not log_y),
        ),
        color=alt.Color(
            "cluster_count:Q",
            scale=alt.Scale(scheme="tableau10", type="log" if log_y else "linear"),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("cluster_count:Q", title="Clusters", format=","),
            alt.Tooltip("size_min:Q", title="Min size", format=","),
            alt.Tooltip("size_max:Q", title="Max size", format=","),
            alt.Tooltip("size_mean:Q", title="Avg size", format=",.1f"),
        ],
    ).properties(width=width, height=height)

    rule = alt.Chart(df).mark_rule(color="#ff7f0e", strokeDash=[8, 4]).encode(
        x=alt.X(f"mean({metric}):Q"),
        tooltip=[alt.Tooltip(f"mean({metric}):Q", title="Global mean", format=",.1f")],
    )

    percentile_text = alt.Chart(df).transform_quantile(
        metric,
        [0.25, 0.5, 0.75],
    ).mark_rule(color="#2ca02c", strokeWidth=1.5).encode(
        x="value:Q",
        tooltip=[
            alt.Tooltip("value:Q", title="Percentile", format=",.1f"),
            alt.Tooltip("percentile:Q", title="Quantile"),
        ],
    )

    return (bars + rule + percentile_text).interactive(bind_x=False)


def main() -> None:
    args = _parse_args()

    stats_path = Path(args.stats)
    df = _load_stats(stats_path, args.metric)

    _enable_theme(args.theme)

    chart = _build_chart(
        df,
        metric=args.metric,
        max_bins=args.max_bins,
        log_y=args.log_y,
        width=args.width,
        height=args.height,
        title=args.title,
    )

    out_path = Path(args.out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    chart.save(str(out_path))

    print(f"[OK] Cluster size distribution saved to {out_path}.")
    print(
        f"    Metric '{args.metric}': min={df[args.metric].min():.0f}, median={df[args.metric].median():.1f}, "
        f"mean={df[args.metric].mean():.1f}, max={df[args.metric].max():.0f}."
    )


if __name__ == "__main__":
    main()
