#!/usr/bin/env python
"""Generate an interactive cluster size distribution dashboard using Altair/Vega-Lite."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional, Sequence

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
        "--mode",
        choices=("hist", "categories"),
        default="categories",
        help="Histogram bins or named ranges for the distribution visualization.",
    )
    parser.add_argument(
        "--bin-thresholds",
        default="10,50,200,500,2000",
        help="Comma-separated inclusive upper bounds for bins when mode=categories.",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=1,
        help="Lower bound for the first categorical bin when mode=categories.",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use a logarithmic scale for the Y axis (counts only).",
    )
    parser.add_argument(
        "--value-mode",
        choices=("count", "share", "both"),
        default="both",
        help="Display raw counts, percentages, or both (categories mode only shows both/percent).",
    )
    parser.add_argument(
        "--show-labels",
        action="store_true",
        help="Overlay text labels on bars (useful when bars are very small).",
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


def _parse_thresholds(raw: str) -> List[int]:
    values: List[int] = []
    for part in raw.split(','):
        cleaned = part.strip()
        if not cleaned:
            continue
        try:
            values.append(int(cleaned))
        except ValueError as exc:
            raise SystemExit(f"Invalid threshold value '{cleaned}'. Use integers separated by commas.") from exc
    if not values:
        raise SystemExit("At least one threshold is required when mode=categories.")
    values = sorted(set(values))
    if any(val <= 0 for val in values):
        raise SystemExit("Thresholds must be positive integers.")
    return values


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


def _prepare_category_counts(df: pd.DataFrame, metric: str, thresholds: Sequence[int], min_size: int) -> pd.DataFrame:
    if min_size <= 0:
        raise SystemExit("min-size must be a positive integer.")
    if not thresholds:
        raise SystemExit("mode=categories requires at least one threshold.")
    lower = min_size
    labels: List[str] = []
    edges = [float(min_size - 1)]
    for upper in thresholds:
        if upper < lower:
            raise SystemExit("Thresholds must be in non-decreasing order and >= min-size.")
        edges.append(float(upper))
        labels.append(f"{lower}-{upper}")
        lower = upper + 1
    edges.append(float('inf'))
    labels.append(f"{lower}+")
    categorized = pd.cut(
        df[metric].astype(float),
        bins=edges,
        labels=labels,
        include_lowest=True,
        right=True,
    )
    category_series = pd.Series(categorized, name="bin_label")
    counts = (
        category_series
        .value_counts(sort=False)
        .rename("cluster_count")
        .reindex(category_series.cat.categories, fill_value=0)
        .reset_index()
        .rename(columns={"index": "bin_label"})
    )
    counts["cluster_count"] = counts["cluster_count"].astype(int)
    counts["_order"] = range(len(labels))
    return counts.sort_values("_order").drop(columns="_order")


def _enable_theme(name: Optional[str]) -> None:
    if not name:
        return
    try:
        alt.theme.enable(name)
    except Exception:
        print(f"[WARN] Altair theme '{name}' is not available. Using default theme.")


def _build_chart(
    df: pd.DataFrame,
    metric: str,
    max_bins: int,
    log_y: bool,
    width: int,
    height: int,
    title: str,
    mode: str,
    thresholds: Sequence[int],
    min_size: int,
    value_mode: str,
    show_labels: bool,
) -> alt.Chart:
    alt.data_transformers.disable_max_rows()

    if mode == "hist":
        if value_mode != "count":
            print("[WARN] value-mode share/both not supported with mode=hist; falling back to counts.")
        base = alt.Chart(df, title=title).transform_bin(
            ["metric_bin", "metric_bin_end"],
            field=metric,
            bin=alt.Bin(maxbins=max_bins, nice=True),
        ).transform_aggregate(
            cluster_count="count()",
            size_mean=f"mean({metric})",
            size_min=f"min({metric})",
            size_max=f"max({metric})",
            groupby=["metric_bin", "metric_bin_end"],
        )

        bars = base.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("metric_bin:Q", title="Games per cluster", axis=alt.Axis(labelAngle=0)),
            x2=alt.X2("metric_bin_end:Q"),
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

        percentile_df = (
            df[metric]
            .quantile([0.25, 0.5, 0.75])
            .rename(index={0.25: "25th", 0.5: "50th", 0.75: "75th"})
            .reset_index()
            .rename(columns={"index": "quantile", metric: "value"})
        )

        percentile_rules = alt.Chart(percentile_df).mark_rule(color="#2ca02c", strokeWidth=1.5).encode(
            x="value:Q",
            tooltip=[
                alt.Tooltip("value:Q", title="Percentile", format=",.1f"),
                alt.Tooltip("quantile:N", title="Quantile"),
            ],
        )

        return (bars + rule + percentile_rules).interactive(bind_x=False)



    freq_df = _prepare_category_counts(df, metric, thresholds, min_size)
    order = freq_df["bin_label"].tolist()
    total_clusters = freq_df["cluster_count"].sum()
    if total_clusters <= 0:
        total_clusters = 1
    freq_df["cluster_pct"] = (freq_df["cluster_count"] / total_clusters) * 100.0

    count_chart = None
    share_chart = None

    if value_mode in ("count", "both"):
        count_data = freq_df if not log_y else freq_df[freq_df["cluster_count"] > 0]
        if count_data.empty:
            count_data = freq_df
        count_height = height if value_mode == "count" else max(200, int(height * 0.6))
        count_title = title if value_mode == "count" else f"{title} – clusters (abs)"
        count_chart = alt.Chart(count_data, title=count_title).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("bin_label:N", sort=order, title="Games per cluster"),
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
                alt.Tooltip("bin_label:N", title="Range"),
                alt.Tooltip("cluster_count:Q", title="Clusters", format=","),
                alt.Tooltip("cluster_pct:Q", title="Share (%)", format=".2f"),
            ],
        ).properties(width=width, height=count_height)

        if show_labels:
            label_data = count_data[count_data["cluster_count"] > 0]
            if not label_data.empty:
                count_chart = count_chart + alt.Chart(label_data).mark_text(
                    align="center",
                    baseline="bottom",
                    dy=-6,
                    color="#333",
                    fontSize=12,
                ).encode(
                    x=alt.X("bin_label:N", sort=order),
                    y=alt.Y("cluster_count:Q"),
                    text=alt.Text("cluster_count:Q", format=","),
                )

    if value_mode in ("share", "both"):
        share_height = height if value_mode == "share" else max(180, int(height * 0.5))
        share_title = title if value_mode == "share" else f"{title} – clusters (%)"
        share_chart = alt.Chart(freq_df, title=share_title).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("bin_label:N", sort=order, title="Games per cluster"),
            y=alt.Y(
                "cluster_pct:Q",
                title="Clusters (%)",
                scale=alt.Scale(type="linear", nice=True, zero=True),
                axis=alt.Axis(format=".1f"),
            ),
            color=alt.Color(
                "cluster_pct:Q",
                scale=alt.Scale(scheme="blues"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("bin_label:N", title="Range"),
                alt.Tooltip("cluster_pct:Q", title="Clusters (%)", format=".2f"),
                alt.Tooltip("cluster_count:Q", title="Clusters", format=","),
            ],
        ).properties(width=width, height=share_height)

        if show_labels:
            label_share = freq_df[freq_df["cluster_pct"] > 0]
            if not label_share.empty:
                share_chart = share_chart + alt.Chart(label_share).mark_text(
                    align="center",
                    baseline="bottom",
                    dy=-6,
                    color="#333",
                    fontSize=12,
                ).encode(
                    x=alt.X("bin_label:N", sort=order),
                    y=alt.Y("cluster_pct:Q"),
                    text=alt.Text("cluster_pct:Q", format=".1f"),
                )

    if value_mode == "count":
        return count_chart if count_chart is not None else alt.Chart().mark_text(text="No data")
    if value_mode == "share":
        return share_chart if share_chart is not None else alt.Chart().mark_text(text="No data")

    if count_chart is None:
        count_chart = alt.Chart().mark_text(text="No count data")
    if share_chart is None:
        share_chart = alt.Chart().mark_text(text="No share data")
    return alt.vconcat(count_chart, share_chart).resolve_scale(y="independent")


def main() -> None:
    args = _parse_args()

    stats_path = Path(args.stats)
    df = _load_stats(stats_path, args.metric)

    _enable_theme(args.theme)

    thresholds: Sequence[int] = []
    if args.mode == "categories":
        thresholds = _parse_thresholds(args.bin_thresholds)

    chart = _build_chart(
        df,
        metric=args.metric,
        max_bins=args.max_bins,
        log_y=args.log_y,
        width=args.width,
        height=args.height,
        title=args.title,
        mode=args.mode,
        thresholds=thresholds,
        min_size=args.min_size,
        value_mode=args.value_mode,
        show_labels=args.show_labels,
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
