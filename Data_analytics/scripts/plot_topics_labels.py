#!/usr/bin/env python
"""Plot topic labels from BERTopic cluster profiling as a simple bar chart.

Reads the JSON produced by profile_clusters_topics.py (cluster_topics.json),
builds readable labels like `action_fantasy_space` (no "cluster_X =" prefix),
and renders an interactive horizontal bar chart (Plotly HTML) ranked by
representative documents.

Usage example:
  python Data_analytics/scripts/plot_topics_labels.py \
    --topics outputs/clustering/cluster_topics.json \
    --out-html outputs/clustering/cluster_topics_labels.html \
    --top 50 --words 3
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot as plot_html


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Plot BERTopic labels from cluster_topics.json as a bar chart")
    ap.add_argument("--topics", default="outputs/clustering/cluster_topics.json", help="Input cluster topics JSON")
    ap.add_argument("--out-html", default="outputs/clustering/cluster_topics_labels.html", help="Output HTML plot path")
    ap.add_argument("--out-png", default=None, help="Optional PNG output path (tries Plotly+kaleido; falls back to Matplotlib)")
    ap.add_argument("--top", type=int, default=50, help="Max topics to plot, sorted by representative docs")
    ap.add_argument("--words", type=int, default=3, help="Number of keywords to include in label if no name is present")
    return ap.parse_args()


def _sanitize_label(text: str) -> str:
    # Lowercase, replace spaces/hyphens with underscore, keep [a-z0-9_]
    t = text.lower().replace("-", "_").replace(" ", "_")
    t = re.sub(r"[^a-z0-9_]+", "", t)
    # Collapse multiple underscores
    t = re.sub(r"_+", "_", t).strip("_")
    return t


def _build_label(row: Dict[str, Any], max_words: int) -> str:
    # Prefer explicit name from BERTopic if present
    name = row.get("name")
    if isinstance(name, str) and name.strip():
        return _sanitize_label(name)
    # Fallback to keywords
    kws = row.get("keywords") or []
    if isinstance(kws, list) and kws:
        words = [str(k) for k in kws[: max_words]]
        return _sanitize_label("_".join(words))
    # Last resort: topic_id
    if row.get("topic_id") is not None:
        return f"topic_{int(row['topic_id'])}"
    return "topic"


def load_topics_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Topics file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = list(data.values())
    if not isinstance(data, list):
        raise SystemExit("Invalid topics JSON structure — expected list or dict")
    return data


def main() -> None:
    args = parse_args()
    topics_path = Path(args.topics)
    out_path = Path(args.out_html)

    records = load_topics_json(topics_path)
    if not records:
        raise SystemExit("No topics found in input JSON")

    # Build dataframe with label and weight
    rows: List[Dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        label = _build_label(rec, args.words)
        weight = rec.get("repr_docs")
        try:
            weight = int(weight) if weight is not None else 0
        except Exception:
            weight = 0
        rows.append({
            "label": label,
            "weight": weight,
            "cluster_id": str(rec.get("cluster_id", "")),
            "topic_id": rec.get("topic_id"),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No valid topic rows to plot")

    # Group by label in case of duplicates; sum weights
    df = df.groupby("label", as_index=False)["weight"].sum()
    df = df.sort_values("weight", ascending=False)
    if args.top:
        df = df.head(max(1, int(args.top)))

    # Plot horizontal bar chart
    fig = go.Figure(
        data=[
            go.Bar(
                x=df["weight"],
                y=df["label"],
                orientation="h",
                text=df["label"],
                textposition="auto",
                marker=dict(color="#3FA7D6"),
            )
        ]
    )

    fig.update_layout(
        title="Topic Labels (BERTopic)",
        xaxis_title="Representative documents",
        yaxis_title="",
        paper_bgcolor="#000000",
        plot_bgcolor="#000000",
        font=dict(color="#f5f5f5"),
        margin=dict(l=10, r=10, t=50, b=10),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plot_html(fig, filename=str(out_path), auto_open=False)
    print(f"[OK] Saved topics label plot to {out_path}")

    # Optional PNG export
    if args.out_png:
        png_path = Path(args.out_png)
        png_path.parent.mkdir(parents=True, exist_ok=True)
        saved = False
        # Try Plotly static export (requires kaleido)
        try:
            import plotly.io as pio  # type: ignore

            pio.write_image(fig, str(png_path), format="png", scale=2)
            print(f"[OK] Saved PNG via Plotly+kaleido -> {png_path}")
            saved = True
        except Exception as _:
            pass
        if not saved:
            # Fallback to Matplotlib
            try:
                import matplotlib.pyplot as plt  # type: ignore

                plt.figure(figsize=(10, max(2, 0.4 * len(df))))
                plt.barh(df["label"], df["weight"], color="#3FA7D6")
                plt.xlabel("Representative documents")
                plt.tight_layout()
                plt.gca().invert_yaxis()  # Highest at top
                plt.savefig(str(png_path), dpi=200)
                plt.close()
                print(f"[OK] Saved PNG via Matplotlib -> {png_path}")
                saved = True
            except Exception as exc:
                print(f"[WARN] Could not export PNG (missing kaleido/matplotlib?): {exc}")
        if not saved:
            print("[HINT] Install 'kaleido' (for Plotly) or 'matplotlib' to enable PNG export.")


if __name__ == "__main__":
    main()
