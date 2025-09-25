#!/usr/bin/env python
"""
Render a 3D UMAP map of cluster medoids enriched with BERTopic topics.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot

try:
    import umap
except ImportError as exc:  # pragma: no cover
    raise SystemExit("umap-learn is required. Install it with `pip install umap-learn`.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="3D UMAP visualization of cluster medoids and BERTopic summaries.")
    parser.add_argument("--medoids", default="models/cluster_medoids.json", help="Input JSON with cluster medoids.")
    parser.add_argument("--stats", default="outputs/clustering/cluster_stats.csv", help="Cluster stats CSV (optional).")
    parser.add_argument("--topics", default="outputs/clustering/cluster_topics.json", help="Cluster topics JSON (optional).")
    parser.add_argument("--borderline", default="outputs/clustering/borderline_games.csv", help="Borderline games CSV (optional).")
    parser.add_argument("--out-html", default="outputs/clustering/cluster_topics_umap.html", help="Output HTML file for the plot.")
    parser.add_argument("--open", action="store_true", help="Open the HTML result after saving.")
    parser.add_argument("--n-neighbors", type=int, default=15, help="UMAP number of neighbors.")
    parser.add_argument("--min-dist", type=float, default=0.1, help="UMAP min_dist parameter.")
    parser.add_argument("--topic-words", type=int, default=5, help="Max number of keywords to show per topic.")
    parser.add_argument("--label-clusters", action="store_true", help="Render cluster ids as text labels.")
    return parser.parse_args()


def load_medoids(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Medoids file not found: {path}")
    content = json.loads(path.read_text(encoding="utf-8"))
    records: list[Dict[str, Any]] = []
    for key, vec in content.items():
        if vec is None:
            continue
        arr = np.asarray(vec, dtype=np.float32)
        records.append({"cluster_id": str(key), "embedding": arr})
    if not records:
        raise SystemExit("No medoids were loaded.")
    return pd.DataFrame(records)


def load_stats(path: Path) -> pd.DataFrame:
    candidate = path
    if not candidate.exists():
        alt = Path("outputs/clustering/stats/cluster_stats.csv")
        if alt.exists():
            candidate = alt
        else:
            return pd.DataFrame()
    df = pd.read_csv(candidate)
    if "cluster_id" not in df.columns:
        raise SystemExit("Stats file must include a cluster_id column.")
    df = df.copy()
    df["cluster_id"] = df["cluster_id"].astype(str)
    return df


def load_topics(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    content = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(content, dict):
        content = list(content.values())
    df = pd.DataFrame(content)
    if df.empty:
        return df
    if "cluster_id" not in df.columns:
        raise SystemExit("Topics file must include a cluster_id field.")
    df = df.copy()
    df["cluster_id"] = df["cluster_id"].astype(str)
    return df


def load_borderline(path: Path) -> pd.DataFrame:
    candidate = path
    if not candidate.exists():
        alt = Path("outputs/clustering/borderline/borderline_games.csv")
        if alt.exists():
            candidate = alt
        else:
            return pd.DataFrame()
    df = pd.read_csv(candidate)
    if "cluster_id" not in df.columns:
        raise SystemExit("Borderline file must include cluster_id.")
    df = df.copy()
    df["cluster_id"] = df["cluster_id"].astype(str)
    return df


def project_umap(vectors: Iterable[np.ndarray], n_neighbors: int, min_dist: float) -> np.ndarray:
    matrix = np.vstack(list(vectors)).astype(np.float32)
    reducer = umap.UMAP(n_components=3, n_neighbors=n_neighbors, min_dist=min_dist, metric="cosine", random_state=42)
    return reducer.fit_transform(matrix)


def deterministic_jitter(value: str, scale: float) -> Tuple[float, float, float]:
    h = hashlib.md5(value.encode("utf-8")).hexdigest()
    nums = [int(h[i:i + 8], 16) / 0xFFFFFFFF for i in range(0, 24, 8)]
    return tuple(scale * (n - 0.5) for n in nums)


def build_hover_text(row: pd.Series, topic_words: int) -> str:
    parts = [f"cluster {row['cluster_id']}"]
    topic_display = row.get("topic_display")
    if isinstance(topic_display, str) and topic_display:
        parts.append(f"topic: {topic_display}")
    elif isinstance(row.get("keywords"), list):
        kw = ", ".join(row["keywords"][:topic_words])
        if kw:
            parts.append(f"topic: {kw}")
    topic_id = row.get("topic_id")
    if pd.notna(topic_id):
        parts.append(f"topic id: {int(topic_id)}")
    size = row.get("size")
    if pd.notna(size):
        parts.append(f"games: {int(size)}")
    border = row.get("borderline_rate")
    if pd.notna(border):
        parts.append(f"borderline pct: {border:.2%}")
    mean_sim = row.get("mean_sim_to_centroid")
    if pd.notna(mean_sim):
        parts.append(f"mean sim: {mean_sim:.3f}")
    return "<br>".join(parts)


def main() -> None:
    args = parse_args()

    medoids_path = Path(args.medoids)
    stats_path = Path(args.stats)
    topics_path = Path(args.topics)
    borderline_path = Path(args.borderline)
    out_path = Path(args.out_html)

    medoids_df = load_medoids(medoids_path)
    coords = project_umap(medoids_df["embedding"], args.n_neighbors, args.min_dist)

    centers = pd.DataFrame(
        {
            "cluster_id": medoids_df["cluster_id"].values,
            "x": coords[:, 0],
            "y": coords[:, 1],
            "z": coords[:, 2],
        }
    )

    stats_df = load_stats(stats_path)
    if not stats_df.empty:
        centers = centers.merge(stats_df, on="cluster_id", how="left")
        if "n_borderline" in centers.columns and "size" in centers.columns:
            centers["borderline_rate"] = centers["n_borderline"].fillna(0) / centers["size"].replace({0: np.nan})

    topics_df = load_topics(topics_path)
    if not topics_df.empty:
        centers = centers.merge(topics_df, on="cluster_id", how="left")
        if "name" in centers.columns:
            centers["topic_display"] = centers["name"].fillna("").astype(str)
            empty_mask = centers["topic_display"].str.strip() == ""
            if "keywords" in centers.columns:
                centers.loc[empty_mask, "topic_display"] = centers.loc[empty_mask, "keywords"].apply(
                    lambda x: ", ".join(x[: args.topic_words]) if isinstance(x, list) else ""
                )
            centers.loc[centers["topic_display"].str.strip() == "", "topic_display"] = ""

    centers["hover"] = centers.apply(lambda r: build_hover_text(r, args.topic_words), axis=1)

    marker_size = np.full(len(centers), 10.0)
    if "size" in centers.columns:
        size_vals = centers["size"].fillna(centers["size"].median()).clip(lower=1)
        marker_size = np.sqrt(size_vals).clip(lower=6, upper=28)

    color_values = None
    if "topic_id" in centers.columns:
        color_values = centers["topic_id"].fillna(-1).astype(float)
    else:
        try:
            color_values = centers["cluster_id"].astype(int)
        except ValueError:
            color_values = pd.factorize(centers["cluster_id"])[0]

    cluster_trace = go.Scatter3d(
        x=centers["x"],
        y=centers["y"],
        z=centers["z"],
        mode="markers+text" if args.label_clusters else "markers",
        text=centers["cluster_id"] if args.label_clusters else None,
        textposition="top center",
        marker=dict(
            size=marker_size,
            color=color_values,
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="Topic id" if "topic_id" in centers.columns else "Cluster id"),
            opacity=0.9,
        ),
        hoverinfo="text",
        hovertext=centers["hover"],
        name="Cluster medoids",
    )

    traces = [cluster_trace]

    borderline_df = load_borderline(borderline_path)
    if not borderline_df.empty:
        centers_idx = centers.set_index("cluster_id")[["x", "y", "z"]]
        spread = centers[["x", "y", "z"]].std().max()
        scale = float(spread * 0.08 if not np.isnan(spread) else 0.5)
        xs: list[float] = []
        ys: list[float] = []
        zs: list[float] = []
        texts: list[str] = []
        for row in borderline_df.itertuples():
            cid = getattr(row, "cluster_id")
            if cid not in centers_idx.index:
                continue
            cx, cy, cz = centers_idx.loc[cid]
            dx, dy, dz = deterministic_jitter(str(getattr(row, "appid")), scale)
            xs.append(cx + dx)
            ys.append(cy + dy)
            zs.append(cz + dz)
            cmargin = getattr(row, "confidence_margin", np.nan)
            texts.append(
                "appid={appid} | cluster={cluster} | margin={margin}".format(
                    appid=getattr(row, "appid"),
                    cluster=cid,
                    margin=f"{cmargin:.4f}" if isinstance(cmargin, (int, float)) else "na",
                )
            )
        if xs:
            traces.append(
                go.Scatter3d(
                    x=xs,
                    y=ys,
                    z=zs,
                    mode="markers",
                    marker=dict(size=3, opacity=0.3, color="rgba(255, 255, 255, 0.65)"),
                    hoverinfo="text",
                    hovertext=texts,
                    name="Borderline games",
                )
            )

    fig = go.Figure(data=traces)
    fig.update_layout(
        title="Cluster medoids with BERTopic summaries (UMAP 3D)",
        scene=dict(xaxis_title="UMAP-1", yaxis_title="UMAP-2", zaxis_title="UMAP-3"),
        template="plotly_white",
        legend=dict(x=0.02, y=0.98),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plot(fig, filename=str(out_path), auto_open=args.open)
    print(f"OK -> {out_path}")


if __name__ == "__main__":
    main()


