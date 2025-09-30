#!/usr/bin/env python
"""Exporta topicos BERTopic agregados por mes y nivel de experiencia."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


def _read_table(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"No se encontro el archivo requerido: {file_path}")
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix == ".json":
        return pd.read_json(file_path)
    return pd.read_parquet(file_path)


def _ensure_str_ids(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        raise SystemExit(f"Falta la columna '{column}' en el dataset")
    return df[column].astype(str)


def _prepare_month(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.to_period("M").dt.to_timestamp()


def _aggregate_topics(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "month", "experience_group", "topic_id", "topic_name",
            "reviews_count", "group_total", "share_pct", "avg_share", "rank"
        ])

    df = df.copy()
    df["experience_group"] = df["experience_key"].fillna("unknown").replace("", "unknown").astype(str)

    df_all = df.copy()
    df_all["experience_group"] = "all"
    combined = pd.concat([df, df_all], ignore_index=True)

    totals = (
        combined.groupby(["year_month", "experience_group"], dropna=False)["review_id"]
        .nunique()
        .rename("group_total")
        .reset_index()
    )

    topic_counts = (
        combined.groupby(["year_month", "experience_group", "topic_id", "topic_name"], dropna=False)
        .agg(
            reviews_count=("review_id", "nunique"),
            avg_share=("share", lambda s: float(np.nanmean(s)) if len(s) else np.nan),
        )
        .reset_index()
    )

    merged = topic_counts.merge(totals, on=["year_month", "experience_group"], how="left")
    merged["share_pct"] = np.where(
        merged["group_total"] > 0,
        merged["reviews_count"] / merged["group_total"],
        np.nan,
    )

    def _rank_block(block: pd.DataFrame) -> pd.DataFrame:
        ordered = block.sort_values(by=["reviews_count", "avg_share"], ascending=[False, False])
        head = ordered.head(top_n).copy()
        head["rank"] = range(1, len(head) + 1)
        return head

    ranked = (
        merged.groupby(["year_month", "experience_group"], group_keys=False)
        .apply(_rank_block)
        .reset_index(drop=True)
    )

    ranked["month"] = ranked["year_month"].dt.strftime("%Y-%m")
    ranked = ranked[[
        "month", "experience_group", "topic_id", "topic_name",
        "reviews_count", "group_total", "share_pct", "avg_share", "rank"
    ]].sort_values(["month", "experience_group", "rank"])
    return ranked


def export_topics_by_experience(
    reviews_path: str | Path,
    topics_path: str | Path,
    output_path: str | Path,
    top_n: int,
) -> Path:
    reviews_df = _read_table(reviews_path)
    topics_df = _read_table(topics_path)

    if reviews_df.empty or topics_df.empty:
        print("[WARN] Alguno de los datasets esta vacio; se generara un CSV sin filas.")
        out_df = _aggregate_topics(pd.DataFrame(), top_n)
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(out_file, index=False)
        return out_file

    reviews_df = reviews_df.copy()
    reviews_df["review_id"] = _ensure_str_ids(reviews_df, "review_id")
    if "experience_key" not in reviews_df.columns:
        raise SystemExit("El dataset de resenas no contiene 'experience_key'. Ejecuta review_segments primero.")
    if "review_date" not in reviews_df.columns:
        raise SystemExit("El dataset de resenas no tiene 'review_date'.")
    reviews_df["experience_key"] = reviews_df["experience_key"].astype(str)

    topics_df = topics_df.copy()
    topics_df["review_id"] = _ensure_str_ids(topics_df, "review_id")
    if "share" not in topics_df.columns:
        topics_df["share"] = np.nan
    if "topic_id" not in topics_df.columns:
        topics_df["topic_id"] = topics_df.get("topic_name", pd.Series(range(len(topics_df))))
    topics_df["topic_id"] = topics_df["topic_id"].fillna("unknown").astype(str)
    if "topic_name" not in topics_df.columns:
        topics_df["topic_name"] = topics_df["topic_id"]
    topics_df["topic_name"] = topics_df["topic_name"].fillna("(sin_nombre)").astype(str)

    merged = topics_df.merge(
        reviews_df[["review_id", "appid", "review_date", "experience_key"]],
        on="review_id",
        how="left",
    )

    merged = merged.dropna(subset=["review_date", "topic_name"])
    if merged.empty:
        print("[WARN] Tras combinar resenas y topicos no hay filas; CSV vacio.")
        out_df = _aggregate_topics(pd.DataFrame(), top_n)
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(out_file, index=False)
        return out_file

    merged["year_month"] = _prepare_month(merged["review_date"])
    merged = merged.dropna(subset=["year_month"])
    if merged.empty:
        print("[WARN] No se pudieron inferir fechas mensuales validas; CSV vacio.")
        out_df = _aggregate_topics(pd.DataFrame(), top_n)
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(out_file, index=False)
        return out_file

    ranked = _aggregate_topics(merged, top_n)
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    ranked.to_csv(out_file, index=False)
    print(f"[OK] CSV generado en -> {out_file}")
    return out_file


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exporta topicos por experiencia y mes")
    parser.add_argument(
        "--reviews",
        default="data/warehouse/reviews_with_segments.parquet",
        help="Ruta al parquet/CSV con resenas segmentadas",
    )
    parser.add_argument(
        "--topics",
        default="outputs/events/reviews_topics.parquet",
        help="Ruta al parquet/CSV con topicos dominantes por resena",
    )
    parser.add_argument(
        "--out",
        default="outputs/events/topics_by_experience.csv",
        help="Ruta de salida CSV",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Numero maximo de topicos por mes y segmento a conservar",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    export_topics_by_experience(
        reviews_path=args.reviews,
        topics_path=args.topics,
        output_path=args.out,
        top_n=max(1, args.top_n),
    )


if __name__ == "__main__":
    main()
