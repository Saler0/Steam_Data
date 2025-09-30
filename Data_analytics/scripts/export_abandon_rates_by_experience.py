#!/usr/bin/env python
"""Aggregate abandonment metrics by experience level and time window for dashboard use."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


def _read_table(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"Input file not found: {file_path}")
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix == ".json":
        return pd.read_json(file_path)
    return pd.read_parquet(file_path)


def _prepare_dataframe(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "review_date" not in df.columns:
        raise SystemExit("reviews_with_segments dataset must contain 'review_date'.")
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce", utc=True)
    df = df.dropna(subset=["review_date"])  # drop rows without valid date
    df["month"] = df["review_date"].dt.to_period(freq).dt.to_timestamp()

    # Normalise experience_key; treat missing/blank as "unknown"
    df["experience_group"] = df.get("experience_key")
    df["experience_group"] = df["experience_group"].fillna("unknown").replace("", "unknown")
    df["experience_group"] = df["experience_group"].astype(str)

    # Coerce booleans/numeric fields of interest
    df["abandon_after_30d"] = df.get("abandon_after_30d").astype("float64")
    df["recommended"] = df.get("recommended").astype("float64")
    df["playtime_at_review"] = pd.to_numeric(df.get("playtime_at_review"), errors="coerce")
    df["playtime_since_review_30d"] = pd.to_numeric(df.get("playtime_since_review_30d"), errors="coerce")

    return df


def _aggregate(df: pd.DataFrame, freq: str, window: Optional[int], min_samples: int) -> pd.DataFrame:
    if df.empty:
        columns = [
            "period",
            "experience_group",
            "reviews_count",
            "abandon_rate",
            "abandon_count",
            "recommended_rate",
            "avg_playtime_at_review",
            "avg_playtime_since_30d",
            "window_size",
        ]
        return pd.DataFrame(columns=columns)

    base = df.copy()

    # Create "all" experience aggregate alongside specific groups
    all_df = base.copy()
    all_df["experience_group"] = "all"
    combined = pd.concat([base, all_df], ignore_index=True)

    grouped = (
        combined.groupby(["month", "experience_group"], dropna=False)
        .agg(
            reviews_count=("review_id", "count"),
            abandon_count=("abandon_after_30d", "sum"),
            abandon_rate=("abandon_after_30d", "mean"),
            recommended_rate=("recommended", "mean"),
            avg_playtime_at_review=("playtime_at_review", "mean"),
            avg_playtime_since_30d=("playtime_since_review_30d", "mean"),
        )
        .reset_index()
    )

    grouped["abandon_rate"] = grouped["abandon_rate"].astype(float)
    grouped["recommended_rate"] = grouped["recommended_rate"].astype(float)
    grouped["avg_playtime_at_review"] = grouped["avg_playtime_at_review"].astype(float)
    grouped["avg_playtime_since_30d"] = grouped["avg_playtime_since_30d"].astype(float)

    grouped = grouped[grouped["reviews_count"] >= min_samples]

    if grouped.empty:
        grouped["window_size"] = window or 1
        grouped["period"] = pd.NaT
        return grouped

    grouped = grouped.sort_values(["experience_group", "month"]).reset_index(drop=True)

    if window and window > 1:
        def _apply_window(sub: pd.DataFrame) -> pd.DataFrame:
            sub = sub.sort_values("month")
            rolling = (
                sub.set_index("month")
                .rolling(window=window, min_periods=1)
                .agg({
                    "reviews_count": "sum",
                    "abandon_count": "sum",
                    "abandon_rate": "mean",
                    "recommended_rate": "mean",
                    "avg_playtime_at_review": "mean",
                    "avg_playtime_since_30d": "mean",
                })
            )
            rolling = rolling.reset_index()
            rolling["experience_group"] = sub["experience_group"].iloc[0]
            return rolling

        rolled = (
            grouped.groupby("experience_group", group_keys=False)
            .apply(_apply_window)
            .reset_index(drop=True)
        )
        rolled["window_size"] = window
        result = rolled
    else:
        grouped["window_size"] = 1
        result = grouped

    result = result.rename(columns={"month": "period"})
    result["period"] = result["period"].dt.strftime("%Y-%m-%d")

    ordered_cols = [
        "period",
        "experience_group",
        "reviews_count",
        "abandon_count",
        "abandon_rate",
        "recommended_rate",
        "avg_playtime_at_review",
        "avg_playtime_since_30d",
        "window_size",
    ]
    return result[ordered_cols]


def export_abandon_rates(
    reviews_path: str | Path,
    output_path: str | Path,
    freq: str,
    window: Optional[int],
    min_samples: int,
) -> Path:
    df = _read_table(reviews_path)
    df = _prepare_dataframe(df, freq)
    aggregated = _aggregate(df, freq, window, min_samples)

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_file.suffix.lower() == ".parquet":
        aggregated.to_parquet(out_file, index=False)
    else:
        aggregated.to_csv(out_file, index=False)
    print(f"[OK] Abandon rates exported -> {out_file}")
    return out_file


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export abandonment rates by experience and time window")
    parser.add_argument(
        "--reviews",
        default="data/warehouse/reviews_with_segments.parquet",
        help="Path to enriched reviews dataset",
    )
    parser.add_argument(
        "--out",
        default="outputs/events/abandon_rates_by_experience.csv",
        help="Destination CSV/Parquet path",
    )
    parser.add_argument(
        "--freq",
        default="M",
        help="Pandas offset alias for grouping (e.g. 'M' for month, 'W' for week)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=1,
        help="Rolling window size (>=1). Use 1 for no rolling aggregation.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum number of reviews required to report a metric",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    window = max(1, args.window or 1)
    export_abandon_rates(
        reviews_path=args.reviews,
        output_path=args.out,
        freq=args.freq,
        window=window,
        min_samples=max(1, args.min_samples),
    )


if __name__ == "__main__":
    main()
