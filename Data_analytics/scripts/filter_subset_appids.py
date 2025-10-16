#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


def _read_parquet_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[INFO] No existe {path}; nada que filtrar")
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        # fallback simple
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Filtra datasets de reviews y tópicos a un subset de APPIDs")
    ap.add_argument("--appids", nargs="*", help="Lista de appids a conservar (si no, usa env APPIDS)")
    ap.add_argument("--reviews-in", default="data/warehouse/reviews_with_segments.parquet")
    ap.add_argument("--reviews-out", default="data/warehouse/reviews_with_segments_subset.parquet")
    ap.add_argument("--topics-in", default="outputs/events/reviews_topics.parquet")
    ap.add_argument("--topics-out", default="outputs/events/reviews_topics_subset.parquet")
    return ap.parse_args(list(argv) if argv is not None else None)


def main() -> None:
    args = parse_args()
    # Leer appids desde args o env
    appids: List[str] = []
    if args.appids:
        appids = [str(a) for a in args.appids if str(a).strip()]
    else:
        env_apps = os.getenv("APPIDS", "").strip()
        if env_apps:
            appids = [a for a in env_apps.split() if a]

    if not appids:
        print("[INFO] Sin APPIDs; se mantienen datasets completos")
        return

    keep = set(appids)

    rev_in = Path(args.reviews_in)
    rev_out = Path(args.reviews_out)
    top_in = Path(args.topics_in)
    top_out = Path(args.topics_out)

    # Filtrar reviews por appid
    df_reviews = _read_parquet_safe(rev_in)
    if not df_reviews.empty and "appid" in df_reviews.columns:
        dfr = df_reviews[df_reviews["appid"].astype(str).isin(keep)].copy()
        rev_out.parent.mkdir(parents=True, exist_ok=True)
        dfr.to_parquet(rev_out, index=False)
        print(f"[OK] reviews_with_segments subset -> {rev_out} ({len(dfr)})")
    else:
        print(f"[INFO] No reviews para filtrar o falta columna 'appid': {rev_in}")
        dfr = pd.DataFrame(columns=["review_id"])  # para join condicional

    # Filtrar tópicos llevando review_id de reviews filtradas si es posible
    df_topics = _read_parquet_safe(top_in)
    if not df_topics.empty:
        dft = df_topics.copy()
        if not dfr.empty and "review_id" in dft.columns and "review_id" in dfr.columns:
            dft = dft.merge(dfr[["review_id"]], on="review_id", how="inner")
        top_out.parent.mkdir(parents=True, exist_ok=True)
        dft.to_parquet(top_out, index=False)
        print(f"[OK] reviews_topics subset -> {top_out} ({len(dft)})")
    else:
        print(f"[INFO] No topics para filtrar: {top_in}")


if __name__ == "__main__":
    main()

