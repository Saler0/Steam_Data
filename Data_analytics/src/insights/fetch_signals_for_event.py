#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Descarga señales de Twitch y YouTube para un (appid, mes) directamente desde las APIs."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml

from src.ingestion.twitch import load_twitch_monthly
from src.ingestion.youtube import load_youtube_monthly
from src.utils.io import write_parquet_any


def _save_if_not_empty(df: pd.DataFrame | None, out_path: Path) -> None:
    if df is None or df.empty:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_any(df, out_path)
    print(f"[OK] Datos guardados en {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Descarga señales sociales para un appid y mes concretos")
    parser.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML")
    parser.add_argument("--appid", required=True, help="AppID de Steam")
    parser.add_argument("--name", required=True, help="Nombre del juego para consultas en APIs")
    parser.add_argument("--year_month", required=True, help="Mes objetivo en formato YYYY-MM")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    signals_cfg = cfg.get("signals", {})

    # Normaliza a inicio de mes (naive) para que coincida con el resto del pipeline
    target_month = pd.Timestamp(f"{args.year_month}-01").to_period("M").to_timestamp()
    month_list = [target_month]
    appid = str(args.appid)

    # Twitch
    twitch_cfg = signals_cfg.get("twitch", {})
    if twitch_cfg.get("mode") == "api":
        print("Fetching Twitch data...")
        twitch_df = load_twitch_monthly(
            appid,
            twitch_cfg,
            target_months=month_list,
            game_name=args.name,
            force_refresh=True,
        )
        if twitch_df is not None and not twitch_df.empty:
            cache_dir = Path(twitch_cfg.get("api_cache_dir", "data/external/twitch/api_cache"))
            _save_if_not_empty(twitch_df, cache_dir / f"{appid}_{args.year_month}.parquet")
        else:
            print("[WARN] No se obtuvieron resultados de Twitch para la ventana solicitada.")

    # YouTube
    youtube_cfg = signals_cfg.get("youtube", {})
    if youtube_cfg.get("mode") == "api":
        print("Fetching YouTube data...")
        youtube_df = load_youtube_monthly(
            appid,
            youtube_cfg,
            target_months=month_list,
            game_name=args.name,
            force_refresh=True,
        )
        if youtube_df is not None and not youtube_df.empty:
            cache_dir = Path(youtube_cfg.get("api_cache_dir", "data/external/youtube/api_cache"))
            _save_if_not_empty(youtube_df, cache_dir / f"{appid}_{args.year_month}.parquet")
        else:
            print("[WARN] No se obtuvieron resultados de YouTube para la ventana solicitada.")


if __name__ == "__main__":
    main()
