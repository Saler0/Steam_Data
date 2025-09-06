#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Descarga on-demand señales para un (appid, mes) desde las APIs."""
import argparse
import yaml
import requests
from pathlib import Path
import pandas as pd
from src.utils.io import write_parquet_any

def get_twitch_token(client_id: str, client_secret: str) -> str:
    """Obtiene un token de acceso de aplicación de Twitch."""
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()["access_token"]

def get_twitch_game_id(game_name: str, client_id: str, token: str) -> str | None:
    """Busca el ID de un juego en Twitch a partir de su nombre."""
    url = "https://api.twitch.tv/helix/games"
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    params = {"name": game_name}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json().get("data", [])
    return data[0]["id"] if data else None

def fetch_twitch_videos(game_id: str, client_id: str, token: str, start_time: str, end_time: str) -> list:
    """Obtiene vídeos de Twitch para un juego en un rango de fechas."""
    url = "https://api.twitch.tv/helix/videos"
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    params = {"game_id": game_id, "started_at": start_time, "ended_at": end_time, "first": 100}
    videos = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        videos.extend(data.get("data", []))
        cursor = data.get("pagination", {}).get("cursor")
        if not cursor: break
        params["after"] = cursor
    return videos

def fetch_youtube_videos(query: str, api_key: str, start_time: str, end_time: str) -> list:
    """Busca vídeos en YouTube con una query en un rango de fechas."""
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet", "q": query, "key": api_key, "type": "video",
        "publishedAfter": start_time, "publishedBefore": end_time, "maxResults": 50
    }
    videos = []
    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        videos.extend(data.get("items", []))
        token = data.get("nextPageToken")
        if not token: break
        params["pageToken"] = token
    return videos

def main():
    """
    Punto de entrada. Parsea argumentos y llama a las funciones de fetch de las APIs
    para descargar y guardar datos de Twitch y YouTube para un juego y mes específicos.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    ap.add_argument("--appid", required=True, help="ID de la aplicación de Steam.")
    ap.add_argument("--name", required=True, help="Nombre del juego para las búsquedas en APIs.")
    ap.add_argument("--year_month", required=True, help="Mes a analizar (formato YYYY-MM).")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config, 'r'))
    
    start_date = pd.Timestamp(args.year_month + "-01", tz="UTC")
    end_date = start_date + pd.offsets.MonthEnd(1)
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    
    signal_cfg = cfg.get("signals", {})
    
    # --- Fetch Twitch ---
    twitch_cfg = signal_cfg.get("twitch", {})
    if twitch_cfg.get("mode") == "api":
        print("Fetching Twitch data...")
        try:
            token = get_twitch_token(twitch_cfg["client_id"], twitch_cfg["client_secret"])
            game_id = get_twitch_game_id(args.name, twitch_cfg["client_id"], token)
            if game_id:
                videos = fetch_twitch_videos(game_id, twitch_cfg["client_id"], token, start_iso, end_iso)
                if videos:
                    df = pd.DataFrame(videos)
                    out_dir = Path(twitch_cfg["api_cache_dir"]); out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = out_dir / f"{args.appid}_{args.year_month}.parquet"
                    write_parquet_any(df, out_path)
                    print(f"[OK] Twitch data saved to {out_path}")
        except Exception as e:
            print(f"[ERROR] Failed to fetch Twitch data: {e}")

    # --- Fetch YouTube ---
    youtube_cfg = signal_cfg.get("youtube", {})
    if youtube_cfg.get("mode") == "api":
        print("Fetching YouTube data...")
        try:
            videos = fetch_youtube_videos(f"{args.name} gameplay review", youtube_cfg["api_key"], start_iso, end_iso)
            if videos:
                df = pd.DataFrame([item["snippet"] for item in videos])
                out_dir = Path(youtube_cfg["api_cache_dir"]); out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{args.appid}_{args.year_month}.parquet"
                write_parquet_any(df, out_path)
                print(f"[OK] YouTube data saved to {out_path}")
        except Exception as e:
            print(f"[ERROR] Failed to fetch YouTube data: {e}")

if __name__ == "__main__":
    main()
