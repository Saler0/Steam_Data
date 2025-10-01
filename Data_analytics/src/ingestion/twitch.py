from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from src.utils.io import read_parquet_any, write_parquet_any

_TOKEN_CACHE: dict[tuple[str, str], dict[str, float | str]] = {}


def _normalize_months(months: Optional[Iterable]) -> list[pd.Timestamp]:
    if not months:
        return []
    out: list[pd.Timestamp] = []
    for value in months:
        if value is None:
            continue
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            continue
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        out.append(ts.to_period("M").to_timestamp(tz="UTC"))
    return sorted(set(out))


def _ensure_token(client_id: str, client_secret: str) -> str:
    cache_key = (client_id, client_secret)
    cached = _TOKEN_CACHE.get(cache_key)
    now = time.time()
    if cached and cached.get("expires_at", 0) > now + 60:
        return str(cached["token"])
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    response = requests.post(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    expires_in = float(payload.get("expires_in", 3600))
    token = payload["access_token"]
    _TOKEN_CACHE[cache_key] = {"token": token, "expires_at": now + expires_in}
    return token


def _get_game_id(game_name: str, client_id: str, token: str) -> Optional[str]:
    url = "https://api.twitch.tv/helix/games"
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    params = {"name": game_name}
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json().get("data", [])
    if not data:
        return None
    return data[0].get("id")


def _fetch_videos(game_id: str, client_id: str, token: str, start_iso: str, end_iso: str) -> list[dict]:
    url = "https://api.twitch.tv/helix/videos"
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    params: dict[str, str] = {
        "game_id": game_id,
        "started_at": start_iso,
        "ended_at": end_iso,
        "first": "100",
        "type": "archive",
    }
    videos: list[dict] = []
    while True:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        videos.extend(payload.get("data", []))
        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor:
            break
        params["after"] = cursor
    return videos


def _aggregate_videos(appid: str, videos: list[dict]) -> pd.DataFrame:
    if not videos:
        return pd.DataFrame()
    df = pd.DataFrame(videos)
    if df.empty or "published_at" not in df.columns:
        return pd.DataFrame()
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df = df.dropna(subset=["published_at"])
    if df.empty:
        return pd.DataFrame()
    df["year_month"] = df["published_at"].dt.to_period("M").dt.to_timestamp()
    df["view_count"] = pd.to_numeric(df.get("view_count"), errors="coerce").fillna(0)
    agg = (
        df.groupby("year_month")
        .agg(viewers=("view_count", "sum"), videos=("id", "count"))
        .reset_index()
    )
    agg["appid"] = str(appid)
    return agg


def _load_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet_any(path)
    except Exception:
        return pd.DataFrame()


def _save_cache(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_any(df, path)


def load_twitch_monthly(
    appid: str,
    cfg: dict,
    *,
    target_months: Optional[Iterable] = None,
    game_name: Optional[str] = None,
    force_refresh: bool = False,
) -> pd.DataFrame | None:
    mode = (cfg or {}).get("mode", "file")
    if mode == "file":
        path = Path(cfg.get("file", f"data/external/twitch/monthly_{appid}.csv"))
        if not path.exists():
            return None
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.rename(columns={"date": "year_month"})
        df["year_month"] = df["year_month"].dt.to_period("M").dt.to_timestamp()
        df["appid"] = str(appid)
        return df

    if mode != "api":
        return None

    if not game_name:
        return None

    months = _normalize_months(target_months)
    if months:
        start_date = months[0].tz_convert(None) if getattr(months[0], 'tzinfo', None) else months[0]
        end_date = months[-1].tz_convert(None) if getattr(months[-1], 'tzinfo', None) else months[-1]
    else:
        months_back = int(cfg.get("months_back", 6))
        end_date = pd.Timestamp.utcnow()
        start_date = end_date - pd.DateOffset(months=months_back)
    # Extend range to cover full months
    start_date = pd.Timestamp(start_date).to_period("M").to_timestamp(tz="UTC")
    end_date = (pd.Timestamp(end_date).to_period("M").to_timestamp(tz="UTC") + pd.offsets.MonthEnd(1))

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    if not client_id or not client_secret:
        raise ValueError("Twitch API mode requires client_id and client_secret")

    cache_dir = Path(cfg.get("api_cache_dir", "data/external/twitch/api_cache"))
    cache_path = cache_dir / f"{appid}.parquet"
    cache_df = _load_cache(cache_path)

    if not force_refresh and not cache_df.empty:
        cache_df["year_month"] = pd.to_datetime(cache_df["year_month"], errors="coerce")
        if months and set(months).issubset(set(cache_df["year_month"])):
            result = cache_df
        else:
            result = cache_df
    else:
        result = cache_df

    need_fetch = force_refresh or result.empty or not months or not set(months).issubset(set(result["year_month"]))

    if need_fetch:
        token = _ensure_token(client_id, client_secret)
        game_id = _get_game_id(game_name, client_id, token)
        if not game_id:
            return cache_df if not cache_df.empty else None
        videos = _fetch_videos(game_id, client_id, token, start_date.isoformat(), end_date.isoformat())
        fetched_df = _aggregate_videos(appid, videos)
        if not fetched_df.empty:
            if not cache_df.empty:
                merged = pd.concat([cache_df, fetched_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=["year_month"], keep="last")
                merged = merged.sort_values("year_month").reset_index(drop=True)
            else:
                merged = fetched_df
            _save_cache(merged, cache_path)
            result = merged

    if result is None or result.empty:
        return result

    if months:
        result = result[result["year_month"].isin(months)]
    result["appid"] = str(appid)
    return result.reset_index(drop=True)
