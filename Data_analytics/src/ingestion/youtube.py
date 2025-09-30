from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from src.utils.io import read_parquet_any, write_parquet_any

_YT_ENDPOINT_SEARCH = "https://www.googleapis.com/youtube/v3/search"
_YT_ENDPOINT_VIDEOS = "https://www.googleapis.com/youtube/v3/videos"


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
    return sorted(set(out))gi


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


def _search_videos(query: str, api_key: str, start_iso: str, end_iso: str, max_results: int = 200) -> list[dict]:
    params = {
        "part": "snippet",
        "q": query,
        "key": api_key,
        "type": "video",
        "publishedAfter": start_iso,
        "publishedBefore": end_iso,
        "maxResults": 50,
        "order": "date",
    }
    items: list[dict] = []
    while True:
        response = requests.get(_YT_ENDPOINT_SEARCH, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        items.extend(payload.get("items", []))
        if len(items) >= max_results:
            break
        token = payload.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
    return items[:max_results]


def _fetch_video_stats(video_ids: list[str], api_key: str) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    if not video_ids:
        return stats
    chunk_size = 50
    for i in range(0, len(video_ids), chunk_size):
        chunk = video_ids[i : i + chunk_size]
        params = {
            "part": "statistics",
            "id": ",".join(chunk),
            "key": api_key,
        }
        response = requests.get(_YT_ENDPOINT_VIDEOS, params=params, timeout=30)
        response.raise_for_status()
        for item in response.json().get("items", []):
            vid = item.get("id")
            stats[vid] = item.get("statistics", {})
    return stats


def _aggregate_results(appid: str, items: list[dict], stats: dict[str, dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()
    rows = []
    for item in items:
        snippet = item.get("snippet", {})
        vid = item.get("id", {}).get("videoId")
        published_at = pd.to_datetime(snippet.get("publishedAt"), errors="coerce", utc=True)
        if pd.isna(published_at):
            continue
        view_count = None
        like_count = None
        if vid and vid in stats:
            view_count = pd.to_numeric(stats[vid].get("viewCount"), errors="coerce")
            like_count = pd.to_numeric(stats[vid].get("likeCount"), errors="coerce")
        rows.append({
            "year_month": published_at.to_period("M").to_timestamp(),
            "video_id": vid,
            "view_count": view_count if pd.notna(view_count) else 0,
            "like_count": like_count if pd.notna(like_count) else 0,
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    agg = (
        df.groupby("year_month")
        .agg(
            mentions=("video_id", "count"),
            views=("view_count", "sum"),
            likes=("like_count", "sum"),
        )
        .reset_index()
    )
    agg["appid"] = str(appid)
    return agg


def load_youtube_monthly(
    appid: str,
    cfg: dict,
    *,
    target_months: Optional[Iterable] = None,
    game_name: Optional[str] = None,
    force_refresh: bool = False,
) -> pd.DataFrame | None:
    mode = (cfg or {}).get("mode", "file")
    if mode == "file":
        path = Path(cfg.get("file", f"data/external/youtube/monthly_{appid}.csv"))
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

    api_key = cfg.get("api_key")
    if not api_key:
        raise ValueError("YouTube API mode requires api_key")

    query_template = cfg.get("query_template", "{name} gameplay review")
    if "{name}" in query_template:
        query = query_template.format(name=game_name or "")
    else:
        query = query_template
    if not query.strip():
        return None

    months = _normalize_months(target_months)
    if months:
        start_date = months[0]
        end_date = months[-1]
    else:
        months_back = int(cfg.get("months_back", 6))
        end_date = pd.Timestamp.utcnow().tz_localize("UTC")
        start_date = end_date - pd.DateOffset(months=months_back)
    start_iso = start_date.to_period("M").to_timestamp(tz="UTC").isoformat()
    end_iso = (end_date.to_period("M").to_timestamp(tz="UTC") + pd.offsets.MonthEnd(1)).isoformat()

    cache_dir = Path(cfg.get("api_cache_dir", "data/external/youtube/api_cache"))
    cache_path = cache_dir / f"{appid}.parquet"
    cache_df = _load_cache(cache_path)

    if force_refresh:
        cache_df = pd.DataFrame()

    need_fetch = force_refresh or cache_df.empty
    if not need_fetch and months:
        cache_df["year_month"] = pd.to_datetime(cache_df["year_month"], errors="coerce")
        if not set(months).issubset(set(cache_df["year_month"])):
            need_fetch = True

    if need_fetch:
        items = _search_videos(query, api_key, start_iso, end_iso, max_results=int(cfg.get("max_results", 200)))
        video_ids = [item.get("id", {}).get("videoId") for item in items if item.get("id")]
        video_ids = [vid for vid in video_ids if vid]
        stats = _fetch_video_stats(video_ids, api_key) if cfg.get("include_statistics", True) else {}
        fetched_df = _aggregate_results(appid, items, stats)
        if not fetched_df.empty:
            if not cache_df.empty:
                merged = pd.concat([cache_df, fetched_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=["year_month"], keep="last")
                merged = merged.sort_values("year_month").reset_index(drop=True)
            else:
                merged = fetched_df
            _save_cache(merged, cache_path)
            cache_df = merged

    if cache_df.empty:
        return None

    result = cache_df
    if months:
        result = result[result["year_month"].isin(months)]
    result["appid"] = str(appid)
    return result.reset_index(drop=True)
