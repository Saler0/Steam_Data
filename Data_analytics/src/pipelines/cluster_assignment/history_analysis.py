"""Storytelling utilities for cluster competitors.

This module detects eras and peaks in player activity, extracts review topics
with BERTopic around peaks, and aggregates optional social/granger signals.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import ast
import json
import numpy as np
import pandas as pd

from src.utils.io import read_parquet_any

try:
    from bertopic import BERTopic
    _BERTOPIC_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    _BERTOPIC_AVAILABLE = False


@dataclass
class TimeSeriesConfig:
    players_path: str
    reviews_path: Optional[str] = None
    reviews_text_column: str = "review_text"
    reviews_language_column: Optional[str] = "language"
    reviews_timestamp_column: str = "timestamp"
    preferred_languages: Optional[Iterable[str]] = ("en", "es")


@dataclass
class SocialConfig:
    twitch_path: Optional[str] = None
    youtube_path: Optional[str] = None
    timestamp_column: str = "date"
    value_column: str = "value"


@dataclass
class StorytellingConfig:
    ts: TimeSeriesConfig
    social: Optional[SocialConfig] = None
    events_path: Optional[str] = None
    topics_path: Optional[str] = None
    granger_summary_path: Optional[str] = None
    t_ref: Optional[pd.Timestamp] = None
    start_date: Optional[pd.Timestamp] = None
    end_date: Optional[pd.Timestamp] = None
    min_points: int = 90
    peak_window_days: int = 30
    peak_z_threshold: float = 2.0
    min_peak_gap: int = 21
    topic_window_days: int = 14
    min_topic_docs: int = 300
    bertopic_model: str = "paraphrase-multilingual-MiniLM-L12-v2"

def load_precomputed_events(path: Optional[str], appid: str) -> List[Dict[str, Any]]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    df = read_parquet_any(path) if p.suffix in {'.parquet', '.pq'} or p.is_dir() else pd.read_csv(path)
    if df.empty or 'appid' not in df.columns:
        return []
    df = df[df['appid'].astype(str) == str(appid)].copy()
    if df.empty:
        return []
    date_col = next((c for c in ['event_date', 'year_month', 'date', 'timestamp'] if c in df.columns), None)
    if not date_col:
        return []
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])
    if df.empty:
        return []
    if 'variable' in df.columns:
        df = df[df['variable'].astype(str).str.lower().isin(['players', 'player_count', 'active_players'])]
    direction_col = next((c for c in ['direction', 'type', 'event_type'] if c in df.columns), None)
    z_col = next((c for c in ['zscore', 'z', 'z_score'] if c in df.columns), None)
    delta_col = next((c for c in ['delta_vs_30d', 'delta', 'delta_ratio'] if c in df.columns), None)
    events: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        direction_val = row.get(direction_col) if direction_col else None
        if isinstance(direction_val, str) and direction_val.lower() not in {'peak', 'increase', 'rise', 'up'}:
            continue
        z_val = float(row.get(z_col)) if z_col and pd.notna(row.get(z_col)) else None
        if z_val is not None and z_val < 0:
            continue
        events.append({
            'date': pd.to_datetime(row[date_col]),
            'zscore': float(z_val) if z_val is not None else 0.0,
            'delta_vs_30d': float(row.get(delta_col) or 0.0),
        })
    events.sort(key=lambda e: e['date'])
    return events



def _parse_topics_value(value: Any) -> List[str]:
    if isinstance(value, list):
        items: List[str] = []
        for item in value:
            if isinstance(item, str):
                items.append(item)
            elif isinstance(item, dict):
                if 'Name' in item:
                    items.append(str(item['Name']))
                elif 'label' in item:
                    items.append(str(item['label']))
                elif 'keywords' in item and isinstance(item['keywords'], (list, tuple)):
                    items.append(', '.join(str(k) for k in item['keywords']))
                else:
                    items.append(str(item))
            else:
                items.append(str(item))
        return items
    if isinstance(value, dict):
        if 'Name' in value:
            return [str(value['Name'])]
        if 'label' in value:
            return [str(value['label'])]
        if 'keywords' in value and isinstance(value['keywords'], (list, tuple)):
            return [', '.join(str(k) for k in value['keywords'])]
        return [str(value)]
    if isinstance(value, str):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(value)
            except Exception:
                continue
            items = _parse_topics_value(parsed)
            if items:
                return items
        return [value]
    return []



def load_topics_lookup(path: Optional[str], appid: str) -> Dict[pd.Timestamp, List[str]]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    df = read_parquet_any(path) if p.suffix in {'.parquet', '.pq'} or p.is_dir() else pd.read_csv(path)
    if df.empty or 'appid' not in df.columns:
        return {}
    df = df[df['appid'].astype(str) == str(appid)].copy()
    if df.empty:
        return {}
    date_col = next((c for c in ['event_year_month', 'event_date', 'year_month', 'date'] if c in df.columns), None)
    if not date_col:
        return {}
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])
    if df.empty:
        return {}
    topics_col = next((c for c in ['topics', 'topic_labels', 'labels'] if c in df.columns), None)
    if not topics_col:
        return {}
    lookup: Dict[pd.Timestamp, List[str]] = {}
    for _, row in df.iterrows():
        topics = _parse_topics_value(row.get(topics_col))
        if not topics:
            continue
        key = pd.to_datetime(row[date_col]).normalize()
        lookup[key] = [str(t) for t in topics]
    return lookup



def _read_timeseries(path: str, appid: str, value_candidates: Iterable[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    df = read_parquet_any(path) if p.suffix in {".parquet", ".pq"} or p.is_dir() else pd.read_csv(path)
    if df.empty:
        return df
    # Normalise appid to string
    if "appid" not in df.columns:
        raise ValueError(f"Timeseries file {path} must include an 'appid' column")
    df = df[df["appid"].astype(str) == str(appid)].copy()
    if df.empty:
        return df
    # Detect date column
    for col in ("date", "timestamp", "day", "ds", "year_month"):
        if col in df.columns:
            date_col = col
            break
    else:
        raise ValueError(f"Could not infer date column for timeseries in {path}")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df = df.sort_values(date_col)
    value_col = None
    for cand in value_candidates:
        if cand in df.columns:
            value_col = cand
            break
    if value_col is None:
        raise ValueError(f"Timeseries for {appid} in {path} is missing expected value columns {value_candidates}")
    df = df[[date_col, value_col]].rename(columns={date_col: "date", value_col: "value"})
    # Aggregate if duplicates
    df = df.groupby("date", as_index=False)["value"].sum()
    return df


def _to_daily(ts: pd.DataFrame) -> pd.DataFrame:
    if ts.empty:
        return ts
    ts = ts.set_index("date").sort_index()
    freq = pd.infer_freq(ts.index)
    if freq is None:
        freq = "D"
    start, end = ts.index.min(), ts.index.max()
    daily_index = pd.date_range(start=start, end=end, freq="D")
    ts = ts.reindex(daily_index)
    ts.index.name = "date"
    ts["value"] = ts["value"].interpolate(limit_direction="both")
    return ts.reset_index()


def _normalise_series(ts: pd.Series) -> pd.Series:
    if ts.empty:
        return ts
    max_val = float(ts.max())
    if not np.isfinite(max_val) or max_val == 0:
        return ts.fillna(0.0)
    return ts / max_val


def _clip_timeseries(ts: pd.DataFrame, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> pd.DataFrame:
    if ts.empty:
        return ts
    df = ts.copy()
    if start is not None:
        start_dt = pd.to_datetime(start)
        df = df[df['date'] >= start_dt]
    if end is not None:
        end_dt = pd.to_datetime(end)
        df = df[df['date'] <= end_dt]
    return df.reset_index(drop=True)


def compute_activity_metrics(ts: pd.DataFrame, t_ref: Optional[pd.Timestamp] = None) -> Dict[str, float]:
    if ts.empty:
        return {"r7": 0.0, "r14": 0.0, "r30": 0.0, "r90": 0.0, "d30": 0.0, "d90": 0.0, "zpeak30": 0.0}
    ts = ts.copy()
    ts = _to_daily(ts)
    if t_ref is None:
        t_ref = ts["date"].max()
    ts = ts[ts["date"] <= t_ref]
    if ts.empty:
        return {"r7": 0.0, "r14": 0.0, "r30": 0.0, "r90": 0.0, "d30": 0.0, "d90": 0.0, "zpeak30": 0.0}
    ts_norm = _normalise_series(ts["value"])
    def mean_window(window: int) -> float:
        recent = ts_norm.tail(window)
        return float(recent.mean()) if len(recent) else 0.0
    def slope(window: int) -> float:
        recent = ts.tail(window)
        if len(recent) < 3:
            return 0.0
        y = recent["value"].to_numpy()
        x = np.arange(len(y))
        slope_val, _ = np.polyfit(x, y, 1)
        return float(slope_val)
    metrics = {
        "r7": mean_window(7),
        "r14": mean_window(14),
        "r30": mean_window(30),
        "r90": mean_window(90),
        "d30": slope(30),
        "d90": slope(90),
        "zpeak30": 0.0,
    }
    last30 = ts.tail(30)
    if len(last30) > 5:
        mu = float(last30["value"].mean())
        sigma = float(last30["value"].std(ddof=0) or 1.0)
        peak_val = float(last30["value"].max())
        metrics["zpeak30"] = (peak_val - mu) / sigma if sigma else 0.0
    return metrics


def categorise_competitor(metrics: Dict[str, float], launch_overlap: bool = False, launch_mode: bool = False) -> str:
    r30, r7, r90 = metrics.get("r30", 0.0), metrics.get("r7", 0.0), metrics.get("r90", 0.0)
    d30, zpeak30 = metrics.get("d30", 0.0), metrics.get("zpeak30", 0.0)
    today = (r30 >= 0.25) and (r7 >= 0.20) and (d30 >= 0 or zpeak30 >= 2.0) and (not launch_mode or launch_overlap)
    if today:
        return "today"
    recent = (r90 >= 0.20) or (zpeak30 >= 2.0)
    if recent:
        return "recent"
    return "historical"


def _detect_trend(value: pd.Series, slope_window: int = 30, slope_threshold: float = 0.0) -> pd.Series:
    slopes = value.rolling(window=slope_window, min_periods=max(3, slope_window // 2)).apply(
        lambda arr: np.polyfit(np.arange(len(arr)), arr, 1)[0], raw=True
    )
    slopes = slopes.fillna(0.0)
    trend = pd.Series(index=value.index, dtype="object")
    trend[slopes > slope_threshold] = "up"
    trend[slopes < -slope_threshold] = "down"
    trend[(slopes >= -slope_threshold) & (slopes <= slope_threshold)] = "flat"
    return trend.ffill().bfill()


def detect_eras(ts: pd.DataFrame) -> List[Dict[str, Any]]:
    if ts.empty:
        return []
    ts = _to_daily(ts)
    ts = ts.set_index("date")
    trend = _detect_trend(ts["value"], slope_window=30, slope_threshold=0.0)
    quantiles = ts["value"].quantile([0.33, 0.66]).to_dict()
    eras: List[Dict[str, Any]] = []
    current = None
    for date, tr in trend.items():
        if current is None or tr != current["trend"]:
            if current is not None:
                eras.append(current)
            current = {"start": date, "end": date, "trend": tr, "values": [float(ts.loc[date, "value"])]}
        else:
            current["end"] = date
            current["values"].append(float(ts.loc[date, "value"]))
    if current is not None:
        eras.append(current)
    def phase_from_values(vals: List[float], trend_label: str) -> str:
        if not vals:
            return "desconocido"
        avg = float(np.mean(vals))
        if trend_label == "up":
            return "crecimiento"
        if trend_label == "down":
            return "declive"
        low, high = quantiles.get(0.33, 0.0), quantiles.get(0.66, 0.0)
        if avg <= low:
            return "introduccion"
        if avg >= high:
            return "madurez"
        return "meseta"
    formatted: List[Dict[str, Any]] = []
    for era in eras:
        values = era.pop("values")
        formatted.append(
            {
                "start": era["start"].strftime("%Y-%m-%d"),
                "end": era["end"].strftime("%Y-%m-%d"),
                "trend": era["trend"],
                "phase": phase_from_values(values, era["trend"]),
                "avg_players": float(np.mean(values)),
            }
        )
    return formatted


def detect_peaks(ts: pd.DataFrame, window_days: int = 30, min_gap: int = 21, z_threshold: float = 2.0) -> List[Dict[str, Any]]:
    if ts.empty:
        return []
    ts = _to_daily(ts)
    ts = ts.set_index("date")
    rolling_mean = ts["value"].rolling(window_days, min_periods=max(5, window_days // 2)).mean()
    rolling_std = ts["value"].rolling(window_days, min_periods=max(5, window_days // 2)).std()
    z_scores = (ts["value"] - rolling_mean) / rolling_std.replace(0, np.nan)
    z_scores = z_scores.dropna()
    peaks: List[Dict[str, Any]] = []
    last_peak_date: Optional[pd.Timestamp] = None
    for date, z in z_scores.sort_values(ascending=False).items():
        if z < z_threshold:
            break
        if last_peak_date is not None and abs((date - last_peak_date).days) < min_gap:
            continue
        delta_vs_30d = 0.0
        window_start = date - pd.Timedelta(days=window_days)
        prev_window = ts.loc[window_start:date]["value"]
        if len(prev_window) > 0:
            delta_vs_30d = float((ts.loc[date, "value"] - prev_window.mean()) / prev_window.mean()) if prev_window.mean() else 0.0
        peaks.append(
            {
                "date": date,
                "zscore": float(z),
                "value": float(ts.loc[date, "value"]),
                "delta_vs_30d": delta_vs_30d,
            }
        )
        last_peak_date = date
    return peaks


def _load_reviews(cfg: TimeSeriesConfig, appid: str, peak_date: pd.Timestamp, window_days: int) -> pd.DataFrame:
    if not cfg.reviews_path:
        return pd.DataFrame()
    p = Path(cfg.reviews_path)
    if not p.exists():
        return pd.DataFrame()
    df = read_parquet_any(cfg.reviews_path) if p.suffix in {".parquet", ".pq"} or p.is_dir() else pd.read_csv(cfg.reviews_path)
    if df.empty:
        return df
    if "appid" not in df.columns:
        raise ValueError("Reviews dataset must include an 'appid' column")
    df = df[df["appid"].astype(str) == str(appid)].copy()
    if df.empty:
        return df
    ts_col = cfg.reviews_timestamp_column
    if ts_col not in df.columns:
        raise ValueError(f"Reviews dataset is missing timestamp column '{ts_col}'")
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col])
    start = peak_date - pd.Timedelta(days=window_days)
    end = peak_date + pd.Timedelta(days=window_days)
    df = df[(df[ts_col] >= start) & (df[ts_col] <= end)]
    if cfg.preferred_languages and cfg.reviews_language_column and cfg.reviews_language_column in df.columns:
        df = df[df[cfg.reviews_language_column].isin(cfg.preferred_languages)]
    text_col = cfg.reviews_text_column
    if text_col not in df.columns:
        raise ValueError(f"Reviews dataset must include text column '{text_col}'")
    df = df.dropna(subset=[text_col])
    return df


def extract_topics_for_peak(cfg: StorytellingConfig, appid: str, peak_date: pd.Timestamp) -> List[str]:
    reviews = _load_reviews(cfg.ts, appid, peak_date, cfg.topic_window_days)
    if reviews.empty:
        return []
    texts = reviews[cfg.ts.reviews_text_column].astype(str).tolist()
    if len(texts) < cfg.min_topic_docs:
        return []
    if not _BERTOPIC_AVAILABLE:
        print("[WARN] BERTopic no disponible; se omiten topics.")
        return []
    try:
        topic_model = BERTopic(embedding_model=cfg.bertopic_model, calculate_probabilities=False, verbose=False)
        topics, _ = topic_model.fit_transform(texts)
        info = topic_model.get_topic_info()
        # Excluir el t?pico -1 (outliers)
        info = info[info["Topic"] >= 0]
        top_topics = info.head(4)["Name"].tolist()
        return [str(t) for t in top_topics]
    except Exception as exc:  # pragma: no cover - dependencias externas
        print(f"[WARN] BERTopic fall? para app {appid}: {exc}")
        return []


def load_social_signal(cfg: Optional[SocialConfig], appid: str, peak_date: pd.Timestamp, window_days: int) -> Dict[str, Any]:
    if cfg is None:
        return {}
    social_summary: Dict[str, Any] = {}
    for label, path in (("twitch", cfg.twitch_path), ("youtube", cfg.youtube_path)):
        if not path:
            continue
        p = Path(path)
        if not p.exists():
            continue
        df = read_parquet_any(path) if p.suffix in {".parquet", ".pq"} or p.is_dir() else pd.read_csv(path)
        if df.empty or "appid" not in df.columns:
            continue
        df = df[df["appid"].astype(str) == str(appid)].copy()
        if df.empty or cfg.timestamp_column not in df.columns or cfg.value_column not in df.columns:
            continue
        df[cfg.timestamp_column] = pd.to_datetime(df[cfg.timestamp_column], errors="coerce")
        df = df.dropna(subset=[cfg.timestamp_column])
        start = peak_date - pd.Timedelta(days=window_days)
        end = peak_date + pd.Timedelta(days=window_days)
        window = df[(df[cfg.timestamp_column] >= start) & (df[cfg.timestamp_column] <= end)][cfg.value_column]
        if window.empty:
            continue
        mu = float(window.mean())
        sigma = float(window.std(ddof=0) or 1.0)
        latest = float(window.iloc[-1])
        z = (latest - mu) / sigma if sigma else 0.0
        social_summary[f"{label}_z"] = z
    return social_summary


def load_granger_info(summary_path: Optional[str], appid: str) -> Dict[str, Any]:
    if not summary_path or not Path(summary_path).exists():
        return {}
    df = read_parquet_any(summary_path) if summary_path.endswith(".parquet") else pd.read_csv(summary_path)
    if df.empty or "appid" not in df.columns:
        return {}
    df = df[df["appid"].astype(str) == str(appid)]
    if df.empty:
        return {}
    keep_cols = [c for c in (
        "best_lag",
        "best_ccf",
        "best_pval",
        "granger_xy_pmin",
        "granger_xy_sig",
        "granger_xy_p_fdr",
        "granger_xy_sig_fdr",
        "granger_yx_pmin",
        "granger_yx_sig",
        "granger_yx_p_fdr",
        "granger_yx_sig_fdr",
    ) if c in df.columns]
    return df[keep_cols].to_dict(orient="records")[0] if keep_cols else {}


def build_story(cfg: StorytellingConfig, appid: str) -> Dict[str, Any]:
    players = _read_timeseries(cfg.ts.players_path, appid, value_candidates=['players', 'avg_players', 'value'])
    players_daily = _to_daily(players) if not players.empty else players
    eras = detect_eras(players)
    precomputed_events = load_precomputed_events(cfg.events_path, appid)
    topics_lookup = load_topics_lookup(cfg.topics_path, appid)
    key_peaks: List[Dict[str, Any]] = []
    if precomputed_events:
        for event in precomputed_events:
            date = event['date']
            entry = {
                'date': date.strftime('%Y-%m-%d'),
                'z': round(float(event.get('zscore', 0.0)), 2),
                'delta_vs_30d': round(float(event.get('delta_vs_30d', 0.0)), 3),
            }
            topics = topics_lookup.get(date.normalize()) or topics_lookup.get(date)
            if topics:
                entry['topics'] = topics
            social_info = load_social_signal(cfg.social, appid, date, cfg.topic_window_days)
            if social_info:
                entry['social'] = social_info
            key_peaks.append(entry)
    else:
        peaks = detect_peaks(players, window_days=cfg.peak_window_days, min_gap=cfg.min_peak_gap, z_threshold=cfg.peak_z_threshold)
        for peak in peaks:
            date = peak['date']
            topics = topics_lookup.get(date.normalize()) or topics_lookup.get(date)
            if topics is None:
                topics = extract_topics_for_peak(cfg, appid, date)
            social_info = load_social_signal(cfg.social, appid, date, cfg.topic_window_days)
            entry = {
                'date': date.strftime('%Y-%m-%d'),
                'z': round(peak['zscore'], 2),
                'delta_vs_30d': round(peak['delta_vs_30d'], 3),
            }
            if topics:
                entry['topics'] = topics
            if social_info:
                entry['social'] = social_info
            key_peaks.append(entry)
    players_recent = _clip_timeseries(players_daily, cfg.start_date, cfg.end_date) if (cfg.start_date or cfg.end_date) else players_daily.copy()
    granger = load_granger_info(cfg.granger_summary_path, appid)
    global_takeaways: List[str] = []
    for peak in key_peaks:
        topics = ', '.join(peak.get('topics', [])) or 'sin tópicos claros'
        global_takeaways.append(f"Pico {peak['date']} (z={peak['z']}) asociado a {topics}.")
    if granger:
        if granger.get('granger_xy_sig_fdr') or granger.get('granger_xy_sig'):
            global_takeaways.append('Las reseñas anteceden a los jugadores (Granger significativa).')
        if granger.get('granger_yx_sig_fdr') or granger.get('granger_yx_sig'):
            global_takeaways.append('Los jugadores anteceden a las reseñas (Granger significativa).')
    metrics_global = compute_activity_metrics(players_daily, cfg.t_ref)
    global_story = {
        'eras': eras,
        'key_peaks': key_peaks,
        'takeaways': global_takeaways,
    }
    recent_story = None
    metrics_recent: Dict[str, float] | None = None
    start_iso = cfg.start_date.strftime('%Y-%m-%d') if cfg.start_date is not None else None
    end_iso = cfg.end_date.strftime('%Y-%m-%d') if cfg.end_date is not None else None
    if cfg.start_date or cfg.end_date:
        if players_recent is not None and not players_recent.empty and len(players_recent) >= cfg.min_points:
            metrics_recent = compute_activity_metrics(players_recent, cfg.t_ref or (cfg.end_date or players_recent['date'].max()))
            recent_story = {
                'window': {'start': start_iso, 'end': end_iso},
                'status': 'ok',
                'metrics': metrics_recent,
                'causality': granger or {},
            }
        else:
            recent_story = {
                'window': {'start': start_iso, 'end': end_iso},
                'status': 'too_short_window',
                'metrics': {},
            }
    takeaways = list(global_takeaways)
    if recent_story:
        if recent_story['status'] == 'ok':
            takeaways.append('Ventana reciente indica actividad suficiente para análisis (métricas recientes calculadas).')
        else:
            takeaways.append('Ventana reciente demasiado corta para análisis de causalidad.')
    return {
        'global': global_story,
        'recent': recent_story,
        'takeaways': takeaways,
        'granger': granger,
        'metrics': metrics_global,
        'metrics_recent': metrics_recent,
        'activity_timeseries': players_daily.to_dict(orient='records') if not players_daily.empty else [],
        'activity_timeseries_recent': players_recent.to_dict(orient='records') if (cfg.start_date or cfg.end_date) and players_recent is not None and not players_recent.empty else [],
        'eras': global_story['eras'],
        'key_peaks': global_story['key_peaks'],
    }


