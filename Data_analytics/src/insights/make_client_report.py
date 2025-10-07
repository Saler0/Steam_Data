#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera un estudio de mercado (JSON) para un juego del cliente usando artefactos precomputados.
- Codifica la descripcion del cliente en un embedding (SentenceTransformer)
- Asigna un cluster (por medoid si existe, si no por vecino mas cercano)
- Obtiene los K vecinos (competidores) mas similares
- Anexa insights disponibles por competidor (CCF, eventos, topicos, explicaciones)
- Calcula un analisis minimo de reglas para el cliente (p. ej., precio vs mediana de cluster)
"""
from __future__ import annotations
import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, StatisticsError
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import os
import sys

import numpy as np
import pandas as pd
import yaml
import mlflow
# Ensure project root is importable when running as a script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.config_utils import expand_env_in_obj
from src.utils.mlflow_utils import log_mlflow_params, log_mlflow_metrics
from src.insights.neighbor_strategy import EmbeddingIndex, select_competitor_neighbors
from src.insights.text_templates import (
    build_topic_slug,
    describe_metadata,
    describe_cluster_note,
    describe_peak_reasons,
    summarize_context,
    generate_review_highlights,
    describe_takeaways,
    describe_pricing,
    describe_topic_insight,
    describe_key_signal,
    summarize_global_relevance,
    compose_client_narrative,
)

from src.utils.io import (
    read_parquet_any, read_csv_any, read_json_any,
    write_json_any, path_exists
)


def _build_doc_from_client(client: Dict[str, Any], doc_fields: Dict[str, Any]) -> str:
    parts: List[str] = []
    for field in doc_fields.get('text_fields', []):
        if client.get(field):
            parts.append(str(client[field]))
    for field in doc_fields.get('tag_fields', []):
        tags = client.get(field, [])
        if tags:
            parts.append(" ".join([str(t).replace(" ", "_") for t in tags]))
    return " \n".join([p for p in parts if p])


def _load_any_df(p: str) -> pd.DataFrame:
    if not p:
        return pd.DataFrame()
    if p.endswith('.csv'):
        return read_csv_any(p)
    if p.endswith('.json'):
        return read_json_any(p)
    return read_parquet_any(p)


def _safe_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(val):
        return None
    return val


def _safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        val = int(float(value))
    except (TypeError, ValueError):
        return None
    return val


def _mean_or_none(values: Sequence[Optional[float]]) -> Optional[float]:
    filtered = [v for v in values if v is not None and not math.isnan(v)]
    if not filtered:
        return None
    try:
        return float(mean(filtered))
    except StatisticsError:
        return None


def _to_str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value)
    if not text.strip():
        return []
    for sep in [';', '|']:
        text = text.replace(sep, ',')
    return [part.strip() for part in text.split(',') if part.strip()]


def _normalize_tokens(tokens: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for token in tokens:
        if token is None:
            continue
        cleaned = str(token).strip()
        if not cleaned:
            continue
        normalized.append(cleaned.lower())
    return normalized


def _infer_modes_from_tokens(tokens: Iterable[str]) -> List[str]:
    modes: set[str] = set()
    for token in tokens:
        if not token:
            continue
        t = str(token).strip().lower()
        if not t:
            continue
        if 'pvp' in t:
            modes.add('pvp')
        if 'pve' in t or 'player vs environment' in t:
            modes.add('pve')
        if 'coop' in t or 'co-op' in t or 'cooperative' in t:
            modes.add('coop')
        if 'multiplayer' in t:
            modes.add('multiplayer')
        if 'single' in t:
            modes.add('singleplayer')
    return sorted(modes)


def _load_cluster_stats_df(path: str) -> pd.DataFrame:
    if not path or not path_exists(path):
        return pd.DataFrame()
    df = _load_any_df(path)
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    if 'cluster_id' in df.columns:
        df['cluster_id'] = df['cluster_id'].astype(str)
    return df


def _load_cluster_topics_map(path: str) -> Dict[str, List[Dict[str, Any]]]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {}
    topics_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    items: Iterable[Any]
    if isinstance(data, dict):
        items = data.get('clusters') or data.get('data') or []
    else:
        items = data
    for item in items:
        if not isinstance(item, dict):
            continue
        cid = item.get('cluster_id')
        if cid is None:
            continue
        entry = {
            'topic_id': item.get('topic_id'),
            'name': item.get('name'),
            'keywords': item.get('keywords') or [],
            'repr_docs': _safe_int(item.get('repr_docs')),
            'share': _safe_float(item.get('share')),
            'recent_share': _safe_float(item.get('recent_share')),
            'recent_share_delta': _safe_float(item.get('recent_share_delta')),
            'avg_sentiment': _safe_float(item.get('avg_sentiment')),
            'coherence': _safe_float(item.get('coherence')),
            'takeaway': item.get('takeaway'),
            'examples': item.get('examples') or [],
        }
        clean_entry = {k: v for k, v in entry.items() if v not in (None, [], {})}
        clean_entry['keywords'] = clean_entry.get('keywords', [])
        if isinstance(clean_entry.get('examples'), list):
            clean_entry['examples'] = [ex for ex in clean_entry['examples'] if ex not in (None, {}, [])]
        topics_map[str(cid)].append(clean_entry)
    for key, values in topics_map.items():
        values.sort(key=lambda x: ((x.get('share') or 0.0), (x.get('repr_docs') or 0)), reverse=True)
    return topics_map


def _extract_languages(client: Dict[str, Any], meta_row: Optional[pd.Series]) -> List[str]:
    languages = _to_str_list(client.get('languages'))
    if languages:
        return languages
    if meta_row is not None:
        for col in ('languages', 'supported_languages', 'supported_languages_list'):
            if col in meta_row and pd.notna(meta_row[col]):
                langs = _to_str_list(meta_row[col])
                if langs:
                    return langs
    return []


def _cluster_appids(clu_df: pd.DataFrame, cluster_id: Optional[int]) -> List[str]:
    if cluster_id is None or clu_df.empty:
        return []
    cid = str(cluster_id)
    subset = clu_df[clu_df['cluster_id'].astype(str) == cid]
    if subset.empty:
        return []
    return subset['appid'].astype(str).tolist()


def _compute_average_age_years(appids: List[str], meta_df: pd.DataFrame) -> Optional[float]:
    if not appids or meta_df.empty:
        return None
    ages: List[float] = []
    today = datetime.now(timezone.utc)
    for aid in appids:
        subset = meta_df[meta_df['appid'].astype(str) == aid]
        if subset.empty:
            continue
        value = subset.iloc[0].get('release_date')
        if value is None or (isinstance(value, float) and math.isnan(value)):
            continue
        dt = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt):
            continue
        if getattr(dt, 'tzinfo', None) is None:
            try:
                dt = dt.tz_localize('UTC')
            except Exception:
                dt = dt.tz_localize('UTC', nonexistent='NaT', ambiguous='NaT')
        dt_utc = dt.to_pydatetime().astimezone(timezone.utc)
        age_years = (today - dt_utc).days / 365.25
        ages.append(age_years)
    if not ages:
        return None
    return round(float(sum(ages) / len(ages)), 2)


def _classify_saturation(cluster_size: Optional[int], diagnostics: Dict[str, Any]) -> str:
    if diagnostics.get('diluted'):
        return 'alto'
    avg_sim = diagnostics.get('average_in_similarity')
    if avg_sim is not None and avg_sim < 0.8:
        return 'alto'
    if cluster_size is not None and cluster_size >= 500:
        return 'alto'
    if cluster_size is not None and cluster_size >= 200:
        return 'medio'
    return 'bajo'


def _compose_cluster_note(saturation: str, diagnostics: Dict[str, Any]) -> str:
    micro = diagnostics.get('microsegmentation') or {}
    micro_desc = ''
    if micro.get('applied'):
        kept = micro.get('kept')
        original = micro.get('original')
        if kept and original:
            micro_desc = f"Microsegmento refinado ({kept}/{original} intra-cluster)"
    silhouette = diagnostics.get('silhouette_proxy')
    return describe_cluster_note(saturation, micro_desc, silhouette)


def _topic_payload_from_map(cid: Optional[int], topics_map: Dict[str, List[Dict[str, Any]]], limit: int = 5) -> List[Dict[str, Any]]:
    if cid is None:
        return []
    items = topics_map.get(str(cid)) or []
    payload: List[Dict[str, Any]] = []
    for idx, item in enumerate(items[:limit]):
        keywords = item.get('keywords') or []
        entry = {
            'topic_id': item.get('topic_id'),
            'name': item.get('name') or build_topic_slug(idx, keywords),
            'keywords': keywords,
            'repr_docs': item.get('repr_docs'),
            'share': item.get('share'),
            'recent_share': item.get('recent_share'),
            'recent_share_delta': item.get('recent_share_delta'),
            'avg_sentiment': item.get('avg_sentiment'),
            'coherence': item.get('coherence'),
            'examples': item.get('examples') or [],
            'takeaway': item.get('takeaway'),
        }
        entry['keywords'] = entry.get('keywords', [])
        entry['examples'] = [ex for ex in entry.get('examples', []) if ex not in (None, {}, [])]
        entry = {k: v for k, v in entry.items() if v not in (None, [], {})}
        payload.append(entry)
    return payload






def _maybe_parse_json_like(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text and text[0] in '{[' and text[-1] in ']}' and len(text) >= 2:
            try:
                return json.loads(text)
            except Exception:
                return value
    return value


def _standardize_period(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        ts = pd.to_datetime(value, errors='coerce')
    except Exception:
        text = str(value).strip()
        return text or None
    if pd.isna(ts):
        text = str(value).strip()
        return text or None
    if isinstance(ts, pd.Timestamp):
        return ts.strftime('%Y-%m-%d')
    if isinstance(ts, datetime):
        return ts.strftime('%Y-%m-%d')
    return str(ts)


def _sanitize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        value = _maybe_parse_json_like(value)
        if isinstance(value, pd.Timestamp):
            value = value.strftime('%Y-%m-%d')
        elif isinstance(value, datetime):
            value = value.isoformat()
        clean[key] = value
    return clean


def _extract_social_snapshot(event_row: Dict[str, Any]) -> Dict[str, Any]:
    social: Dict[str, Any] = {}
    youtube: Dict[str, Any] = {}
    twitch: Dict[str, Any] = {}
    mapping = {
        'z_views': ['youtube_z', 'youtube_z_views', 'youtube_zscore'],
        'best_lag_d': ['youtube_best_lag_d', 'youtube_lag'],
        'best_ccf': ['youtube_best_ccf'],
    }
    for out_key, candidates in mapping.items():
        for cand in candidates:
            if cand in event_row:
                value = _safe_float(event_row.get(cand))
                if value is not None:
                    youtube[out_key] = value
                    break
    if 'best_lag_d' not in youtube and 'youtube_best_lag_d' in event_row:
        alt = _safe_int(event_row.get('youtube_best_lag_d'))
        if alt is not None:
            youtube['best_lag_d'] = alt
    creators = event_row.get('youtube_top_creators')
    creators = _maybe_parse_json_like(creators)
    if isinstance(creators, list) and creators:
        youtube['top_creators'] = creators
    if youtube:
        social['youtube'] = youtube

    mapping_tw = {
        'z_concurrent': ['twitch_z', 'twitch_zscore'],
        'best_lag_d': ['twitch_best_lag_d', 'twitch_lag'],
        'best_ccf': ['twitch_best_ccf'],
    }
    for out_key, candidates in mapping_tw.items():
        for cand in candidates:
            if cand in event_row:
                value = _safe_float(event_row.get(cand))
                if value is not None:
                    twitch[out_key] = value
                    break
    if twitch:
        social['twitch'] = twitch
    return social


def _compose_event_context(event_row: Dict[str, Any]) -> Dict[str, Any]:
    context: Dict[str, Any] = {}
    steam: Dict[str, Any] = {}
    external: Dict[str, Any] = {}
    summary = event_row.get('context_summary') or event_row.get('summary')
    for key, value in list(event_row.items()):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            continue
        if key.startswith('steam_'):
            steam[key.replace('steam_', '')] = value
        elif key in {'press_mentions', 'reddit_threads', 'news_mentions', 'external_mentions'}:
            external[key] = value
    if steam:
        context['steam'] = steam
    if external:
        context['external'] = external
    if summary:
        context['summary'] = summary
    return context


def _prepare_topic_entry(topic: Dict[str, Any]) -> Dict[str, Any]:
    entry = {
        'topic_id': topic.get('topic_id'),
        'name': topic.get('name'),
        'keywords': topic.get('keywords') or [],
        'share': _safe_float(topic.get('share')),
        'recent_share': _safe_float(topic.get('recent_share')),
        'recent_share_delta': _safe_float(topic.get('recent_share_delta')),
        'avg_sentiment': _safe_float(topic.get('avg_sentiment')),
        'coherence': _safe_float(topic.get('coherence')),
        'repr_docs': _safe_int(topic.get('repr_docs')),
        'takeaway': topic.get('takeaway') or topic.get('summary'),
        'examples': topic.get('examples') or [],
    }
    entry['keywords'] = entry.get('keywords', [])
    entry['examples'] = [ex for ex in entry.get('examples', []) if ex not in (None, {}, [])]
    return {k: v for k, v in entry.items() if v not in (None, [], {})}




def _prepare_review_segments(raw: Any) -> Dict[str, Any]:
    data = _maybe_parse_json_like(raw)
    if isinstance(data, dict) and 'review_segments' in data and not data.get('median_hours'):
        candidate = data.get('review_segments')
        if isinstance(candidate, dict):
            data = candidate
    if not isinstance(data, dict):
        return {}

    result: Dict[str, Any] = {}
    median_hours = _safe_float(data.get('median_hours'))
    if median_hours is not None:
        result['median_hours'] = median_hours

    scope_payload: Dict[str, Any] = {}
    scope = data.get('scope') or {}
    if isinstance(scope, dict):
        month = scope.get('month') or scope.get('period') or scope.get('date')
        if month is not None:
            scope_payload['month'] = str(month)
        for key in ('reviews_total', 'pos', 'neg', 'neutral'):
            value = scope.get(key)
            if value is not None:
                scoped_val = _safe_int(value)
                if scoped_val is None:
                    scoped_val = _safe_float(value)
                if scoped_val is not None:
                    scope_payload[key] = scoped_val
        if scope_payload:
            result['scope'] = scope_payload

    experience_map = data.get('by_experience') or data.get('experience_segments') or {}
    if isinstance(experience_map, dict):
        prepared_segments: Dict[str, Dict[str, Any]] = {}
        for label, payload in experience_map.items():
            if not isinstance(payload, dict):
                continue
            segment: Dict[str, Any] = {}
            count_val = payload.get('count')
            if count_val is not None:
                count_int = _safe_int(count_val)
                if count_int is not None:
                    segment['count'] = count_int
            for key in ('share', 'pos', 'neg', 'abandon_rate_30d', 'gifted_share'):
                value = payload.get(key)
                if value is not None:
                    numeric = _safe_float(value)
                    if numeric is not None:
                        segment[key] = numeric
            phase = payload.get('phase')
            if isinstance(phase, dict):
                segment['phase'] = {k: bool(v) for k, v in phase.items() if v is not None}
            bertopic_items = payload.get('bertopic') or []
            prepared_topics: List[Dict[str, Any]] = []
            if isinstance(bertopic_items, (list, tuple)):
                for item in bertopic_items:
                    if isinstance(item, dict):
                        topic_entry = _prepare_topic_entry(item)
                        if topic_entry:
                            prepared_topics.append(topic_entry)
            if prepared_topics:
                segment['bertopic'] = prepared_topics
            if segment:
                prepared_segments[str(label)] = segment
        if prepared_segments:
            result['by_experience'] = prepared_segments
            auto_highlights = generate_review_highlights(prepared_segments)
            if auto_highlights:
                result['highlights'] = auto_highlights

    highlights = data.get('highlights')
    if isinstance(highlights, (list, tuple)):
        cleaned = [str(item).strip() for item in highlights if str(item).strip()]
        if cleaned:
            existing_list = result.get('highlights', [])
            existing_set = set(existing_list)
            combined = existing_list + [h for h in cleaned if h not in existing_set]
            result['highlights'] = combined

    return result if result else {}

def _split_topics_by_sentiment(topics: List[Dict[str, Any]], period: Optional[str]) -> Dict[str, List[Dict[str, Any]]]:
    positive: List[Dict[str, Any]] = []
    negative: List[Dict[str, Any]] = []
    for topic in topics:
        topic_period = _standardize_period(topic.get('event_year_month') or topic.get('year_month') or topic.get('date'))
        if period and topic_period and topic_period != period:
            continue
        polarity = str(topic.get('relevance_polarity') or '').lower()
        sentiment = _safe_float(topic.get('avg_sentiment'))
        entry = _prepare_topic_entry(topic)
        if polarity == 'negative' or (polarity == '' and sentiment is not None and sentiment < -0.1):
            negative.append(entry)
        else:
            positive.append(entry)
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    if positive:
        buckets['positive'] = positive
    if negative:
        buckets['negative'] = negative
    return buckets



def _compute_cluster_context(cluster_id: Optional[int], diagnostics: Dict[str, Any], cluster_stats: pd.DataFrame,
                             topics_map: Dict[str, List[Dict[str, Any]]], clu_df: pd.DataFrame, meta_df: pd.DataFrame) -> Dict[str, Any]:
    if cluster_id is None:
        return {}
    stats_row = pd.DataFrame()
    if not cluster_stats.empty:
        stats_row = cluster_stats[cluster_stats['cluster_id'] == str(cluster_id)]
    cluster_size = _safe_int(stats_row.iloc[0]['size']) if not stats_row.empty and 'size' in stats_row.columns else None
    micro_info = diagnostics.get('microsegmentation') or {}
    microsegment_size = _safe_int(micro_info.get('kept')) or _safe_int(micro_info.get('original'))
    if microsegment_size is None:
        microsegment_size = diagnostics.get('intra_candidates')
    avg_age_years = _compute_average_age_years(_cluster_appids(clu_df, cluster_id), meta_df)
    saturation = _classify_saturation(cluster_size, diagnostics)
    note = _compose_cluster_note(saturation, diagnostics)
    return {
        'microsegment_size': microsegment_size,
        'cluster_size': cluster_size,
        'saturation': saturation,
        'avg_age_years': avg_age_years,
        'note': note,
        'topics': _topic_payload_from_map(cluster_id, topics_map),
    }


def _extract_metadata_row(meta_df: pd.DataFrame, appid: str) -> Dict[str, Any]:
    if meta_df.empty:
        return {}
    subset = meta_df[meta_df['appid'].astype(str) == appid]
    if subset.empty:
        return {}
    return subset.iloc[0].to_dict()


def _build_business_fit(client_meta: Dict[str, Any], competitor_meta: Dict[str, Any]) -> Dict[str, Any]:
    client_tags = set(_normalize_tokens(client_meta.get('tags_normalized') or []))
    comp_tags = set(_normalize_tokens(competitor_meta.get('tags_normalized') or []))
    tags_overlap = len(client_tags & comp_tags) if client_tags and comp_tags else 0
    client_modes = set(client_meta.get('modes') or [])
    comp_modes = set(competitor_meta.get('modes') or [])
    mode_match = sorted(client_modes & comp_modes) if client_modes and comp_modes else []
    client_free = client_meta.get('is_free')
    comp_free = competitor_meta.get('is_free')
    monetization = None
    if client_free is not None and comp_free is not None:
        monetization = 'compatible' if client_free == comp_free else 'mismatch'
    elif competitor_meta.get('price') is not None and client_meta.get('price') is not None:
        monetization = 'compatible'
    return {
        'tags_overlap': tags_overlap,
        'monetization': monetization,
        'mode_match': mode_match,
    }


def _extract_metrics_block(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    metrics: Dict[str, Optional[float]] = {}
    for key in ('r7', 'r30', 'r90', 'zpeak7', 'zpeak30', 'avg_players', 'avg_players_30d', 'players_zscore'):
        if key in row:
            metrics[key] = _safe_float(row.get(key))
    return {k: v for k, v in metrics.items() if v is not None}


def _summarize_granger(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    best = max(rows, key=lambda item: _safe_float(item.get('best_ccf')) or 0.0)
    summary = {
        'granger_xy_sig_fdr': bool(best.get('best_significant_fdr') or best.get('granger_xy_sig') or False),
        'best_lag': _safe_int(best.get('best_lag') or best.get('lag')),
        'best_ccf': _safe_float(best.get('best_ccf')),
    }
    if 'lead_or_lag' in best:
        summary['lead_or_lag'] = best.get('lead_or_lag')
    return summary


def _derive_eras(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    eras: List[Dict[str, Any]] = []
    for evt in events:
        phase = evt.get('phase') or evt.get('lifecycle_phase')
        trend = evt.get('trend') or evt.get('slope')
        start = evt.get('start') or evt.get('start_date') or evt.get('window_start')
        end = evt.get('end') or evt.get('end_date') or evt.get('window_end')
        avg_players = _safe_float(evt.get('avg_players') or evt.get('players_avg'))
        if not phase and not trend:
            continue
        eras.append({
            'start': start,
            'end': end,
            'phase': phase,
            'trend': trend,
            'avg_players': avg_players,
        })
    return eras


def _extract_key_peaks(events: List[Dict[str, Any]], topics: List[Dict[str, Any]], explanations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    peaks: List[Dict[str, Any]] = []
    for evt in events:
        zscore = _safe_float(evt.get('zscore') or evt.get('z') or evt.get('zvalue'))
        if zscore is None:
            continue
        date_label = evt.get('year_month') or evt.get('date') or evt.get('timestamp')
        context_dict = evt.get('context') or {}
        social = evt.get('social_daily_micro') or {}
        why = describe_peak_reasons(context_dict, social)
        context_summary = summarize_context(context_dict, social)
        context_payload = dict(context_dict) if isinstance(context_dict, dict) else {}
        if context_summary:
            context_payload['summary'] = context_summary
        peak = {
            'date_or_month': date_label,
            'zscore': zscore,
            'why': why,
            'topics': evt.get('topics') or [],
        }
        review_segments = evt.get('review_segments')
        if review_segments:
            peak['review_segments'] = review_segments
        if social:
            peak['social_daily_micro'] = social
        if context_payload:
            peak['context'] = context_payload
        peaks.append(peak)
    peaks.sort(key=lambda item: item.get('zscore') or 0.0, reverse=True)
    if explanations:
        for peak in peaks:
            same_period = [exp for exp in explanations if (exp.get('year_month') or exp.get('date')) == peak.get('date_or_month')]
            if same_period:
                ctx = dict(same_period[0])
                existing = peak.get('context') or {}
                merged = {**existing, **ctx}
                if 'summary' not in merged and existing.get('summary'):
                    merged['summary'] = existing['summary']
                peak['context'] = merged
    return peaks[:5]





def _merge_takeaways(peaks: List[Dict[str, Any]], granger: Dict[str, Any]) -> List[str]:
    if not peaks:
        return []
    primary = peaks[0]
    return describe_takeaways(primary.get('date_or_month'), primary.get('zscore'), primary.get('why'), granger)





def _build_story(sections: Dict[str, Any]) -> Dict[str, Any]:
    events = sections.get('events') or []
    topics = sections.get('topics') or []
    explanations = sections.get('explanations') or []
    peaks = _extract_key_peaks(events, topics, explanations)
    granger_summary = _summarize_granger(sections.get('ccf_granger') or [])
    story = {
        'eras': _derive_eras(events),
        'key_peaks': peaks,
        'granger': granger_summary,
        'takeaways': _merge_takeaways(peaks, granger_summary),
    }
    return story


def _aggregate_real_competitors_topics(real_competitors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aggregated: Dict[int, Dict[str, Any]] = {}
    for comp in real_competitors:
        topics = comp.get('topics') or []
        weight = comp.get('metrics', {}).get('zpeak30') or comp.get('similarity') or 0.0
        for topic in topics:
            tid = topic.get('topic_id')
            if tid is None:
                continue
            try:
                tid_int = int(tid)
            except Exception:
                continue
            existing = aggregated.setdefault(tid_int, {
                'topic_id': tid_int,
                'name': topic.get('name'),
                'keywords': topic.get('keywords') or [],
                'share_values': [],
                'recent_share_values': [],
                'recent_delta_values': [],
                'sentiment_values': [],
                'coherence_values': [],
                'takeaways_raw': [],
                'top_competitors': [],
            })
            share = _safe_float(topic.get('share'))
            recent_share = _safe_float(topic.get('recent_share'))
            recent_delta = _safe_float(topic.get('recent_share_delta'))
            sentiment = _safe_float(topic.get('avg_sentiment'))
            coherence = _safe_float(topic.get('coherence'))
            takeaway = topic.get('takeaway') or topic.get('summary')
            if share is not None:
                existing['share_values'].append(share)
            if recent_share is not None:
                existing['recent_share_values'].append(recent_share)
            if recent_delta is not None:
                existing['recent_delta_values'].append(recent_delta)
            if sentiment is not None:
                existing['sentiment_values'].append(sentiment)
            if coherence is not None:
                existing['coherence_values'].append(coherence)
            if takeaway:
                existing['takeaways_raw'].append(str(takeaway))
            if weight:
                existing['top_competitors'].append({
                    'appid': comp.get('appid'),
                    'name': comp.get('name'),
                    'weight': round(float(weight), 3),
                })
    result: List[Dict[str, Any]] = []
    for entry in aggregated.values():
        top_comp = entry.pop('top_competitors')
        entry['share'] = _mean_or_none(entry.pop('share_values'))
        entry['recent_share'] = _mean_or_none(entry.pop('recent_share_values'))
        entry['recent_share_delta'] = _mean_or_none(entry.pop('recent_delta_values'))
        entry['avg_sentiment'] = _mean_or_none(entry.pop('sentiment_values'))
        entry['coherence'] = _mean_or_none(entry.pop('coherence_values'))
        raw_takeaways = entry.pop('takeaways_raw')
        dedup_takeaways: List[str] = []
        seen: set[str] = set()
        for note in raw_takeaways:
            cleaned = note.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup_takeaways.append(cleaned)
            if len(dedup_takeaways) >= 5:
                break
        if dedup_takeaways:
            entry['takeaways'] = dedup_takeaways
        top_comp.sort(key=lambda item: item['weight'], reverse=True)
        entry['top_competitors'] = top_comp[:5]
        result.append(entry)
    result.sort(key=lambda item: item.get('recent_share_delta') or 0.0, reverse=True)
    return result

def _classify_topic_insights(topics: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    insights = {
        'trending_topics': [],
        'declining_topics': [],
        'risk_topics': [],
        'opportunity_topics': [],
    }
    for topic in topics:
        tid = topic.get('topic_id')
        if tid is None:
            continue
        reason = describe_topic_insight(topic)
        delta = topic.get('recent_share_delta') or 0.0
        sentiment = topic.get('avg_sentiment') or 0.0
        share = topic.get('share') or 0.0
        if delta is not None and delta > 0.05 and sentiment > -0.2:
            insights['trending_topics'].append({'topic_id': tid, 'reason': reason})
        if delta is not None and delta < -0.05:
            insights['declining_topics'].append({'topic_id': tid, 'reason': reason})
        if sentiment is not None and sentiment < -0.3 and share and share > 0.1:
            insights['risk_topics'].append({'topic_id': tid, 'reason': reason})
        if share is not None and share < 0.05 and delta and delta > 0.03:
            insights['opportunity_topics'].append({'topic_id': tid, 'reason': reason})
    return insights


def _pricing_position(client_price: Optional[float], competitor_prices: List[float], threshold: float) -> Dict[str, Any]:
    avg_price = round(float(sum(competitor_prices) / len(competitor_prices)), 2) if competitor_prices else None
    position = 'desconocido'
    delta_pct: Optional[float] = None
    if client_price is not None and avg_price:
        try:
            delta_pct = (float(client_price) - float(avg_price)) / float(avg_price) if avg_price else None
        except ZeroDivisionError:
            delta_pct = None
        if delta_pct is not None:
            if abs(delta_pct) <= threshold:
                position = 'aligned'
            elif delta_pct < 0:
                position = 'cheap'
            else:
                position = 'expensive'
    description: Optional[str] = None
    if delta_pct is not None:
        pct_display = abs(delta_pct) * 100
        emphasis_cut = threshold * 100 * 1.75
        if position == 'aligned':
            description = 'Precio alineado al promedio de los competidores reales'
        elif position == 'cheap':
            description = 'Claramente por debajo del promedio de los competidores reales' if pct_display > emphasis_cut else 'Ligeramente por debajo del promedio de los competidores reales'
        elif position == 'expensive':
            description = 'Claramente por encima del promedio de los competidores reales' if pct_display > emphasis_cut else 'Ligeramente por encima del promedio de los competidores reales'
    result = {
        'client_price': client_price,
        'avg_competitors_price': avg_price,
        'position': position,
        'threshold': threshold,
    }
    if delta_pct is not None:
        result['delta_pct'] = round(delta_pct, 4)
    result['description'] = description or describe_pricing(delta_pct)
    return result


def _build_real_competitors_summary(real_competitors: List[Dict[str, Any]], client_price: Optional[float], threshold: float) -> Dict[str, Any]:
    total = len(real_competitors)
    intra = sum(1 for comp in real_competitors if comp.get('intra_cluster'))
    cross = total - intra
    lifecycle_counts: Dict[str, int] = defaultdict(int)
    competitor_prices: List[float] = []
    key_signals: List[str] = []
    top_today: List[Tuple[float, Dict[str, Any]]] = []
    for comp in real_competitors:
        cat = (comp.get('category') or 'unknown').lower()
        lifecycle_counts[cat] += 1
        price = comp.get('price')
        if price is not None:
            competitor_prices.append(float(price))
        story = comp.get('story') or {}
        peaks_story = story.get('key_peaks') or []
        if peaks_story:
            key_signals.append(describe_key_signal(peaks_story[0]))
        if cat == 'today':
            metric = comp.get('metrics', {}).get('zpeak30') or comp.get('similarity') or 0.0
            top_today.append((float(metric), comp))
    key_signals = key_signals[:5]
    top_today.sort(key=lambda item: item[0], reverse=True)
    top_today_preview = [{
        'appid': item[1].get('appid'),
        'name': item[1].get('name'),
        'zpeak30': item[1].get('metrics', {}).get('zpeak30'),
        'why': (item[1].get('story', {}).get('takeaways') or [''])[0],
    } for item in top_today[:5]]
    pricing = _pricing_position(client_price, competitor_prices, threshold)
    return {
        'counts': {
            'total': total,
            'intra_cluster': intra,
            'cross_cluster': cross,
        },
        'lifecycle': {
            'today': lifecycle_counts.get('today', 0),
            'recent': lifecycle_counts.get('recent', 0),
            'historical': lifecycle_counts.get('historical', 0),
        },
        'pricing_position': pricing.get('description') or pricing.get('position'),
        'pricing_details': pricing,
        'key_signals': key_signals,
        'top_today_preview': top_today_preview,
    }


def _build_peak_analysis(sections: Dict[str, Any]) -> List[Dict[str, Any]]:
    events = sections.get('events') or []
    topics = sections.get('topics') or []
    explanations = sections.get('explanations') or []
    ccf_monthly = sections.get('ccf_granger_monthly') or {}
    peaks: List[Dict[str, Any]] = []
    for evt in events:
        zscore = _safe_float(evt.get('zscore') or evt.get('z') or evt.get('zvalue'))
        if zscore is None:
            continue
        period = _standardize_period(evt.get('year_month') or evt.get('date') or evt.get('date_or_month'))
        window_before = _safe_int(evt.get('window_before_months') or evt.get('window_before') or evt.get('window_left'))
        window_after = _safe_int(evt.get('window_after_months') or evt.get('window_after') or evt.get('window_right'))
        window = {
            'before': window_before if window_before is not None else 2,
            'after': window_after if window_after is not None else 2,
        }
        context_dict = evt.get('context') or {}
        social = evt.get('social_daily_micro') or _extract_social_snapshot(evt)
        why = describe_peak_reasons(context_dict, social)
        context_summary = summarize_context(context_dict, social)
        context_payload = dict(context_dict) if isinstance(context_dict, dict) else {}
        if context_summary:
            context_payload['summary'] = context_summary
        explanation_match = None
        for exp in explanations:
            exp_key = _standardize_period(exp.get('year_month') or exp.get('date'))
            if period and exp_key == period:
                explanation_match = exp
                break
        if explanation_match:
            exp_clean = _sanitize_record(explanation_match)
            context_payload.update(exp_clean)
            if context_summary and 'summary' not in context_payload:
                context_payload['summary'] = context_summary
        topic_buckets = _split_topics_by_sentiment(topics, period)
        bertopic = {k: v for k, v in topic_buckets.items() if v}
        review_segments = evt.get('review_segments')
        peak_entry = {
            'year_month': period,
            'zscore': zscore,
            'window_months': window,
            'context': context_payload,
            'social_daily_micro': social,
            'why': why,
        }
        if bertopic:
            peak_entry['bertopic'] = bertopic
        if review_segments:
            peak_entry['review_segments'] = review_segments
        if ccf_monthly:
            peak_entry['ccf_granger_monthly'] = ccf_monthly
        granger_source = {}
        if isinstance(ccf_monthly, dict):
            best = ccf_monthly.get('players_vs_pos_rate') or {}
            granger_source = {
                'granger_xy_sig_fdr': bool(best.get('granger_xy_sig_fdr') or best.get('granger_xy_sig')),
                'best_lag': best.get('best_lag_m') or best.get('best_lag'),
                'best_ccf': best.get('best_ccf'),
            }
        takeaways = describe_takeaways(period, zscore, why, granger_source)
        if takeaways:
            peak_entry['overall_takeaways'] = takeaways
        peaks.append(peak_entry)
    peaks.sort(key=lambda item: item.get('zscore') or 0.0, reverse=True)
    return peaks[:5]





def _build_competitors_peaks(real_competitors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for comp in real_competitors:
        appid = comp.get('appid')
        for peak in comp.get('story', {}).get('key_peaks') or []:
            date_label = peak.get('date_or_month') or peak.get('year_month') or peak.get('date')
            entry = {
                'appid': appid,
                'date_or_month': date_label,
                'zscore': _safe_float(peak.get('zscore') or peak.get('z')),
                'why': peak.get('why') or peak.get('label') or peak.get('reason'),
            }
            refs: Dict[str, Any] = {}
            if appid and date_label:
                refs['report'] = f"outputs/reports/{appid}.json#peak:{date_label}"
            if refs:
                entry['refs'] = refs
            entry = {k: v for k, v in entry.items() if v not in (None, '', {})}
            collected.append(entry)
    collected.sort(key=lambda item: item.get('zscore') or 0.0, reverse=True)
    return collected[:20]


def _get_nested(cfg: Dict[str, Any], path: Sequence[str], default: Any = None) -> Any:
    current: Any = cfg
    for key in path:
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current[key]
    return current


def _build_methodology(params_cfg: Dict[str, Any], diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    correlation_cfg = _get_nested(params_cfg, ['correlation'], {}) or {}
    peaks_cfg = _get_nested(params_cfg, ['peaks'], {}) or {}
    windows_cfg = _get_nested(params_cfg, ['windows'], {}) or {}
    real_competitors_cfg = _get_nested(params_cfg, ['real_competitors'], {}) or {}
    bertopic_cfg = _get_nested(params_cfg, ['bertopic'], {}) or {}
    return {
        'time_granularity': windows_cfg.get('time_granularity', 'monthly'),
        'windows': {
            'roll_len_players': windows_cfg.get('roll_len_players', 6),
            'roll_len_reviews': windows_cfg.get('roll_len_reviews', 6),
            'peak_window_months': windows_cfg.get('peak_window_months', 2),
            'min_peak_separation_months': windows_cfg.get('min_peak_separation_months', 2),
        },
        'peaks': {
            'z_threshold': peaks_cfg.get('z_threshold', 2.0),
            'method': peaks_cfg.get('method', 'zscore'),
        },
        'correlation': {
            'lags_months': correlation_cfg.get('lags_months', [1, 2]),
            'prewhitening': correlation_cfg.get('prewhitening', True),
            'fdr': correlation_cfg.get('fdr', True),
        },
        'real_competitors': real_competitors_cfg if real_competitors_cfg else diagnostics.get('neighbors_config', {}),
        'bertopic': bertopic_cfg or {},
    }


def _load_medoids_for_neighbors(path: str) -> Dict[str, np.ndarray]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {}
    medoids: Dict[str, np.ndarray] = {}
    if isinstance(data, dict):
        for cid, vec in data.items():
            try:
                medoids[str(cid)] = np.asarray(vec, dtype=np.float32)
            except Exception:
                continue
    return medoids


def _neighbor_user_config(params_cfg: Dict[str, Any]) -> Dict[str, Any]:
    strategy_cfg = params_cfg.get('neighbor_strategy') or {}
    client_cfg = params_cfg.get('client_report') or {}
    legacy_cfg = client_cfg.get('neighbors_config') or {}

    merged: Dict[str, Any] = {}
    def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> None:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                _merge(base[key], value)
            else:
                base[key] = value

    if isinstance(strategy_cfg, dict):
        _merge(merged, strategy_cfg)
    if isinstance(legacy_cfg, dict):
        _merge(merged, legacy_cfg)
    return merged


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _fallback_neighbors(vec_q: np.ndarray, emb_df: pd.DataFrame, clu_df: pd.DataFrame,
                        meta_df: pd.DataFrame, top_k: int, same_cluster_only: bool,
                        client_cluster_id: Optional[int]) -> List[Dict[str, Any]]:
    if emb_df.empty:
        return []
    ids = emb_df['appid'].astype(str).tolist()
    try:
        matrix = np.vstack(emb_df['embedding'].apply(np.asarray).to_list()).astype(np.float32)
    except Exception:
        return []
    sims = matrix @ vec_q
    order = np.argsort(-sims)
    results: List[Dict[str, Any]] = []
    for idx in order:
        appid = ids[idx]
        if same_cluster_only and client_cluster_id is not None and not clu_df.empty:
            subset = clu_df[clu_df['appid'].astype(str) == appid]
            if subset.empty or int(subset.iloc[0]['cluster_id']) != int(client_cluster_id):
                continue
        meta_row = meta_df[meta_df['appid'].astype(str) == appid].head(1) if not meta_df.empty else pd.DataFrame()
        clu_row = clu_df[clu_df['appid'].astype(str) == appid].head(1) if not clu_df.empty else pd.DataFrame()
        results.append({
            'appid': appid,
            'name': None if meta_row.empty else meta_row.iloc[0].get('name'),
            'cluster_id': None if clu_row.empty else int(clu_row.iloc[0]['cluster_id']),
            'similarity': float(sims[idx]),
        })
        if len(results) >= top_k:
            break
    return results


def _assign_cluster(vec_q: np.ndarray, medoids_path: str, clusters_df: pd.DataFrame) -> int | None:
    # Preferir medoids si existen
    mp = Path(medoids_path)
    if mp.exists():
        try:
            medoids = json.loads(mp.read_text(encoding='utf-8'))
            # Expect dict {cluster_id: [float,...]}
            best_cid, best_sim = None, -1e9
            v = vec_q.astype(np.float32)
            for cid, centroid in medoids.items():
                cvec = np.asarray(centroid, dtype=np.float32)
                sim = float(np.dot(v, cvec))
                if sim > best_sim:
                    best_cid, best_sim = int(cid), sim
            return best_cid
        except Exception:
            pass
    # Alternativa: usar el clÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºster del vecino mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡s cercano (se resuelve en la llamada)
    return None


def _collect_sections_for_appid(aid: str, ccf: pd.DataFrame, events: pd.DataFrame,
                                topics: pd.DataFrame, expl: pd.DataFrame,
                                review_segments: pd.DataFrame | None = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    review_df = review_segments if review_segments is not None else pd.DataFrame()
    review_map: Dict[str, Dict[str, Any]] = {}
    if not review_df.empty and 'appid' in review_df.columns:
        subset = review_df[review_df['appid'].astype(str) == aid]
        for _, row in subset.iterrows():
            rec = _sanitize_record(row.to_dict())
            period = _standardize_period(rec.get('year_month') or rec.get('date') or rec.get('date_or_month') or rec.get('period'))
            raw_payload = rec.get('review_segments') or rec.get('segments') or rec
            prepared = _prepare_review_segments(raw_payload)
            if prepared:
                key = period or str(rec.get('date') or rec.get('id') or len(review_map))
                review_map[key] = prepared

    if not ccf.empty:
        sub = ccf[ccf['appid'].astype(str) == aid]
        if not sub.empty:
            keep = [c for c in ['pair_name', 'best_lag', 'best_ccf', 'best_pval', 'best_significant_fdr',
                                'lead_or_lag', 'granger_xy_pmin', 'granger_yx_pmin', 'granger_xy_sig', 'granger_yx_sig']
                    if c in sub.columns]
            if keep:
                out['ccf_granger'] = sub[keep].to_dict(orient='records')
            pair_summary: Dict[str, Dict[str, Any]] = {}
            for row in sub.to_dict(orient='records'):
                pair = str(row.get('pair_name') or row.get('pair') or '').strip()
                if not pair:
                    continue
                entry = {
                    'best_lag_m': _safe_int(row.get('best_lag_m') or row.get('best_lag') or row.get('lag')),
                    'best_ccf': _safe_float(row.get('best_ccf')),
                    'pval': _safe_float(row.get('best_pval') or row.get('pvalue')),
                    'granger_xy_sig_fdr': bool(row.get('best_significant_fdr') or row.get('granger_xy_sig') or row.get('granger_xy_sig_fdr')),
                }
                lead = row.get('lead_or_lag')
                if lead not in (None, ''):
                    entry['lead_or_lag'] = lead
                entry = {k: v for k, v in entry.items() if v not in (None, '', {})}
                if entry:
                    pair_summary[pair] = entry
            if pair_summary:
                out['ccf_granger_monthly'] = pair_summary

    if not events.empty:
        sub = events[events['appid'].astype(str) == aid].copy()
        if not sub.empty:
            records: List[Dict[str, Any]] = []
            for _, row in sub.iterrows():
                rec = _sanitize_record(row.to_dict())
                if 'year_month' in rec:
                    rec['year_month'] = _standardize_period(rec['year_month'])
                if 'date' in rec:
                    rec['date'] = _standardize_period(rec['date'])
                social = _extract_social_snapshot(rec)
                if social:
                    rec['social_daily_micro'] = social
                if not rec.get('context'):
                    context = _compose_event_context(rec)
                    if context:
                        rec['context'] = context
                review_payload = rec.get('review_segments')
                prepared_review = _prepare_review_segments(review_payload) if review_payload else {}
                if (not prepared_review) and review_map:
                    key = _standardize_period(rec.get('year_month') or rec.get('date') or rec.get('date_or_month'))
                    prepared_review = review_map.get(key) or {}
                if prepared_review:
                    rec['review_segments'] = prepared_review
                else:
                    rec.pop('review_segments', None)
                records.append(rec)
            out['events'] = records

    if not topics.empty:
        sub = topics[topics['appid'].astype(str) == aid].copy()
        if not sub.empty:
            sanitized: List[Dict[str, Any]] = []
            alerts: List[Dict[str, Any]] = []
            for _, row in sub.iterrows():
                rec = _sanitize_record(row.to_dict())
                if 'event_year_month' in rec:
                    rec['event_year_month'] = _standardize_period(rec['event_year_month'])
                if 'avg_sentiment' in rec:
                    rec['avg_sentiment'] = _safe_float(rec.get('avg_sentiment'))
                sanitized.append(rec)
                if str(rec.get('relevance_polarity') or '').lower() == 'negative':
                    alert = {
                        'year_month': rec.get('event_year_month'),
                        'relevance_polarity': rec.get('relevance_polarity'),
                    }
                    zval = rec.get('players_zscore')
                    if zval is not None:
                        alert['players_zscore'] = _safe_float(zval)
                    alert = {k: v for k, v in alert.items() if v not in (None, '', {})}
                    if alert.get('year_month'):
                        alerts.append(alert)
            if sanitized:
                out['topics'] = sanitized
                pol_counts: Dict[str, int] = {}
                lbl_counts: Dict[str, int] = {}
                for rec in sanitized:
                    pol = str(rec.get('relevance_polarity') or '').lower()
                    if pol:
                        pol_counts[pol] = pol_counts.get(pol, 0) + 1
                    lbl = str(rec.get('relevance_label') or rec.get('relevance_label_final') or '').lower()
                    if lbl:
                        lbl_counts[lbl] = lbl_counts.get(lbl, 0) + 1
                total_rows = len(sanitized)
                negative_ratio = (pol_counts.get('negative', 0) / total_rows) if total_rows else 0.0
                out['relevance_summary'] = {
                    'polarity_counts': pol_counts,
                    'label_counts': lbl_counts,
                    'negative_ratio': negative_ratio,
                    'total_topic_rows': total_rows,
                }
                if alerts:
                    out['alerts'] = alerts

    if not expl.empty:
        sub = expl[expl['appid'].astype(str) == aid].copy()
        if not sub.empty:
            sanitized_expl: List[Dict[str, Any]] = []
            for _, row in sub.iterrows():
                rec = _sanitize_record(row.to_dict())
                if 'year_month' in rec:
                    rec['year_month'] = _standardize_period(rec['year_month'])
                sanitized_expl.append(rec)
            sanitized_expl.sort(key=lambda item: item.get('year_month') or '')
            out['explanations'] = sanitized_expl
    return out


def _price_rule(client_price: float | None, cluster_prices: List[float], cfg_params: Dict[str, Any]) -> str:
    if client_price is None or not cluster_prices:
        return "no_disponible"
    try:
        p = float(client_price)
        m = float(np.median([x for x in cluster_prices if x is not None]))
        bajo = float(cfg_params.get('regla_precio', {}).get('bajo_umbral', 0.10))
        alto = float(cfg_params.get('regla_precio', {}).get('alto_umbral', 0.10))
    except Exception:
        return "no_disponible"
    if p < m * (1.0 - bajo):
        return "Juego econÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³mico frente al segmento"
    if p < m * (1.0 + alto):
        return "Precio alineado al segmento"
    return "Precio por encima del segmento"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--client_file', required=True, help='JSON con info del cliente (appid, name, description, tags, price, release_date, ...)')
    ap.add_argument('--embeddings', default='data/processed/embeddings.parquet')
    ap.add_argument('--clusters', default='data/processed/clusters.parquet')
    ap.add_argument('--metadata', default='data/processed/game_metadata.parquet')
    ap.add_argument('--ccf', default='outputs/ccf_analysis/summary.parquet')
    ap.add_argument('--events', default='outputs/events/events.parquet')
    ap.add_argument('--topics', default='outputs/events/topics_scored.parquet')
    ap.add_argument('--explanations', default='outputs/events/explanations.parquet')
    ap.add_argument('--review_segments', default='outputs/events/review_segments.parquet', help='Parquet/JSON con segmentos de reseÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±as por pico.')
    # Reglas offline deshabilitadas: por defecto vacío (no carga)
    ap.add_argument('--rules', default='')
    ap.add_argument('--rules_dir', default='')
    ap.add_argument('--emb_config', default='configs/embeddings.yaml')
    ap.add_argument('--cluster_stats', default='outputs/clustering/cluster_stats.csv', help='CSV con metricas agregadas por cluster.')
    ap.add_argument('--cluster_topics', default='outputs/clustering/cluster_topics.json', help='JSON con topicos representativos por cluster.')
    ap.add_argument('--medoids', default='models/cluster_medoids.json')
    ap.add_argument('--top_k', type=int, default=15)
    ap.add_argument('--same_cluster_only', type=lambda x: str(x).lower() in ['1', 'true', 'yes'], default=True)
    ap.add_argument('--out', default=None, help='Ruta de salida JSON; por defecto outputs/reports/client_{id}.json')
    ap.add_argument('--params', default='configs/params.yaml')
    ap.add_argument('--version', default='1.4', help='Version del payload del reporte de cliente.')
    ap.add_argument('--language', default='en', help='Language for narrative (en/es).')
    args = ap.parse_args()

    client = json.loads(Path(args.client_file).read_text(encoding='utf-8'))
    client_id = str(client.get('appid') or client.get('id') or 'client')

    emb_df = _load_any_df(args.embeddings)
    clu_df = _load_any_df(args.clusters)
    meta_df = _load_any_df(args.metadata)
    ccf_df = _load_any_df(args.ccf) if path_exists(args.ccf) else pd.DataFrame()
    events_df = _load_any_df(args.events) if path_exists(args.events) else pd.DataFrame()
    topics_df = _load_any_df(args.topics) if path_exists(args.topics) else pd.DataFrame()
    expl_df = _load_any_df(args.explanations) if path_exists(args.explanations) else pd.DataFrame()
    review_segments_df = _load_any_df(args.review_segments) if path_exists(args.review_segments) else pd.DataFrame()
    rules_df = _load_any_df(args.rules) if (args.rules and path_exists(args.rules)) else pd.DataFrame()

    cluster_stats_df = _load_cluster_stats_df(args.cluster_stats)
    cluster_topics_map = _load_cluster_topics_map(args.cluster_topics)

    if not rules_df.empty:
        if 'appid' not in rules_df.columns and 'app_id' in rules_df.columns:
            rules_df = rules_df.rename(columns={'app_id': 'appid'})
        rules_df = rules_df.copy()
        rules_df['appid'] = rules_df['appid'].astype(str)

    if emb_df.empty or 'embedding' not in emb_df.columns:
        raise SystemExit('Embeddings no disponibles. Ejecuta el pipeline base primero.')
    emb_df = emb_df.copy()
    emb_df['appid'] = emb_df['appid'].astype(str)

    if not clu_df.empty and 'appid' in clu_df.columns:
        clu_df = clu_df.copy()
        clu_df['appid'] = clu_df['appid'].astype(str)

    if not meta_df.empty and 'appid' in meta_df.columns:
        meta_df = meta_df.copy()
        meta_df['appid'] = meta_df['appid'].astype(str)

    params_cfg: Dict[str, Any] = {}
    if Path(args.params).exists():
        params_cfg = expand_env_in_obj(yaml.safe_load(Path(args.params).read_text(encoding='utf-8')) or {})
    neighbor_cfg = _neighbor_user_config(params_cfg) or {}
    if args.top_k:
        neighbor_cfg = {**neighbor_cfg, 'target_total': args.top_k}
    if args.same_cluster_only is not None:
        neighbor_cfg = {**neighbor_cfg, 'same_cluster_only': bool(args.same_cluster_only)}

    emb_cfg: Dict[str, Any] = {}
    if Path(args.emb_config).exists():
        emb_cfg = yaml.safe_load(Path(args.emb_config).read_text(encoding='utf-8')) or {}
    doc_fields = emb_cfg.get('document_fields', {"text_fields": ["name", "description"], "tag_fields": ["tags"]})
    model_name = emb_cfg.get('embedding_model', 'all-MiniLM-L6-v2')

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    normalize = bool(emb_cfg.get('normalize_embeddings', True))
    doc = _build_doc_from_client(client, doc_fields)
    if not doc:
        raise SystemExit('El client_file no tiene campos suficientes para construir el documento de embedding.')
    vec = model.encode([doc], normalize_embeddings=normalize, show_progress_bar=False)[0].astype(np.float32)

    cid = _assign_cluster(vec, args.medoids, clu_df)
    if cid is None and not clu_df.empty:
        try:
            ids = emb_df['appid'].tolist()
            matrix = np.vstack(emb_df['embedding'].apply(np.asarray).to_list()).astype(np.float32)
            sims = matrix @ vec
            idx = int(np.argmax(sims))
            aid = ids[idx]
            rclu = clu_df[clu_df['appid'] == aid].head(1)
            if not rclu.empty:
                cid = _safe_int(rclu.iloc[0].get('cluster_id'))
        except Exception:
            cid = None
    if cid is not None:
        cid = int(cid)

    client_meta_dict = _extract_metadata_row(meta_df, client_id)
    client_tags = _to_str_list(client.get('tags')) or _to_str_list(client_meta_dict.get('tags'))
    client_genres = _to_str_list(client.get('genres')) or _to_str_list(client_meta_dict.get('genres'))
    client_categories = _to_str_list(client.get('categories')) or _to_str_list(client_meta_dict.get('categories'))
    client_modes = _infer_modes_from_tokens(client_categories + _to_str_list(client_meta_dict.get('modes')))
    client_price = _safe_float(client.get('price') if client.get('price') is not None else client_meta_dict.get('price'))
    client_is_free = client.get('is_free')
    if client_is_free is None:
        meta_flag = client_meta_dict.get('is_free')
        if isinstance(meta_flag, bool):
            client_is_free = meta_flag
    if client_is_free is None and client_price is not None:
        client_is_free = client_price == 0
    client_languages = _extract_languages(client, client_meta_dict)
    client_release_date = client.get('release_date') or client_meta_dict.get('release_date')
    if client_release_date is not None:
        try:
            client_release_date = str(pd.to_datetime(client_release_date).date())
        except Exception:
            client_release_date = str(client_release_date)
    client_description = client.get('description') or client_meta_dict.get('description') or client_meta_dict.get('short_description')
    client_profile = {
        'tags': client_tags,
        'tags_normalized': _normalize_tokens(client_tags),
        'genres': client_genres,
        'categories': client_categories,
        'modes': client_modes,
        'price': client_price,
        'is_free': client_is_free,
        'languages': client_languages,
    }

    medoids_map = _load_medoids_for_neighbors(args.medoids)
    emb_index = EmbeddingIndex.from_dataframe(emb_df)

    neighbor_metadata = {
        'genres': client_genres,
        'tags': client_tags,
        'categories': client_categories,
        'modes': client_modes,
        'price': client_price,
        'is_free': client_is_free,
        'name': client.get('name'),
    }
    neighbors: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {'neighbors_config': neighbor_cfg}
    if emb_index.matrix.size:
        neighbors, diagnostics_raw = select_competitor_neighbors(
            query_vec=vec,
            query_metadata=neighbor_metadata,
            query_appid=None,
            query_cluster_id=cid,
            embeddings=emb_index,
            clusters_df=clu_df,
            metadata_df=meta_df,
            medoids=medoids_map if medoids_map else None,
            user_cfg=neighbor_cfg,
        )
        diagnostics.update(diagnostics_raw)
    if not neighbors:
        neighbors = _fallback_neighbors(vec, emb_df, clu_df, meta_df, args.top_k, bool(args.same_cluster_only), cid)
    for item in neighbors:
        item['similarity'] = float(item.get('similarity', 0.0))
        if 'score' in item and item['score'] is not None:
            item['score'] = float(item['score'])
        if 'cluster_id' in item and item['cluster_id'] is not None:
            item['cluster_id'] = _safe_int(item['cluster_id'])
        if 'source' not in item:
            same_cluster = cid is not None and item.get('cluster_id') == cid
            item['source'] = 'intra' if same_cluster else 'cross'
    diagnostics.setdefault('selected', len(neighbors))

    competitors: List[Dict[str, Any]] = []
    real_competitors: List[Dict[str, Any]] = []
    cluster_prices: List[float] = []

    for entry in neighbors:
        appid = str(entry.get('appid'))
        if not appid:
            continue
        meta_row = _extract_metadata_row(meta_df, appid)
        comp_tags = _to_str_list(meta_row.get('tags')) or _to_str_list(meta_row.get('steamspy_tags'))
        comp_genres = _to_str_list(meta_row.get('genres'))
        comp_categories = _to_str_list(meta_row.get('categories'))
        comp_modes = _infer_modes_from_tokens(comp_categories + _to_str_list(meta_row.get('modes')))
        comp_price = _safe_float(meta_row.get('price') or meta_row.get('final_price'))
        if comp_price is not None:
            cluster_prices.append(comp_price)
        comp_is_free = meta_row.get('is_free')
        if comp_is_free is None and comp_price is not None:
            comp_is_free = comp_price == 0
        comp_languages = _to_str_list(meta_row.get('languages') or meta_row.get('supported_languages'))
        comp_release = meta_row.get('release_date')
        if comp_release is not None:
            try:
                comp_release = str(pd.to_datetime(comp_release).date())
            except Exception:
                comp_release = str(comp_release)
        comp_profile = {
            'tags_normalized': _normalize_tokens(comp_tags),
            'modes': comp_modes,
            'price': comp_price,
            'is_free': comp_is_free,
            'genres': comp_genres,
            'tags': comp_tags,
            'categories': comp_categories,
        }
        sections = _collect_sections_for_appid(appid, ccf_df, events_df, topics_df, expl_df, review_segments_df)
        rule_row: Dict[str, Any] = {}
        if not rules_df.empty and 'appid' in rules_df.columns:
            rsub = rules_df[rules_df['appid'] == appid]
            if not rsub.empty:
                rule_row = rsub.iloc[0].to_dict()
        engagement: Dict[str, Any] = {}
        if rule_row:
            playtime_ratio = rule_row.get('playtime_ratio')
            hours_last_2w = rule_row.get('hours_last_2w')
            experiencia = rule_row.get('experiencia')
            abandono_lbl = rule_row.get('abandono')
            new_players_flag = 1 if isinstance(experiencia, str) and experiencia.lower().startswith('nuevo') else 0
            abandonment_flag = 1 if isinstance(abandono_lbl, str) and 'abandono' in abandono_lbl.lower() else 0
            engagement = {
                'playtime_ratio': _safe_float(playtime_ratio),
                'recent_hours_2w': _safe_float(hours_last_2w),
                'experience_label': experiencia,
                'abandonment_label': abandono_lbl,
                'new_players_flag': new_players_flag,
                'abandonment_flag': abandonment_flag,
            }
            engagement = {k: v for k, v in engagement.items() if v not in (None, '')}
        metrics_block = _extract_metrics_block(rule_row)
        category = rule_row.get('category') or rule_row.get('lifecycle') or rule_row.get('lifecycle_category')
        business_fit = _build_business_fit(client_profile, comp_profile)
        story = _build_story(sections)
        metadata_block = {
            'genres': comp_genres or None,
            'tags': comp_tags or None,
            'categories': comp_categories or None,
            'modes': comp_modes or None,
            'languages': comp_languages or None,
            'release_date': comp_release,
        }
        metadata_block = {k: v for k, v in metadata_block.items() if v not in (None, [], {})}
        competitor_entry: Dict[str, Any] = {
            'appid': appid,
            'name': entry.get('name') or meta_row.get('name'),
            'cluster_id': entry.get('cluster_id'),
            'similarity': entry.get('similarity'),
            'source': entry.get('source'),
            'score': entry.get('score'),
            'intra_cluster': entry.get('source') != 'cross',
            'category': category,
            'price': comp_price,
            'business_fit': business_fit,
            'metrics': metrics_block,
            'story': story,
            'topics': sections.get('topics', []),
            'events': sections.get('events', []),
            'ccf_granger': sections.get('ccf_granger', []),
            'explanations': sections.get('explanations', []),
            'alerts': sections.get('alerts'),
            'relevance_summary': sections.get('relevance_summary'),
            'metadata': metadata_block,
        }
        if engagement:
            competitor_entry['engagement'] = engagement
        competitors.append(competitor_entry)
        real_entry: Dict[str, Any] = {
            'appid': competitor_entry['appid'],
            'name': competitor_entry['name'],
            'cluster_id': competitor_entry['cluster_id'],
            'similarity': competitor_entry['similarity'],
            'category': competitor_entry['category'],
            'intra_cluster': competitor_entry['intra_cluster'],
            'business_fit': competitor_entry['business_fit'],
            'metrics': competitor_entry['metrics'],
            'story': competitor_entry['story'],
            'topics': competitor_entry['topics'],
            'price': competitor_entry['price'],
        }
        if engagement:
            real_entry['engagement'] = engagement
        real_competitors.append(real_entry)

    neighbors_payload: List[Dict[str, Any]] = []
    for item in neighbors:
        payload = {
            'appid': item.get('appid'),
            'name': item.get('name'),
            'cluster_id': item.get('cluster_id'),
            'similarity': item.get('similarity'),
        }
        if item.get('source') is not None:
            payload['source'] = item.get('source')
        if item.get('score') is not None:
            payload['score'] = item.get('score')
        neighbors_payload.append(payload)

    def _global_relevance_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        pol_counts: Dict[str, int] = {}
        lbl_counts: Dict[str, int] = {}
        negative_months: List[str] = []
        high_months: List[str] = []
        total = 0
        for comp in rows:
            for topic in comp.get('topics') or []:
                pol = str(topic.get('relevance_polarity') or '').lower()
                lbl = str(topic.get('relevance_label') or topic.get('relevance_label_final') or '').lower()
                ym = topic.get('event_year_month') or topic.get('year_month')
                if pol:
                    pol_counts[pol] = pol_counts.get(pol, 0) + 1
                if lbl:
                    lbl_counts[lbl] = lbl_counts.get(lbl, 0) + 1
                if pol == 'negative' and ym:
                    negative_months.append(str(ym))
                if lbl == 'high' and ym:
                    high_months.append(str(ym))
                total += 1
        negative_ratio = (pol_counts.get('negative', 0) / total) if total else 0.0

        def _dedup_keep(seq: List[str], limit: int = 20) -> List[str]:
            seen: set[str] = set()
            out: List[str] = []
            for value in seq:
                if value not in seen:
                    seen.add(value)
                    out.append(value)
                if len(out) >= limit:
                    break
            return out

        return {
            'polarity_counts': pol_counts,
            'label_counts': lbl_counts,
            'negative_ratio': negative_ratio,
            'negative_months': _dedup_keep(negative_months),
            'high_months': _dedup_keep(high_months),
            'total_topic_rows': total,
            'competitors_with_negative': int(sum(1 for comp in rows if any((t.get('relevance_polarity') or '').lower() == 'negative' for t in comp.get('topics') or []))),
        }

    global_rel_summary = _global_relevance_summary(competitors)
    global_rel_summary["notes"] = summarize_global_relevance(global_rel_summary)
    real_competitors_topics = _aggregate_real_competitors_topics(real_competitors)
    topic_insights = _classify_topic_insights(real_competitors_topics)
    client_sections = _collect_sections_for_appid(client_id, ccf_df, events_df, topics_df, expl_df, review_segments_df)
    peak_analysis = _build_peak_analysis(client_sections)
    competitors_peaks = _build_competitors_peaks(real_competitors)
    diagnostics['neighbors_total'] = len(neighbors)
    cluster_context = _compute_cluster_context(cid, diagnostics, cluster_stats_df, cluster_topics_map, clu_df, meta_df)
    price_threshold = float(params_cfg.get('regla_precio', {}).get('alto_umbral', 0.10))
    real_competitors_summary = _build_real_competitors_summary(real_competitors, client_price, price_threshold)
    rules_analysis = {
        'regla_precio': _price_rule(client_price, cluster_prices, params_cfg),
        'pricing_vs_real_competitors': _pricing_position(client_price, [c.get('price') for c in real_competitors if c.get('price') is not None], price_threshold),
        'relevance_summary_global': global_rel_summary,
    }
    methodology = _build_methodology(params_cfg, diagnostics)

    provenance = {
        'embeddings_parquet': args.embeddings,
        'clusters_parquet': args.clusters,
        'metadata_parquet': args.metadata,
        'ccf_summary_parquet': args.ccf,
        'events_parquet': args.events,
        'topics_parquet': args.topics,
        'explanations_parquet': args.explanations,
        'medoids_json': args.medoids,
        'cluster_stats_csv': args.cluster_stats,
        'cluster_topics_json': args.cluster_topics,
    }

    report = {
        'appid': client_id,
        'generated_at': _now_iso(),
        'version': args.version,
        'metadata': {
            'name': client.get('name') or client_meta_dict.get('name'),
            'description': client_description,
            'price': client_price,
            'tags': client_tags or None,
            'release_date': client_release_date,
            'languages': client_languages or None,
        },
        'cluster': {'cluster_id': cid},
        'cluster_context': cluster_context,
        'neighbors': neighbors_payload,
        'competitors': competitors,
        'real_competitors': real_competitors,
        'real_competitors_summary': real_competitors_summary,
        'real_competitors_topics': real_competitors_topics,
        'topic_insights': topic_insights,
        'peak_analysis': peak_analysis,
        'competitors_peaks': competitors_peaks,
        'rules_analysis': rules_analysis,
        'methodology': methodology,
        'provenance': provenance,
    }

    # Compose a concise narrative paragraph (no Reddit fields)
    try:
        report['narrative'] = compose_client_narrative(report, lang=args.language)
    except Exception:
        report['narrative'] = ''

    out_path = Path(args.out) if args.out else Path('outputs/reports') / f"client_{client_id}.json"

    mlf_cfg = params_cfg.get('mlflow', {}) if isinstance(params_cfg, dict) else {}
    use_mlflow = bool(mlf_cfg.get('enabled', True))

    _validate_report(report)

    if use_mlflow:
        mlflow.set_experiment(mlf_cfg.get('experiment', 'Steam Analytics'))
        mlflow.start_run(run_name=mlf_cfg.get('run_name_prefix', 'client_report_'))
        try:
            log_mlflow_params({
                'top_k': args.top_k,
                'same_cluster_only': bool(args.same_cluster_only),
                'client_id': client_id,
                'cluster_id': cid,
            })
            write_json_any(report, out_path, indent=2)
            mlflow.log_artifact(str(out_path))
            log_mlflow_metrics({'competitors': len(real_competitors), 'neighbors': len(neighbors)})
        finally:
            mlflow.end_run()
    else:
        write_json_any(report, out_path, indent=2)
    print(f"[OK] Reporte de cliente -> {out_path}")


def _validate_report(obj: Dict[str, Any]) -> None:
    schema_path = Path('schemas/client_report.schema.json')
    basic_ok = (
        isinstance(obj.get('appid'), str)
        and isinstance(obj.get('generated_at'), str)
        and isinstance(obj.get('version'), str)
        and isinstance(obj.get('metadata'), dict)
        and isinstance(obj.get('cluster'), dict)
        and isinstance(obj.get('cluster_context'), dict)
        and isinstance(obj.get('neighbors'), list)
        and isinstance(obj.get('competitors'), list)
        and isinstance(obj.get('real_competitors'), list)
        and isinstance(obj.get('real_competitors_summary'), dict)
        and isinstance(obj.get('real_competitors_topics'), list)
        and isinstance(obj.get('topic_insights'), dict)
        and isinstance(obj.get('peak_analysis'), list)
        and isinstance(obj.get('competitors_peaks'), list)
        and isinstance(obj.get('rules_analysis'), dict)
        and isinstance(obj.get('methodology'), dict)
        and isinstance(obj.get('provenance'), dict)
    )
    if not basic_ok:
        raise ValueError('Reporte de cliente no cumple con la estructura minima requerida.')
    if schema_path.exists():
        try:
            import jsonschema
            schema = json.loads(schema_path.read_text(encoding='utf-8'))
            jsonschema.validate(instance=obj, schema=schema)
        except ImportError:
            pass

if __name__ == '__main__':
    main()


