#!/usr/bin/env python
"""Construye segmentos de resenas por pico y redacta highlights para el reporte de cliente.

Lee eventos (picos) y resenas etiquetadas con experiencia, abandonos, regalos y topicos BERTopic
para producir un parquet con la estructura esperada por make_client_report.py.

Entrada recomendada:
  - --events: outputs/events/events.parquet (generado por detectar picos)
  - --reviews: Dataset a nivel resena con:
      appid, review_id, review_date, recommended (bool/int), playtime_at_review (horas),
      playtime_since_review_30d (opcional), abandon_after_30d (bool), gifted (bool),
      early_access (bool), post_launch (bool), median_playtime_app (float)
  - --topics: Dataset opcional con columnas review_id, topic_id, topic_name, share, avg_sentiment
      (p.ej. salida de BERTopic por resena)
  - --output: outputs/events/review_segments.parquet

El script agrupa resenas dentro de la ventana de cada evento (por defecto el mes del pico,
configurable con --window-before/--window-after) y genera metricas por segmento de experiencia
(New/Intermedio/Experto/Veterano) y frases resaltando hallazgos.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipelines.decision_rules.reglas_decision import experiencia_jugador

# Orden estable para los segmentos
EXPERIENCE_ORDER = ["Nuevo", "Intermedio", "Experto", "Veterano"]
SEGMENT_KEY_MAP = {
    "Nuevo": "new",
    "Intermedio": "intermediate",
    "Experto": "expert",
    "Veterano": "veteran",
}

def _load_any_df(path: str | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(p)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    if p.suffix.lower() == ".json":
        return pd.read_json(p)
    raise SystemExit(f"Formato no soportado: {p}")


def _to_bool(value: Any) -> Optional[bool]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, np.integer)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return pd.to_datetime(value, utc=True).to_pydatetime()
    except Exception:
        return None


def _infer_experience(row: pd.Series) -> Optional[str]:
    hours = _safe_float(row.get("playtime_at_review"))
    median = _safe_float(row.get("median_playtime_app"))
    if hours is None or median is None:
        return None
    label = experiencia_jugador(hours, median)
    if label.startswith("Datos"):
        return None
    return label


def _experience_key(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    label_norm = label.strip().capitalize()
    if label_norm not in EXPERIENCE_ORDER:
        return None
    return SEGMENT_KEY_MAP[label_norm]


def _aggregate_segment(rows: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    total = len(rows)
    out["count"] = int(total)
    if total == 0:
        return out

    recommended = rows.get("recommended")
    if recommended is not None:
        pos = float((recommended == True).sum())
        neg = float((recommended == False).sum())
        out["pos"] = round(pos / total, 2)
        out["neg"] = round(neg / total, 2)

    gifted = rows.get("gifted")
    if gifted is not None:
        out["gifted_share"] = round(float(gifted.sum()) / total, 2)

    abandon = rows.get("abandon_after_30d")
    if abandon is not None:
        out["abandon_rate_30d"] = round(float(abandon.sum()) / total, 2)

    playtime = rows.get("playtime_at_review")
    if playtime is not None:
        median_hours = _safe_float(rows["playtime_at_review"].median())
        if median_hours is not None:
            out.setdefault("stats", {})["median_playtime"] = round(median_hours, 2)

    # Phase flags
    early_access = rows.get("early_access")
    post_launch = rows.get("post_launch")
    phase_block: Dict[str, bool] = {}
    if early_access is not None:
        phase_block["early_access"] = bool(early_access.any())
    if post_launch is not None:
        phase_block["post_launch"] = bool(post_launch.any())
    if phase_block:
        out["phase"] = phase_block

    return out


def _attach_topics(topics_df: pd.DataFrame, review_ids: Iterable[Any], top_n: int = 3) -> List[Dict[str, Any]]:
    if topics_df.empty:
        return []
    subset = topics_df[topics_df['review_id'].isin(review_ids)]
    if subset.empty:
        return []
    grouped = subset.groupby(['topic_id', 'topic_name'], dropna=False)
    payload: List[Dict[str, Any]] = []
    for (topic_id, name), block in grouped:
        share = _safe_float(block['share'].mean()) if 'share' in block.columns else None
        sentiment = _safe_float(block['avg_sentiment'].mean()) if 'avg_sentiment' in block.columns else None
        examples: List[Dict[str, Any]] = []
        if 'review_id' in block.columns and 'snippet' in block.columns:
            sample = block[['review_id', 'snippet']].dropna().head(3)
            examples = [
                {
                    'review_id': str(row['review_id']),
                    'snippet': row['snippet']
                }
                for _, row in sample.iterrows()
            ]
        entry = {
            'topic_id': _safe_int(topic_id),
            'name': name,
            'share': share,
            'avg_sentiment': sentiment,
        }
        if examples:
            entry['examples'] = examples
        payload.append(entry)
    payload.sort(key=lambda item: item.get('share') or 0.0, reverse=True)
    return payload[:top_n]


def _generate_highlights(segments: Dict[str, Dict[str, Any]]) -> List[str]:
    highlights: List[str] = []
    new_seg = segments.get('new')
    if new_seg and new_seg.get('abandon_rate_30d') is not None:
        rate = new_seg['abandon_rate_30d']
        pct = int(round(rate * 100))
        if rate >= 0.7:
            highlights.append(f"Abandono muy alto en nuevos ({pct}%): revisar onboarding/rendimiento.")
        elif rate >= 0.4:
            highlights.append(f"Abandono elevado en nuevos ({pct}%).")
    exp_seg = segments.get('expert')
    vet_seg = segments.get('veteran')
    if exp_seg and vet_seg:
        pos_expert = exp_seg.get('pos')
        pos_veteran = vet_seg.get('pos')
        abandon_expert = exp_seg.get('abandon_rate_30d')
        abandon_veteran = vet_seg.get('abandon_rate_30d')
        if pos_expert and pos_expert > 0.5 and pos_veteran and pos_veteran > 0.5:
            if (abandon_expert is None or abandon_expert < 0.3) and (abandon_veteran is None or abandon_veteran < 0.3):
                highlights.append("Expertos y veteranos critican balance/endgame pero mantienen baja tasa de abandono.")
    if not highlights and new_seg and new_seg.get('share') and new_seg['share'] > 0.4:
        pct = int(round(new_seg['share'] * 100))
        highlights.append(f"Jugadores nuevos concentran {pct}% de las resenas del pico.")
    return highlights


def build_review_segments(
    events_df: pd.DataFrame,
    reviews_df: pd.DataFrame,
    topics_df: pd.DataFrame,
    window_before_days: int,
    window_after_days: int,
) -> pd.DataFrame:
    if events_df.empty:
        raise SystemExit("Eventos vacio; nada que procesar.")
    if reviews_df.empty:
        raise SystemExit("Resenas vacias; no se pueden generar segmentos.")

    # Preprocesar resenas
    reviews = reviews_df.copy()
    if 'appid' not in reviews.columns:
        raise SystemExit("El dataset de resenas debe contener la columna 'appid'.")
    reviews['appid'] = reviews['appid'].astype(str)
    if 'review_date' not in reviews.columns:
        raise SystemExit("El dataset de resenas debe contener 'review_date'.")
    # Normaliza timestamps a naive UTC para evitar comparaciones tz-aware vs tz-naive
    reviews['review_date'] = pd.to_datetime(reviews['review_date'], errors='coerce', utc=True).dt.tz_localize(None)
    reviews = reviews.dropna(subset=['review_date'])

    if 'experience_label' in reviews.columns:
        reviews['experience_label'] = reviews['experience_label'].fillna('').astype(str)
    else:
        reviews['experience_label'] = reviews.apply(_infer_experience, axis=1).fillna('')

    reviews['experience_key'] = reviews['experience_label'].apply(_experience_key)
    reviews = reviews[reviews['experience_key'].notna()]

    # Recomendacion y otros flags
    reviews['recommended'] = reviews['recommended'].apply(_to_bool)
    if 'gifted' in reviews.columns:
        reviews['gifted'] = reviews['gifted'].apply(_to_bool).fillna(False)
    else:
        reviews['gifted'] = False
    if 'abandon_after_30d' in reviews.columns:
        reviews['abandon_after_30d'] = reviews['abandon_after_30d'].apply(_to_bool).fillna(False)
    else:
        reviews['abandon_after_30d'] = False
    if 'early_access' in reviews.columns:
        reviews['early_access'] = reviews['early_access'].apply(_to_bool)
    if 'post_launch' in reviews.columns:
        reviews['post_launch'] = reviews['post_launch'].apply(_to_bool)

    topics = topics_df.copy()
    if not topics.empty:
        required_cols = {'review_id', 'topic_id', 'topic_name'}
        if not required_cols.issubset(set(topics.columns)):
            topics = pd.DataFrame()

    records: List[Dict[str, Any]] = []

    for _, event in events_df.iterrows():
        appid = str(event.get('appid'))
        if not appid:
            continue
        peak_dt = _parse_datetime(event.get('date') or event.get('year_month'))
        if peak_dt is None:
            # intentar con year_month como primer dia del mes
            ym = event.get('year_month')
            if ym:
                try:
                    peak_dt = pd.to_datetime(str(ym)).to_pydatetime()
                except Exception:
                    peak_dt = None
        if peak_dt is None:
            continue
        start = peak_dt - timedelta(days=window_before_days)
        end = peak_dt + timedelta(days=window_after_days)

        rel = reviews[(reviews['appid'] == appid) & (reviews['review_date'].between(start, end))]
        if rel.empty:
            continue

        total_reviews = len(rel)
        pos_total = int(rel['recommended'].sum()) if 'recommended' in rel.columns else None
        neg_total = total_reviews - pos_total if pos_total is not None else None
        median_hours = _safe_float(rel['playtime_at_review'].median()) if 'playtime_at_review' in rel.columns else None

        segments: Dict[str, Dict[str, Any]] = {}
        for label in EXPERIENCE_ORDER:
            key = SEGMENT_KEY_MAP[label]
            block = rel[rel['experience_key'] == key]
            if block.empty:
                continue
            seg_payload = _aggregate_segment(block)
            seg_payload['share'] = round(len(block) / total_reviews, 3)
            if not topics.empty and 'review_id' in rel.columns:
                seg_payload['bertopic'] = _attach_topics(topics, block['review_id'])
            segments[key] = seg_payload

        if not segments:
            continue

        highlights = _generate_highlights(segments)

        scope = {
            'month': peak_dt.strftime('%Y-%m'),
            'reviews_total': total_reviews
        }
        if pos_total is not None:
            scope['pos'] = pos_total
        if neg_total is not None:
            scope['neg'] = neg_total

        payload = {
            'appid': appid,
            'year_month': peak_dt.strftime('%Y-%m'),
            'review_segments': {
                'median_hours': round(median_hours, 2) if median_hours is not None else None,
                'scope': scope,
                'by_experience': segments,
                'highlights': highlights
            }
        }
        records.append(payload)

    if not records:
        return pd.DataFrame()

    out_df = pd.DataFrame(records)
    out_df['review_segments'] = out_df['review_segments'].apply(lambda x: {k: v for k, v in x.items() if v})
    return out_df


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Construye segmentos de resenas por pico")
    ap.add_argument('--events', required=True, help='Parquet/CSV con eventos detectados (outputs/events/events.parquet).')
    ap.add_argument('--reviews', required=True, help='Dataset a nivel resena con metricas necesarias.')
    ap.add_argument('--topics', default=None, help='Dataset opcional con topicos BERTopic por resena.')
    ap.add_argument('--window-before', type=int, default=30, help='Dias antes del pico a considerar (default: 30).')
    ap.add_argument('--window-after', type=int, default=15, help='Dias despues del pico (default: 15).')
    ap.add_argument('--output', required=True, help='Ruta de salida (Parquet o JSON).')
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    events_df = _load_any_df(args.events)
    reviews_df = _load_any_df(args.reviews)
    topics_df = _load_any_df(args.topics)

    result_df = build_review_segments(events_df, reviews_df, topics_df, args.window_before, args.window_after)
    if result_df.empty:
        print("[WARN] No se generaron segmentos de resenas (sin resenas dentro de las ventanas definidas).")
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() in {'.json'}:
        result_df.to_json(out_path, orient='records', indent=2)
    else:
        result_df.to_parquet(out_path, index=False)
    print(f"[OK] Segmentos de resenas -> {out_path}")


if __name__ == '__main__':
    main()
