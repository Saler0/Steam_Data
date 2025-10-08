#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Lee outputs/events/topics.parquet (o ruta indicada) y persiste tópicos por juego en MongoDB.

Colección sugerida: analytics_topics
 - Clave: {appid: str, event_year_month: datetime (opcional)}
 - Campos: topics: List[Dict], updated_at

Uso:
  python Data_analytics/scripts/offline_topics_to_mongo.py \
    --parquet outputs/events/topics.parquet \
    --mongo-uri "mongodb://localhost:27017" \
    --mongo-db analytics \
    --mongo-coll analytics_topics \
    [--aggregate-by-app]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

# Ensure project root imports
import sys
import pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.utils.io import read_parquet_any
from src.utils.mongo_utils import MongoWriter


def _normalize_topics(topics: Any) -> List[Dict[str, Any]]:
    if topics is None:
        return []
    if isinstance(topics, list):
        return [t for t in topics if isinstance(t, dict)]
    return []


def main() -> None:
    ap = argparse.ArgumentParser(description="Persistir tópicos por juego en MongoDB")
    ap.add_argument('--parquet', default='outputs/events/topics.parquet')
    ap.add_argument('--mongo-uri', default=os.getenv('MONGO_URI', 'mongodb://localhost:27017'))
    ap.add_argument('--mongo-db', default=os.getenv('MONGO_DB_ANALYTICS', 'analytics'))
    ap.add_argument('--mongo-coll', default=os.getenv('MONGO_COLL_TOPICS', 'analytics_topics'))
    ap.add_argument('--aggregate-by-app', action='store_true', help='Guardar 1 doc por appid agregando temas de todos los meses')
    args = ap.parse_args()

    df = read_parquet_any(args.parquet)
    if df is None or len(df) == 0:
        print('[WARN] No hay datos de tópicos para persistir')
        return

    df = df.copy()
    if 'appid' not in df.columns:
        raise SystemExit('El parquet no contiene columna appid')

    df['appid'] = df['appid'].astype(str)
    if 'event_year_month' in df.columns:
        # Convertir a datetime si viene como string
        try:
            df['event_year_month'] = pd.to_datetime(df['event_year_month'])
        except Exception:
            pass

    writer = MongoWriter(args.mongo_uri, args.mongo_db, args.mongo_coll)
    now = datetime.now(timezone.utc)

    if args.aggregate_by_app:
        # Un documento por appid: topics_agg: [ ... ]
        agg_rows = []
        for appid, g in df.groupby('appid'):
            topics_all: List[Dict[str, Any]] = []
            for _, r in g.iterrows():
                topics_all.extend(_normalize_topics(r.get('topics')))
            agg_rows.append({
                'appid': str(appid),
                'topics_agg': topics_all,
            })
        ops = []
        for row in agg_rows:
            filt = {'appid': row['appid']}
            update = {'$set': {'topics_agg': row['topics_agg'], 'updated_at': now}}
            ops.append((filt, update))
        writer.bulk_upsert(ops)
        print(f"[OK] Guardados tópicos agregados para {len(agg_rows)} appids en {args.mongo_db}.{args.mongo_coll}")
        return

    # Por defecto: 1 documento por (appid, event_year_month)
    ops = []
    for _, r in df.iterrows():
        appid = str(r['appid'])
        topics = _normalize_topics(r.get('topics'))
        ev_ym = r.get('event_year_month')
        filt: Dict[str, Any] = {'appid': appid}
        if pd.notna(ev_ym):
            filt['event_year_month'] = pd.to_datetime(ev_ym)
        update = {'$set': {'topics': topics, 'updated_at': now}}
        ops.append((filt, update))
    writer.bulk_upsert(ops)
    print(f"[OK] Guardados {len(ops)} documentos de tópicos en {args.mongo_db}.{args.mongo_coll}")


if __name__ == '__main__':
    main()

