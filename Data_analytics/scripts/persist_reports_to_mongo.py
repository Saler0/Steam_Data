#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Persiste reportes por juego (outputs/reports/*.json) en MongoDB, 1 doc por appid.

- Elimina campos innecesarios (por defecto: 'provenance').
- Permite filtrar por lista de appids (p. ej., competidores de un cliente).

Uso:
  python Data_analytics/scripts/persist_reports_to_mongo.py \
    --reports-dir outputs/reports \
    --mongo-uri mongodb://localhost:27017 \
    --mongo-db analytics \
    --mongo-coll app_reports \
    --drop-fields provenance

Opcional (filtrar):
  --appids-file outputs/clients/client_poc-stellar_appids.txt
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

from dotenv import load_dotenv

# Ensure project root imports
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

load_dotenv(dotenv_path=ROOT.parent / '.env')

from src.utils.mongo_utils import MongoWriter


def _read_appids_file(path: str) -> Set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    text = p.read_text(encoding='utf-8')
    # admite separador por espacios, nuevas líneas o comas
    raw = [t.strip() for t in text.replace('\n', ' ').replace(',', ' ').split(' ') if t.strip()]
    return set(map(str, raw))


def _iter_reports(reports_dir: str, allow: Set[str] | None = None) -> Iterable[Path]:
    base = Path(reports_dir)
    if not base.exists():
        return []
    for p in base.glob('*.json'):
        # espera nombres tipo "{appid}.json" o "client_*.json"; filtramos los de appid
        name = p.stem
        if name.startswith('client_'):
            continue
        if allow and name not in allow:
            continue
        yield p


def _drop_fields(obj: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    if not fields:
        return obj
    out = {k: v for k, v in obj.items() if k not in fields}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description='Persistir reportes por appid en MongoDB')
    ap.add_argument('--reports-dir', default='outputs/reports')
    ap.add_argument('--appids-file', default=None, help='Archivo con appids (espacios/nuevas líneas/comas) para filtrar')
    ap.add_argument('--mongo-uri', default=os.getenv('MONGO_URI', 'mongodb://localhost:27017'))
    ap.add_argument('--mongo-db', default=os.getenv('MONGO_DB_ANALYTICS', 'analytics'))
    ap.add_argument('--mongo-coll', default=os.getenv('MONGO_COLL_APPREPORTS', 'app_reports'))
    ap.add_argument('--drop-fields', default='provenance', help='Campos a eliminar, separados por comas')
    args = ap.parse_args()

    allow: Set[str] | None = None
    if args.appids_file:
        allow = _read_appids_file(args.appids_file)

    drop_fields = [f.strip() for f in (args.drop_fields or '').split(',') if f.strip()]
    writer = MongoWriter(args.mongo_uri, args.mongo_db, args.mongo_coll)
    now = datetime.now(timezone.utc)

    n = 0
    for path in _iter_reports(args.reports_dir, allow):
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        appid = str(data.get('appid') or path.stem)
        clean = _drop_fields(data, drop_fields)
        update = {'$set': {**clean, 'updated_at': now}}
        writer.upsert({'_id': appid}, update, set_on_insert={'created_at': now})
        n += 1
    print(f"[OK] Upserts completados: {n} documentos en {args.mongo_db}.{args.mongo_coll}")


if __name__ == '__main__':
    main()

