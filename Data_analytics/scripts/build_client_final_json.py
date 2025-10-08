#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestador para construir el JSON final de cliente en MongoDB.

Flujo:
  0) (offline) Analytics guarda tópicos por juego en Mongo (analytics_topics)
  1) Se recibe JSON del cliente -> se upserta base en 'client_profiles'
  2) Se hace el PoC de vecinos (competidores) -> upsert 'neighbors'
  3) Se anexan resultados de reglas (si existen precomputados) -> upsert 'decision_rules_neighbors'
  4) Se consultan tópicos de vecinos (passo 0) -> upsert 'analytics_neighbors.topics'

Uso ejemplo:
  python Data_analytics/scripts/build_client_final_json.py \
    --client-id poc-stellar \
    --client-file Data_analytics/configs/clients/poc-stellar.json \
    --mongo-uri mongodb://localhost:27017 \
    --mongo-db exploitation_zone \
    --mongo-coll client_profiles
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import yaml

# Ensure project root imports
import os
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.insights.neighbor_strategy import EmbeddingIndex, select_competitor_neighbors
from src.utils.mongo_utils import MongoWriter


def _to_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return [str(v).strip() for v in x if str(v).strip()]
    text = str(x)
    if not text.strip():
        return []
    for sep in (';', '|'):
        text = text.replace(sep, ',')
    return [p.strip() for p in text.split(',') if p.strip()]


def _build_doc_from_client(client: Dict[str, Any], doc_fields: Dict[str, Any]) -> str:
    parts: List[str] = []
    for field in (doc_fields.get('text_fields') or ["name", "description"]):
        val = client.get(field)
        if val:
            parts.append(str(val))
    for field in (doc_fields.get('tag_fields') or ["tags"]):
        tags = client.get(field) or []
        if tags:
            parts.append(" ".join([str(t).replace(" ", "_") for t in tags]))
    return " \n".join([p for p in parts if p])


def _assign_cluster(vec: np.ndarray, medoids_path: str, clu_df: pd.DataFrame) -> Optional[int]:
    try:
        p = Path(medoids_path)
        if not p.exists():
            return None
        med = json.loads(p.read_text(encoding='utf-8'))
        best_cid, best_sim = None, -1.0
        for rec in med:
            cid = rec.get('cluster_id')
            center = np.asarray(rec.get('center') or rec.get('medoid_vector'))
            if center is None or center.size == 0:
                continue
            center = center.astype(np.float32)
            sim = float(np.dot(center, vec))
            if sim > best_sim:
                best_sim, best_cid = sim, int(cid) if cid is not None else None
        return best_cid
    except Exception:
        return None


def _load_params(path: str) -> Dict[str, Any]:
    p = Path(path)
    if p.exists():
        return yaml.safe_load(p.read_text(encoding='utf-8')) or {}
    return {}


def main() -> None:
    ap = argparse.ArgumentParser(description="Construir/actualizar JSON final de cliente en Mongo")
    ap.add_argument('--client-file', help='Ruta al JSON del cliente (si no, usar flags)')
    ap.add_argument('--client-id', default='client-001')
    ap.add_argument('--name')
    ap.add_argument('--description')
    ap.add_argument('--tags')
    ap.add_argument('--price', type=float)
    ap.add_argument('--release-date')

    # Artefactos/configs
    ap.add_argument('--embeddings', default='data/processed/embeddings/embeddings.parquet')
    ap.add_argument('--clusters', default='data/processed/clusters.parquet')
    ap.add_argument('--metadata', default='data/processed/game_metadata.parquet')
    ap.add_argument('--params', default='configs/params.yaml')
    ap.add_argument('--emb-config', default='configs/embeddings.yaml')
    ap.add_argument('--medoids', default='models/cluster_medoids.json')

    # Mongo destino
    ap.add_argument('--mongo-uri', default=os.getenv('MONGO_URI', 'mongodb://localhost:27017'))
    ap.add_argument('--mongo-db', default=os.getenv('MONGO_DB_CLIENTS', 'exploitation_zone'))
    ap.add_argument('--mongo-coll', default=os.getenv('MONGO_COLL_CLIENTS', 'client_profiles'))

    # Mongo analytics (tópicos)
    ap.add_argument('--analytics-db', default=os.getenv('MONGO_DB_ANALYTICS', 'analytics'))
    ap.add_argument('--analytics-coll', default=os.getenv('MONGO_COLL_TOPICS', 'analytics_topics'))

    # Reglas precomputadas (opcional)
    ap.add_argument('--rules-parquet', default='data/with_rules/with_rules.parquet')

    args = ap.parse_args()

    # 1) Base cliente
    if args.client_file:
        client = json.loads(Path(args.client_file).read_text(encoding='utf-8'))
        client_id = args.client_id or client.get('appid') or 'client-001'
    else:
        client_id = args.client_id
        client = {
            'appid': client_id,
            'name': args.name,
            'description': args.description,
            'tags': _to_list(args.tags),
            'price': args.price,
            'release_date': args.release_date,
        }

    writer = MongoWriter(args.mongo_uri, args.mongo_db, args.mongo_coll)
    now = datetime.now(timezone.utc)

    base_doc = {
        '_id': client_id,
        'client_input': client,
        'status': {'stage': 1, 'note': 'cliente recibido'},
        'updated_at': now,
    }
    writer.upsert({'_id': client_id}, {'$set': base_doc}, set_on_insert={'created_at': now})

    # 2) Vecinos (competidores)
    emb_path = Path(args.embeddings)
    clu_path = Path(args.clusters)
    meta_path = Path(args.metadata)
    if not emb_path.exists() or not clu_path.exists():
        raise SystemExit('Embeddings o clusters parquet no encontrados.')
    emb_df = pd.read_parquet(emb_path)
    clu_df = pd.read_parquet(clu_path)
    meta_df = pd.read_parquet(meta_path) if meta_path.exists() else pd.DataFrame()
    emb_index = EmbeddingIndex.from_dataframe(emb_df)

    params = _load_params(args.params)
    neighbor_cfg = (params.get('neighbor_strategy') or {}).copy()

    # Embedding modelo
    emb_cfg = {}
    p_emb = Path(args.emb_config)
    if p_emb.exists():
        try:
            emb_cfg = yaml.safe_load(p_emb.read_text(encoding='utf-8')) or {}
        except Exception:
            emb_cfg = {}
    doc_fields = emb_cfg.get('document_fields', {"text_fields": ["name", "description"], "tag_fields": ["tags"]})
    model_name = emb_cfg.get('embedding_model', 'all-MiniLM-L6-v2')
    normalize = bool(emb_cfg.get('normalize_embeddings', True))

    from sentence_transformers import SentenceTransformer
    doc = _build_doc_from_client(client, doc_fields)
    if not doc:
        raise SystemExit('El cliente no tiene campos suficientes para generar el embedding.')
    model = SentenceTransformer(model_name)
    vec = model.encode([doc], normalize_embeddings=normalize, show_progress_bar=False)[0].astype(np.float32)

    cid = _assign_cluster(vec, args.medoids, clu_df)

    neighbor_meta = {
        'genres': [],
        'tags': client.get('tags') or [],
        'categories': [],
        'modes': [],
        'price': client.get('price'),
        'is_free': (client.get('price') == 0) if client.get('price') is not None else None,
        'name': client.get('name'),
    }
    neighbors, diagnostics = select_competitor_neighbors(
        query_vec=vec,
        query_metadata=neighbor_meta,
        query_appid=None,
        query_cluster_id=cid,
        embeddings=emb_index,
        clusters_df=clu_df,
        metadata_df=meta_df,
        medoids=None,
        user_cfg=neighbor_cfg,
    )

    appids = [str(n.get('appid')) for n in neighbors if n.get('appid')]
    writer.upsert(
        {'_id': client_id},
        {'$set': {
            'neighbors': neighbors,
            'neighbors_appids': appids,
            'diagnostics': {'assigned_cluster_id': cid, 'selected': len(appids), 'config': neighbor_cfg},
            'status': {'stage': 2, 'note': 'vecinos seleccionados'},
            'updated_at': now,
        }}
    )

    # 3) Reglas de decisión (si existen precomputadas)
    rules_path = Path(args.rules_parquet)
    rules_attached = False
    if rules_path.exists() and appids:
        try:
            df_rules = pd.read_parquet(rules_path)
            key_col = 'appid' if 'appid' in df_rules.columns else ('app_id' if 'app_id' in df_rules.columns else None)
            if key_col:
                df_sel = df_rules[df_rules[key_col].astype(str).isin(appids)].copy()
                # Reducir columnas a algo manejable
                keep_cols = [c for c in df_sel.columns if c in (
                    key_col, 'precio', 'saturacion1', 'saturacion2', 'saturacion3', 'actividad',
                    'experiencia', 'abandono', 'limitaciones', 'eval_limitaciones', 'publishers', 'idiomas', 'resena_extra'
                )]
                if key_col not in keep_cols:
                    keep_cols = [key_col] + keep_cols
                df_small = df_sel[keep_cols]
                rules_list = []
                for _, r in df_small.iterrows():
                    rec = {k: r[k] for k in df_small.columns}
                    rec['appid'] = str(rec.pop(key_col))
                    rules_list.append(rec)
                writer.upsert(
                    {'_id': client_id},
                    {'$set': {
                        'decision_rules_neighbors': rules_list,
                        'status': {'stage': 3, 'note': 'reglas anexadas (neighbors)'},
                        'updated_at': now,
                    }}
                )
                rules_attached = True
        except Exception:
            pass

    # 4) Tópicos de vecinos desde Mongo analytics
    # Usamos una consulta por lotes para traer por appids; caemos a vacio si no hay conexión
    try:
        from pymongo import MongoClient as _MC
        mcli = _MC(args.mongo_uri)
        topics_docs = list(mcli[args.analytics_db][args.analytics_coll].find({'appid': {'$in': appids}})) if appids else []
        try:
            mcli.close()
        except Exception:
            pass
        topics_map: Dict[str, Any] = {}
        for d in topics_docs:
            aid = str(d.get('appid'))
            # Soporta dos modos: por mes ('topics') o agregado ('topics_agg')
            if 'topics_agg' in d:
                topics_map[aid] = {'topics_agg': d.get('topics_agg')}
            else:
                entry = {
                    'event_year_month': d.get('event_year_month'),
                    'topics': d.get('topics'),
                }
                if aid not in topics_map:
                    topics_map[aid] = {'by_month': []}
                topics_map[aid].setdefault('by_month', []).append(entry)

        writer.upsert(
            {'_id': client_id},
            {'$set': {
                'analytics_neighbors': {'topics': topics_map},
                'status': {'stage': 4, 'note': 'tópicos de vecinos anexados'},
                'updated_at': now,
            }}
        )
    except Exception:
        # No bloquear si Mongo analytics no está accesible
        pass

    # Final
    final_note = 'completo' if rules_attached else 'completo (sin reglas)'
    writer.upsert({'_id': client_id}, {'$set': {'status': {'stage': 5, 'note': final_note}, 'updated_at': now}})
    print(f"[OK] Cliente '{client_id}' actualizado en {args.mongo_db}.{args.mongo_coll}")


if __name__ == '__main__':
    main()

