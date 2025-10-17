#!/usr/bin/env python
from __future__ import annotations

"""
Builds a client profile, selects nearest neighbors, and writes helper outputs
to drive the subset analytics pipeline.

Outputs under outputs/clients/:
 - client_{id}.json (normalized client file if built from flags)
 - client_{id}_neighbors.json (neighbors with score/similarity/source)
 - client_{id}_appids.txt (space-separated appids for subset run)
 - client_{id}_diagnostics.json (optional stats)
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

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


def _assign_cluster(vec: np.ndarray, medoids_path: str, clu_df: pd.DataFrame) -> int | None:
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Prepare client neighbors and appids for subset pipeline")
    ap.add_argument('--client-file', help='Path to client JSON (if omitted, build from flags)')
    ap.add_argument('--client-id', default='client-001')
    ap.add_argument('--name')
    ap.add_argument('--description')
    ap.add_argument('--tags')
    ap.add_argument('--price', type=float)
    ap.add_argument('--release-date')

    # Artifacts/configs
    ap.add_argument('--embeddings', default='data/processed/embeddings/embeddings.parquet')
    ap.add_argument('--clusters', default='data/processed/clusters.parquet')
    ap.add_argument('--metadata', default='data/processed/game_metadata.parquet')
    ap.add_argument('--params', default='configs/params.yaml')
    ap.add_argument('--emb-config', default='configs/embeddings.yaml')
    ap.add_argument('--medoids', default='models/cluster_medoids.json')

    args = ap.parse_args()

    # Load embeddings/clusters/metadata
    emb_path = Path(args.embeddings)
    clu_path = Path(args.clusters)
    meta_path = Path(args.metadata)
    if not emb_path.exists() or not clu_path.exists():
        raise SystemExit('Embeddings or clusters parquet not found.')
    emb_df = pd.read_parquet(emb_path)
    clu_df = pd.read_parquet(clu_path)
    meta_df = pd.read_parquet(meta_path) if meta_path.exists() else pd.DataFrame()
    emb_index = EmbeddingIndex.from_dataframe(emb_df)

    # Load params
    params = yaml.safe_load(Path(args.params).read_text(encoding='utf-8')) if Path(args.params).exists() else {}
    neighbor_cfg = (params.get('neighbor_strategy') or {}).copy()

    # Build/load client
    out_dir = Path('outputs/clients'); out_dir.mkdir(parents=True, exist_ok=True)
    if args.client_file:
        client_path = Path(args.client_file)
        client = json.loads(client_path.read_text(encoding='utf-8'))
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
        client_path = Path(f'configs/clients/{client_id}.json')
        client_path.parent.mkdir(parents=True, exist_ok=True)
        client_path.write_text(json.dumps(client, ensure_ascii=False, indent=2), encoding='utf-8')

    # Embedding model config
    emb_cfg = yaml.safe_load(Path(args.emb_config).read_text(encoding='utf-8')) if Path(args.emb_config).exists() else {}
    doc_fields = emb_cfg.get('document_fields', {"text_fields": ["name", "description"], "tag_fields": ["tags"]})
    model_name = emb_cfg.get('embedding_model', 'all-MiniLM-L6-v2')
    normalize = bool(emb_cfg.get('normalize_embeddings', True))

    # Encode client text
    from sentence_transformers import SentenceTransformer
    doc = _build_doc_from_client(client, doc_fields)
    if not doc:
        raise SystemExit('Client has insufficient fields to encode a profile document.')
    model = SentenceTransformer(model_name)
    vec = model.encode([doc], normalize_embeddings=normalize, show_progress_bar=False)[0].astype(np.float32)

    # Assign cluster (optional) for diagnostics
    cid = _assign_cluster(vec, args.medoids, clu_df)

    # Neighbor selection
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

    # Outputs
    neigh_json = out_dir / f'client_{client_id}_neighbors.json'
    neigh_appids = out_dir / f'client_{client_id}_appids.txt'
    diag_json = out_dir / f'client_{client_id}_diagnostics.json'
    neigh_json.write_text(json.dumps(neighbors, ensure_ascii=False, indent=2), encoding='utf-8')
    appids = [str(n.get('appid')) for n in neighbors if n.get('appid')]
    neigh_appids.write_text(' '.join(appids), encoding='utf-8')
    diag = {
        'client_id': client_id,
        'assigned_cluster_id': cid,
        'selected': len(appids),
        'config': neighbor_cfg,
    }
    diag_json.write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"[OK] Neighbors: {neigh_json} | AppIDs: {neigh_appids}")


if __name__ == '__main__':
    main()

