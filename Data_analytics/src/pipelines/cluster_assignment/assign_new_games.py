#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Asigna nuevos juegos a clústeres existentes usando los centroides/medoids guardados.

Entradas:
- Embeddings de los nuevos juegos (Parquet/CSV/JSON con columnas: appid, embedding)
- Medoids/centroides en JSON: { cluster_id: [float, ...] }

Salida:
- Parquet con columnas: appid, cluster_id, cluster_version, assigned_date

Uso:
  python -m src.pipelines.cluster_assignment.assign_new_games \
      --embeddings data/processed/embeddings_new.parquet \
      --medoids models/cluster_medoids.json \
      --out data/processed/clusters_assigned.parquet \
      --cluster_version 202501
"""
from __future__ import annotations
import argparse
from pathlib import Path
import json
from datetime import datetime
import numpy as np
import pandas as pd

from src.utils.io import read_parquet_any, read_csv_any, read_json_any, write_parquet_any


def _load_any_df(p: str) -> pd.DataFrame:
    if p.endswith('.csv'):
        return read_csv_any(p)
    if p.endswith('.json'):
        return read_json_any(p)
    return read_parquet_any(p)


def _assign_cluster(vec: np.ndarray, medoids: dict[str, list[float]]) -> int | None:
    if not medoids:
        return None
    best_cid, best_sim = None, -1e18
    v = vec.astype(np.float32)
    for cid, centroid in medoids.items():
        try:
            cvec = np.asarray(centroid, dtype=np.float32)
            sim = float(np.dot(v, cvec))
            if sim > best_sim:
                best_cid, best_sim = int(cid), sim
        except Exception:
            continue
    return best_cid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--embeddings', required=True, help='Embeddings de nuevos juegos (appid, embedding).')
    ap.add_argument('--medoids', default='models/cluster_medoids.json', help='JSON con centroides/medoides.')
    ap.add_argument('--out', required=True, help='Ruta de salida Parquet.')
    ap.add_argument('--cluster_version', default=None, help='Versión de clúster (YYYYMM). Si no, usa mes actual.')
    args = ap.parse_args()

    emb = _load_any_df(args.embeddings)
    if emb.empty:
        print('[WARN] No hay embeddings para asignar. Abortando.')
        write_parquet_any(pd.DataFrame(columns=['appid','cluster_id','cluster_version','assigned_date']), args.out)
        return

    mp = Path(args.medoids)
    if not mp.exists():
        raise FileNotFoundError(f'No se encontró medoids/centroides en {mp}. Ejecuta clustering primero.')
    medoids = json.loads(mp.read_text(encoding='utf-8'))

    # Normalizar columnas esperadas
    if 'appid' not in emb.columns:
        raise SystemExit('El fichero de embeddings debe contener columna appid.')
    if 'embedding' not in emb.columns:
        raise SystemExit('El fichero de embeddings debe contener columna embedding (vector).')

    ver = args.cluster_version or datetime.now().strftime('%Y%m')
    assigned_date = pd.Timestamp.now().normalize()

    rows = []
    for _, r in emb.iterrows():
        try:
            cid = _assign_cluster(np.asarray(r['embedding'], dtype=np.float32), medoids)
            if cid is not None:
                rows.append({'appid': str(r['appid']), 'cluster_id': int(cid),
                             'cluster_version': str(ver), 'assigned_date': assigned_date})
        except Exception:
            continue

    out = pd.DataFrame(rows)
    write_parquet_any(out, args.out)
    print(f'[OK] Asignaciones guardadas en -> {args.out}')


if __name__ == '__main__':
    main()

