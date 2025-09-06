#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Exporta 1 JSON por juego con TODO el contexto del pipeline."""
from __future__ import annotations
import argparse
import yaml
import json
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from functools import partial
import multiprocessing
from src.utils.config_utils import expand_env_in_obj
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    print("[WARN] Ray no está instalado. Usando 'multiprocessing' para paralelizar en local.")

# Importa tus funciones de utilidad para la lectura de archivos
# Es crucial que estas funciones estén disponibles.
from src.utils.io import read_parquet_any, read_csv_any, read_json_any
from src.utils.faiss_utils import load_faiss_index, search_faiss_index

# -----------------------------------------------------------------------------
# Funciones auxiliares
# -----------------------------------------------------------------------------
def _load_df(p):
    p = str(p)
    try:
        if p.endswith('.csv'):
            return read_csv_any(p)
        if p.endswith('.parquet') or p.endswith('.pq'):
            return read_parquet_any(p)
        if p.endswith('.json'):
            return read_json_any(p)
        return read_parquet_any(p)
    except Exception:
        return pd.DataFrame()

def _validate_app_report(obj: dict):
    """Validación mínima y opcional contra JSON Schema para el reporte por appid."""
    required_top = [
        'appid', 'generated_at', 'metadata', 'cluster', 'neighbors',
        'ccf_granger', 'events', 'topics', 'explanations', 'rules_analysis', 'provenance'
    ]
    for k in required_top:
        if k not in obj:
            raise ValueError(f"Reporte inválido: falta la clave '{k}'")
    schema_path = Path('schemas/app_report.schema.json')
    if schema_path.exists():
        try:
            import jsonschema
            schema = json.loads(schema_path.read_text(encoding='utf-8'))
            jsonschema.validate(instance=obj, schema=schema)
        except ImportError:
            pass

def _neighbors(appid, df_emb, df_clu, df_meta, top_k=15, same_cluster_only=True):
    ids = df_emb['appid'].astype(str).tolist()
    vecs = np.vstack(df_emb['embedding'].apply(np.asarray).to_list()).astype(np.float32)
    id2idx = {a: i for i, a in enumerate(ids)}
    if appid not in id2idx:
        return []
    idx = id2idx[appid]
    q = vecs[idx]
    sims = vecs @ q
    sims[idx] = -np.inf
    if same_cluster_only and not df_clu.empty:
        c0 = int(df_clu[df_clu['appid'].astype(str) == appid].iloc[0]['cluster_id']) if appid in set(df_clu['appid'].astype(str)) else None
        if c0 is not None:
            allowed = set(df_clu[df_clu['cluster_id'] == c0]['appid'].astype(str).tolist())
            allowed.discard(appid)
            for i, a in enumerate(ids):
                if a not in allowed:
                    sims[i] = -np.inf
    order = np.argsort(-sims)[:top_k]
    out = []
    for i in order:
        aid = ids[i]
        rmeta = df_meta[df_meta['appid'].astype(str) == aid].head(1)
        rclu = df_clu[df_clu['appid'].astype(str) == aid].head(1)
        out.append({"appid": aid, "name": None if rmeta.empty else rmeta.iloc[0].get('name'),
                     "cluster_id": None if rclu.empty else int(rclu.iloc[0]['cluster_id']), "similarity": float(sims[i])})
    return out

def _neighbors_via_faiss(appid: str, df_emb: pd.DataFrame, df_clu: pd.DataFrame, df_meta: pd.DataFrame,
                         top_k: int, same_cluster_only: bool,
                         faiss_index_path: str, ids_path: str) -> list[dict]:
    try:
        idx_path = Path(faiss_index_path)
        idp = Path(ids_path)
        if not (idx_path.exists() and idp.exists()):
            return []
        index = load_faiss_index(str(idx_path))
        ids = __import__('json').loads(idp.read_text(encoding='utf-8'))
        id2idx = {str(a): i for i, a in enumerate(ids)}
        if appid not in id2idx:
            return []
        q_idx = id2idx[appid]
        # construir consulta desde df_emb para mantener normalización consistente
        vecs = np.vstack(df_emb['embedding'].apply(np.asarray).to_list()).astype(np.float32)
        q = vecs[q_idx:q_idx+1]
        D, I = search_faiss_index(index, q, top_k + 1)
        sims = D[0]
        idxs = I[0]
        out = []
        for dist, i in zip(sims, idxs):
            if i == q_idx or i < 0 or i >= len(ids):
                continue
            aid = str(ids[i])
            if same_cluster_only and not df_clu.empty:
                myc = df_clu[df_clu['appid'].astype(str) == appid]
                oc = df_clu[df_clu['appid'].astype(str) == aid]
                if not myc.empty and not oc.empty:
                    if int(myc.iloc[0]['cluster_id']) != int(oc.iloc[0]['cluster_id']):
                        continue
            rmeta = df_meta[df_meta['appid'].astype(str) == aid].head(1)
            rclu = df_clu[df_clu['appid'].astype(str) == aid].head(1)
            out.append({
                "appid": aid,
                "name": None if rmeta.empty else rmeta.iloc[0].get('name'),
                "cluster_id": None if rclu.empty else int(rclu.iloc[0]['cluster_id']),
                "similarity": float(dist)
            })
            if len(out) >= top_k:
                break
        return out
    except Exception:
        return []

# -----------------------------------------------------------------------------
# Función principal del reporte (paralelizable)
# -----------------------------------------------------------------------------
def build_report_for_appid(appid, cfg, data_dict):
    """Genera un reporte JSON para un único appid."""
    meta = data_dict['meta']
    emb = data_dict['emb']
    clu = data_dict['clu']
    ccf = data_dict['ccf']
    events = data_dict['events']
    topics = data_dict['topics']
    expl = data_dict['expl']
    rules = data_dict['rules'] # <-- Nueva línea

    report_dir = Path(cfg.get('report_output_dir', 'outputs/reports'))
    mrow = meta[meta['appid'].astype(str) == appid].head(1)

    metadata = {}
    for col in ['appid', 'name', 'genres', 'categories', 'release_date', 'price']:
        if not mrow.empty and col in mrow.columns:
            val = mrow.iloc[0][col]
            if 'date' in col:
                try:
                    metadata[col] = str(pd.to_datetime(val).date())
                except Exception:
                    metadata[col] = str(val)
            else:
                metadata[col] = val

    cluster_info = {}
    if not clu.empty and appid in set(clu['appid'].astype(str)):
        r = clu[clu['appid'].astype(str) == appid].iloc[0]
        cluster_info = {"cluster_id": int(r.get('cluster_id')) if r.get('cluster_id') == r.get('cluster_id') else None}

    # Preferir FAISS persistido si existe
    neigh = []
    fcfg = cfg.get('neighbors_faiss', {})
    neigh = _neighbors_via_faiss(
        appid, emb, clu, meta,
        top_k=int(cfg.get('neighbors_top_k', 15)),
        same_cluster_only=bool(cfg.get('neighbors_same_cluster_only', True)),
        faiss_index_path=fcfg.get('index_path', 'models/embeddings.faiss'),
        ids_path=fcfg.get('ids_path', 'models/emb_ids.json'),
    )
    if not neigh:
        neigh = _neighbors(appid, emb, clu, meta, top_k=int(cfg.get('neighbors_top_k', 15)), same_cluster_only=bool(cfg.get('neighbors_same_cluster_only', True)))

    ccf_section = []
    if not ccf.empty:
        sub = ccf[ccf['appid'].astype(str) == appid].copy()
        if not sub.empty:
            keep = [c for c in ['pair_name', 'best_lag', 'best_ccf', 'best_pval', 'best_significant_fdr', 'lead_or_lag', 'granger_xy_pmin', 'granger_yx_pmin', 'granger_xy_sig', 'granger_yx_sig'] if c in sub.columns]
            ccf_section = sub[keep].to_dict(orient='records')

    events_section = []
    if not events.empty:
        sub = events[events['appid'].astype(str) == appid].copy()
        if not sub.empty:
            sub['year_month'] = pd.to_datetime(sub['year_month']).dt.strftime('%Y-%m-%d')
            events_section = sub.to_dict(orient='records')

    topics_section = []
    if not topics.empty:
        sub = topics[topics['appid'].astype(str) == appid].copy()
        if not sub.empty:
            if 'event_year_month' in sub.columns:
                sub['event_year_month'] = pd.to_datetime(sub['event_year_month']).dt.strftime('%Y-%m-%d')
            if 'anchor_year_month' in sub.columns:
                sub['anchor_year_month'] = pd.to_datetime(sub['anchor_year_month']).dt.strftime('%Y-%m-%d')
            topics_section = sub.to_dict(orient='records')

    explanations_section = []
    if not expl.empty:
        sub = expl[expl['appid'].astype(str) == appid].copy()
        if not sub.empty and 'year_month' in sub.columns:
            sub['year_month'] = pd.to_datetime(sub['year_month']).dt.strftime('%Y-%m-%d')
            explanations_section = sub.sort_values('year_month').to_dict(orient='records')

    rules_section = {} # <-- Nueva sección para las reglas
    if not rules.empty:
        sub = rules[rules['appid'].astype(str) == appid].head(1)
        if not sub.empty:
            rules_section = sub.to_dict(orient='records')[0]

    report = {"appid": appid, "generated_at": datetime.utcnow().isoformat() + "Z",
              "metadata": metadata, "cluster": cluster_info, "neighbors": neigh,
              "ccf_granger": ccf_section, "events": events_section,
              "topics": topics_section, "explanations": explanations_section,
              "rules_analysis": rules_section, # <-- Añadido al reporte final
              "provenance": {k: str(v) for k, v in {
                  "metadata_parquet": cfg.get('metadata_parquet', 'data/processed/game_metadata.parquet'),
                  "embeddings_parquet": cfg.get('embeddings_parquet', 'data/processed/embeddings.parquet'),
                  "clusters_parquet": cfg.get('clusters_parquet', 'data/processed/clusters.parquet'),
                  "ccf_summary_parquet": cfg.get('ccf_summary_parquet', 'outputs/ccf_analysis/summary.parquet'),
                  "events_parquet": cfg.get('events_parquet', 'outputs/events/events.parquet'),
                  "topics_parquet": cfg.get('topics_parquet', 'outputs/events/topics.parquet'),
                  "explanations_parquet": cfg.get('explanations_parquet', 'outputs/events/explanations.parquet'),
              "rules_parquet": 'data/with_rules/with_rules.parquet'
              }.items()}}
    _validate_app_report(report)

    outp = Path(cfg.get('report_output_dir', 'outputs/reports')) / f"{appid}.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    return f"[OK] Reporte -> {outp}"

# -----------------------------------------------------------------------------
# Lógica de orquestación principal
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/events.yaml")
    ap.add_argument("--appid", help="Un solo appid para generar el reporte.")
    ap.add_argument("--top_k", type=int, default=15)
    ap.add_argument("--mode", choices=['local', 'cluster'], default='local', help="Modo de ejecución: local (multiprocessing) o cluster (ray).")
    args = ap.parse_args()

    cfg = expand_env_in_obj(yaml.safe_load(open(args.config, 'r'))) if Path(args.config).exists() else {}
    cfg.setdefault('report_output_dir', 'outputs/reports')
    cfg.setdefault('neighbors_top_k', args.top_k)

    print("[INFO] Cargando todos los datasets. Esto se hace una vez.")
    data_dict = {
        'meta': _load_df(cfg.get('metadata_parquet', 'data/processed/game_metadata.parquet')),
        'emb': _load_df(cfg.get('embeddings_parquet', 'data/processed/embeddings.parquet')),
        'clu': _load_df(cfg.get('clusters_parquet', 'data/processed/clusters.parquet')),
        'ccf': _load_df(cfg.get('ccf_summary_parquet', 'outputs/ccf_analysis/summary.parquet')),
        'events': _load_df(cfg.get('events_parquet', 'outputs/events/events.parquet')),
        'topics': _load_df(cfg.get('topics_parquet', 'outputs/events/topics.parquet')),
        'expl': _load_df(cfg.get('explanations_parquet', 'outputs/events/explanations.parquet')),
        'rules': _load_df('data/with_rules/with_rules.parquet')
    }

    if data_dict['emb'].empty:
        raise SystemExit("No se encontraron embeddings. Por favor, ejecuta el pipeline de embeddings primero.")

    if args.appid:
        appids = [str(args.appid)]
    else:
        emb_appids = set(data_dict['emb']['appid'].astype(str).tolist())
        clu_df = data_dict.get('clu', pd.DataFrame())
        cluster_filter = cfg.get('cluster_filter')
        if cluster_filter and not clu_df.empty and 'cluster_id' in clu_df.columns:
            subset = clu_df[clu_df['cluster_id'].isin(cluster_filter)]['appid'].astype(str).tolist()
            appids = [a for a in subset if a in emb_appids]
        else:
            appids = list(emb_appids)

    if args.mode == 'local':
        print(f"[INFO] Ejecutando en modo 'local' con {multiprocessing.cpu_count()} núcleos.")
        with multiprocessing.Pool() as pool:
            func = partial(build_report_for_appid, cfg=cfg, data_dict=data_dict)
            results = pool.map(func, appids)
        for res in results:
            print(res)

    elif args.mode == 'cluster':
        if not RAY_AVAILABLE:
            raise ImportError("Ray no está instalado. Instálalo con 'pip install ray' para usar el modo 'cluster'.")

        print("[INFO] Inicializando Ray...")
        ray.init(ignore_reinit_error=True)

        @ray.remote
        def ray_build_report_for_appid(appid, cfg, data_dict):
            return build_report_for_appid(appid, cfg, data_dict)

        ray_data_dict = ray.put(data_dict)

        print(f"[INFO] Lanzando tareas Ray para {len(appids)} juegos...")
        futures = [ray_build_report_for_appid.remote(appid, cfg, ray_data_dict) for appid in appids]

        results = ray.get(futures)
        for res in results:
            print(res)

        ray.shutdown()

if __name__ == "__main__":
    main()
