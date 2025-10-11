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
import sys
import os

# Add project root to Python path to allow imports from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        return super(NumpyEncoder, self).default(obj)

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
            try:
                jsonschema.validate(instance=obj, schema=schema)
            except Exception:
                pass
        except ImportError:
            pass

# --- Override with relaxed validator for lean JSON ---
def _validate_app_report(obj: dict):
    """Validación mínima (lean) para permitir omitir cluster/neighbors y secciones.

    Requiere solo claves básicas; el esquema completo se aplica de forma best-effort.
    """
    required_top = ['appid', 'generated_at', 'metadata', 'provenance']
    for k in required_top:
        if k not in obj:
            raise ValueError(f"Reporte inválido: falta la clave '{k}'")
    schema_path = Path('schemas/app_report.schema.json')
    if schema_path.exists():
        try:
            import jsonschema
            schema = json.loads(schema_path.read_text(encoding='utf-8'))
            try:
                jsonschema.validate(instance=obj, schema=schema)
            except Exception:
                pass
        except ImportError:
            pass

try:
    from sqlalchemy import create_engine  # optional for PostgreSQL export
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

def _jsonable(v):
    import numpy as _np
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (_np.integer,)):
        return int(v)
    if isinstance(v, (_np.floating,)):
        return float(v)
    if isinstance(v, (_np.bool_,)):
        return bool(v)
    if isinstance(v, (list, dict)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)

def _records_to_df_with_appid(appid: str, records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        row = {k: _jsonable(v) for k, v in (r or {}).items()}
        row.setdefault('appid', str(appid))
        rows.append(row)
    return pd.DataFrame(rows)

def _pg_uri_from_env() -> str | None:
    uri = os.getenv('POSTGRES_URI')
    if uri:
        return uri
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USER')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    port = os.getenv('POSTGRES_PORT', '5432')
    if host and user and pwd and db:
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return None

def _export_section_pg(table: str, appid: str, records: list[dict], schema: str | None = None) -> None:
    if not records:
        return
    if not _SQLA_OK:
        print(f"[WARN] SQLAlchemy no disponible; no se exporta {table}")
        return
    uri = _pg_uri_from_env()
    if not uri:
        print(f"[WARN] Variables POSTGRES_* no configuradas; no se exporta {table}")
        return
    df = _records_to_df_with_appid(appid, records)
    if df.empty:
        return
    try:
        engine = create_engine(uri)
        df.to_sql(table, engine, schema=schema or os.getenv('POSTGRES_SCHEMA', 'public'), if_exists='append', index=False)
        print(f"[OK] Exportado {len(df)} filas -> {schema or os.getenv('POSTGRES_SCHEMA', 'public')}.{table}")
    except Exception as exc:
        print(f"[WARN] Fallo exportando '{table}' a PostgreSQL: {exc}")

def _neighbors(appid, df_emb, df_clu, df_meta, top_k=15, same_cluster_only=True, min_similarity=0.0):
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
    order = np.argsort(-sims)
    out = []
    for i in order:
        if sims[i] < min_similarity:
            continue
        aid = ids[i]
        rmeta = df_meta[df_meta['appid'].astype(str) == aid].head(1)
        rclu = df_clu[df_clu['appid'].astype(str) == aid].head(1)
        out.append({"appid": aid, "name": None if rmeta.empty else rmeta.iloc[0].get('name'),
                     "cluster_id": None if rclu.empty else int(rclu.iloc[0]['cluster_id']), "similarity": float(sims[i])})
        if len(out) >= top_k:
            break
    return out


def _neighbors_via_faiss(appid: str, df_emb: pd.DataFrame, df_clu: pd.DataFrame, df_meta: pd.DataFrame,
                         top_k: int, same_cluster_only: bool, min_similarity: float,
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
        query_k = min(len(ids), max(top_k * 3, top_k + 1))
        D, I = search_faiss_index(index, q, query_k)
        sims = D[0]
        idxs = I[0]
        out = []
        for dist, i in zip(sims, idxs):
            if i == q_idx or i < 0 or i >= len(ids):
                continue
            if float(dist) < min_similarity:
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
    evcorr = data_dict.get('event_corr', pd.DataFrame())

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

    neighbors_cfg = cfg.get('neighbors', {})
    top_k = int(neighbors_cfg.get('top_k', cfg.get('neighbors_top_k', 15)))
    same_cluster_only = bool(neighbors_cfg.get('same_cluster_only', cfg.get('neighbors_same_cluster_only', True)))
    min_similarity = float(neighbors_cfg.get('min_similarity', cfg.get('neighbors_min_similarity', 0.0)))

    # Preferir FAISS persistido si existe
    neigh = []
    fcfg = cfg.get('neighbors_faiss', {})
    neigh = _neighbors_via_faiss(
        appid, emb, clu, meta,
        top_k=top_k,
        same_cluster_only=same_cluster_only,
        min_similarity=min_similarity,
        faiss_index_path=fcfg.get('index_path', 'models/embeddings.faiss'),
        ids_path=fcfg.get('ids_path', 'models/emb_ids.json'),
    )
    if not neigh:
        neigh = _neighbors(appid, emb, clu, meta, top_k=top_k, same_cluster_only=same_cluster_only, min_similarity=min_similarity)

    ccf_section = []
    sub_ccf = pd.DataFrame()
    if not ccf.empty:
        sub_ccf = ccf[ccf['appid'].astype(str) == appid].copy()
        if not sub_ccf.empty:
            keep = [c for c in ['pair_name', 'best_lag', 'best_ccf', 'best_pval', 'best_significant_fdr', 'lead_or_lag', 'granger_xy_pmin', 'granger_yx_pmin', 'granger_xy_sig', 'granger_yx_sig'] if c in sub_ccf.columns]
            ccf_section = sub_ccf[keep].to_dict(orient='records')

    events_section = []
    if not events.empty:
        sub = events[events['appid'].astype(str) == appid].copy()
        if not sub.empty:
            sub['year_month'] = pd.to_datetime(sub['year_month']).dt.strftime('%Y-%m-%d')
            events_section = sub.to_dict(orient='records')

    topics_section = []
    negative_topic_alerts = []
    if not topics.empty:
        sub = topics[topics['appid'].astype(str) == appid].copy()
        if not sub.empty:
            if 'event_year_month' in sub.columns:
                sub['event_year_month'] = pd.to_datetime(sub['event_year_month']).dt.strftime('%Y-%m-%d')
            if 'anchor_year_month' in sub.columns:
                sub['anchor_year_month'] = pd.to_datetime(sub['anchor_year_month']).dt.strftime('%Y-%m-%d')
            # Extraer alertas por polaridad negativa si las hay
            if 'relevance_polarity' in sub.columns:
                neg = sub[sub['relevance_polarity'] == 'negative'].copy()
                if not neg.empty:
                    cols_keep = [c for c in ['event_year_month', 'relevance_polarity', 'players_zscore'] if c in neg.columns]
                    neg = neg[cols_keep]
                    negative_topic_alerts = neg.rename(columns={'event_year_month': 'year_month'}).to_dict(orient='records')
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

    event_correlation_section = []
    sub_evc = pd.DataFrame()
    if isinstance(evcorr, pd.DataFrame) and not evcorr.empty:
        sub_evc = evcorr[evcorr['appid'].astype(str) == appid].copy()
        if not sub_evc.empty:
            if 'event_t0' in sub_evc.columns:
                sub_evc['event_t0'] = pd.to_datetime(sub_evc['event_t0']).dt.strftime('%Y-%m-%d')
            keep_cols = [c for c in [
                'event_t0','z_players_t0','players_t0','metric_y','lag_star','rho_star','p_perm',
                'drop_1m','half_life_months','pre_players_mean_3m','post_players_mean_3m',
                'pre_neg_mean_3m','post_neg_mean_3m','neg_delta_mean_3m','jaccard_overlap_peaks',
                'pattern_launch_bad_reception'
            ] if c in sub_evc.columns]
            event_correlation_section = sub_evc[keep_cols].to_dict(orient='records')

    # Hierarchical decision: prefer Granger if significant; else fallback to event-based
    causal_decision = {"method": "none", "reason": "no data"}
    chosen_granger = []
    chosen_events = []
    # Granger criteria: any significant flag present
    if not sub_ccf.empty:
        sig_cols = [c for c in ['best_significant_fdr','granger_xy_sig','granger_yx_sig'] if c in sub_ccf.columns]
        granger_ok = False
        if sig_cols:
            granger_ok = bool(sub_ccf[sig_cols].fillna(False).any().any())
        if granger_ok:
            causal_decision = {"method": "granger", "reason": "significant pairs present"}
            chosen_granger = ccf_section
        else:
            causal_decision = {"method": "event_fallback", "reason": "no significant granger pairs"}
    elif isinstance(sub_evc, pd.DataFrame) and not sub_evc.empty:
        causal_decision = {"method": "event_fallback", "reason": "no granger data"}
    # Choose top event candidates for compact summary
    if isinstance(sub_evc, pd.DataFrame) and not sub_evc.empty and causal_decision["method"] != "granger":
        # prioritize p_perm <= 0.1, then by |rho_star|, else pattern flag
        tmp = sub_evc.copy()
        if 'p_perm' in tmp.columns:
            tmp['p_ok'] = tmp['p_perm'] <= 0.10
        else:
            tmp['p_ok'] = False
        if 'rho_star' in tmp.columns:
            tmp['abs_rho'] = tmp['rho_star'].abs()
        else:
            tmp['abs_rho'] = 0.0
        if 'pattern_launch_bad_reception' not in tmp.columns:
            tmp['pattern_launch_bad_reception'] = False
        tmp = tmp.sort_values(['p_ok','abs_rho','pattern_launch_bad_reception'], ascending=[False, False, False])
        keep_cols_small = [c for c in ['event_t0','metric_y','lag_star','rho_star','p_perm','drop_1m','half_life_months','pattern_launch_bad_reception'] if c in tmp.columns]
        chosen_events = tmp[keep_cols_small].head(5).to_dict(orient='records')
    

    review_segments_section = []
    abandonment_summary = {}

    # Exportar secciones a PostgreSQL y omitirlas del JSON final
    _export_section_pg('events', appid, events_section)
    _export_section_pg('topics', appid, topics_section)
    _export_section_pg('explanations', appid, explanations_section)
    _export_section_pg('event_correlation', appid, event_correlation_section)
    _export_section_pg('ccf_granger', appid, ccf_section if isinstance(ccf_section, list) else [])

    report = {"appid": appid, "generated_at": datetime.utcnow().isoformat() + "Z",
              "metadata": metadata,
              # JSON ligero: sin cluster/neighbors ni secciones exportadas
              "abandonment": abandonment_summary,
              "alerts": negative_topic_alerts,
              "rules_analysis": rules_section, # <-- Añadido al reporte final
              "causal_inference": {
                  "method": causal_decision.get('method'),
                  "reason": causal_decision.get('reason'),
                  "granger": chosen_granger if chosen_granger else [],
                  "events": chosen_events if chosen_events else []
              },
              "provenance": {k: str(v) for k, v in {
                  "metadata_parquet": cfg.get('metadata_parquet', 'data/processed/game_metadata.parquet'),
                  "embeddings_parquet": cfg.get('embeddings_parquet', 'data/processed/embeddings.parquet'),
                  "clusters_parquet": cfg.get('clusters_parquet', 'data/processed/clusters.parquet'),
                  "ccf_summary_parquet": cfg.get('ccf_summary_parquet', 'outputs/ccf_analysis/summary.parquet'),
                  "events_parquet": cfg.get('events_parquet', 'outputs/events/events.parquet'),
                  "topics_parquet": cfg.get('topics_parquet', 'outputs/events/topics.parquet'),
                  "explanations_parquet": cfg.get('explanations_parquet', 'outputs/events/explanations.parquet'),
                  "event_correlation_parquet": 'outputs/ccf_analysis/event_correlation.parquet',
              "rules_parquet": 'data/with_rules/with_rules.parquet'
              }.items()}}
    _validate_app_report(report)

    outp = Path(cfg.get('report_output_dir', 'outputs/reports')) / f"{appid}.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(report, ensure_ascii=False, indent=2, cls=NumpyEncoder), encoding="utf-8")

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
    # Preferir tópicos anotados con CCF si existen
    topics_default = Path('outputs/events/topics_scored.parquet')
    if topics_default.exists():
        cfg['topics_parquet'] = str(topics_default)
    data_dict = {
        'meta': _load_df(cfg.get('metadata_parquet', 'data/processed/game_metadata.parquet')),
        'emb': _load_df(cfg.get('embeddings_parquet', 'data/processed/embeddings.parquet')),
        'clu': _load_df(cfg.get('clusters_parquet', 'data/processed/clusters.parquet')),
        'ccf': _load_df(cfg.get('ccf_summary_parquet', 'outputs/ccf_analysis/summary.parquet')),
        'events': _load_df(cfg.get('events_parquet', 'outputs/events/events.parquet')),
        'topics': _load_df(cfg.get('topics_parquet', 'outputs/events/topics.parquet')),
        'expl': _load_df(cfg.get('explanations_parquet', 'outputs/events/explanations.parquet')),
        'rules': _load_df('data/with_rules/with_rules.parquet'),
        'event_corr': _load_df('outputs/ccf_analysis/event_correlation.parquet')
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
