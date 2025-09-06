#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera un estudio de mercado (JSON) para un juego del cliente usando artefactos precomputados.
- Codifica la descripción del cliente en un embedding (SentenceTransformer)
- Asigna un clúster (por medoid si existe, si no por vecino más cercano)
- Obtiene los K vecinos (competidores) más similares
- Anexa insights disponibles por competidor (CCF, eventos, tópicos, explicaciones)
- Calcula un análisis mínimo de reglas para el cliente (p. ej., precio vs mediana de clúster)
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
import yaml
from typing import Dict, Any, List
import mlflow
from src.utils.config_utils import expand_env_in_obj
from src.utils.mlflow_utils import log_mlflow_params, log_mlflow_metrics
from pathlib import Path

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


def _neighbors_for_client(vec_q: np.ndarray, emb_df: pd.DataFrame, clu_df: pd.DataFrame,
                          meta_df: pd.DataFrame, top_k: int, same_cluster_only: bool,
                          client_cluster_id: int | None) -> List[Dict[str, Any]]:
    ids = emb_df['appid'].astype(str).tolist()
    vecs = np.vstack(emb_df['embedding'].apply(np.asarray).to_list()).astype(np.float32)
    sims = vecs @ vec_q
    order = np.argsort(-sims)
    out = []
    for i in order:
        aid = ids[i]
        if same_cluster_only and client_cluster_id is not None:
            sub = clu_df[clu_df['appid'].astype(str) == aid]
            if sub.empty or int(sub.iloc[0]['cluster_id']) != int(client_cluster_id):
                continue
        rmeta = meta_df[meta_df['appid'].astype(str) == aid].head(1)
        rclu = clu_df[clu_df['appid'].astype(str) == aid].head(1) if not clu_df.empty else pd.DataFrame()
        out.append({
            "appid": aid,
            "name": None if rmeta.empty else rmeta.iloc[0].get('name'),
            "cluster_id": None if rclu.empty else int(rclu.iloc[0]['cluster_id']),
            "similarity": float(sims[i])
        })
        if len(out) >= top_k:
            break
    return out


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
    # Alternativa: usar el clúster del vecino más cercano (se resuelve en la llamada)
    return None


def _collect_sections_for_appid(aid: str, ccf: pd.DataFrame, events: pd.DataFrame,
                                topics: pd.DataFrame, expl: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not ccf.empty:
        sub = ccf[ccf['appid'].astype(str) == aid]
        if not sub.empty:
            keep = [c for c in ['pair_name', 'best_lag', 'best_ccf', 'best_pval', 'best_significant_fdr',
                                'lead_or_lag', 'granger_xy_pmin', 'granger_yx_pmin', 'granger_xy_sig', 'granger_yx_sig']
                    if c in sub.columns]
            out['ccf_granger'] = sub[keep].to_dict(orient='records')

    if not events.empty:
        sub = events[events['appid'].astype(str) == aid].copy()
        if not sub.empty and 'year_month' in sub.columns:
            sub['year_month'] = pd.to_datetime(sub['year_month']).dt.strftime('%Y-%m-%d')
            out['events'] = sub.to_dict(orient='records')

    if not topics.empty:
        sub = topics[topics['appid'].astype(str) == aid].copy()
        if not sub.empty:
            if 'event_year_month' in sub.columns:
                sub['event_year_month'] = pd.to_datetime(sub['event_year_month']).dt.strftime('%Y-%m-%d')
            out['topics'] = sub.to_dict(orient='records')

    if not expl.empty:
        sub = expl[expl['appid'].astype(str) == aid].copy()
        if not sub.empty and 'year_month' in sub.columns:
            sub['year_month'] = pd.to_datetime(sub['year_month']).dt.strftime('%Y-%m-%d')
            out['explanations'] = sub.sort_values('year_month').to_dict(orient='records')
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
        return "Juego económico frente al segmento"
    if p < m * (1.0 + alto):
        return "Precio alineado al segmento"
    return "Precio por encima del segmento"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--client_file', required=True, help='JSON con info del cliente (appid, name, description, tags, price, release_date, ...)')
    ap.add_argument('--embeddings', default='data/processed/embeddings.parquet')
    ap.add_argument('--clusters', default='data/processed/clusters.parquet')
    ap.add_argument('--metadata', default='data/processed/game_metadata.parquet')
    ap.add_argument('--ccf', default='outputs/ccf_analysis/summary.parquet')
    ap.add_argument('--events', default='outputs/events/events.parquet')
    ap.add_argument('--topics', default='outputs/events/topics.parquet')
    ap.add_argument('--explanations', default='outputs/events/explanations.parquet')
    ap.add_argument('--rules_dir', default='data/with_rules/')
    ap.add_argument('--emb_config', default='configs/embeddings.yaml')
    ap.add_argument('--medoids', default='models/cluster_medoids.json')
    ap.add_argument('--top_k', type=int, default=15)
    ap.add_argument('--same_cluster_only', type=lambda x: str(x).lower() in ['1','true','yes'], default=True)
    ap.add_argument('--out', default=None, help='Ruta de salida JSON; por defecto outputs/reports/client_{id}.json')
    ap.add_argument('--params', default='configs/params.yaml')
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

    if emb_df.empty:
        raise SystemExit('Embeddings no disponibles. Ejecuta el pipeline base primero.')

    emb_cfg = yaml.safe_load(Path(args.emb_config).read_text(encoding='utf-8')) if Path(args.emb_config).exists() else {}
    doc_fields = emb_cfg.get('document_fields', {"text_fields": ["name", "description"], "tag_fields": ["tags"]})
    model_name = emb_cfg.get('embedding_model', 'all-MiniLM-L6-v2')

    # Generar embedding del documento del cliente
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(model_name)
    normalize = bool(emb_cfg.get('normalize_embeddings', True))
    doc = _build_doc_from_client(client, doc_fields)
    if not doc:
        raise SystemExit('El client_file no tiene campos suficientes para construir el documento de embedding.')
    vec = model.encode([doc], normalize_embeddings=normalize, show_progress_bar=False)[0].astype(np.float32)

    # Asignación de clúster
    cid = _assign_cluster(vec, args.medoids, clu_df)
    if cid is None and not clu_df.empty:
        # fallback by nearest neighbor
        ids = emb_df['appid'].astype(str).tolist()
        vecs = np.vstack(emb_df['embedding'].apply(np.asarray).to_list()).astype(np.float32)
        sims = vecs @ vec
        j = int(np.argmax(sims))
        aid = ids[j]
        rclu = clu_df[clu_df['appid'].astype(str) == aid].head(1)
        if not rclu.empty:
            cid = int(rclu.iloc[0]['cluster_id'])

    # Vecinos como competidores
    neigh = _neighbors_for_client(vec, emb_df, clu_df, meta_df, args.top_k, args.same_cluster_only, cid)

    # Detalle por competidor
    competitors = []
    for n in neigh:
        aid = n['appid']
        sections = _collect_sections_for_appid(aid, ccf_df, events_df, topics_df, expl_df)
        competitors.append({**n, **sections})

    # Reglas mínimas para el cliente
    params_cfg = expand_env_in_obj(yaml.safe_load(Path(args.params).read_text(encoding='utf-8'))) if Path(args.params).exists() else {}
    # Estimar distribución de precios del clúster a partir de vecinos
    cluster_prices = []
    if meta_df is not None and len(meta_df) > 0:
        # use neighbors meta to get price
        ids_set = {n['appid'] for n in neigh}
        sub = meta_df[meta_df['appid'].astype(str).isin(ids_set)]
        if 'price' in sub.columns:
            cluster_prices = [x for x in sub['price'].tolist()]
    client_price = client.get('price')
    rules_analysis = {
        'regla_precio': _price_rule(client_price, cluster_prices, params_cfg)
    }

    report = {
        "appid": client_id,
        "metadata": {
            "name": client.get('name'),
            "description": client.get('description'),
            "price": client.get('price'),
            "tags": client.get('tags'),
            "release_date": client.get('release_date'),
        },
        "cluster": {"cluster_id": cid},
        "neighbors": neigh,
        "competitors": competitors,
        "rules_analysis": rules_analysis,
        "provenance": {
            "embeddings_parquet": args.embeddings,
            "clusters_parquet": args.clusters,
            "metadata_parquet": args.metadata,
            "ccf_summary_parquet": args.ccf,
            "events_parquet": args.events,
            "topics_parquet": args.topics,
            "explanations_parquet": args.explanations,
            "medoids_json": args.medoids,
        }
    }

    out_path = Path(args.out) if args.out else Path('outputs/reports') / f"client_{client_id}.json"

    # MLflow opcional desde params_cfg.mlflow
    mlf_cfg = params_cfg.get('mlflow', {})
    use_mlflow = bool(mlf_cfg.get('enabled', True))
    # Validación mínima contra JSON Schema (si disponible)
    def _validate_report(obj: Dict[str, Any]):
        schema_path = Path('schemas/client_report.schema.json')
        # Comprobación ligera si jsonschema no está disponible
        basic_ok = (
            isinstance(obj.get('appid'), str) and
            isinstance(obj.get('metadata'), dict) and
            isinstance(obj.get('cluster'), dict) and
            isinstance(obj.get('neighbors'), list) and
            isinstance(obj.get('competitors'), list) and
            isinstance(obj.get('rules_analysis'), dict) and
            isinstance(obj.get('provenance'), dict)
        )
        if not basic_ok:
            raise ValueError("Reporte de cliente no cumple con la estructura mínima requerida.")
        if schema_path.exists():
            try:
                import json
                import jsonschema
                schema = json.loads(schema_path.read_text(encoding='utf-8'))
                jsonschema.validate(instance=obj, schema=schema)
            except ImportError:
                # Sin jsonschema, nos quedamos con la validación mínima
                pass

    _validate_report(report)

    if use_mlflow:
        mlflow.set_experiment(mlf_cfg.get('experiment', 'Steam Analytics'))
        mlflow.start_run(run_name=mlf_cfg.get('run_name_prefix', 'client_report_'))
        try:
            log_mlflow_params({
                "top_k": args.top_k,
                "same_cluster_only": args.same_cluster_only,
                "client_id": client_id
            })
            write_json_any(report, out_path, indent=2)
            # Registrar artefacto
            mlflow.log_artifact(str(out_path))
            log_mlflow_metrics({"competitors": len(competitors)})
        finally:
            mlflow.end_run()
    else:
        write_json_any(report, out_path, indent=2)
    print(f"[OK] Reporte de cliente -> {out_path}")


if __name__ == '__main__':
    main()
