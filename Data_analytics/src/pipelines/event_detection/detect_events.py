#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Detecta picos/caídas por mes (o trimestral si escaso) con z-score, paralelizado con Ray.
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
from pymongo import MongoClient
import mlflow
try:
    import ray
    RAY_AVAILABLE = True
except Exception:
    RAY_AVAILABLE = False
import os
import multiprocessing
from multiprocessing.pool import Pool

from src.utils.io import read_parquet_any, write_parquet_any, path_exists
from src.utils.timeseries import dlog

def _read_preagg(players_path: str | None, reviews_path: str | None, appid: str, date_filter: dict | None = None) -> pd.DataFrame | None:
    try:
        dfs = []
        if reviews_path and path_exists(reviews_path):
            rv = None
            try:
                import pyarrow.dataset as ds
                ds_rv = ds.dataset(reviews_path, format='parquet')
                expr = (ds.field('appid') == str(appid))
                if date_filter and date_filter.get('start'):
                    expr = expr & (ds.field('year_month') >= pd.to_datetime(date_filter['start']))
                if date_filter and date_filter.get('end'):
                    expr = expr & (ds.field('year_month') < pd.to_datetime(date_filter['end']))
                rv = ds_rv.to_table(filter=expr).to_pandas()
            except Exception:
                rv = read_parquet_any(reviews_path)
                rv = rv[rv['appid'].astype(str) == str(appid)].copy()
            if not rv.empty:
                dfs.append(rv[['year_month', 'pos', 'neg', 'total_reviews']] if 'total_reviews' in rv.columns else rv[['year_month','pos','neg']])
        if players_path and path_exists(players_path):
            pl = None
            try:
                import pyarrow.dataset as ds
                ds_pl = ds.dataset(players_path, format='parquet')
                expr = (ds.field('appid') == str(appid))
                if date_filter and date_filter.get('start'):
                    expr = expr & (ds.field('year_month') >= pd.to_datetime(date_filter['start']))
                if date_filter and date_filter.get('end'):
                    expr = expr & (ds.field('year_month') < pd.to_datetime(date_filter['end']))
                pl = ds_pl.to_table(filter=expr).to_pandas()
            except Exception:
                pl = read_parquet_any(players_path)
                pl = pl[pl['appid'].astype(str) == str(appid)].copy()
            if not pl.empty:
                dfs.append(pl[['year_month', 'players']])
        if not dfs:
            return None
        out = None
        for d in dfs:
            out = d if out is None else pd.merge(out, d, on='year_month', how='outer')
        return out.sort_values('year_month').fillna(0)
    except Exception:
        return None

def load_data_for_appid(appid: str, cfg_players: dict, cfg_reviews: dict, preagg: dict | None = None) -> pd.DataFrame:
    """
    Carga y une datos de jugadores (desde CSV) y reseñas (desde MongoDB) para un appid.
    """
    appid_int = int(appid)

    # 0) Intentar leer preagregados si están configurados
    if preagg:
        df_pre = _read_preagg(preagg.get('players_monthly'), preagg.get('reviews_monthly'), appid, date_filter=cfg.get('date_filter'))
        if df_pre is not None and not df_pre.empty:
            return df_pre
    
    # Cargar jugadores desde CSV
    players_df = pd.DataFrame()
    path_pattern = cfg_players.get('dir_pattern')
    if path_pattern:
        path = Path(path_pattern.format(appid=appid))
        if path.exists():
            players_df = pd.read_csv(path)
            players_df['date'] = pd.to_datetime(players_df['date'], errors='coerce')
            players_df = players_df.dropna(subset=['date'])
            players_df['year_month'] = players_df['date'].dt.to_period('M').dt.to_timestamp()
            players_df = players_df.groupby('year_month')['players'].sum().reset_index()

    # Cargar reseñas desde MongoDB
    client = MongoClient(cfg_reviews['uri'])
    col = client[cfg_reviews['database']][cfg_reviews['collection']]
    cur = col.find({"appid": {"$in": [appid, appid_int]}}, {"_id": 0, "timestamp_created": 1, "voted_up": 1})
    rows = list(cur)
    client.close()
    
    if not rows and players_df.empty:
        return pd.DataFrame()

    reviews_agg = pd.DataFrame()
    if rows:
        reviews_df = pd.DataFrame(rows)
        reviews_df['date'] = pd.to_datetime(reviews_df['timestamp_created'], unit='s', errors='coerce')
        reviews_df['year_month'] = reviews_df['date'].dt.to_period('M').dt.to_timestamp()
        pos = reviews_df[reviews_df['voted_up'] == True].groupby('year_month').size().rename('pos')
        neg = reviews_df[reviews_df['voted_up'] == False].groupby('year_month').size().rename('neg')
        reviews_agg = pd.concat([pos, neg], axis=1).fillna(0).reset_index()
        reviews_agg['total_reviews'] = reviews_agg['pos'] + reviews_agg['neg']

    if players_df.empty:
        return reviews_agg
    if reviews_agg.empty:
        return players_df
    
    merged_df = pd.merge(players_df, reviews_agg, on='year_month', how='outer').fillna(0)
    return merged_df.sort_values('year_month')

def detect_events_for_series(df: pd.DataFrame, variable: str, z_thresh: float) -> pd.DataFrame:
    """
    Calcula el z-score sobre la tasa de crecimiento logarítmica (dlog) de una serie y detecta eventos.
    """
    s = df[['year_month', variable]].copy().set_index('year_month')[variable].sort_index()
    s_dlog = dlog(s).dropna()
    
    if len(s_dlog) < 6:
        return pd.DataFrame()

    z_scores = (s_dlog - s_dlog.mean()) / s_dlog.std(ddof=0)
    events = z_scores[z_scores.abs() >= z_thresh]

    if events.empty:
        return pd.DataFrame()

    results = []
    for date, z in events.items():
        results.append({
            'year_month': date,
            'variable': variable,
            'direction': 'peak' if z > 0 else 'drop',
            'zscore': float(z),
            'value': float(s.get(date, 0)),
            'growth_rate': float(s_dlog.get(date, 0))
        })
    return pd.DataFrame(results)

def _detect_events_for_game_sync(args: tuple) -> pd.DataFrame:
    """Detecta y retorna eventos para un único appid (versión sincronizada)."""
    appid, cfg = args
    cfg_players = cfg.get('players_data', {})
    cfg_reviews = cfg.get('mongo_connection', {})
    z_thresh = float(cfg['detection']['zscore_threshold'])
    
    df_ts = load_data_for_appid(appid, cfg_players, cfg_reviews, cfg.get('preaggregated'))
    
    if df_ts is None or df_ts.empty or len(df_ts) < 12:
        return pd.DataFrame()
    
    all_events = []
    variables = ['players', 'pos', 'neg', 'total_reviews']
    for var in variables:
        if var in df_ts.columns:
            events_df = detect_events_for_series(df_ts, var, z_thresh)
            if not events_df.empty:
                events_df['appid'] = str(appid)
                all_events.append(events_df)
    
    return pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

_detect_events_for_game_ray = None
if RAY_AVAILABLE:
    _detect_events_for_game_ray = ray.remote(_detect_events_for_game_sync)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config, 'r'))
    
    mode = cfg['parallelization'].get('mode', 'multiprocessing')
    
    with mlflow.start_run(run_name=f"detect_events_{mode}"):
        clusters_path = cfg.get('input_paths', {}).get('clusters_parquet', 'data/processed/clusters.parquet')
        if not Path(clusters_path).exists():
            raise FileNotFoundError(f"Archivo de clústeres no encontrado en {clusters_path}")
        
        clusters_df = read_parquet_any(clusters_path)
        # Filtro opcional por clúster desde config (events.yaml)
        cluster_filter = cfg.get('cluster_filter')
        if cluster_filter is not None and len(cluster_filter) > 0 and 'cluster_id' in clusters_df.columns:
            clusters_df = clusters_df[clusters_df['cluster_id'].isin(cluster_filter)]
        all_appids = clusters_df['appid'].astype(str).unique()
        
        print(f"Detectando eventos para {len(all_appids)} juegos de forma paralela con {mode}...")
        
        if mode == 'ray' and RAY_AVAILABLE:
            if not ray.is_initialized():
                print("[INFO] Inicializando Ray...")
                ray.init(address=cfg.get('ray_cluster', {}).get('address', 'auto'))
            futures = [_detect_events_for_game_ray.remote(appid, cfg) for appid in all_appids]
            results = ray.get(futures)
            ray.shutdown()
        elif mode == 'ray' and not RAY_AVAILABLE:
            print("[WARN] Ray no está disponible. Cambiando a 'multiprocessing'.")
            with Pool(multiprocessing.cpu_count()) as pool:
                tasks = [(appid, cfg) for appid in all_appids]
                results = pool.map(_detect_events_for_game_sync, tasks)
        elif mode == 'multiprocessing':
            with Pool(multiprocessing.cpu_count()) as pool:
                tasks = [(appid, cfg) for appid in all_appids]
                results = pool.map(_detect_events_for_game_sync, tasks)
        else:
            raise ValueError(f"Modo de paralelización desconocido: {mode}. Usa 'ray' o 'multiprocessing'.")

        all_events = [res for res in results if not res.empty]
        
        if all_events:
            final_df = pd.concat(all_events, ignore_index=True)
            out_path = Path(cfg.get('output_dir', 'outputs/events')) / 'events.parquet'
            out_path.parent.mkdir(parents=True, exist_ok=True)
            write_parquet_any(final_df, out_path)
            mlflow.log_artifact(str(out_path))
            mlflow.log_metric("total_games_analyzed", len(all_appids))
            mlflow.log_metric("events_detected", len(final_df))
            print(f"[OK] Eventos detectados guardados en -> {out_path}")
        else:
            print("[WARN] No se detectaron eventos. Creando fichero vacío.")
            mlflow.log_metric("total_games_analyzed", len(all_appids))
            mlflow.log_metric("events_detected", 0)

if __name__ == "__main__":
    main()
