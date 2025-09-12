#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Realiza un análisis de Correlación Cruzada (CCF) y Causalidad de Granger
entre series temporales de juegos de forma paralela usando Ray o multiprocessing.
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import mlflow
from pymongo import MongoClient
from typing import Dict, Any, List

# Importaciones condicionales para Ray y multiprocessing
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
try:
    from multiprocessing import Pool, cpu_count
    MULTIPROCESSING_AVAILABLE = True
except ImportError:
    MULTIPROCESSING_AVAILABLE = False

# Importaciones de utilidades del proyecto
from src.utils.io import read_parquet_any, write_parquet_any, write_csv_any, makedirs_if_local, path_exists
from src.utils.timeseries import dlog
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from statsmodels.stats.multitest import multipletests

def _apply_transform(series: pd.Series, method: str, cfg: Dict[str, Any]) -> pd.Series:
    s = series.copy()
    if method == 'dlog':
        return dlog(s.replace(0, np.nan)).dropna()
    if method == 'diff':
        return s.diff().dropna()
    if method == 'diff2':
        return s.diff().diff().dropna()
    if method == 'sqrt':
        return np.sqrt(s.clip(lower=0)).replace([np.inf, -np.inf], np.nan).dropna()
    if method == 'sqrt_diff':
        return np.sqrt(s.clip(lower=0)).diff().dropna()
    if method == 'log1p_diff':
        return np.log1p(s.clip(lower=0)).diff().dropna()
    if method == 'seasonal_diff':
        period = int(cfg.get('stationarity', {}).get('seasonal', {}).get('period', 12))
        return s.diff(period).dropna()
    return s

def _transform_to_stationary_new(series: pd.Series, transform_methods: List[str], cfg: Dict[str, Any]) -> pd.Series | None:
    series.name = series.name if series.name else 'series'
    if series.empty or np.std(series) == 0:
        return None
    alpha = float(cfg.get('stationarity', {}).get('adf_alpha', 0.05))
    for m in transform_methods:
        try:
            cand = _apply_transform(series, m, cfg)
            if cand is None or cand.empty or np.std(cand) == 0:
                continue
            ok, _ = check_stationarity(cand, alpha)
            if ok:
                print(f"[INFO] Transformación '{m}' aplicada a {series.name} -> estacionaria.")
                return cand
        except Exception:
            continue
    return None
from statsmodels.tsa.arima.model import ARIMA

def _read_players(cfg: dict, appid: str, preagg_path: str | None = None) -> pd.DataFrame | None:
    """Carga los datos de jugadores activos desde ficheros CSV."""
    # Preagregado
    if preagg_path and path_exists(preagg_path):
        try:
            import pyarrow.dataset as ds
            ds_pl = ds.dataset(preagg_path, format='parquet')
            tbl = ds_pl.to_table(filter=ds.field('appid') == str(appid))
            df = tbl.to_pandas()
        except Exception:
            df = read_parquet_any(preagg_path)
            df = df[df['appid'].astype(str) == str(appid)].copy()
        if df is not None and not df.empty:
            if 'date' in df.columns:
                df['year_month'] = pd.to_datetime(df['date']).dt.to_period('M').dt.to_timestamp()
                df = df.drop(columns=['date'])
            return df[['year_month','players']].sort_values('year_month')
    pat = cfg.get('dir_pattern')
    fil = cfg.get('file')
    if pat:
        p = Path(pat.format(appid=appid))
        if not p.exists(): return None
        df = pd.read_csv(p)
    elif fil and Path(fil).exists():
        df = pd.read_csv(fil)
        df = df[df['appid'].astype(str) == str(appid)]
    else:
        return None
        
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['year_month'] = df['date'].dt.to_period('M').dt.to_timestamp()
    g = df.groupby('year_month')['players'].sum().reset_index()
    return g

def _read_reviews(cfg: dict, appid: str, preagg_path: str | None = None) -> pd.DataFrame:
    """Carga y agrega los datos de reseñas desde MongoDB."""
    # Preagregado
    if preagg_path and path_exists(preagg_path):
        try:
            import pyarrow.dataset as ds
            ds_rv = ds.dataset(preagg_path, format='parquet')
            tbl = ds_rv.to_table(filter=ds.field('appid') == str(appid))
            df = tbl.to_pandas()
        except Exception:
            df = read_parquet_any(preagg_path)
            df = df[df['appid'].astype(str) == str(appid)].copy()
        if df is not None and not df.empty:
            return df[['year_month','pos','neg','total_reviews']].sort_values('year_month') if 'total_reviews' in df.columns else df[['year_month','pos','neg']].sort_values('year_month')
    cli = MongoClient(cfg['uri'])
    col = cli[cfg['database']][cfg['collection']]
    cur = col.find({"appid": {"$in": [appid, int(appid)]}}, {"_id": 0, "timestamp_created": 1, "voted_up": 1})
    rows = list(cur)
    cli.close()
    if not rows: return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['timestamp_created'], unit='s', errors='coerce')
    df['year_month'] = df['date'].dt.to_period('M').dt.to_timestamp()
    pos = df[df['voted_up'] == True].groupby('year_month').size().rename('pos')
    neg = df[df['voted_up'] == False].groupby('year_month').size().rename('neg')
    out = pd.concat([pos, neg], axis=1).fillna(0).reset_index()
    out['total_reviews'] = out['pos'] + out['neg']
    return out

def _ccf_series(x: pd.Series, y: pd.Series, max_lag: int) -> dict:
    """Calcula la Correlación Cruzada (CCF) entre dos series para diferentes desfases."""
    out = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            xs = x.iloc[-lag:]
            ys = y.iloc[:len(xs)]
        elif lag > 0:
            ys = y.iloc[lag:]
            xs = x.iloc[:len(ys)]
        else:
            xs, ys = x, y
        
        if len(xs) < 5 or xs.isnull().all() or ys.isnull().all() or np.std(xs) == 0 or np.std(ys) == 0:
            out[lag] = np.nan
            continue
        
        out[lag] = float(np.corrcoef(xs, ys)[0, 1])
    return out

def check_stationarity(series: pd.Series, alpha: float = 0.05) -> tuple[bool, float]:
    """Realiza el test de Dickey-Fuller Aumentado (ADF)."""
    series = series.dropna()
    if len(series) < 10 or np.std(series) == 0: return False, 1.0
    result = adfuller(series)
    p_value = result[1]
    return p_value < alpha, p_value

def _transform_to_stationary(series: pd.Series, transform_methods: List[str]) -> pd.Series | None:
    """
    Intenta varias transformaciones para hacer una serie estacionaria.
    Retorna la primera serie transformada que pasa el test de ADF o None si ninguna lo logra.
    """
    series.name = series.name if series.name else 'series'
    if series.empty or np.std(series) == 0:
        return None
    
    # Intenta la transformación `dlog`
    if 'dlog' in transform_methods:
        dlog_series = dlog(series.replace(0, np.nan)).dropna()
        if not dlog_series.empty and np.std(dlog_series) > 0:
            is_stationary, _ = check_stationarity(dlog_series)
            if is_stationary:
                print(f"[INFO] Se usó dlog para {series.name}. Serie estacionaria.")
                return dlog_series
    
    # Intenta la diferenciación simple
    if 'diff' in transform_methods:
        diff_series = series.diff().dropna()
        if not diff_series.empty and np.std(diff_series) > 0:
            is_stationary, _ = check_stationarity(diff_series)
            if is_stationary:
                print(f"[INFO] Se usó diferenciación para {series.name}. Serie estacionaria.")
                return diff_series
                
    # Intenta la diferenciación de segundo orden
    if 'diff2' in transform_methods:
        diff2_series = series.diff().diff().dropna()
        if not diff2_series.empty and np.std(diff2_series) > 0:
            is_stationary, _ = check_stationarity(diff2_series)
            if is_stationary:
                print(f"[INFO] Se usó diferenciación de segundo orden para {series.name}. Serie estacionaria.")
                return diff2_series
                
    # Intenta la transformación de raíz cuadrada
    if 'sqrt' in transform_methods:
        # Solo aplicable a series con valores positivos
        if (series >= 0).all():
            sqrt_series = np.sqrt(series).dropna()
            if not sqrt_series.empty and np.std(sqrt_series) > 0:
                is_stationary, _ = check_stationarity(sqrt_series)
                if is_stationary:
                    print(f"[INFO] Se usó raíz cuadrada para {series.name}. Serie estacionaria.")
                    return sqrt_series
    
    # Si ninguna de las transformaciones anteriores funciona, devuelve None
    return None

def analyze_pair(df: pd.DataFrame, predictor: str, target: str, cfg: dict) -> dict | None:
    """Realiza el análisis completo (preblanqueo, CCF, Granger) para un par de series."""
    x = df[predictor].dropna()
    y = df[target].dropna()
    try:
        ar_model = ARIMA(x, order=(1,0,0)).fit()
        x_whitened = ar_model.resid
        phi = ar_model.params.get('ar.L1', 0.0)
        y_filtered = y - phi * y.shift(1)
        y_filtered = y_filtered.dropna()
    except Exception:
        x_whitened, y_filtered = x, y
    
    x_final, y_final = x_whitened.align(y_filtered, join='inner')
    if len(x_final) < 8: return None
    
    ccf_results = _ccf_series(x_final.reset_index(drop=True), y_final.reset_index(drop=True), int(cfg.get('ccf_lags', 6)))
    if not ccf_results or all(pd.isna(v) for v in ccf_results.values()): return None
    best_lag = max(ccf_results, key=lambda k: abs(ccf_results.get(k, 0) or 0))
    best_ccf = ccf_results.get(best_lag)

    granger_cfg = cfg.get('granger', {})
    gxy_pmin, gyx_pmin = None, None
    try:
        res_xy = grangercausalitytests(df[[target, predictor]].dropna(), maxlag=granger_cfg['maxlag'], verbose=False)
        gxy_pmin = min([res[0]['ssr_chi2test'][1] for lag, res in res_xy.items()])
    except Exception: pass
    try:
        res_yx = grangercausalitytests(df[[predictor, target]].dropna(), maxlag=granger_cfg['maxlag'], verbose=False)
        gyx_pmin = min([res[0]['ssr_chi2test'][1] for lag, res in res_yx.items()])
    except Exception: pass
    
    return {
        'best_lag': best_lag, 'best_ccf': best_ccf,
        'granger_xy_pmin': gxy_pmin, 'granger_xy_sig': gxy_pmin is not None and gxy_pmin < granger_cfg['alpha'],
        'granger_yx_pmin': gyx_pmin, 'granger_yx_sig': gyx_pmin is not None and gyx_pmin < granger_cfg['alpha']
    }

def _process_single_game(appid: str, cfg: Dict[str, Any]) -> List[Dict]:
    """
    Función que encapsula el análisis para un solo juego.
    Retorna una lista de resultados (uno por cada par analizado).
    """
    game_results = []
    pre = cfg.get('preaggregated', {})
    df_raw = _read_reviews(cfg['mongo_connection'], appid, pre.get('reviews_monthly'))
    players_df = _read_players(cfg['players_data'], appid, pre.get('players_monthly'))
    if players_df is not None and not players_df.empty:
        df_raw = pd.merge(df_raw, players_df, on='year_month', how='outer').sort_values('year_month').fillna(0)
    
    if df_raw is None or df_raw.empty or len(df_raw) < 12:
        return game_results
        
    df_transformed = df_raw.copy()
    
    for pair in cfg.get('ccf_pairs', []):
        predictor_name, target_name = pair['predictor'], pair['target']
        
        if predictor_name not in df_transformed.columns or target_name not in df_transformed.columns:
            continue
        
        # Transformación del predictor
        methods = cfg.get('stationarity', {}).get('transforms', ['dlog', 'diff', 'diff2', 'sqrt'])
        transformed_predictor = _transform_to_stationary_new(df_transformed[predictor_name], methods, cfg)
        if transformed_predictor is None:
            print(f"[WARN] Serie {predictor_name} para {appid} no pudo ser estacionada. Se omite par.")
            continue
            
        # Transformación del target
        transformed_target = _transform_to_stationary_new(df_transformed[target_name], methods, cfg)
        if transformed_target is None:
            print(f"[WARN] Serie {target_name} para {appid} no pudo ser estacionada. Se omite par.")
            continue
            
        # Actualizar el dataframe con las series transformadas
        df_analysis = pd.DataFrame({
            'year_month': df_transformed['year_month'],
            predictor_name: transformed_predictor,
            target_name: transformed_target
        }).dropna(subset=[predictor_name, target_name])
        
        if len(df_analysis) < 8:
            print(f"[WARN] Después de transformar, menos de 8 puntos de datos. Se omite par.")
            continue
            
        analysis_results = analyze_pair(df_analysis, predictor_name, target_name, cfg)
        if analysis_results:
            analysis_results.update({
                'appid': str(appid), 'pair_name': pair['name']
            })
            game_results.append(analysis_results)
    return game_results

# El decorador @ray.remote debe estar en el nivel superior del módulo.
if RAY_AVAILABLE:
    _process_single_game_ray = ray.remote(_process_single_game)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config, 'r'))
    
    parallel_mode = cfg.get('parallel_mode', 'multiprocessing')
    
    # 1. Inicializar la plataforma de paralelismo
    if parallel_mode == 'ray' and RAY_AVAILABLE:
        ray_cfg = cfg.get('ray_cluster', {})
        ray.init(address=ray_cfg.get('address', 'auto'), ignore_reinit_error=True)
        print("[INFO] Usando Ray para paralelización distribuida.")
    elif parallel_mode == 'multiprocessing' and MULTIPROCESSING_AVAILABLE:
        num_processes = cfg.get('num_processes', cpu_count())
        print(f"[INFO] Usando multiprocessing con {num_processes} procesos.")
    else:
        print("[WARN] No se ha especificado un modo de paralelización válido o las librerías no están instaladas. Ejecutando en modo secuencial.")
        parallel_mode = 'sequential'

    # 2. Iniciar MLflow y cargar datos
    mlflow_cfg = cfg.get('mlflow', {})
    if mlflow_cfg.get('enabled', False):
        mlflow.set_experiment(mlflow_cfg.get('experiment', 'Steam Analytics'))

    with mlflow.start_run(run_name=f"{mlflow_cfg.get('run_name_prefix', '')}ccf_analysis"):
        mlflow.log_dict(cfg, "config.yaml")
        clusters = read_parquet_any(cfg['input_path']['clusters_parquet'])
        # Filtro opcional por clúster desde config
        cluster_filter = cfg.get('cluster_filter')
        if cluster_filter is not None and len(cluster_filter) > 0 and 'cluster_id' in clusters.columns:
            clusters = clusters[clusters['cluster_id'].isin(cluster_filter)]
        appids_to_process = clusters['appid'].astype(str).unique()
        
        # 3. Lanzar las tareas y recolectar resultados
        all_results = []
        if parallel_mode == 'ray':
            futures = [_process_single_game_ray.remote(appid, cfg) for appid in appids_to_process]
            list_of_results_lists = ray.get(futures)
            all_results = [item for sublist in list_of_results_lists for item in sublist]
            ray.shutdown()
        elif parallel_mode == 'multiprocessing':
            with Pool(processes=num_processes) as pool:
                list_of_results_lists = pool.starmap(_process_single_game, [(appid, cfg) for appid in appids_to_process])
                all_results = [item for sublist in list_of_results_lists for item in sublist]
        else: # Modo secuencial
            for appid in appids_to_process:
                all_results.extend(_process_single_game(appid, cfg))

        # 4. Consolidar, guardar y loguear los resultados finales
        if all_results:
            df_summary = pd.DataFrame(all_results)
            out_dir = Path(cfg['output_dir']); makedirs_if_local(out_dir)
            out_path_pq = out_dir / 'summary.parquet'
            out_path_csv = out_dir / 'summary.csv'
            # --- Corrección FDR (Benjamini–Hochberg) sobre p-values de Granger ---
            try:
                alpha = float(cfg.get('granger', {}).get('alpha', 0.05))
                # XY
                mask_xy = df_summary['granger_xy_pmin'].notna()
                if mask_xy.any():
                    rej_xy, p_xy_corr, _, _ = multipletests(df_summary.loc[mask_xy, 'granger_xy_pmin'].values, alpha=alpha, method='fdr_bh')
                    df_summary.loc[mask_xy, 'granger_xy_p_fdr'] = p_xy_corr
                    df_summary.loc[mask_xy, 'granger_xy_sig_fdr'] = rej_xy
                else:
                    df_summary['granger_xy_p_fdr'] = np.nan
                    df_summary['granger_xy_sig_fdr'] = False
                # YX
                mask_yx = df_summary['granger_yx_pmin'].notna()
                if mask_yx.any():
                    rej_yx, p_yx_corr, _, _ = multipletests(df_summary.loc[mask_yx, 'granger_yx_pmin'].values, alpha=alpha, method='fdr_bh')
                    df_summary.loc[mask_yx, 'granger_yx_p_fdr'] = p_yx_corr
                    df_summary.loc[mask_yx, 'granger_yx_sig_fdr'] = rej_yx
                else:
                    df_summary['granger_yx_p_fdr'] = np.nan
                    df_summary['granger_yx_sig_fdr'] = False
            except Exception as e:
                print(f"[WARN] No se pudo aplicar FDR: {e}")

            write_parquet_any(df_summary, out_path_pq)
            write_csv_any(df_summary, out_path_csv, index=False)
            
            games_analyzed = df_summary['appid'].nunique()
            stationary_series_count = len(df_summary)
            
            mlflow.log_metric("games_analyzed", games_analyzed)
            mlflow.log_metric("stationary_series_count", stationary_series_count)
            mlflow.log_metric("significant_granger_xy_pct", df_summary['granger_xy_sig'].mean() * 100)
            mlflow.log_metric("significant_granger_yx_pct", df_summary['granger_yx_sig'].mean() * 100)
            # Métricas tras FDR
            if 'granger_xy_sig_fdr' in df_summary.columns:
                mlflow.log_metric("significant_granger_xy_pct_fdr", float(df_summary['granger_xy_sig_fdr'].mean() * 100))
            if 'granger_yx_sig_fdr' in df_summary.columns:
                mlflow.log_metric("significant_granger_yx_pct_fdr", float(df_summary['granger_yx_sig_fdr'].mean() * 100))
            
            mlflow.log_artifact(str(out_path_pq))
            mlflow.log_artifact(str(out_path_csv))
            print(f"[OK] CCF summary guardado en -> {out_path_pq}")
        else:
            print("[WARN] No se generó summary (faltan datos o series no estacionarias).")
            mlflow.log_metric("games_analyzed", 0)
            mlflow.log_metric("stationary_series_count", 0)
    
if __name__ == "__main__":
    main()
