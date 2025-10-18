#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Realiza un análisis de Correlación Cruzada (CCF) y Causalidad de Granger
entre series temporales de juegos de forma paralela usando Ray o multiprocessing.
"""
import argparse
from datetime import datetime
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import os
import sys

# Ensure project root is importable when running as a script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import mlflow
from pymongo import MongoClient
from typing import Dict, Any, List, Tuple

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
from src.utils.config_utils import expand_env_in_obj
from statsmodels.tsa.stattools import adfuller, grangercausalitytests, kpss
from statsmodels.stats.multitest import multipletests
from statsmodels.stats.diagnostic import acorr_ljungbox

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
    for m in transform_methods:
        try:
            cand = _apply_transform(series, m, cfg)
            if cand is None or cand.empty or np.std(cand) == 0:
                continue
            ok, _, _ = check_stationarity_dual(cand, cfg)
            if ok:
                print(f"[INFO] Transformación '{m}' aplicada a {series.name} -> estacionaria.")
                return cand
        except Exception:
            continue
    return None

def _evaluate_stationarity_methods(series: pd.Series, transform_methods: List[str], cfg: Dict[str, Any]) -> Tuple[pd.Series | None, str | None, List[Dict[str, Any]]]:
    """
    Try all provided transformations and compute ADF/KPSS for each.

    Returns:
    - chosen_series: first transformed series that passes stationarity (ADF+optional KPSS), or None
    - chosen_method: method name chosen, or None
    - tests: list of dicts with per-method results: method, ok, p_adf, p_kpss, n, std
    """
    out_tests: List[Dict[str, Any]] = []
    s = pd.Series(series).copy()
    s.name = s.name if s.name else 'series'
    chosen_series: pd.Series | None = None
    chosen_method: str | None = None
    for m in transform_methods:
        rec: Dict[str, Any] = {"method": m}
        try:
            cand = _apply_transform(s, m, cfg)
            if cand is None or len(pd.Series(cand).dropna()) < 3 or float(np.std(cand)) == 0.0:
                rec.update({"ok": False, "p_adf": np.nan, "p_kpss": np.nan, "n": int(len(cand) if cand is not None else 0), "std": float(np.std(cand)) if cand is not None else np.nan})
            else:
                ok, p_adf, p_kpss = check_stationarity_dual(pd.Series(cand).dropna(), cfg)
                rec.update({"ok": bool(ok), "p_adf": float(p_adf), "p_kpss": (float(p_kpss) if p_kpss is not None else np.nan), "n": int(len(cand)), "std": float(np.std(cand))})
                if ok and chosen_series is None:
                    chosen_series = cand
                    chosen_method = m
        except Exception:
            rec.update({"ok": False, "p_adf": np.nan, "p_kpss": np.nan, "n": 0, "std": np.nan})
        out_tests.append(rec)
    return chosen_series, chosen_method, out_tests

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
            # Fallback if filter returned empty due to dtype mismatch (str vs int)
            if df is None or df.empty:
                raise ValueError('empty_after_arrow_filter')
        except Exception:
            df = read_parquet_any(preagg_path)
            if 'appid' in df.columns:
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
            if df is None or df.empty:
                raise ValueError('empty_after_arrow_filter')
        except Exception:
            df = read_parquet_any(preagg_path)
            if 'appid' in df.columns:
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
    """Calcula la CCF por desfase con z-score por ventana para estabilidad numérica."""
    out: dict[int, float] = {}
    x = pd.Series(x).astype(float).dropna().reset_index(drop=True)
    y = pd.Series(y).astype(float).dropna().reset_index(drop=True)
    n = min(len(x), len(y))
    if n < 5:
        return {lag: np.nan for lag in range(-max_lag, max_lag + 1)}
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            xs = x.iloc[-lag:]
            ys = y.iloc[:len(xs)]
        elif lag > 0:
            ys = y.iloc[lag:]
            xs = x.iloc[:len(ys)]
        else:
            xs, ys = x, y
        if len(xs) < 5 or len(ys) < 5:
            out[lag] = np.nan
            continue
        xs = xs.astype(float)
        ys = ys.astype(float)
        xs_std = xs.std(ddof=0)
        ys_std = ys.std(ddof=0)
        if xs_std == 0 or ys_std == 0:
            out[lag] = np.nan
            continue
        xs = (xs - xs.mean()) / xs_std
        ys = (ys - ys.mean()) / ys_std
        out[lag] = float(np.corrcoef(xs, ys)[0, 1])
    return out

def check_stationarity(series: pd.Series, alpha: float = 0.05) -> tuple[bool, float]:
    """Realiza el test de Dickey-Fuller Aumentado (ADF)."""
    series = series.dropna()
    if len(series) < 10 or np.std(series) == 0:
        return False, 1.0
    result = adfuller(series)
    p_value = float(result[1])
    return p_value < alpha, p_value


def check_stationarity_dual(series: pd.Series, cfg: Dict[str, Any]) -> tuple[bool, float, float | None]:
    """Decisión conjunta ADF + KPSS (opcional).

    - ADF rechaza H0 (p_adf < adf_alpha) → estacionaria
    - KPSS no rechaza H0 (p_kpss >= kpss_alpha) → estacionaria
    - Si KPSS.disabled → usa solo ADF.
    """
    series = series.dropna()
    if len(series) < 10 or np.std(series) == 0:
        return False, 1.0, None
    st = cfg.get('stationarity', {}) or {}
    adf_alpha = float(st.get('adf_alpha', 0.05))
    ok_adf, p_adf = check_stationarity(series, adf_alpha)
    kcfg = st.get('kpss', {}) or {}
    if not kcfg.get('enabled', False):
        return ok_adf, p_adf, None
    try:
        reg = str(kcfg.get('regression', 'c'))
        kpss_alpha = float(kcfg.get('alpha', 0.05))
        stat, p_kpss, _, _ = kpss(series, regression=reg, nlags='auto')
        ok_kpss = p_kpss >= kpss_alpha
        return (ok_adf and ok_kpss), p_adf, float(p_kpss)
    except Exception:
        return ok_adf, p_adf, None

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
        df_xy = pd.DataFrame({target: y_final, predictor: x_final}).dropna()
        if len(df_xy) >= (granger_cfg.get('maxlag', 3) + 2):
            res_xy = grangercausalitytests(df_xy[[target, predictor]], maxlag=granger_cfg['maxlag'], verbose=False)
            gxy_pmin = min([res[0]['ssr_chi2test'][1] for lag, res in res_xy.items()])
    except Exception:
        pass
    try:
        df_yx = pd.DataFrame({predictor: x_final, target: y_final}).dropna()
        if len(df_yx) >= (granger_cfg.get('maxlag', 3) + 2):
            res_yx = grangercausalitytests(df_yx[[predictor, target]], maxlag=granger_cfg['maxlag'], verbose=False)
            gyx_pmin = min([res[0]['ssr_chi2test'][1] for lag, res in res_yx.items()])
    except Exception:
        pass
    
    # P-valores de estacionariedad (ADF/KPSS) sobre las series utilizadas
    try:
        sx_ok, sx_adf_p, sx_kpss_p = check_stationarity_dual(pd.Series(x_final).astype(float), cfg)
    except Exception:
        sx_adf_p, sx_kpss_p = np.nan, np.nan
    try:
        sy_ok, sy_adf_p, sy_kpss_p = check_stationarity_dual(pd.Series(y_final).astype(float), cfg)
    except Exception:
        sy_adf_p, sy_kpss_p = np.nan, np.nan

    out = {
        'best_lag': best_lag, 'best_ccf': best_ccf,
        'granger_xy_pmin': gxy_pmin, 'granger_xy_sig': gxy_pmin is not None and gxy_pmin < granger_cfg['alpha'],
        'granger_yx_pmin': gyx_pmin, 'granger_yx_sig': gyx_pmin is not None and gyx_pmin < granger_cfg['alpha'],
        'adf_p_x': sx_adf_p, 'kpss_p_x': sx_kpss_p,
        'adf_p_y': sy_adf_p, 'kpss_p_y': sy_kpss_p,
    }
    # Whiteness (Ljung–Box) sobre residuales del preblanqueo
    try:
        lb_cfg = ((cfg.get('whiteness') or {}).get('ljung_box') or {})
        if lb_cfg.get('enabled', False):
            h = int(lb_cfg.get('h', 12))
            alpha_lb = float(lb_cfg.get('alpha', 0.05))
            ser = pd.Series(x_whitened).dropna()
            if len(ser) > h + 2:
                lb = acorr_ljungbox(ser, lags=[h], return_df=True)
                p_lb = float(lb['lb_pvalue'].iloc[-1]) if 'lb_pvalue' in lb.columns else float(lb.iloc[-1]['lb_pvalue'])
                out['ljung_p'] = p_lb
                out['ljung_ok'] = (p_lb >= alpha_lb)
    except Exception:
        pass
    return out

def _process_single_game(appid: str, cfg: Dict[str, Any]) -> Dict[str, List[Dict]]:
    """
    Analiza un juego y retorna:
    - summary: lista de filas (una por par) con best_lag, best_ccf, p-values, flags FDR (se llena en main)
    - consistency: filas por mes con máscara de consistencia según el best_lag del par
    """
    results_summary: List[Dict] = []
    results_consistency: List[Dict] = []
    results_stationarity: List[Dict] = []
    pre = cfg.get('preaggregated', {})
    df_raw = _read_reviews(cfg['mongo_connection'], appid, pre.get('reviews_monthly'))
    players_df = _read_players(cfg['players_data'], appid, pre.get('players_monthly'))
    if players_df is not None and not players_df.empty:
        if df_raw.empty:
            df_raw = players_df.fillna(0)
        else:
            df_raw = pd.merge(df_raw, players_df, on='year_month', how='outer').sort_values('year_month').fillna(0)
    
    if df_raw is None or df_raw.empty:
        return {"summary": [], "consistency": [], "stationarity_tests": []}
        
    df_transformed = df_raw.copy()
    # Re-muestreo trimestral para fallback cuando mensual es corto/degenerado
    def _to_quarterly(df_monthly: pd.DataFrame) -> pd.DataFrame:
        dfq = df_monthly.copy()
        if 'year_month' in dfq.columns:
            dfq['year_month'] = pd.to_datetime(dfq['year_month'])
            dfq = dfq.set_index('year_month')
        cols = [c for c in ['players','pos','neg','total_reviews'] if c in dfq.columns]
        if not cols:
            return pd.DataFrame()
        agg = dfq[cols].resample('QS').sum(min_count=1)
        agg = agg.reset_index()
        agg = agg.rename(columns={'index': 'year_month'})
        return agg
    df_quarterly = _to_quarterly(df_transformed)
    
    for pair in cfg.get('ccf_pairs', []):
        predictor_name, target_name = pair['predictor'], pair['target']
        # Enforce only players vs reviews (no reviews vs reviews)
        review_vars = {"pos", "neg", "total_reviews"}
        if not (predictor_name == "players" and target_name in review_vars):
            continue
        
        if predictor_name not in df_transformed.columns or target_name not in df_transformed.columns:
            continue

        # Nuevo flujo con fallback trimestral + consistencia
        methods = cfg.get('stationarity', {}).get('transforms', ['dlog', 'diff', 'diff2', 'sqrt'])
        # Intento mensual
        transformed_predictor = _transform_to_stationary_new(df_transformed[predictor_name], methods, cfg)
        transformed_target = _transform_to_stationary_new(df_transformed[target_name], methods, cfg)
        freq_used = 'M'
        df_analysis = None
        if transformed_predictor is not None and transformed_target is not None:
            df_analysis = pd.DataFrame({
                'year_month': pd.to_datetime(df_transformed['year_month']),
                predictor_name: transformed_predictor,
                target_name: transformed_target
            }).dropna(subset=[predictor_name, target_name])

        maxlag = int((cfg.get('granger') or {}).get('maxlag', 3))
        need_fallback = (df_analysis is None) or (len(df_analysis) < max(8, 8 * maxlag))
        if need_fallback and df_quarterly is not None and not df_quarterly.empty:
            tp_q = _transform_to_stationary_new(df_quarterly.get(predictor_name, pd.Series(dtype=float)), methods, cfg)
            tt_q = _transform_to_stationary_new(df_quarterly.get(target_name, pd.Series(dtype=float)), methods, cfg)
            if tp_q is not None and tt_q is not None:
                df_analysis = pd.DataFrame({
                    'year_month': pd.to_datetime(df_quarterly['year_month']),
                    predictor_name: tp_q,
                    target_name: tt_q
                }).dropna(subset=[predictor_name, target_name])
                freq_used = 'Q'

        if df_analysis is None or len(df_analysis) < 8:
            print(f"[WARN] {appid} par {predictor_name}->{target_name}: insuficiente tras fallback")
            continue

        analysis_results = analyze_pair(df_analysis, predictor_name, target_name, cfg)
        if analysis_results:
            analysis_results.update({
                'appid': str(appid), 'pair_name': pair['name'], 'freq': freq_used, 'n_eff': int(len(df_analysis))
            })
            results_summary.append(analysis_results)

            # Stationarity tests per transform method for the used frequency
            try:
                src_df = df_quarterly if freq_used == 'Q' else df_transformed
                tp, tp_method, tp_tests = _evaluate_stationarity_methods(src_df[predictor_name], methods, cfg)
                tt, tt_method, tt_tests = _evaluate_stationarity_methods(src_df[target_name], methods, cfg)
                for rec in tp_tests:
                    rec.update({
                        'appid': str(appid), 'pair_name': pair['name'], 'series': predictor_name, 'role': 'predictor', 'freq': freq_used,
                        'selected': bool(rec.get('method') == tp_method)
                    })
                for rec in tt_tests:
                    rec.update({
                        'appid': str(appid), 'pair_name': pair['name'], 'series': target_name, 'role': 'target', 'freq': freq_used,
                        'selected': bool(rec.get('method') == tt_method)
                    })
                results_stationarity.extend(tp_tests)
                results_stationarity.extend(tt_tests)
            except Exception:
                pass

            # --- Cálculo de consistencia mensual con el best_lag ---
            try:
                best_lag = int(analysis_results['best_lag'])
                s_x = df_analysis.set_index('year_month')[predictor_name].astype(float)
                s_y = df_analysis.set_index('year_month')[target_name].astype(float)
                s_x_aligned = s_x.shift(best_lag)
                aligned = (
                    pd.DataFrame({'x_aligned': s_x_aligned, 'y': s_y})
                    .dropna()
                    .sort_index()
                )
                if not aligned.empty:
                    win = int(((cfg.get('consistency') or {}).get('window') or 3))
                    min_abs_corr = float(((cfg.get('consistency') or {}).get('min_abs_corr') or 0.2))
                    sign_consistent = np.sign(aligned['x_aligned']) == np.sign(aligned['y'])
                    local_corr = aligned['x_aligned'].rolling(win).corr(aligned['y'])
                    lead_or_lag = (
                        'predictor_leads' if best_lag > 0 else
                        'predictor_lags' if best_lag < 0 else
                        'simultaneous'
                    )
                    for ts, row in aligned.iterrows():
                        lc = float(local_corr.get(ts)) if pd.notna(local_corr.get(ts)) else np.nan
                        sc = bool(sign_consistent.get(ts)) if ts in sign_consistent.index else False
                        ccf_consistent = sc and (not np.isnan(lc)) and (abs(lc) >= min_abs_corr)
                        results_consistency.append({
                            'appid': str(appid),
                            'pair_name': pair['name'],
                            'year_month': pd.to_datetime(ts),
                            'best_lag': best_lag,
                            'lead_or_lag': lead_or_lag,
                            'local_corr_3m': lc,
                            'sign_consistent': sc,
                            'ccf_consistent': bool(ccf_consistent)
                        })
            except Exception:
                pass

        # Saltar el bloque legacy (ya procesado)
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
            results_summary.append(analysis_results)

            # --- Cálculo de consistencia mensual con el best_lag ---
            try:
                best_lag = int(analysis_results['best_lag'])
                # series transformadas alineadas por fecha
                s_x = df_analysis.set_index('year_month')[predictor_name].astype(float)
                s_y = df_analysis.set_index('year_month')[target_name].astype(float)
                # Alinear predictor con el desfase encontrado: x_aligned[t] = x[t - lag]
                s_x_aligned = s_x.shift(best_lag)
                # Emparejar y limpiar nulos
                aligned = (
                    pd.DataFrame({
                        'x_aligned': s_x_aligned,
                        'y': s_y
                    })
                    .dropna()
                    .sort_index()
                )
                if not aligned.empty:
                    # Consistencia de signo y correlación local en ventana corta
                    sign_consistent = np.sign(aligned['x_aligned']) == np.sign(aligned['y'])
                    # correlación rodante (3 meses por defecto)
                    win = int(((cfg.get('consistency') or {}).get('window') or 3))
                    min_abs_corr = float(((cfg.get('consistency') or {}).get('min_abs_corr') or 0.2))
                    local_corr = aligned['x_aligned'].rolling(win).corr(aligned['y'])
                    lead_or_lag = (
                        'predictor_leads' if best_lag > 0 else
                        'predictor_lags' if best_lag < 0 else
                        'simultaneous'
                    )
                    for ts, row in aligned.iterrows():
                        lc = float(local_corr.get(ts)) if pd.notna(local_corr.get(ts)) else np.nan
                        sc = bool(sign_consistent.get(ts)) if ts in sign_consistent.index else False
                        ccf_consistent = sc and (not np.isnan(lc)) and (abs(lc) >= min_abs_corr)
                        results_consistency.append({
                            'appid': str(appid),
                            'pair_name': pair['name'],
                            'year_month': pd.to_datetime(ts),
                            'best_lag': best_lag,
                            'lead_or_lag': lead_or_lag,
                            'local_corr_3m': lc,
                            'sign_consistent': sc,
                            'ccf_consistent': bool(ccf_consistent)
                        })
            except Exception:
                pass

    return {"summary": results_summary, "consistency": results_consistency, "stationarity_tests": results_stationarity}

# El decorador @ray.remote debe estar en el nivel superior del módulo.
    if RAY_AVAILABLE:
        _process_single_game_ray = ray.remote(_process_single_game)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = expand_env_in_obj(yaml.safe_load(open(args.config, 'r')))
    
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

    script_name = Path(__file__).stem
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    base_run_name = f"{mlflow_cfg.get('run_name_prefix', '')}{script_name}_{ts}"
    with mlflow.start_run(run_name=base_run_name):
        mlflow.log_dict(cfg, "config.yaml")
        clusters = read_parquet_any(cfg['input_path']['clusters_parquet'])
        # Filtro opcional por clúster desde config
        cluster_filter = cfg.get('cluster_filter')
        if cluster_filter is not None and len(cluster_filter) > 0 and 'cluster_id' in clusters.columns:
            clusters = clusters[clusters['cluster_id'].isin(cluster_filter)]
        appids_to_process = clusters['appid'].astype(str).unique()
        try:
            mlflow.set_tag("n_appids", int(len(appids_to_process)))
            mlflow.set_tag("script", script_name)
            mlflow.set_tag("timestamp", ts)
        except Exception:
            pass
        
        # 3. Lanzar las tareas y recolectar resultados
        all_results_summary: List[Dict] = []
        all_results_consistency: List[Dict] = []
        all_results_stationarity: List[Dict] = []
        if parallel_mode == 'ray':
            futures = [_process_single_game_ray.remote(appid, cfg) for appid in appids_to_process]
            results = ray.get(futures)
            for r in results:
                all_results_summary.extend(r.get('summary', []))
                all_results_consistency.extend(r.get('consistency', []))
                all_results_stationarity.extend(r.get('stationarity_tests', []))
            ray.shutdown()
        elif parallel_mode == 'multiprocessing':
            with Pool(processes=num_processes) as pool:
                results = pool.starmap(_process_single_game, [(appid, cfg) for appid in appids_to_process])
                for r in results:
                    all_results_summary.extend(r.get('summary', []))
                    all_results_consistency.extend(r.get('consistency', []))
                    all_results_stationarity.extend(r.get('stationarity_tests', []))
        else: # Modo secuencial
            for appid in appids_to_process:
                r = _process_single_game(appid, cfg)
                all_results_summary.extend(r.get('summary', []))
                all_results_consistency.extend(r.get('consistency', []))
                all_results_stationarity.extend(r.get('stationarity_tests', []))

        # 4. Consolidar, guardar y loguear los resultados finales
        if all_results_summary:
            df_summary = pd.DataFrame(all_results_summary)
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
            try:
                if 'freq' in df_summary.columns:
                    mlflow.log_metric("used_quarterly_pct", float((df_summary['freq'] == 'Q').mean() * 100))
                if 'n_eff' in df_summary.columns:
                    mlflow.log_metric("median_effective_points", float(df_summary['n_eff'].median()))
            except Exception:
                pass
            mlflow.log_metric("significant_granger_xy_pct", df_summary['granger_xy_sig'].mean() * 100)
            mlflow.log_metric("significant_granger_yx_pct", df_summary['granger_yx_sig'].mean() * 100)
            # Métricas tras FDR
            if 'granger_xy_sig_fdr' in df_summary.columns:
                mlflow.log_metric("significant_granger_xy_pct_fdr", float(df_summary['granger_xy_sig_fdr'].mean() * 100))
            if 'granger_yx_sig_fdr' in df_summary.columns:
                mlflow.log_metric("significant_granger_yx_pct_fdr", float(df_summary['granger_yx_sig_fdr'].mean() * 100))

            # Métrica de blancura de residuales (si disponible)
            if 'ljung_ok' in df_summary.columns:
                try:
                    mask = df_summary['ljung_ok'].notna()
                    if mask.any():
                        mlflow.log_metric("whiteness_ok_pct", float(df_summary.loc[mask, 'ljung_ok'].mean() * 100))
                except Exception:
                    pass
            
            mlflow.log_artifact(str(out_path_pq))
            mlflow.log_artifact(str(out_path_csv))
            print(f"[OK] CCF summary guardado en -> {out_path_pq}")
            # Nested runs por appid con métricas y artefactos por juego
            try:
                for app in sorted(df_summary['appid'].astype(str).unique()):
                    sub = df_summary[df_summary['appid'].astype(str) == str(app)].copy()
                    if sub.empty:
                        continue
                    per_app_csv = out_dir / f"summary_{app}.csv"
                    write_csv_any(sub, per_app_csv, index=False)
                    with mlflow.start_run(run_name=f"{base_run_name}__{app}", nested=True):
                        mlflow.set_tag("appid", str(app))
                        try:
                            mlflow.log_metric("pairs", int(len(sub)))
                            mlflow.log_metric("significant_granger_xy_pct", float(sub['granger_xy_sig'].mean() * 100))
                            if 'granger_xy_sig_fdr' in sub.columns:
                                mlflow.log_metric("significant_granger_xy_pct_fdr", float(sub['granger_xy_sig_fdr'].mean() * 100))
                            if 'freq' in sub.columns:
                                mlflow.log_metric("used_quarterly", float((sub['freq'] == 'Q').any()))
                        except Exception:
                            pass
                        try:
                            mlflow.log_artifact(str(per_app_csv))
                        except Exception:
                            pass
            except Exception:
                pass
        else:
            print("[WARN] No se generó summary (faltan datos o series no estacionarias).")
            mlflow.log_metric("games_analyzed", 0)
            mlflow.log_metric("stationary_series_count", 0)

        # 5. Guardar consistencia mensual si existe
        if all_results_consistency:
            df_cons = pd.DataFrame(all_results_consistency)
            out_dir = Path(cfg['output_dir']); makedirs_if_local(out_dir)
            cons_path = out_dir / 'consistency.parquet'
            write_parquet_any(df_cons, cons_path)
            try:
                mlflow.log_artifact(str(cons_path))
            except Exception:
                pass
            print(f"[OK] CCF consistency guardado en -> {cons_path}")

        # 6. Guardar y loguear pruebas de estacionariedad (ADF/KPSS) por metodo
        try:
            if all_results_stationarity:
                df_tests = pd.DataFrame(all_results_stationarity)
                out_dir = Path(cfg['output_dir']); makedirs_if_local(out_dir)
                tests_path = out_dir / 'stationarity_tests.csv'
                write_csv_any(df_tests, tests_path, index=False)
                try:
                    mlflow.log_artifact(str(tests_path))
                except Exception:
                    pass
                # metricas agregadas: distribucion de metodo seleccionado
                if 'selected' in df_tests.columns and 'method' in df_tests.columns:
                    sel = df_tests[df_tests['selected'] == True]
                    if not sel.empty:
                        total_sel = float(len(sel))
                        for m, cnt in sel['method'].value_counts().items():
                            mlflow.log_metric(f"stationarity_selected_count_{m}", int(cnt))
                            mlflow.log_metric(f"stationarity_selected_pct_{m}", float(100.0 * cnt / total_sel))
                print(f"[OK] Stationarity tests guardado en -> {tests_path}")
        except Exception as e:
            print(f"[WARN] No se pudo guardar/loguear stationarity_tests: {e}")
    
if __name__ == "__main__":
    main()
