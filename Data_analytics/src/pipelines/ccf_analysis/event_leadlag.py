#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Event-focused lead–lag analysis around peaks/drops for small-n cases.

Computes short-window correlations and decay metrics when Granger/CCF are
weak due to short or degenerate series. Designed to complement the existing
CCF pipeline, not replace it.

Inputs (prefer preaggregated monthly parquet):
 - players: data/warehouse/players_monthly.parquet (columns: appid, year_month, players)
 - reviews: data/warehouse/reviews_monthly.parquet (columns: appid, year_month, pos, neg[, total_reviews])

Config (configs/ccf_analysis.yaml):
 event_mode:
   enabled: true
   z_threshold: 2.0
   window_months: 2
   max_lag_event: 2
   permutations: 500
   alpha_event: 0.10
   min_drop_1m: -0.15
   use_negative_pct: true

Output:
 - outputs/ccf_analysis/event_correlation.parquet
   One row per (appid, t0) peak above threshold with metrics:
   [lag_star, rho_star, p_perm, drop_1m, half_life_months, pre/post means, neg_pct_delta, jaccard_overlap, pattern_flag]
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple
import numpy as np
import pandas as pd
import yaml
import os
import sys

# Ensure project root in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.utils.io import read_parquet_any, write_parquet_any
import mlflow


def _zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors='coerce')
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series(index=s.index, dtype=float)
    return (s - mu) / sd


def _local_maxima(z: pd.Series, thr: float) -> list:
    # Select months with z > thr and local maxima (strict vs neighbors when available)
    peaks = []
    for i, (t, val) in enumerate(z.items()):
        if not pd.notna(val) or val <= thr:
            continue
        prev_ok = (i == 0) or (val > list(z.values)[i - 1])
        next_ok = (i == len(z) - 1) or (val > list(z.values)[i + 1])
        if prev_ok and next_ok:
            peaks.append(t)
    # Fallback: if none strict, accept all z>thr
    if not peaks:
        peaks = [t for t, v in z.items() if pd.notna(v) and v > thr]
    return peaks


def _align_window(df: pd.DataFrame, center: pd.Timestamp, months: int) -> pd.DataFrame:
    start = (center.to_period('M') - months).to_timestamp()
    end = (center.to_period('M') + months).to_timestamp()
    return df[(df['year_month'] >= start) & (df['year_month'] <= end)].copy()


def _ccf_short(x: pd.Series, y: pd.Series, max_lag: int) -> Tuple[int, float]:
    """Return (lag_star, rho_star) maximizing |rho| on [-max_lag, +max_lag]."""
    x = pd.Series(x).astype(float)
    y = pd.Series(y).astype(float)
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
        if len(xs) < 3 or len(ys) < 3:
            out[lag] = np.nan
            continue
        xs_std = xs.std(ddof=0)
        ys_std = ys.std(ddof=0)
        if xs_std == 0 or ys_std == 0:
            out[lag] = np.nan
            continue
        xs = (xs - xs.mean()) / xs_std
        ys = (ys - ys.mean()) / ys_std
        out[lag] = float(np.corrcoef(xs, ys)[0, 1])
    # pick lag with max |rho|
    if not out:
        return 0, np.nan
    lag_star = max(out, key=lambda k: abs(out[k]) if pd.notna(out[k]) else -1)
    return lag_star, out.get(lag_star, np.nan)


def _perm_pvalue_at_lag(x: pd.Series, y: pd.Series, lag: int, n_perm: int = 500, seed: int = 13) -> float:
    rng = np.random.default_rng(seed)
    # build aligned pair for the given lag
    if lag < 0:
        xs = x.iloc[-lag:]
        ys = y.iloc[:len(xs)]
    elif lag > 0:
        ys = y.iloc[lag:]
        xs = x.iloc[:len(ys)]
    else:
        xs, ys = x, y
    xs = xs.astype(float)
    ys = ys.astype(float)
    # standardize
    def stdz(a):
        sd = a.std(ddof=0)
        return (a - a.mean()) / sd if sd and not np.isnan(sd) else a * np.nan
    xs = stdz(xs)
    ys = stdz(ys)
    if xs.isna().any() or ys.isna().any() or len(xs) < 3:
        return np.nan
    rho_obs = float(np.corrcoef(xs, ys)[0, 1])
    cnt = 0
    for _ in range(int(n_perm)):
        ys_perm = pd.Series(rng.permutation(ys.values))
        rho_perm = float(np.corrcoef(xs.values, ys_perm.values)[0, 1])
        if abs(rho_perm) >= abs(rho_obs):
            cnt += 1
    return (cnt + 1) / (n_perm + 1)


def _half_life(players: pd.Series, t0: pd.Timestamp) -> Optional[int]:
    try:
        p0 = float(players.loc[t0])
    except Exception:
        return None
    if not np.isfinite(p0) or p0 <= 0:
        return None
    thr = p0 / np.e
    # forward months
    idx = players.index
    if t0 not in idx:
        return None
    start_pos = idx.get_loc(t0)
    for h in range(1, len(idx) - start_pos):
        t = idx[start_pos + h]
        if float(players.loc[t]) <= thr:
            return h
    return None


def _jaccard_overlap(z_players: pd.Series, z_neg: pd.Series, thr: float, window_idx: pd.Index) -> float:
    a = set([t for t in window_idx if (t in z_players.index) and pd.notna(z_players.loc[t]) and (z_players.loc[t] > thr)])
    b = set([t for t in window_idx if (t in z_neg.index) and pd.notna(z_neg.loc[t]) and (z_neg.loc[t] > thr)])
    u = a.union(b)
    if not u:
        return float('nan')
    return len(a.intersection(b)) / len(u)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='configs/ccf_analysis.yaml')
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r', encoding='utf-8'))
    cfg = cfg or {}
    em = (cfg.get('event_mode') or {})
    z_thr = float(em.get('z_threshold', 2.0))
    win = int(em.get('window_months', 2))
    max_lag = int(em.get('max_lag_event', 2))
    n_perm = int(em.get('permutations', 500))
    min_drop_1m = float(em.get('min_drop_1m', -0.15))
    use_neg_pct = bool(em.get('use_negative_pct', True))

    preagg = cfg.get('preaggregated') or {}
    players_path = preagg.get('players_monthly', 'data/warehouse/players_monthly.parquet')
    reviews_path = preagg.get('reviews_monthly', 'data/warehouse/reviews_monthly.parquet')

    if not Path(players_path).exists() or not Path(reviews_path).exists():
        raise SystemExit('Preaggregated monthly parquet not found. Run preagg stages first.')

    df_players = read_parquet_any(players_path)
    df_reviews = read_parquet_any(reviews_path)
    if df_players.empty or df_reviews.empty:
        raise SystemExit('Empty preaggregated inputs.')

    # Normalize dtypes and dates
    df_players['appid'] = df_players['appid'].astype(str)
    df_reviews['appid'] = df_reviews['appid'].astype(str)
    for df, col in [(df_players, 'year_month'), (df_reviews, 'year_month')]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    mlflow_cfg = (cfg.get('mlflow') or {})
    use_mlflow = bool(mlflow_cfg.get('enabled', False))
    if use_mlflow:
        try:
            mlflow.set_experiment(mlflow_cfg.get('experiment', 'Steam Analytics'))
        except Exception:
            use_mlflow = False

    run_ctx = mlflow.start_run(run_name=f"{mlflow_cfg.get('run_name_prefix','')}event_leadlag") if use_mlflow else None
    try:
        if use_mlflow:
            mlflow.log_dict(cfg, 'config.yaml')
            mlflow.log_params({
                'z_threshold': z_thr,
                'window_months': win,
                'max_lag_event': max_lag,
                'permutations': n_perm,
                'min_drop_1m': min_drop_1m,
                'use_negative_pct': use_neg_pct,
            })

        appids = sorted(set(df_players['appid']).intersection(set(df_reviews['appid'])))
        rows = []

        for app in appids:
            pl = df_players[df_players['appid'] == app][['year_month', 'players']].dropna()
            rv = df_reviews[df_reviews['appid'] == app][['year_month', 'pos', 'neg']].fillna(0)
            if pl.empty or rv.empty:
                continue
            # Build full monthly index
            idx = pd.date_range(start=min(pl['year_month'].min(), rv['year_month'].min()),
                                end=max(pl['year_month'].max(), rv['year_month'].max()), freq='MS')
            s_players = pl.set_index('year_month')['players'].reindex(idx).interpolate(limit_direction='both')
            pos = rv.set_index('year_month')['pos'].reindex(idx).fillna(0)
            neg = rv.set_index('year_month')['neg'].reindex(idx).fillna(0)
            total = pos + neg
            neg_pct = (neg / total.replace(0, np.nan)).clip(0, 1)

            z_players = _zscore(s_players)
            z_neg = _zscore(neg_pct if use_neg_pct else neg)

            # Detect candidate peaks
            peaks = _local_maxima(z_players, z_thr)
            for t0 in peaks:
                # Windowed data
                win_idx = pd.date_range((pd.Timestamp(t0).to_period('M') - win).to_timestamp(),
                                        (pd.Timestamp(t0).to_period('M') + win).to_timestamp(), freq='MS')
                x = s_players.reindex(win_idx)
                y = (neg_pct if use_neg_pct else neg).reindex(win_idx)
                # If too small, skip
                if x.dropna().shape[0] < 3 or y.dropna().shape[0] < 3:
                    continue
                lag_star, rho_star = _ccf_short(x, y, max_lag=max_lag)
                p_perm = _perm_pvalue_at_lag(x, y, lag_star, n_perm=n_perm)

                # Drop next month
                t1 = (pd.Timestamp(t0).to_period('M') + 1).to_timestamp()
                drop_1m = np.nan
                if t1 in s_players.index and pd.notna(s_players.get(t0, np.nan)) and pd.notna(s_players.get(t1, np.nan)):
                    p0 = float(s_players.get(t0))
                    p1 = float(s_players.get(t1))
                    if p0:
                        drop_1m = (p1 - p0) / p0

                # Half-life
                hl = _half_life(s_players, pd.Timestamp(t0))

                # Pre/Post means (3 months pre, 3 post)
                pre_idx = pd.date_range((pd.Timestamp(t0).to_period('M') - 3).to_timestamp(),
                                        (pd.Timestamp(t0).to_period('M') - 1).to_timestamp(), freq='MS')
                post_idx = pd.date_range((pd.Timestamp(t0).to_period('M') + 1).to_timestamp(),
                                         (pd.Timestamp(t0).to_period('M') + 3).to_timestamp(), freq='MS')
                pre_players = s_players.reindex(pre_idx).mean()
                post_players = s_players.reindex(post_idx).mean()
                pre_negp = (neg_pct if use_neg_pct else neg).reindex(pre_idx).mean()
                post_negp = (neg_pct if use_neg_pct else neg).reindex(post_idx).mean()
                negp_delta = (post_negp - pre_negp) if pd.notna(post_negp) and pd.notna(pre_negp) else np.nan

                # Overlap of peaks (Jaccard) within window
                j_overlap = _jaccard_overlap(z_players, z_neg, z_thr, win_idx)

                pattern_flag = bool(((z_neg.get(t0, np.nan) > z_thr) or (z_neg.get(t1, np.nan) > z_thr)) and
                                     (pd.notna(drop_1m) and drop_1m <= min_drop_1m))

                rows.append({
                    'appid': app,
                    'event_t0': pd.Timestamp(t0),
                    'z_players_t0': float(z_players.get(t0, np.nan)),
                    'players_t0': float(s_players.get(t0, np.nan)),
                    'metric_y': 'neg_pct' if use_neg_pct else 'neg',
                    'lag_star': int(lag_star),
                    'rho_star': float(rho_star) if pd.notna(rho_star) else np.nan,
                    'p_perm': float(p_perm) if pd.notna(p_perm) else np.nan,
                    'drop_1m': float(drop_1m) if pd.notna(drop_1m) else np.nan,
                    'half_life_months': int(hl) if hl is not None else np.nan,
                    'pre_players_mean_3m': float(pre_players) if pd.notna(pre_players) else np.nan,
                    'post_players_mean_3m': float(post_players) if pd.notna(post_players) else np.nan,
                    'pre_neg_mean_3m': float(pre_negp) if pd.notna(pre_negp) else np.nan,
                    'post_neg_mean_3m': float(post_negp) if pd.notna(post_negp) else np.nan,
                    'neg_delta_mean_3m': float(negp_delta) if pd.notna(negp_delta) else np.nan,
                    'jaccard_overlap_peaks': float(j_overlap) if pd.notna(j_overlap) else np.nan,
                    'pattern_launch_bad_reception': pattern_flag,
                })

        out_dir = Path('outputs/ccf_analysis')
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / 'event_correlation.parquet'
        out_df = pd.DataFrame(rows)
        write_parquet_any(out_df, out_path)
        print(f"[OK] Event lead-lag metrics -> {out_path} ({len(out_df)} rows)")
        if use_mlflow:
            try:
                mlflow.log_artifact(str(out_path))
                mlflow.log_metric('event_pairs', float(len(out_df)))
            except Exception:
                pass
    finally:
        if run_ctx is not None:
            mlflow.end_run()


if __name__ == '__main__':
    main()
