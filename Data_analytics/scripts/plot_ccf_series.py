#!/usr/bin/env python
from __future__ import annotations

"""
Plot series used in CCF/Granger before and after stationarity.

Inputs (from configs/ccf_analysis.yaml):
 - preaggregated.reviews_monthly (Parquet)
 - preaggregated.players_monthly (Parquet)

Outputs:
 - PNG at outputs/ccf_analysis/plots/{appid}_series.png with a 4x2 grid:
   rows = players | pos | neg | total_reviews ; cols = original | stationarized
"""

import argparse
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
import pandas as pd


def read_cfg(path: str) -> dict:
    import yaml
    return yaml.safe_load(open(path, 'r', encoding='utf-8'))


def _read_parquet_filter_app(path: str | None, appid: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        import pyarrow.dataset as ds
        dsr = ds.dataset(str(p), format='parquet')
        tbl = dsr.to_table(filter=(ds.field('appid') == str(appid)))
        df = tbl.to_pandas()
    except Exception:
        df = pd.read_parquet(p)
        df = df[df['appid'].astype(str) == str(appid)].copy()
    return df


def _to_month(df: pd.DataFrame, col: str) -> pd.Series:
    dt = pd.to_datetime(df[col], errors='coerce')
    return dt.dt.to_period('M').dt.to_timestamp()


def _series_from_preagg(cfg: dict, appid: str) -> pd.DataFrame:
    pre = cfg.get('preaggregated') or {}
    rv_pq = pre.get('reviews_monthly')
    pl_pq = pre.get('players_monthly')
    rv = _read_parquet_filter_app(rv_pq, appid)
    pl = _read_parquet_filter_app(pl_pq, appid)
    if rv.empty and pl.empty:
        return pd.DataFrame()
    out = None
    if not pl.empty:
        pl = pl.copy()
        ym = _to_month(pl, 'year_month' if 'year_month' in pl.columns else 'date')
        pl = pd.DataFrame({'year_month': ym, 'players': pl.get('players', np.nan)})
        out = pl
    if not rv.empty:
        rv = rv.copy()
        if 'year_month' not in rv.columns:
            if 'date' in rv.columns:
                rv['year_month'] = _to_month(rv, 'date')
            else:
                rv['year_month'] = pd.to_datetime(rv['year_month'], errors='coerce')
        cols = [c for c in ['pos','neg','total_reviews'] if c in rv.columns]
        rv = rv[['year_month'] + cols]
        out = rv if out is None else pd.merge(out, rv, on='year_month', how='outer')
    out = out.sort_values('year_month').reset_index(drop=True).fillna(0)
    return out


def _apply_transform(s: pd.Series, method: str, period: int = 12) -> pd.Series:
    s = pd.Series(s).astype(float)
    if method == 'dlog':
        return (np.log(s.replace(0, np.nan)) - np.log(s.replace(0, np.nan)).shift(1)).dropna()
    if method == 'diff':
        return s.diff().dropna()
    if method == 'diff2':
        return s.diff().diff().dropna()
    if method == 'sqrt':
        return np.sqrt(s.clip(lower=0)).dropna()
    if method == 'sqrt_diff':
        return np.sqrt(s.clip(lower=0)).diff().dropna()
    if method == 'log1p_diff':
        return np.log1p(s.clip(lower=0)).diff().dropna()
    if method == 'seasonal_diff':
        return s.diff(period).dropna()
    return s


def _is_stationary(s: pd.Series, adf_alpha: float, use_kpss: bool, kpss_alpha: float) -> Tuple[bool, float, float | None]:
    p_adf, p_kpss = np.nan, None
    try:
        from statsmodels.tsa.stattools import adfuller, kpss as kpss_test
    except Exception:
        # If statsmodels missing, assume OK if variance is present
        return (s.std(ddof=0) > 0), float('nan'), None
    try:
        p_adf = float(adfuller(s.dropna())[1])
    except Exception:
        p_adf = float('nan')
    ok_adf = (not np.isnan(p_adf)) and (p_adf < adf_alpha)
    p_kpss_val = None
    if use_kpss:
        try:
            _, p_kpss_val, _, _ = kpss_test(s.dropna(), regression='c', nlags='auto')
        except Exception:
            p_kpss_val = None
    ok_kpss = True if p_kpss_val is None else (p_kpss_val > kpss_alpha)
    return (ok_adf and ok_kpss), p_adf, p_kpss_val


def _stationarize_best(s: pd.Series, methods: list[str], adf_alpha: float, use_kpss: bool, kpss_alpha: float, period: int) -> Tuple[pd.Series, str]:
    for m in methods:
        cand = _apply_transform(s, m, period)
        if cand is None or cand.empty or float(np.std(cand)) == 0.0:
            continue
        ok, _, _ = _is_stationary(cand, adf_alpha, use_kpss, kpss_alpha)
        if ok:
            return cand, m
    # fallback: dlog or diff
    fallback = _apply_transform(s, 'dlog')
    if fallback.empty:
        fallback = _apply_transform(s, 'diff')
    return fallback, 'fallback'


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Plot original vs stationarized series for CCF inputs')
    ap.add_argument('--config', default='configs/ccf_analysis.yaml')
    ap.add_argument('--appid', required=True)
    ap.add_argument('--out', default=None, help='Output PNG path; default in outputs/ccf_analysis/plots/{appid}_series.png')
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = read_cfg(args.config)
    df = _series_from_preagg(cfg, str(args.appid))
    if df.empty:
        out_path = Path(args.out) if args.out else (Path('outputs/ccf_analysis/plots') / f"{args.appid}_series_no_data.png")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()
        print(f"[INFO] No data for appid {args.appid}. Created empty file: {out_path}")
        return
    df = df.set_index(pd.to_datetime(df['year_month'])).drop(columns=['year_month'])

    # Config
    st_cfg = cfg.get('stationarity', {}) or {}
    methods = st_cfg.get('transforms', ['dlog','diff','diff2','sqrt'])
    adf_alpha = float(st_cfg.get('adf_alpha', 0.05))
    kpss_cfg = st_cfg.get('kpss', {}) or {}
    use_kpss = bool(kpss_cfg.get('enabled', True))
    kpss_alpha = float(kpss_cfg.get('alpha', 0.05))
    season_period = int((st_cfg.get('seasonal') or {}).get('period', 12))

    vars_ = [c for c in ['players','pos','neg','total_reviews'] if c in df.columns]
    if not vars_:
        out_path = Path(args.out) if args.out else (Path('outputs/ccf_analysis/plots') / f"{args.appid}_series_no_vars.png")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()
        print(f"[INFO] No variables to plot for appid {args.appid}. Created empty file: {out_path}")
        return

    # Build original + stationarized
    transformed = {}
    chosen = {}
    for v in vars_:
        s = pd.Series(df[v].astype(float)).dropna()
        t, name = _stationarize_best(s, methods, adf_alpha, use_kpss, kpss_alpha, season_period)
        transformed[v] = t
        chosen[v] = name

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    rows = len(vars_)
    fig, axes = plt.subplots(rows, 2, figsize=(14, 3.2 * rows), sharex=False)
    if rows == 1:
        axes = np.array([axes])
    for i, v in enumerate(vars_):
        # Original
        ax = axes[i, 0]
        df[v].plot(ax=ax, color='#3b82f6', linewidth=1.5)
        ax.set_title(f"{v} — original")
        ax.grid(True, alpha=0.3)
        # Stationarized
        ax2 = axes[i, 1]
        transformed[v].plot(ax=ax2, color='#ef4444', linewidth=1.2)
        ax2.set_title(f"{v} — stationarized ({chosen[v]})")
        ax2.grid(True, alpha=0.3)
    plt.tight_layout()

    out_path = Path(args.out) if args.out else (Path('outputs/ccf_analysis/plots') / f"{args.appid}_series.png")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=130)
    print(f"[OK] Plot guardado en -> {out_path}")


if __name__ == '__main__':
    main()

