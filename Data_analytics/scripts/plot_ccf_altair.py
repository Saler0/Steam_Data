#!/usr/bin/env python
from __future__ import annotations

"""
Altair dashboard: CCF inputs (original vs stationarized) + quality metrics.

Generates an interactive HTML per appid with:
 - Time series (players, pos, neg, total_reviews): original vs stationarized
 - Stationarity proportions and ADF/KPSS distributions (if stationarity_tests.csv exists)
 - Granger significance rates pre/post FDR (if summary.parquet exists)

Usage:
  python scripts/plot_ccf_altair.py --config configs/ccf_analysis.yaml --appid 281990

Output:
  outputs/ccf_analysis/plots/altair_{appid}.html
"""

import argparse
from pathlib import Path
from typing import Iterable, Tuple, Optional
import numpy as np
import pandas as pd


def _read_yaml(path: str) -> dict:
    import yaml
    return yaml.safe_load(open(path, 'r', encoding='utf-8')) or {}


def _read_parquet_filter_app(path: Optional[str], appid: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        import pyarrow.dataset as ds  # type: ignore
        dsr = ds.dataset(str(p), format='parquet')
        tbl = dsr.to_table(filter=(ds.field('appid') == str(appid)))
        df = tbl.to_pandas()
    except Exception:
        df = pd.read_parquet(p)
        df = df[df['appid'].astype(str) == str(appid)].copy()
    return df


def _to_month(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors='coerce')
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
        ym = _to_month(pl['year_month'] if 'year_month' in pl.columns else pl.get('date'))
        pl = pd.DataFrame({'year_month': ym, 'players': pl.get('players', np.nan)})
        out = pl
    if not rv.empty:
        rv = rv.copy()
        if 'year_month' not in rv.columns:
            if 'date' in rv.columns:
                rv['year_month'] = _to_month(rv['date'])
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


def _is_stationary(s: pd.Series, adf_alpha: float, use_kpss: bool, kpss_alpha: float) -> Tuple[bool, float, Optional[float]]:
    p_adf, p_kpss = np.nan, None
    try:
        from statsmodels.tsa.stattools import adfuller, kpss as kpss_test
    except Exception:
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
    fb = _apply_transform(s, 'dlog')
    if fb.empty:
        fb = _apply_transform(s, 'diff')
    return fb, 'fallback'


def _build_series_layers(df: pd.DataFrame, cfg: dict) -> Tuple[pd.DataFrame, dict]:
    st_cfg = cfg.get('stationarity', {}) or {}
    methods = st_cfg.get('transforms', ['dlog','diff','diff2','sqrt'])
    adf_alpha = float(st_cfg.get('adf_alpha', 0.05))
    kpss_cfg = st_cfg.get('kpss', {}) or {}
    use_kpss = bool(kpss_cfg.get('enabled', True))
    kpss_alpha = float(kpss_cfg.get('alpha', 0.05))
    period = int((st_cfg.get('seasonal') or {}).get('period', 12))

    base = df.copy()
    base = base.set_index(pd.to_datetime(base['year_month'])).drop(columns=['year_month'])
    vars_ = [c for c in ['players','pos','neg','total_reviews'] if c in base.columns]
    chosen: dict[str,str] = {}
    rows: list[dict] = []
    for v in vars_:
        s = pd.Series(base[v].astype(float)).dropna()
        t, name = _stationarize_best(s, methods, adf_alpha, use_kpss, kpss_alpha, period)
        chosen[v] = name
        # Original
        for ts, val in s.items():
            rows.append({'date': pd.to_datetime(ts), 'variable': v, 'kind': 'Original', 'value': float(val)})
        # Stationary
        for ts, val in t.items():
            rows.append({'date': pd.to_datetime(ts), 'variable': v, 'kind': f'Stationary ({name})', 'value': float(val)})
    long = pd.DataFrame(rows)
    return long, chosen


def _load_quality_tables(ccf_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    # stationarity_tests.csv and summary.parquet
    tests = pd.DataFrame()
    summary = pd.DataFrame()
    cand_tests = [ccf_dir / 'stationarity_tests.csv']
    cand_summary = [ccf_dir / 'summary.parquet']
    # Also consider subset output dir
    subset_dir = ccf_dir / 'subset_neighbors'
    cand_tests.append(subset_dir / 'stationarity_tests.csv')
    cand_summary.append(subset_dir / 'summary.parquet')
    for p in cand_tests:
        if p.exists():
            try:
                tests = pd.read_csv(p)
                break
            except Exception:
                pass
    for p in cand_summary:
        if p.exists():
            try:
                summary = pd.read_parquet(p)
                break
            except Exception:
                pass
    return tests, summary


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Altair dashboard for CCF inputs and quality metrics')
    ap.add_argument('--config', default='configs/ccf_analysis.yaml')
    ap.add_argument('--appid', required=True)
    ap.add_argument('--out', default=None)
    ap.add_argument('--ccf-dir', default='outputs/ccf_analysis')
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    try:
        import altair as alt  # type: ignore
    except Exception:
        raise SystemExit('Altair no está instalado en este entorno.')

    args = parse_args(argv)
    cfg = _read_yaml(args.config)
    df = _series_from_preagg(cfg, str(args.appid))
    if df.empty:
        raise SystemExit('No hay datos para el appid. Verifica preaggregated.* en configs.')

    long, chosen = _build_series_layers(df, cfg)

    # Base encodings
    base = alt.Chart(long).transform_calculate(
        tooltip_date="year(datum.date)+'-'+(month(datum.date)+1)"
    )
    color = alt.Color('kind:N', scale=alt.Scale(scheme='set1'))
    tooltip = [
        alt.Tooltip('date:T', title='Date'),
        alt.Tooltip('variable:N', title='Series'),
        alt.Tooltip('kind:N', title='Type'),
        alt.Tooltip('value:Q', title='Value', format='.3f'),
    ]
    line = base.mark_line(point=False, interpolate='monotone').encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('value:Q', title='Value'),
        color=color,
        tooltip=tooltip,
    )
    facets = line.facet(
        row=alt.Row('variable:N', header=alt.Header(title='Series', labelLimit=200)),
        columns=2,
    ).resolve_scale(y='independent')

    # Quality panels (if available)
    tests, summary = _load_quality_tables(Path(args.ccf_dir))
    charts = [facets.properties(title=f'AppID {args.appid} — Original vs Stationary ({", ".join([f"{k}:{v}" for k,v in chosen.items()])})')]

    if not tests.empty:
        # Proportion stationary by series (ok==True)
        df_ok = tests.copy()
        if 'ok' in df_ok.columns:
            df_ok['ok'] = df_ok['ok'].astype(bool)
            agg = df_ok.groupby(['series'])['ok'].mean().reset_index()
            bar_ok = alt.Chart(agg).mark_bar(color='#10b981').encode(
                x=alt.X('series:N', title='Series'),
                y=alt.Y('ok:Q', title='Stationary %', axis=alt.Axis(format='%')),
                tooltip=[alt.Tooltip('ok:Q', format='.1%'), 'series']
            ).properties(title='Stationarity Rate by Series')
            charts.append(bar_ok)
        # ADF/KPSS p-value distributions
        if 'p_adf' in tests.columns:
            hist_adf = alt.Chart(tests).mark_bar(color='#6366f1', opacity=0.8).encode(
                x=alt.X('p_adf:Q', bin=alt.Bin(maxbins=30), title='ADF p-value'),
                y=alt.Y('count()', title='Count'),
                tooltip=['count()']
            ).properties(title='ADF p-value distribution')
            charts.append(hist_adf)
        if 'p_kpss' in tests.columns:
            hist_kpss = alt.Chart(tests.dropna(subset=['p_kpss'])).mark_bar(color='#f59e0b', opacity=0.8).encode(
                x=alt.X('p_kpss:Q', bin=alt.Bin(maxbins=30), title='KPSS p-value'),
                y=alt.Y('count()', title='Count'),
                tooltip=['count()']
            ).properties(title='KPSS p-value distribution')
            charts.append(hist_kpss)

    if not summary.empty:
        df_sum = summary.copy()
        # Overall significance rates
        def _rate(col: str) -> float:
            s = df_sum[col]
            s = s.dropna()
            return float((s.astype(bool)).mean()) if len(s) else 0.0
        overall = pd.DataFrame({
            'metric': ['Granger XY', 'Granger XY (FDR)', 'Granger YX', 'Granger YX (FDR)'],
            'rate': [
                _rate('granger_xy_sig'), _rate('granger_xy_sig_fdr') if 'granger_xy_sig_fdr' in df_sum.columns else 0.0,
                _rate('granger_yx_sig'), _rate('granger_yx_sig_fdr') if 'granger_yx_sig_fdr' in df_sum.columns else 0.0,
            ]
        })
        bar_sig = alt.Chart(overall).mark_bar().encode(
            x=alt.X('metric:N', title='Metric'),
            y=alt.Y('rate:Q', title='Share', axis=alt.Axis(format='%')),
            color=alt.Color('metric:N', legend=None, scale=alt.Scale(scheme='tableau10')),
            tooltip=[alt.Tooltip('rate:Q', format='.1%'), 'metric']
        ).properties(title='Granger significance (overall)')
        charts.append(bar_sig)

    # Compose
    dashboard = alt.vconcat(*charts).configure_title(anchor='start')

    # Save HTML (uses CDN for vega-lite)
    out = Path(args.out) if args.out else (Path('outputs/ccf_analysis/plots') / f'altair_{args.appid}.html')
    out.parent.mkdir(parents=True, exist_ok=True)
    dashboard.save(str(out))
    print(f'[OK] Dashboard Altair -> {out}')


if __name__ == '__main__':
    main()

