#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CCF/Granger en Spark usando Pandas UDF por appid (aprox. FDR por appid).
Lee preagregados mensuales (players/reviews), une por appid/month y aplica análisis por pares definidos en configs.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)
from pyspark.sql.functions import pandas_udf, PandasUDFType


def build_schema():
    return StructType([
        StructField('appid', StringType(), False),
        StructField('pair_name', StringType(), False),
        StructField('best_lag', IntegerType(), True),
        StructField('best_ccf', DoubleType(), True),
        StructField('granger_xy_pmin', DoubleType(), True),
        StructField('granger_yx_pmin', DoubleType(), True),
        StructField('granger_xy_p_fdr', DoubleType(), True),
        StructField('granger_yx_p_fdr', DoubleType(), True),
        StructField('granger_xy_sig_fdr', StringType(), True),
        StructField('granger_yx_sig_fdr', StringType(), True),
    ])


def analyze_pairs_pdf(pdf: pd.DataFrame, pairs: list[dict], maxlag: int, alpha: float) -> pd.DataFrame:
    from statsmodels.tsa.stattools import ccf
    from statsmodels.tsa.stattools import adfuller
    from statsmodels.tsa.stattools import grangercausalitytests
    from statsmodels.stats.multitest import multipletests

    def _dlog(s):
        s = pd.Series(s).astype(float)
        return np.log(s.replace(0, np.nan)).diff().dropna()

    out_rows = []
    appid = str(pdf['appid'].iloc[0]) if len(pdf) else None
    pdf = pdf.sort_values('year_month')
    for p in pairs:
        xname, yname = p['predictor'], p['target']
        if xname not in pdf.columns or yname not in pdf.columns:
            continue
        x = pdf[xname].astype(float)
        y = pdf[yname].astype(float)
        # transform basic: dlog
        x2 = _dlog(x)
        y2 = _dlog(y)
        df2 = pd.DataFrame({xname: x2, yname: y2}).dropna()
        if len(df2) < 8:
            continue
        # CCF across lags
        vals = {}
        for lag in range(-maxlag, maxlag+1):
            if lag < 0:
                xs = x2[-lag:]
                ys = y2[:len(xs)]
            elif lag > 0:
                ys = y2[lag:]
                xs = x2[:len(ys)]
            else:
                xs, ys = x2, y2
            if len(xs) < 5 or xs.std() == 0 or ys.std() == 0:
                vals[lag] = np.nan
                continue
            vals[lag] = float(np.corrcoef(xs, ys)[0, 1])
        best_lag = max(vals, key=lambda k: abs(vals.get(k) if vals.get(k) is not None else 0))
        best_ccf = vals.get(best_lag)
        # Granger
        gxy = None
        gyx = None
        try:
            res_xy = grangercausalitytests(df2[[yname, xname]], maxlag=maxlag, verbose=False)
            gxy = min([res[0]['ssr_chi2test'][1] for lag, res in res_xy.items()])
        except Exception:
            pass
        try:
            res_yx = grangercausalitytests(df2[[xname, yname]], maxlag=maxlag, verbose=False)
            gyx = min([res[0]['ssr_chi2test'][1] for lag, res in res_yx.items()])
        except Exception:
            pass
        out_rows.append({'appid': appid, 'pair_name': p['name'], 'best_lag': best_lag, 'best_ccf': best_ccf,
                         'granger_xy_pmin': gxy, 'granger_yx_pmin': gyx})
    out = pd.DataFrame(out_rows)
    if not out.empty:
        # FDR por appid
        for col in ['granger_xy_pmin', 'granger_yx_pmin']:
            mask = out[col].notna()
            if mask.any():
                rej, p_corr, _, _ = multipletests(out.loc[mask, col].values, alpha=alpha, method='fdr_bh')
                tag = col.replace('_pmin', '')
                out.loc[mask, f'{tag}_p_fdr'] = p_corr
                out.loc[mask, f'{tag}_sig_fdr'] = rej
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r'))
    pre = cfg.get('preaggregated', {})
    reviews_pq = pre.get('reviews_monthly', 'data/warehouse/reviews_monthly.parquet')
    players_pq = pre.get('players_monthly', 'data/warehouse/players_monthly.parquet')

    spark = SparkSession.builder.appName('ccf_spark').getOrCreate()
    rv = spark.read.parquet(reviews_pq)
    pl = spark.read.parquet(players_pq)
    facts = rv.join(pl, on=['year_month', 'appid'], how='outer').fillna({'pos':0,'neg':0,'total_reviews':0,'players':0})

    # Filtro por clúster opcional
    clu_path = cfg.get('input_path', {}).get('clusters_parquet', 'data/processed/clusters.parquet')
    if Path(clu_path).exists() and cfg.get('cluster_filter'):
        clusters = spark.read.parquet(clu_path).select(F.col('appid').cast('string').alias('c_appid'), 'cluster_id')
        clusters = clusters.where(F.col('cluster_id').isin([int(x) for x in cfg['cluster_filter']]))
        facts = facts.join(clusters, facts['appid'].cast('string') == clusters['c_appid'], 'inner').drop('c_appid')

    pairs = cfg.get('ccf_pairs', [])
    max_lag = int(cfg.get('ccf_lags', 6))
    alpha = float(cfg.get('granger', {}).get('alpha', 0.05))

    schema = build_schema()

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def udf_analyze(pdf: pd.DataFrame) -> pd.DataFrame:
        return analyze_pairs_pdf(pdf, pairs, max_lag, alpha)

    res = facts.groupBy('appid').apply(udf_analyze)
    out_dir = Path(cfg.get('output_dir', 'outputs/ccf_analysis'))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = str(out_dir / 'summary.parquet')
    res.write.mode('overwrite').parquet(out_path)
    print(f"[OK] CCF Spark summary -> {out_path}")
    spark.stop()


if __name__ == '__main__':
    main()

