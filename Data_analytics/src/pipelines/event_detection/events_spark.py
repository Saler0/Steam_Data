#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Eventos (Spark): detecta picos/caídas por appid y variable usando z-score sobre dlog mensual.

Entradas esperadas (Parquet particionado por year_month):
- data/warehouse/reviews_monthly.parquet (columns: appid,pos,neg[,total_reviews],year_month)
- data/warehouse/players_monthly.parquet (columns: appid,players,year_month)

Salida:
- outputs/events/events.parquet (appid, year_month, variable, direction, zscore, value, growth_rate)
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r'))
    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)

    pre = cfg.get('preaggregated', {})
    reviews_pq = pre.get('reviews_monthly', 'data/warehouse/reviews_monthly.parquet')
    players_pq = pre.get('players_monthly', 'data/warehouse/players_monthly.parquet')
    zthr = float(((cfg.get('detection') or {}).get('zscore_threshold') or 1.5))

    spark = (
        SparkSession.builder
        .appName('events_spark')
        .getOrCreate()
    )

    # Leer preagregados
    rv = spark.read.parquet(reviews_pq)
    pl = spark.read.parquet(players_pq)

    # Unir por appid,year_month (outer) y rellenar nulos
    df = rv.join(pl, on=['year_month','appid'], how='outer') \
           .fillna({'pos': 0, 'neg': 0, 'players': 0, 'total_reviews': 0})

    # Filtro opcional por clúster
    clu_path = cfg.get('clusters_parquet', cfg.get('input_paths', {}).get('clusters_parquet', 'data/processed/clusters.parquet'))
    if Path(clu_path).exists():
        clusters = spark.read.parquet(clu_path).select(F.col('appid').cast('string').alias('c_appid'), 'cluster_id')
        if cfg.get('cluster_filter'):
            clusters = clusters.where(F.col('cluster_id').isin([int(x) for x in cfg['cluster_filter']]))
        df = df.join(clusters, df['appid'].cast('string') == clusters['c_appid'], 'inner').drop('c_appid')

    # Variables a procesar
    vars_ = []
    if 'players' in df.columns: vars_.append('players')
    if 'pos' in df.columns: vars_.append('pos')
    if 'neg' in df.columns: vars_.append('neg')
    if 'total_reviews' in df.columns: vars_.append('total_reviews')

    # Para cada variable, calcular dlog y z-score dentro de appid
    events = None
    for v in vars_:
        w = Window.partitionBy('appid').orderBy('year_month')
        # dlog = log(x) - log(lag(x))
        dlog = (F.log(F.when(F.col(v) <= 0, None).otherwise(F.col(v))) -
                F.log(F.lag(F.when(F.col(v) <= 0, None).otherwise(F.col(v))).over(w)))
        tmp = df.withColumn('growth_rate', dlog)
        # media y desv por appid
        mu = F.avg('growth_rate').over(Window.partitionBy('appid'))
        sd = F.stddev_pop('growth_rate').over(Window.partitionBy('appid'))
        tmp = tmp.withColumn('zscore', (F.col('growth_rate') - mu) / sd)
        tmp = tmp.where(F.col('growth_rate').isNotNull())
        ev = tmp.where(F.abs(F.col('zscore')) >= F.lit(zthr)) \
                 .select('appid', 'year_month',
                         F.lit(v).alias('variable'),
                         F.when(F.col('zscore') > 0, F.lit('peak')).otherwise(F.lit('drop')).alias('direction'),
                         'zscore', F.col(v).alias('value'), 'growth_rate')
        events = ev if events is None else events.unionByName(ev)

    out_path = str(outdir / 'events.parquet')
    if events is None:
        # escribir vacío
        spark.createDataFrame([], schema='appid string, year_month timestamp, variable string, direction string, zscore double, value double, growth_rate double') \
             .write.mode('overwrite').parquet(out_path)
    else:
        events.write.mode('overwrite').parquet(out_path)
    print(f"[OK] Eventos (Spark) guardados en -> {out_path}")
    spark.stop()


if __name__ == '__main__':
    main()

