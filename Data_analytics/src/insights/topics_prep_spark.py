#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Prepara ventanas de reseñas por (appid, event_year_month) en Spark a partir de Mongo.
Guarda un Parquet con columnas: appid, event_year_month, texts (array<string>), count.

Config (configs/events.yaml):
- bertopic.window_months
- bertopic.max_docs_per_event
- mongo_connection (reviews): uri, database, collection
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r'))
    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)

    events_path = str(outdir / 'events.parquet')
    if not Path(events_path).exists():
        raise SystemExit('No existe outputs/events/events.parquet. Ejecuta eventos primero.')

    mcfg = cfg.get('mongo_connection', {})
    win = int(((cfg.get('bertopic') or {}).get('window_months') or 2))
    max_docs = int(((cfg.get('bertopic') or {}).get('max_docs_per_event') or 20000))

    spark = SparkSession.builder.appName('topics_prep_spark').getOrCreate()
    ev = spark.read.parquet(events_path).select('appid', F.col('year_month').alias('event_year_month'))
    ev = ev.dropDuplicates(['appid', 'event_year_month'])

    # Leer reseñas desde Mongo con conector; si no, abortar con instrucción
    try:
        rv = (spark.read.format('mongodb')
              .option('uri', mcfg['uri'])
              .option('database', mcfg['database'])
              .option('collection', mcfg['collection'])
              .load())
    except Exception as e:
        raise SystemExit(f"Conector Mongo para Spark no disponible: {e}. Ejecuta esta etapa donde esté el conector o usa el pipeline actual.")

    rv = rv.select(F.col('appid').cast('string').alias('appid'),
                   F.col('review').cast('string').alias('review'),
                   'votes_up', 'votes_helpful', 'helpful',
                   F.to_timestamp(F.from_unixtime('timestamp_created')).alias('ts'),
                   'language')
    rv = rv.where(F.col('language') == F.lit('english'))

    # Expandir eventos a ventanas [event - win, event + win]
    ev = ev.withColumn('start_ts', F.add_months('event_year_month', -win)) \
           .withColumn('end_ts', F.add_months('event_year_month', win))

    # Join por appid y rango temporal
    joined = ev.join(rv, on='appid', how='inner') \
              .where((F.col('ts') >= F.col('start_ts')) & (F.col('ts') <= F.col('end_ts')))

    # Limitar por evento: sample limitado
    # Estrategia: calcular row_number por (appid,event), luego cortar a max_docs
    w = F.window('ts', '1 month')  # no usado en ranking; solo placeholder si lo necesitáramos
    from pyspark.sql.window import Window
    we = Window.partitionBy('appid', 'event_year_month').orderBy(F.desc('votes_up'))
    cand = joined.withColumn('rn', F.row_number().over(we)) \
                 .where(F.col('rn') <= F.lit(max_docs))

    # Recoger textos por evento
    out = cand.groupBy('appid', 'event_year_month') \
              .agg(F.collect_list('review').alias('texts'), F.count('*').alias('count'))

    out_path = str(outdir / 'topics_prep.parquet')
    out.write.mode('overwrite').partitionBy('appid').parquet(out_path)
    print(f"[OK] Ventanas de reseñas preparadas en -> {out_path}")
    spark.stop()


if __name__ == '__main__':
    main()

