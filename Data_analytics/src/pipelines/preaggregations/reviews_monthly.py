#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Preagrega reseñas mensuales por appid usando Spark si está disponible, con fallback a pandas.
Lee de Mongo (reviews) y escribe Parquet particionado por appid: year_month,pos,neg,total_reviews.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_unixtime, date_trunc
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

from pymongo import MongoClient


def read_mongo_reviews(mongo_uri: str, db: str, coll: str, query: dict | None = None, projection: dict | None = None) -> pd.DataFrame:
    client = MongoClient(mongo_uri)
    try:
        cur = client[db][coll].find(query or {}, projection or {"appid":1, "timestamp_created":1, "voted_up":1, "_id":0})
        rows = list(cur)
        return pd.DataFrame(rows)
    finally:
        try:
            client.close()
        except Exception:
            pass


def preaggregate_pandas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["appid","year_month","pos","neg","total_reviews"])  # type: ignore
    out = df.copy()
    out['appid'] = out['appid'].astype(str)
    out['date'] = pd.to_datetime(out['timestamp_created'], unit='s', errors='coerce')
    out = out.dropna(subset=['date'])
    out['year_month'] = out['date'].dt.to_period('M').dt.to_timestamp()
    pos = out[out['voted_up'] == True].groupby(['appid','year_month']).size().rename('pos')
    neg = out[out['voted_up'] == False].groupby(['appid','year_month']).size().rename('neg')
    g = pd.concat([pos, neg], axis=1).fillna(0).reset_index()
    g['total_reviews'] = g['pos'] + g['neg']
    return g[['appid','year_month','pos','neg','total_reviews']]


def write_partitioned_parquet(df: pd.DataFrame, out_path: str):
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(p, index=False)
    except Exception:
        # fallback simple: escribir como un único parquet sin particiones
        df.to_parquet(p, index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo-uri", required=False)
    ap.add_argument("--db", required=False)
    ap.add_argument("--collection", required=False)
    ap.add_argument("--out", default="data/warehouse/reviews_monthly.parquet")
    ap.add_argument("--config", help="YAML con mongo_connection y preaggregated paths", required=False)
    args = ap.parse_args()

    if args.config:
        import yaml
        cfg = yaml.safe_load(open(args.config, 'r'))
        mongo = (cfg.get('mongo_connection') or
                 (cfg.get('players_data') if 'players_data' in cfg else {}) or
                 {})
        args.mongo_uri = args.mongo_uri or mongo.get('uri')
        args.db = args.db or mongo.get('database') or mongo.get('db')
        args.collection = args.collection or mongo.get('collection')
        pre = (cfg.get('preaggregated') or {})
        args.out = args.out or pre.get('reviews_monthly', args.out)

    if not args.mongo_uri or not args.db or not args.collection:
        raise SystemExit("Faltan parámetros de conexión a Mongo (--mongo-uri, --db, --collection)")

    # Spark path (preferible si disponible): intentar conector Mongo para Spark
    if SPARK_AVAILABLE:
        try:
            spark = (SparkSession.builder
                     .appName("reviews_monthly_preagg")
                     .getOrCreate())
            try:
                sdf = (spark.read
                       .format("mongodb")
                       .option("uri", args.mongo_uri)
                       .option("database", args.db)
                       .option("collection", args.collection)
                       .load())
            except Exception as con_e:
                # Fallback: cargar con pymongo y crear DataFrame Spark
                print(f"[WARN] Conector Mongo para Spark no disponible ({con_e}); usando pymongo->Spark DataFrame.")
                rows = read_mongo_reviews(args.mongo_uri, args.db, args.collection)
                if rows.empty:
                    print("[INFO] No hay reseñas para preagregar.")
                    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
                    pd.DataFrame(columns=["appid","year_month","pos","neg","total_reviews"]).to_parquet(args.out, index=False)
                    return
                sdf = spark.createDataFrame(rows)

            sdf = sdf.withColumn('appid_str', col('appid').cast('string')) \
                     .withColumn('ts', from_unixtime(col('timestamp_created')).cast('timestamp'))
            sdf.createOrReplaceTempView('reviews')
            agg = spark.sql(
                """
                SELECT appid_str AS appid,
                       date_trunc('month', ts) AS year_month,
                       SUM(CASE WHEN voted_up = true THEN 1 ELSE 0 END) AS pos,
                       SUM(CASE WHEN voted_up = false THEN 1 ELSE 0 END) AS neg,
                       COUNT(1) AS total_reviews
                FROM reviews
                WHERE ts IS NOT NULL
                GROUP BY appid_str, date_trunc('month', ts)
                """
            )
            out = args.out
            # Particionar solo por year_month y ordenar por appid dentro de partición para mejorar skipping
            spark.conf.set('spark.sql.shuffle.partitions', 400)
            (agg
             .repartition('year_month')
             .sortWithinPartitions('appid')
             .write
             .mode('overwrite')
             .option('maxRecordsPerFile', 5_000_000)
             .partitionBy('year_month')
             .parquet(out))
            print(f"[OK] Preagregado de reseñas guardado en -> {out}")
            spark.stop()
            return
        except Exception as e:
            print(f"[WARN] Spark falló ({e}); usando pandas.")

    # Fallback pandas
    df = read_mongo_reviews(args.mongo_uri, args.db, args.collection)
    agg = preaggregate_pandas(df)
    write_partitioned_parquet(agg, args.out)
    print(f"[OK] Preagregado de reseñas (pandas) guardado en -> {args.out}")


if __name__ == '__main__':
    main()
