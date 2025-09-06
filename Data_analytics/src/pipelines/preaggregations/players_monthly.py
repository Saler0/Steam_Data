#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Preagrega jugadores mensuales por appid a partir de CSVs en data/external/players/{appid}.csv.
Usa Spark si está disponible; fallback a pandas. Escribe Parquet particionado por appid.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_timestamp, date_trunc, input_file_name, regexp_extract, col
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False


def preaggregate_pandas(dir_path: str) -> pd.DataFrame:
    base = Path(dir_path)
    if not base.exists():
        return pd.DataFrame(columns=['appid','year_month','players'])
    rows = []
    for csv in base.glob('*.csv'):
        try:
            appid = csv.stem
            df = pd.read_csv(csv)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            df['year_month'] = df['date'].dt.to_period('M').dt.to_timestamp()
            g = df.groupby('year_month')['players'].sum().reset_index()
            g['appid'] = str(appid)
            rows.append(g[['appid','year_month','players']])
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=['appid','year_month','players'])
    return pd.concat(rows, ignore_index=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--players_dir', default='data/external/players')
    ap.add_argument('--out', default='data/warehouse/players_monthly.parquet')
    args = ap.parse_args()

    if SPARK_AVAILABLE:
        try:
            spark = SparkSession.builder.appName('players_monthly_preagg').getOrCreate()
            src = str(Path(args.players_dir) / '*.csv')
            sdf = spark.read.option('header', True).csv(src)
            # Extraer appid del nombre de archivo
            fname = input_file_name()
            sdf = sdf.withColumn('appid', regexp_extract(fname, r"players/(.*)\.csv$", 1))
            sdf = sdf.withColumn('ts', to_timestamp(col('date'))) \
                     .dropna(subset=['ts'])
            sdf.createOrReplaceTempView('players')
            outdf = spark.sql(
                """
                SELECT appid, date_trunc('month', ts) AS year_month, SUM(CAST(players AS DOUBLE)) AS players
                FROM players
                GROUP BY appid, date_trunc('month', ts)
                """
            )
            # Particionar solo por year_month y ordenar por appid dentro de partición
            spark.conf.set('spark.sql.shuffle.partitions', 200)
            (outdf
             .repartition('year_month')
             .sortWithinPartitions('appid')
             .write
             .mode('overwrite')
             .option('maxRecordsPerFile', 5_000_000)
             .partitionBy('year_month')
             .parquet(args.out))
            print(f"[OK] Preagregado de jugadores guardado en -> {args.out}")
            spark.stop(); return
        except Exception as e:
            print(f"[WARN] Spark falló ({e}); usando pandas.")

    # Fallback
    agg = preaggregate_pandas(args.players_dir)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(args.out, index=False)
    print(f"[OK] Preagregado de jugadores (pandas) guardado en -> {args.out}")


if __name__ == '__main__':
    main()
