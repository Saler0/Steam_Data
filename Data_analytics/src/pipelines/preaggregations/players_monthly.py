#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Preagrega jugadores mensuales por appid.

Modos soportados:
- Lectura desde PostgreSQL (recomendado): --postgres-host, etc.
- Passthrough mensual (fallback): --file <csv_mensual> con columnas
  ['appid','name','month_date','avg_players'] como genera Data_management.
  Se mapea a esquema estándar (appid:str, year_month:timestamp, players:int).

- Agregado desde diarios por app: --players_dir data/external/players (CSV
  'date,players' por appid). Suma mensual por appid. Usar solo si no hay CSV mensual.

Escribe Parquet particionado por year_month en data/warehouse/players_monthly.parquet.
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import pandas as pd
import argparse

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_timestamp, date_trunc, input_file_name, regexp_extract, col
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

load_dotenv()

def read_from_postgres(pg_uri: str, table: str) -> pd.DataFrame:
    """Lee datos desde PostgreSQL y normaliza el esquema."""
    if not SQLALCHEMY_AVAILABLE:
        raise SystemExit("SQLAlchemy y psycopg2-binary son necesarios para leer desde PostgreSQL.")
    try:
        engine = create_engine(pg_uri)
        df = pd.read_sql_table(table, engine)
        # Lógica de normalización copiada de passthrough_monthly_csv
        if 'appid' in df.columns:
            df['appid'] = df['appid'].astype(str)
        if 'month' in df.columns and 'month_date' not in df.columns:
            df = df.rename(columns={'month': 'month_date'})
        if 'avg_players' not in df.columns and 'players' in df.columns:
            df = df.rename(columns={'players': 'avg_players'})
        df['year_month'] = pd.to_datetime(df['month_date'], errors='coerce')
        df = df.dropna(subset=['year_month'])
        df['players'] = pd.to_numeric(df['avg_players'], errors='coerce').fillna(0).astype(int)
        return df[['appid','year_month','players']]
    except Exception as e:
        print(f"[ERROR] No se pudo leer desde PostgreSQL: {e}")
        return pd.DataFrame(columns=['appid','year_month','players'])

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


def passthrough_monthly_csv(csv_path: str) -> pd.DataFrame:
    """Lee CSV mensual consolidado (Data_management) y normaliza esquema.

    Espera columnas: 'appid', 'name', 'month_date', 'avg_players'.
    Devuelve columnas: 'appid'(str), 'year_month'(timestamp inicio de mes), 'players'(int).
    """
    p = Path(csv_path)
    if not p.exists():
        return pd.DataFrame(columns=['appid','year_month','players'])
    df = pd.read_csv(p)
    # columnas mínimas
    req = {'appid', 'month_date', 'avg_players'}
    missing = req - set(df.columns)
    if missing:
        # intentar alias comunes
        if 'month' in df.columns and 'month_date' not in df.columns:
            df = df.rename(columns={'month': 'month_date'})
        if 'avg_players' not in df.columns and 'players' in df.columns:
            df = df.rename(columns={'players': 'avg_players'})
    # tipos
    if 'appid' in df.columns:
        df['appid'] = df['appid'].astype(str)
    df['year_month'] = pd.to_datetime(df['month_date'], errors='coerce')
    df = df.dropna(subset=['year_month'])
    # usar promedio mensual como 'players'
    df['players'] = pd.to_numeric(df['avg_players'], errors='coerce').fillna(0).astype(int)
    return df[['appid','year_month','players']]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--players_dir', default='data/external/players')
    ap.add_argument('--file', help='CSV mensual consolidado (Data_management steamcharts_data.csv)')
    ap.add_argument('--out', default='data/warehouse/players_monthly.parquet')
    # Argumentos para PostgreSQL
    ap.add_argument('--postgres-host', help='PostgreSQL host')
    ap.add_argument('--postgres-port', default='5432', help='PostgreSQL port')
    ap.add_argument('--postgres-user', help='PostgreSQL user')
    ap.add_argument('--postgres-password', help='PostgreSQL password')
    ap.add_argument('--postgres-db', help='PostgreSQL database')
    ap.add_argument('--postgres-table', default='exploitation_zone', help='PostgreSQL table')
    args = ap.parse_args()

    df = None
    # --- Prioridad 1: Leer desde PostgreSQL ---
    if args.postgres_host and args.postgres_user and args.postgres_password and args.postgres_db:
        pg_uri = f"postgresql://{args.postgres_user}:{args.postgres_password}@{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
        print(f"[INFO] Leyendo desde PostgreSQL, tabla: {args.postgres_table}")
        df = read_from_postgres(pg_uri, args.postgres_table)

    # --- Prioridad 2: CSV mensual consolidado ---
    if df is None and args.file and Path(args.file).exists():
        print(f"[INFO] Leyendo desde archivo CSV consolidado: {args.file}")
        df = passthrough_monthly_csv(args.file)

    if df is not None:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(args.out, index=False)
        print(f"[OK] Players mensual guardado en -> {args.out}")
        return

    # --- Prioridad 3 (Fallback): Spark o Pandas sobre directorio de CSVs ---
    if SPARK_AVAILABLE:
        try:
            spark = SparkSession.builder.appName('players_monthly_preagg').getOrCreate()
            src = str(Path(args.players_dir) / '*.csv')
            sdf = spark.read.option('header', True).csv(src)
            # Extraer appid del nombre de archivo
            fname = input_file_name()
            sdf = sdf.withColumn('appid', regexp_extract(fname, r"players/(.*)\.csv$", 1))
            sdf = sdf.withColumn('ts', to_timestamp(col('date'))).dropna(subset=['ts'])
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
            print(f"[OK] Preagregado de jugadores (Spark) guardado en -> {args.out}")
            spark.stop(); return
        except Exception as e:
            print(f"[WARN] Spark falló ({e}); usando pandas.")

    # Fallback final
    print(f"[INFO] Leyendo desde directorio de CSVs con pandas: {args.players_dir}")
    agg = preaggregate_pandas(args.players_dir)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(args.out, index=False)
    print(f"[OK] Preagregado de jugadores (pandas) guardado en -> {args.out}")


if __name__ == '__main__':
    main()