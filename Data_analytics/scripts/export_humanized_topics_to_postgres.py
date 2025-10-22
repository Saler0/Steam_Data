#!/usr/bin/env python
"""Exporta el fichero de tópicos humanizados a PostgreSQL."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd


def export_to_postgres(
    csv_path: str | Path,
    table_name: str,
    db_name: str,
) -> None:
    """
    Exporta el CSV dado a una tabla en PostgreSQL.
    Usa variables de entorno para la conexión.
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print('[WARN] sqlalchemy no instalada. Omitiendo export a Postgres.')
        return

    uri = os.getenv('POSTGRES_URI')
    if not uri:
        host = os.getenv('POSTGRES_HOST')
        user = os.getenv('POSTGRES_USER')
        pwd = os.getenv('POSTGRES_PASSWORD')
        port = os.getenv('POSTGRES_PORT', '5432')
        if host and user and pwd and db_name:
            uri = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

    if not uri:
        print('[INFO] Conexión a Postgres no configurada; omitiendo export.')
        return

    p = Path(csv_path)
    if not p.exists():
        print(f'[INFO] No existe el archivo de entrada {p}; omitiendo export.')
        return

    df = pd.read_csv(p)
    if df.empty:
        print('[INFO] El archivo CSV de entrada está vacío; omitiendo export.')
        return

    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    if_exists_mode = 'append'
    recreate = os.getenv('POSTGRES_RECREATE', '0').strip().lower() in ('1', 'true', 'yes')
    if recreate:
        if_exists_mode = 'replace'

    try:
        engine = create_engine(uri)
        with engine.connect() as connection:
            df.to_sql(table_name, connection, schema=schema, if_exists=if_exists_mode, index=False)
            print(f'[OK] Exportado {len(df)} filas a Postgres -> "{db_name}".{schema}.{table_name}')
    except Exception as e:
        print(f"[ERROR] Falló la exportación a Postgres: {e}")
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Exporta tópicos humanizados a PostgreSQL.")
    ap.add_argument(
        "--in",
        dest="csv_path",
        default="outputs/events/humanized_topics.csv",
        help="Ruta al CSV con los tópicos humanizados.",
    )
    ap.add_argument("--table", default="topics_humanized", help="Nombre de la tabla de destino en PostgreSQL.")
    ap.add_argument("--db", default="steam_data_db", help="Nombre de la base de datos de destino.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    export_to_postgres(csv_path=args.csv_path, table_name=args.table, db_name=args.db)


if __name__ == '__main__':
    main()
