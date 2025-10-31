#!/usr/bin/env python
import os
import sys
from pathlib import Path
import pandas as pd


def _read_any(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    suf = p.suffix.lower()
    if suf == '.csv':
        return pd.read_csv(p)
    if suf == '.json':
        return pd.read_json(p)
    return pd.read_parquet(p)


def _resolve_uri() -> str | None:
    uri = os.getenv('POSTGRES_URI')
    if uri:
        return uri
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USER')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    port = os.getenv('POSTGRES_PORT', '5432')
    if host and user and pwd and db:
        return f'postgresql://{user}:{pwd}@{host}:{port}/{db}'
    return None


def export_topics_summary(path_in: str = 'outputs/events/enriched_events_with_topics_summary.parquet',
                          table: str = 'events_topics_summary') -> None:
    try:
        from sqlalchemy import create_engine  # type: ignore
    except Exception:
        print('[ERROR] La dependencia "sqlalchemy" no está instalada. Omitiendo export a Postgres.')
        sys.exit(1)

    uri = _resolve_uri()
    if not uri:
        print('[ERROR] La conexión a Postgres no está configurada. Asegúrate de que tu fichero .env contiene las variables POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD y POSTGRES_DB.')
        sys.exit(1)

    p = Path(path_in)
    if not p.exists():
        print(f'[INFO] No existe {p}; no hay topics_summary que exportar (salida limpia).')
        return

    df = _read_any(p)
    if df.empty or 'topics_summary' not in df.columns:
        print('[INFO] Entrada vacía o sin "topics_summary"; no se exporta (salida limpia).')
        return

    # Columnas mínimas para exportar
    cols = ['appid', 'year_month', 'topics_summary']
    missing = [c for c in ['appid', 'year_month'] if c not in df.columns]
    if missing:
        print(f"[ERROR] Faltan columnas requeridas en el fichero de entrada: {missing}. Omitiendo export.")
        sys.exit(1)
    out = df[cols].copy()
    out['appid'] = out['appid'].astype(str)

    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    if_exists = 'append'
    recreate = os.getenv('POSTGRES_RECREATE', '0').strip().lower() in ('1', 'true', 'yes')
    if recreate:
        if_exists = 'replace'

    try:
        engine = create_engine(uri)
        with engine.begin() as conn:
            out.to_sql(table, conn, schema=schema, if_exists=if_exists, index=False)
        print(f"[OK] Exportado topics_summary -> {schema}.{table} ({len(out)})")
    except Exception as e:
        print(f"[ERROR] Falló la exportación de topics_summary: {e}")
        sys.exit(1)


if __name__ == '__main__':
    export_topics_summary()

