
import os
import pandas as pd
import sys

def export_to_postgres():
    """
    Lee un CSV de ratios de abandono y lo exporta a una tabla en PostgreSQL.
    La configuración de la base de datos se lee de variables de entorno.
    """
    try:
        from sqlalchemy import create_engine
        SQLA = True
    except ImportError:
        SQLA = False
        print('[WARN] sqlalchemy no instalada. Omitiendo export a Postgres.')
        sys.exit(0)

    uri = os.getenv('POSTGRES_URI')
    if not uri:
        host = os.getenv('POSTGRES_HOST')
        user = os.getenv('POSTGRES_USER')
        pwd = os.getenv('POSTGRES_PASSWORD')
        db = os.getenv('POSTGRES_DB')
        port = os.getenv('POSTGRES_PORT', '5432')
        if host and user and pwd and db:
            uri = f'postgresql://{user}:{pwd}@{host}:{port}/{db}'

    if not uri or not SQLA:
        print('[INFO] Postgres no configurado; omitiendo export de abandon rates')
        sys.exit(0)

    path_csv = 'outputs/events/abandon_rates_by_experience.csv'
    if not os.path.exists(path_csv):
        print(f'[INFO] No existe CSV de abandon rates ({path_csv}); omitiendo export')
        sys.exit(0)

    df = pd.read_csv(path_csv)
    if df.empty:
        print('[INFO] CSV vacío; omitiendo export')
        sys.exit(0)

    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    table_name = 'abandon_rates_by_experience'
    if_exists_mode = 'append'
    recreate = os.getenv('POSTGRES_RECREATE', '0').strip() in ('1', 'true', 'True', 'yes')
    if recreate:
        if_exists_mode = 'replace'
    
    try:
        engine = create_engine(uri)
        with engine.connect() as connection:
            # Permitir reemplazar la tabla si se solicita (para cambios de esquema como nueva columna appid)
            df.to_sql(table_name, connection, schema=schema, if_exists=if_exists_mode, index=False)
            print(f'[OK] Exportado {len(df)} filas a Postgres -> {schema}.{table_name}')
    except Exception as e:
        print(f"[ERROR] Falló la exportación a Postgres: {e}")
        sys.exit(1)

if __name__ == "__main__":
    export_to_postgres()
