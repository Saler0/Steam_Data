import os
import sys
import pandas as pd


def export_to_postgres():
    """
    Exporta el CSV `outputs/events/topics_by_experience.csv` a una tabla en PostgreSQL.
    Usa POSTGRES_URI o POSTGRES_HOST/USER/PASSWORD/DB (+ POSTGRES_PORT, POSTGRES_SCHEMA).
    Controla recreación de tabla con POSTGRES_RECREATE=1 (replace) o append por defecto.
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
        print('[INFO] Postgres no configurado; omitiendo export de topics_by_experience')
        sys.exit(0)

    path_csv = 'outputs/events/topics_by_experience.csv'
    if not os.path.exists(path_csv):
        print(f'[INFO] No existe CSV de topics_by_experience ({path_csv}); omitiendo export')
        sys.exit(0)

    df = pd.read_csv(path_csv)
    if df.empty:
        print('[INFO] CSV vacío; omitiendo export')
        sys.exit(0)

    # Si existe un mapping de labels humanizados, hacer merge para sustituir topic_name
    # Espera outputs/events/reviews_topics_labels.csv con columnas: topic_id,label
    labels_csv = 'outputs/events/reviews_topics_labels.csv'
    try:
        if os.path.exists(labels_csv):
            mapping = pd.read_csv(labels_csv)
            if {'topic_id', 'label'}.issubset(mapping.columns):
                # Asegurar tipos string para el join
                df['topic_id'] = df['topic_id'].astype(str)
                mapping['topic_id'] = mapping['topic_id'].astype(str)
                df = df.merge(mapping[['topic_id', 'label']], on='topic_id', how='left')
                # Reemplazar topic_name con label si está disponible
                df['topic_name'] = df['label'].fillna(df['topic_name'])
                df = df.drop(columns=['label'])
                print(f"[INFO] Aplicado mapping de labels humanizados: {labels_csv}")
            else:
                print('[WARN] Mapping de labels no tiene columnas requeridas {topic_id,label}; se omite.')
        else:
            print('[INFO] No existe mapping de labels humanizados; se exporta tal cual.')
    except Exception as e:
        print(f"[WARN] No se pudo aplicar mapping de labels: {e}")

    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    table_name = 'topics_by_experience'
    if_exists_mode = 'append'
    recreate = os.getenv('POSTGRES_RECREATE', '0').strip() in ('1', 'true', 'True', 'yes')
    if recreate:
        if_exists_mode = 'replace'

    try:
        engine = create_engine(uri)
        with engine.connect() as connection:
            df.to_sql(table_name, connection, schema=schema, if_exists=if_exists_mode, index=False)
            print(f'[OK] Exportado {len(df)} filas a Postgres -> {schema}.{table_name}')
    except Exception as e:
        print(f"[ERROR] Falló la exportación a Postgres: {e}")
        sys.exit(1)


if __name__ == '__main__':
    export_to_postgres()

